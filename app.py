from flask import Flask, send_from_directory, request
from flask_socketio import SocketIO, emit
import eventlet
import random

# 1. Initialize Eventlet monkey patching first
eventlet.monkey_patch()

app = Flask(__name__, static_folder='public', static_url_path='')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# 2. Global state & flags
simulation_active = False
message_buffer = []
connected_clients = {}  # socket_id: node_name

# Analytics tracking counters
stats = {
    "activeNodes": 0,
    "bufferedCount": 0,
    "totalDispatched": 0,
    "totalDelivered": 0
}

def broadcast_stats():
    stats["activeNodes"] = len(connected_clients)
    stats["bufferedCount"] = len(message_buffer)
    socketio.emit('update-stats', stats)

@app.route('/')
def index():
    return send_from_directory('public', 'index.html')

@socketio.on('connect')
def handle_connect():
    print(f"[Node Connected] ID: {request.sid}")
    broadcast_stats()

@socketio.on('set-simulation')
def handle_set_simulation(data):
    global simulation_active
    simulation_active = data.get('active', False)
    print(f"[Simulation Mode Updated] Active: {simulation_active}")

@socketio.on('register-node')
def handle_register(node_name):
    global message_buffer
    socket_id = request.sid
    connected_clients[socket_id] = node_name
    
    client_list = [{"name": name, "id": sid} for sid, name in connected_clients.items()]
    socketio.emit('update-nodes', client_list)
    
    # Store-Carry-Forward with Priority Sorting (Critical > Urgent > Routine)
    pending_messages = [m for m in message_buffer if m['to'] == node_name]
    
    priority_weight = {'Critical': 3, 'Urgent': 2, 'Routine': 1}
    pending_messages.sort(key=lambda x: priority_weight.get(x.get('priority', 'Routine'), 1), reverse=True)
    
    for msg in pending_messages:
        socketio.emit('receive-bundle', msg, room=socket_id)
        stats["totalDelivered"] += 1
        print(f"[DTN Forwarded] Delivering [{msg.get('priority')}] bundle to {node_name}")
        
        # ---> ADD THIS: Notify original sender of successful delivery <---
        sender_name = msg.get('from')
        for sid, name in connected_clients.items():
            if name == sender_name:
                socketio.emit('bundle-ack', {'node': node_name}, room=sid)
                break
        
    # Remove delivered messages from buffer
    message_buffer = [m for m in message_buffer if m['to'] != node_name]
    broadcast_stats()

@socketio.on('send-bundle')
def handle_send_bundle(bundle):
    global message_buffer
    stats["totalDispatched"] += 1
    print(f"[Bundle Created] From {bundle.get('from')} to {bundle.get('to')} [{bundle.get('priority')}]: {bundle.get('payload')}")
    
    target_sid = None
    for sid, name in connected_clients.items():
        if name == bundle.get('to'):
            target_sid = sid
            break
            
    if target_sid:
        socketio.emit('receive-bundle', bundle, room=target_sid)
        stats["totalDelivered"] += 1
        print(f"[Delivered] Sent directly to online node {bundle.get('to')}")
        
        # ---> ADD THIS: Notify sender for direct delivery too <---
        sender_name = bundle.get('from')
        for sid, name in connected_clients.items():
            if name == sender_name:
                socketio.emit('bundle-ack', {'node': bundle.get('to')}, room=sid)
                break
    else:
        message_buffer.append(bundle)
        print(f"[Store & Carry] Destination {bundle.get('to')} offline. Buffered.")
        
    broadcast_stats()
@socketio.on('run-route-trace')
def handle_route_trace(data):
    target = data.get('target', 'Node-Medic')
    
    possible_paths = [
        f"Node-HQ -> Drone-Relay-02 -> {target} (Cost: 2 Hops, Metric: 0.842)",
        f"Node-HQ -> Mobile-Clinic-3 -> {target} (Cost: 3 Hops, Metric: 0.610)",
        f"Node-HQ -> Store-Forward-Flash -> {target} (Buffered due to link outage)"
    ]
    selected_path = random.choice(possible_paths)
    print(f"[Route Trace Executed] {selected_path}")
    
    socketio.emit('receive-bundle', {
        'from': 'Python-Routing-Daemon',
        'priority': 'Critical',
        'ttl': '24h',
        'payload': f'ROUTE TRACE RESULT: {selected_path}'
    })

# ================= BACKGROUND SIMULATION THREAD =================
def dtn_background_simulator():
    while True:
        socketio.sleep(8)  # Eventlet-safe asynchronous sleep
        
        # GUARD: Skip execution entirely unless Live Simulation Mode is turned ON
        if not simulation_active:
            continue
            
        stats["totalDispatched"] += random.randint(1, 3)
        
        socketio.emit('receive-bundle', {
            'from': 'Drone-Relay-02',
            'priority': 'Routine',
            'ttl': '12h',
            'payload': 'AUTO-SYNC: Periodic PRoPHETv2 beacon handshake completed.'
        })
        broadcast_stats()

@socketio.on('disconnect')
def handle_disconnect():
    socket_id = request.sid
    if socket_id in connected_clients:
        del connected_clients[socket_id]
        
    client_list = [{"name": name, "id": sid} for sid, name in connected_clients.items()]
    socketio.emit('update-nodes', client_list)
    print(f"[Node Disconnected] ID: {socket_id}")
    broadcast_stats()

if __name__ == '__main__':
    print("ResQNet Python Wi-Fi DTN Server running on port 3000")
    socketio.start_background_task(dtn_background_simulator)
    socketio.run(app, host='0.0.0.0', port=3000, debug=True)