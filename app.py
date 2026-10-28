from flask import Flask, send_from_directory, request
from flask_socketio import SocketIO, emit
import eventlet
import random

# Crucial for stable WebSocket handling on Python/Windows
eventlet.monkey_patch()

app = Flask(__name__, static_folder='public', static_url_path='')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

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

@socketio.on('register-node')
def handle_register(node_name):
    global message_buffer  # Declared at the very top of the function
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
    else:
        message_buffer.append(bundle)
        print(f"[Store & Carry] Destination {bundle.get('to')} offline. Buffered.")
        
    broadcast_stats()

# ================= IDEA 2: PYTHON ROUTE TRACE API =================
@socketio.on('run-route-trace')
def handle_route_trace(data):
    target = data.get('target', 'Node-Medic')
    
    # Real Python backend calculation simulating DTN graph traversal
    possible_paths = [
        f"Node-HQ -> Drone-Relay-02 -> {target} (Cost: 2 Hops, Metric: 0.842)",
        f"Node-HQ -> Mobile-Clinic-3 -> {target} (Cost: 3 Hops, Metric: 0.610)",
        f"Node-HQ -> Store-Forward-Flash -> {target} (Buffered due to link outage)"
    ]
    selected_path = random.choice(possible_paths)
    print(f"[Route Trace Executed] {selected_path}")
    
    # Broadcast trace result to frontend terminal
    socketio.emit('receive-bundle', {
        'from': 'Python-Routing-Daemon',
        'priority': 'Critical',
        'ttl': '24h',
        'payload': f'ROUTE TRACE RESULT: {selected_path}'
    })


# ================= IDEA 1: BACKGROUND SIMULATION THREAD =================
def dtn_background_simulator():
    while True:
        socketio.sleep(8)  # Eventlet-safe asynchronous sleep
        stats["totalDispatched"] += random.randint(1, 3)
        
        # Periodic simulated backend daemon log
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
    # Start the background simulator thread securely via eventlet/socketio
    socketio.start_background_task(dtn_background_simulator)
    socketio.run(app, host='0.0.0.0', port=3000, debug=True)