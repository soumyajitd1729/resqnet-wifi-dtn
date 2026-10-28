from flask import Flask, send_from_directory, request
from flask_socketio import SocketIO, emit

app = Flask(__name__, static_folder='public')
socketio = SocketIO(app, cors_allowed_origins="*")

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
    socketio.run(app, host='0.0.0.0', port=3000, debug=True)