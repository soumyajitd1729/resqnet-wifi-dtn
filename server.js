const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

app.use(express.static('public'));

let messageBuffer = [];
let connectedClients = new Map();

// Analytics tracking counters
let stats = {
  activeNodes: 0,
  bufferedCount: 0,
  totalDispatched: 0,
  totalDelivered: 0
};

function broadcastStats() {
  stats.activeNodes = connectedClients.size;
  stats.bufferedCount = messageBuffer.length;
  io.emit('update-stats', stats);
}

io.on('connection', (socket) => {
  console.log(`[Node Connected] ID: ${socket.id}`);
  broadcastStats();
  
  socket.on('register-node', (nodeName) => {
    connectedClients.set(socket.id, { name: nodeName, id: socket.id });
    io.emit('update-nodes', Array.from(connectedClients.values()));
    
    // Store-Carry-Forward with Priority Sorting: 
    // Flush pending messages for this node, sorted by priority (Critical first)
    const pendingMessages = messageBuffer.filter(m => m.to === nodeName);
    
    // Sort queue: Critical > Urgent > Routine
    const priorityWeight = { 'Critical': 3, 'Urgent': 2, 'Routine': 1 };
    pendingMessages.sort((a, b) => (priorityWeight[b.priority] || 1) - (priorityWeight[a.priority] || 1));

    pendingMessages.forEach(msg => {
      socket.emit('receive-bundle', msg);
      stats.totalDelivered++;
      console.log(`[DTN Forwarded] Delivering [${msg.priority}] bundle to ${nodeName}`);
    });

    // Remove delivered messages from buffer
    messageBuffer = messageBuffer.filter(m => m.to !== nodeName);
    broadcastStats();
  });

  socket.on('send-bundle', (bundle) => {
    stats.totalDispatched++;
    console.log(`[Bundle Created] From ${bundle.from} to ${bundle.to} [${bundle.priority}]: "${bundle.payload}"`);
    
    const targetClient = Array.from(connectedClients.values()).find(c => c.name === bundle.to);
    
    if (targetClient) {
      io.to(targetClient.id).emit('receive-bundle', bundle);
      stats.totalDelivered++;
      console.log(`[Delivered] Sent directly to online node ${bundle.to}`);
    } else {
      messageBuffer.push(bundle);
      console.log(`[Store & Carry] Destination ${bundle.to} offline. Buffered.`);
    }
    broadcastStats();
  });

  socket.on('disconnect', () => {
    connectedClients.delete(socket.id);
    io.emit('update-nodes', Array.from(connectedClients.values()));
    console.log(`[Node Disconnected] ID: ${socket.id}`);
    broadcastStats();
  });
});

const PORT = 3000;
http.listen(PORT, '0.0.0.0', () => {
  console.log(`ResQNet Local Wi-Fi DTN Server running on port ${PORT}`);
});