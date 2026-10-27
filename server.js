const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

// Serve static files from a 'public' folder
app.use(express.static('public'));

// DTN Message Buffer (Store-Carry-Forward memory)
let messageBuffer = [];
let connectedClients = new Map();

io.on('connection', (socket) => {
  console.log(`[Node Connected] ID: ${socket.id}`);
  
  // Register client username/node name
  socket.on('register-node', (nodeName) => {
    connectedClients.set(socket.id, { name: nodeName, id: socket.id });
    io.emit('update-nodes', Array.from(connectedClients.values()));
    
    // Deliver any stored messages meant for this node (Store-Carry-Forward delivery)
    const pendingMessages = messageBuffer.filter(m => m.to === nodeName);
    pendingMessages.forEach(msg => {
      socket.emit('receive-bundle', msg);
      console.log(`[DTN Forwarded] Delivering stored bundle to ${nodeName}`);
    });
    // Remove delivered messages from buffer
    messageBuffer = messageBuffer.filter(m => m.to !== nodeName);
  });

  // Handle bundle dispatch
  socket.on('send-bundle', (bundle) => {
    console.log(`[Bundle Created] From ${bundle.from} to ${bundle.to}: "${bundle.payload}"`);
    
    // Check if destination client is currently online
    const targetClient = Array.from(connectedClients.values()).find(c => c.name === bundle.to);
    
    if (targetClient) {
      // Direct live delivery
      io.to(targetClient.id).emit('receive-bundle', bundle);
      console.log(`[Delivered] Sent directly to online node ${bundle.to}`);
    } else {
      // Store-Carry-Forward: Buffer the message on the server until destination reconnects
      messageBuffer.push(bundle);
      console.log(`[Store & Carry] Destination ${bundle.to} offline. Stored in buffer. Total buffered: ${messageBuffer.length}`);
      socket.emit('buffer-status', { bufferedCount: messageBuffer.length });
    }
  });

  socket.on('disconnect', () => {
    connectedClients.delete(socket.id);
    io.io?.emit('update-nodes', Array.from(connectedClients.values()));
    console.log(`[Node Disconnected] ID: ${socket.id}`);
  });
});

const PORT = 3000;
http.listen(PORT, () => {
  console.log(`ResQNet Local Wi-Fi DTN Server running on port ${PORT}`);
});