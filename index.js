import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';

const app = express();
app.use(cors());

// Health check for Render
app.get('/', (req, res) => {
  res.json({ status: 'ok', rooms: rooms.size });
});

const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
  transports: ['websocket', 'polling'],
});

const rooms = new Map();

// Store display names per socket
const socketNames = new Map();

function destroyRoom(token) {
  const room = rooms.get(token);
  if (room) {
    if (room.timer) clearTimeout(room.timer);
    io.to(token).emit('room:expired');
    const sockets = io.sockets.adapter.rooms.get(token);
    if (sockets) {
      for (const socketId of sockets) {
        const socket = io.sockets.sockets.get(socketId);
        if (socket) socket.leave(token);
      }
    }
    rooms.delete(token);
  }
}

function resetRoomTimer(token) {
  const room = rooms.get(token);
  if (!room) return;

  if (room.timer) clearTimeout(room.timer);

  room.timer = setTimeout(() => {
    destroyRoom(token);
  }, 30 * 60 * 1000);
}

io.on('connection', (socket) => {
  let currentToken = null;

  socket.on('room:create', (data) => {
    const { token, key, name } = data;

    if (rooms.has(token)) {
      socket.emit('error', { message: 'Room already exists' });
      return;
    }

    socketNames.set(socket.id, name || 'Anonymous');

    const room = {
      token,
      key,
      users: new Map([[socket.id, name || 'Anonymous']]),
      createdAt: Date.now(),
      lastActivity: Date.now(),
      timer: null,
    };

    rooms.set(token, room);
    socket.join(token);
    currentToken = token;

    socket.emit('room:created', { token });
    resetRoomTimer(token);
  });

  socket.on('room:join', (data) => {
    const { token, name } = data;
    const room = rooms.get(token);

    if (!room) {
      socket.emit('error', { message: 'Room not found' });
      return;
    }

    if (room.users.size >= 2) {
      socket.emit('error', { message: 'Room is full' });
      return;
    }

    const displayName = name || 'Anonymous';
    socketNames.set(socket.id, displayName);
    room.users.set(socket.id, displayName);

    socket.join(token);
    currentToken = token;
    room.lastActivity = Date.now();

    // Get the other user's name
    let peerName = 'Anonymous';
    for (const [userId, userName] of room.users) {
      if (userId !== socket.id) {
        peerName = userName;
        break;
      }
    }

    socket.emit('room:joined', { token, key: room.key, peerName });

    // Notify existing user about the new joiner's name
    socket.to(token).emit('room:peer-joined', { name: displayName });

    // Tell the joiner about the existing user's name
    socket.to(token).emit('room:peer-name', { name: displayName });

    resetRoomTimer(token);
  });

  socket.on('room:peer-request-name', (data) => {
    const room = rooms.get(data.token);
    if (!room) return;

    const myName = socketNames.get(socket.id) || 'Anonymous';
    socket.to(data.token).emit('room:peer-name', { name: myName });
  });

  socket.on('message:send', (data) => {
    const room = rooms.get(data.token);
    if (!room) return;

    room.lastActivity = Date.now();
    resetRoomTimer(data.token);

    const senderName = socketNames.get(socket.id) || 'Anonymous';

    socket.to(data.token).emit('message:receive', {
      id: data.id,
      text: data.text,
      selfDestruct: data.selfDestruct,
      timestamp: data.timestamp,
      senderName,
    });
  });

  socket.on('typing:start', (data) => {
    socket.to(data.token).emit('typing:start');
  });

  socket.on('typing:stop', (data) => {
    socket.to(data.token).emit('typing:stop');
  });

  socket.on('room:leave', (data) => {
    const room = rooms.get(data.token);
    if (!room) return;

    const leavingName = socketNames.get(socket.id) || 'Anonymous';
    room.users.delete(socket.id);
    socketNames.delete(socket.id);
    socket.leave(data.token);

    socket.to(data.token).emit('room:peer-left', { name: leavingName });

    if (room.users.size === 0) {
      destroyRoom(data.token);
    }
  });

  socket.on('disconnect', () => {
    if (currentToken) {
      const room = rooms.get(currentToken);
      if (room) {
        const leavingName = socketNames.get(socket.id) || 'Anonymous';
        room.users.delete(socket.id);
        socketNames.delete(socket.id);
        socket.to(currentToken).emit('room:peer-left', { name: leavingName });

        if (room.users.size === 0) {
          destroyRoom(currentToken);
        }
      }
    }
  });
});

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
  console.log(`GhostLink server running on port ${PORT}`);
});
