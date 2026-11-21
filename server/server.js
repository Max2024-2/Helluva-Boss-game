const http = require('http');
const { randomUUID } = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const partyRooms = new Map();

function sendJson(ws, message) {
  if (ws.readyState === 1) ws.send(JSON.stringify(message));
}

function broadcastRoom(room, message, except) {
  const payload = JSON.stringify(message);
  for (const client of room.clients.values()) {
    if (client.ws !== except && client.ws.readyState === 1) client.ws.send(payload);
  }
}

function leaveParty(ws) {
  const room = ws.partyRoom;
  if (!room || !ws.playerId) return;
  const playerId = ws.playerId;
  room.clients.delete(playerId);
  ws.partyRoom = null;
  ws.playerId = null;
  broadcastRoom(room, { type: 'leave', id: playerId });
  if (room.clients.size === 0) {
    partyRooms.delete(room.code);
  } else if (room.hostId === playerId) {
    room.hostId = room.clients.keys().next().value;
    broadcastRoom(room, { type: 'host', hostId: room.hostId });
  }
}

function handlePartyMessage(ws, data) {
  if (data.length > 8192) {
    ws.close(1009, 'Message too large');
    return;
  }

  let message;
  try {
    message = JSON.parse(data.toString());
  } catch {
    sendJson(ws, { type: 'error', message: 'Messaggio online non valido.' });
    return;
  }

  if (!message || typeof message.type !== 'string') return;

  if (message.type === 'join') {
    if (ws.partyRoom) {
      sendJson(ws, { type: 'error', message: 'Sei già in una stanza.' });
      return;
    }
    const code = typeof message.room === 'string' ? message.room.toUpperCase() : '';
    if (!/^[A-Z0-9]{4,8}$/.test(code)) {
      sendJson(ws, { type: 'error', message: 'Il codice stanza deve avere da 4 a 8 lettere o numeri.' });
      return;
    }

    let room = partyRooms.get(code);
    if (room && room.clients.size >= 4) {
      sendJson(ws, { type: 'error', message: 'Stanza piena (massimo 4 agenti).' });
      return;
    }
    if (!room) {
      room = { code, hostId: null, clients: new Map() };
      partyRooms.set(code, room);
    }

    const id = randomUUID();
    const player = {
      id,
      name: typeof message.name === 'string' ? message.name.slice(0, 16) : 'Agente',
      character: Number.isInteger(message.character) ? Math.max(0, Math.min(2, message.character)) : 0,
      x: 0,
      z: 0,
      yaw: 0,
      kills: 0
    };
    const client = { id, player, ws };
    room.clients.set(id, client);
    if (!room.hostId) room.hostId = id;
    ws.playerId = id;
    ws.partyRoom = room;
    sendJson(ws, {
      type: 'welcome',
      id,
      hostId: room.hostId,
      players: Array.from(room.clients.values())
        .filter(existing => existing.id !== id)
        .map(existing => existing.player)
    });
    broadcastRoom(room, { type: 'peer', player }, ws);
    return;
  }

  const room = ws.partyRoom;
  const client = room && room.clients.get(ws.playerId);
  if (!room || !client) return;

  if (message.type === 'state') {
    const player = message.player;
    if (!player || ![player.x, player.z, player.yaw].every(Number.isFinite) ||
        Math.abs(player.x) > 20 || Math.abs(player.z) > 20) return;
    client.player = {
      ...client.player,
      x: player.x,
      z: player.z,
      yaw: player.yaw,
      kills: Number.isInteger(player.kills) ? Math.max(0, player.kills) : 0
    };
    broadcastRoom(room, { type: 'state', player: client.player }, ws);
  } else if (message.type === 'bots' && room.hostId === client.id) {
    if (!Array.isArray(message.bots) || message.bots.length > 20) return;
    const bots = message.bots.filter(bot =>
      bot && typeof bot.id === 'string' && bot.id.length <= 40 &&
      Number.isFinite(bot.x) && Math.abs(bot.x) <= 22 &&
      Number.isFinite(bot.z) && Math.abs(bot.z) <= 22 &&
      Number.isFinite(bot.hp) && bot.hp >= 0 && bot.hp <= 500
    ).map(bot => ({ id: bot.id, x: bot.x, z: bot.z, hp: bot.hp }));
    broadcastRoom(room, { type: 'bots', bots }, ws);
  } else if (message.type === 'damage') {
    if (typeof message.botId !== 'string' || message.botId.length > 40 ||
        !Number.isFinite(message.amount) || message.amount < 1 || message.amount > 50) return;
    const host = room.clients.get(room.hostId);
    if (host) sendJson(host.ws, { type: 'damage', id: client.id, botId: message.botId, amount: message.amount });
  } else if (message.type === 'shot') {
    const shot = message;
    if (![shot.x, shot.y, shot.z, shot.vx, shot.vz].every(Number.isFinite) ||
        Math.abs(shot.x) > 25 || Math.abs(shot.z) > 25 ||
        Math.abs(shot.vx) > 25 || Math.abs(shot.vz) > 25) return;
    broadcastRoom(room, {
      type: 'shot',
      id: client.id,
      x: shot.x,
      y: shot.y,
      z: shot.z,
      vx: shot.vx,
      vz: shot.vz,
      kind: shot.kind === 'bot' ? 'bot' : 'player'
    }, ws);
  }
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Hell Agency multiplayer server is running.');
  } else {
    res.writeHead(404);
    res.end();
  }
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 8192 });

server.on('upgrade', (req, socket, head) => {
  const pathname = (req.url || '').split('?')[0];
  if (pathname !== '/party') {
    socket.destroy();
    return;
  }
  wss.handleUpgrade(req, socket, head, ws => {
    ws.on('message', data => handlePartyMessage(ws, data));
    ws.on('close', () => leaveParty(ws));
    ws.on('error', () => leaveParty(ws));
  });
});

server.listen(PORT, () => console.log('In ascolto sulla porta ' + PORT));
