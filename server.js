'use strict';
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const KEY = process.env.KEY || 'fu7777-k3x9q';
const MAX_CLIENTS = 8;
const TICK_MS = 5000;       // heartbeat every 5 s
const MAX_MISSED = 4;       // ~20 s of silence = dead socket
const MAX_BACKLOG = 128 * 1024; // drop audio for a client that is not draining (keeps latency low)

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('ok');
});
server.keepAliveTimeout = 120000;
server.headersTimeout = 125000;

// perMessageDeflate off: avoids proxy windowing problems and saves CPU on tiny audio frames
const wss = new WebSocketServer({ server, perMessageDeflate: false, maxPayload: 16 * 1024 });
const clients = new Map(); // device id -> socket

function notify() {
  const msg = clients.size > 1 ? 'peer:1' : 'peer:0';
  for (const c of clients.values()) {
    if (c.readyState === 1) {
      try { c.send(msg); } catch (e) {}
    }
  }
}

function drop(id, ws) {
  if (clients.get(id) === ws) {
    clients.delete(id);
    notify();
  }
}

wss.on('connection', (ws, req) => {
  let u;
  try { u = new URL(req.url, 'http://x'); } catch (e) { ws.close(1008, 'bad url'); return; }
  if (u.searchParams.get('k') !== KEY) {
    ws.close(1008, 'bad key');
    return;
  }
  const id = (u.searchParams.get('id') || Math.random().toString(36).slice(2)).slice(0, 64);

  const old = clients.get(id);
  if (!old && clients.size >= MAX_CLIENTS) {
    // make room by evicting a socket that already stopped answering
    let freed = false;
    for (const [oid, oc] of clients) {
      if (oc.missed > 0) {
        clients.delete(oid);
        try { oc.terminate(); } catch (e) {}
        freed = true;
        break;
      }
    }
    if (!freed) { ws.close(1013, 'full'); return; }
  }
  if (old) {
    clients.delete(id);
    try { old.terminate(); } catch (e) {} // same device reconnecting: the new socket wins at once
  }

  ws.listen = true;
  ws.missed = 0;
  ws._id = id;
  clients.set(id, ws);
  notify();

  ws.on('pong', () => { ws.missed = 0; });

  ws.on('message', (data, isBinary) => {
    ws.missed = 0;
    if (isBinary) {
      if (data.length > 4000) return;
      for (const c of clients.values()) {
        if (c !== ws && c.listen && c.readyState === 1 && c.bufferedAmount < MAX_BACKLOG) {
          try { c.send(data, { binary: true }); } catch (e) {}
        }
      }
    } else {
      const t = data.toString();
      if (t === 'L1') ws.listen = true;
      else if (t === 'L0') ws.listen = false;
      // anything else (e.g. keepalive) just counts as activity
    }
  });

  ws.on('close', () => drop(id, ws));
  ws.on('error', () => {});
});

// heartbeat: native ping + tiny text frame so the client can also detect a silent edge drop
setInterval(() => {
  for (const [id, c] of clients) {
    if (c.readyState !== 1) { drop(id, c); continue; }
    if (++c.missed > MAX_MISSED) {
      try { c.terminate(); } catch (e) {}
      drop(id, c);
      continue;
    }
    try { c.ping(); c.send('h'); } catch (e) {}
  }
}, TICK_MS);

// Railway redeploys send SIGTERM: close nicely so apps reconnect immediately
function shutdown() {
  for (const c of clients.values()) {
    try { c.close(1012, 'restart'); } catch (e) {}
  }
  setTimeout(() => process.exit(0), 800);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
process.on('uncaughtException', (e) => console.error('uncaught', e));
process.on('unhandledRejection', (e) => console.error('unhandled', e));

server.listen(PORT, '0.0.0.0', () => console.log('relay on ' + PORT));
