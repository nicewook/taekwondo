// 태권도 겨루기 — static files + 2-pad room relay (WebSocket)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PUB = path.join(__dirname, 'public');
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.png': 'image/png', '.webmanifest': 'application/manifest+json',
};

const srv = http.createServer((req, res) => {
  let u;
  try { u = decodeURIComponent(req.url.split('?')[0]); } catch { res.writeHead(400); return res.end(); }
  if (u === '/health') { res.writeHead(200); return res.end('ok'); }
  if (u === '/') u = '/index.html';
  const f = path.join(PUB, path.normalize(u));
  if (!f.startsWith(PUB + path.sep)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (e, d) => {
    if (e) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(d);
  });
});

const wss = new WebSocketServer({ server: srv, maxPayload: 64 * 1024 });
const rooms = new Map(); // code -> { host, guest }

function send(ws, o) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); }
function newCode() { let c; do { c = String(1000 + Math.floor(Math.random() * 9000)); } while (rooms.has(c)); return c; }
function leave(ws) {
  const c = ws.room; if (!c) return;
  ws.room = null;
  const r = rooms.get(c); if (!r) return;
  const peer = ws.role === 'host' ? r.guest : r.host;
  rooms.delete(c);
  if (peer) { peer.room = null; send(peer, { t: 'left' }); }
}

wss.on('connection', ws => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'create') {
      leave(ws); const c = newCode();
      rooms.set(c, { host: ws, guest: null }); ws.room = c; ws.role = 'host';
      send(ws, { t: 'created', code: c });
    } else if (m.t === 'join') {
      const c = String(m.code || ''); const r = rooms.get(c);
      if (!r) return send(ws, { t: 'error', msg: '그런 방 번호가 없어요' });
      if (r.guest) return send(ws, { t: 'error', msg: '이미 두 명이 들어간 방이에요' });
      leave(ws); r.guest = ws; ws.room = c; ws.role = 'guest';
      send(r.host, { t: 'paired' }); send(ws, { t: 'paired' });
    } else if (m.t === 'msg') {
      const r = rooms.get(ws.room); if (!r) return;
      const peer = ws.role === 'host' ? r.guest : r.host;
      if (peer && peer.readyState === 1) peer.send(JSON.stringify({ t: 'msg', d: m.d }));
    }
  });
  ws.on('close', () => leave(ws));
});

setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); });
}, 30000);

const PORT = process.env.PORT || 3000;
srv.listen(PORT, () => console.log('taekwondo server on', PORT));
