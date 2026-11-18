// Kids' game relay on Cloudflare Workers + Durable Objects (Workers Free plan).
//   /tkd   taekwondo: WebSocket room relay. Same messages as the old Railway server
//          (create / join / msg -> created / paired / msg / left / error).
//   /bb/*  baseball: SSE (GET /bb/events) + POST /bb/room, /bb/join, /bb/send.
//          Same API as the old Railway bb-relay server.
// Game rules run on the pads. The server only passes messages between two pads.
import { DurableObject } from 'cloudflare:workers';

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type',
};
const json = (code, obj) =>
  new Response(JSON.stringify(obj), { status: code, headers: { 'Content-Type': 'application/json; charset=utf-8', ...CORS } });
const randInt = (n) => crypto.getRandomValues(new Uint32Array(1))[0] % n;
const hex = (n) => [...crypto.getRandomValues(new Uint8Array(n))].map((b) => b.toString(16).padStart(2, '0')).join('');

export default {
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    if (req.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS });
    if (p === '/' || p === '/health' || p === '/bb' || p === '/bb/' || p === '/bb/health') return json(200, { ok: true });
    if (p === '/tkd') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('websocket only', { status: 426 });
      return env.TKD.get(env.TKD.idFromName('lobby')).fetch(req);
    }
    if (p.startsWith('/bb/')) return env.BB.get(env.BB.idFromName('lobby')).fetch(req);
    return json(404, { error: 'not found' });
  },
};

// ---------- taekwondo ----------
// WebSocket Hibernation API: the object can sleep between messages, and each socket
// keeps its {room, role} in its attachment, so rooms survive the sleep.
export class Tkd extends DurableObject {
  async fetch() {
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({});
    return new Response(null, { status: 101, webSocket: client });
  }
  att(ws) { return ws.deserializeAttachment() || {}; }
  inRoom(code) { return this.ctx.getWebSockets().filter((w) => this.att(w).room === code); }
  send(ws, o) { try { ws.send(JSON.stringify(o)); } catch {} }
  leave(ws) {
    const c = this.att(ws).room;
    if (!c) return;
    ws.serializeAttachment({});
    for (const w of this.inRoom(c)) { w.serializeAttachment({}); this.send(w, { t: 'left' }); }
  }
  newCode() {
    const used = new Set(this.ctx.getWebSockets().map((w) => this.att(w).room).filter(Boolean));
    let c;
    do { c = String(1000 + randInt(9000)); } while (used.has(c));
    return c;
  }
  async webSocketMessage(ws, raw) {
    if (typeof raw !== 'string' || raw.length > 65536) return;
    let m;
    try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'create') {
      this.leave(ws);
      const c = this.newCode();
      ws.serializeAttachment({ room: c, role: 'host' });
      this.send(ws, { t: 'created', code: c });
    } else if (m.t === 'join') {
      const c = String(m.code || '');
      const others = this.inRoom(c).filter((w) => w !== ws);
      const host = others.find((w) => this.att(w).role === 'host');
      if (!host) return this.send(ws, { t: 'error', msg: '그런 방 번호가 없어요' });
      if (others.some((w) => this.att(w).role === 'guest')) return this.send(ws, { t: 'error', msg: '이미 두 명이 들어간 방이에요' });
      this.leave(ws);
      ws.serializeAttachment({ room: c, role: 'guest' });
      this.send(host, { t: 'paired' });
      this.send(ws, { t: 'paired' });
    } else if (m.t === 'msg') {
      const c = this.att(ws).room;
      if (!c) return;
      const out = JSON.stringify({ t: 'msg', d: m.d });
      for (const w of this.inRoom(c)) if (w !== ws) { try { w.send(out); } catch {} }
    }
  }
  async webSocketClose(ws, code, reason) {
    this.leave(ws);
    try { ws.close(code, reason); } catch {}
  }
  async webSocketError(ws) { this.leave(ws); }
}

// ---------- baseball ----------
// Rooms live in memory while a pad is connected and are also saved to storage,
// so a room still works if the object restarts (the old server kept rooms 6 hours).
const MAX_ROOMS = 300;
const IDLE_MS = 6 * 60 * 60 * 1000;
const enc = new TextEncoder();

export class Bb extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.rooms = new Map(); // code -> {seats:{A:{token,w},B}, state, t}
    this.timer = null;
  }
  write(w, s) { w.write(enc.encode(s)).catch(() => {}); }
  push(seat, obj) { if (seat && seat.w) this.write(seat.w, `data: ${JSON.stringify(obj)}\n\n`); }
  seatOf(r, token) {
    if (!r || !token) return null;
    for (const k of ['A', 'B']) if (r.seats[k] && r.seats[k].token === token) return k;
    return null;
  }
  async room(code) {
    code = String(code || '');
    if (!/^\d{4}$/.test(code)) return null;
    if (this.rooms.has(code)) return this.rooms.get(code);
    const s = await this.ctx.storage.get('r:' + code);
    if (!s) return null;
    if (this.rooms.has(code)) return this.rooms.get(code);
    const r = {
      seats: { A: s.A ? { token: s.A, w: null } : null, B: s.B ? { token: s.B, w: null } : null },
      state: s.state ?? null,
      t: s.t,
    };
    this.rooms.set(code, r);
    return r;
  }
  save(code, r) {
    return this.ctx.storage.put('r:' + code, { A: r.seats.A?.token || null, B: r.seats.B?.token || null, state: r.state, t: r.t });
  }
  live(r) { return ['A', 'B'].some((k) => r.seats[k] && r.seats[k].w); }
  async prune() {
    const all = await this.ctx.storage.list({ prefix: 'r:' });
    const now = Date.now();
    const dead = [];
    for (const [k, v] of all) {
      const m = this.rooms.get(k.slice(2));
      if (m && this.live(m)) continue;
      if (now - Math.max(v.t || 0, m?.t || 0) > IDLE_MS) dead.push(k);
    }
    for (const k of dead) this.rooms.delete(k.slice(2));
    for (let i = 0; i < dead.length; i += 128) await this.ctx.storage.delete(dead.slice(i, i + 128));
    return all.size - dead.length;
  }
  tick() {
    let open = 0;
    for (const [code, r] of this.rooms) {
      for (const k of ['A', 'B']) if (r.seats[k] && r.seats[k].w) { open++; this.write(r.seats[k].w, ': ping\n\n'); }
      if (!this.live(r)) this.rooms.delete(code); // still in storage
    }
    if (!open && this.timer) { clearInterval(this.timer); this.timer = null; }
  }

  async fetch(req) {
    const u = new URL(req.url);
    const p = u.pathname.replace(/^\/bb/, '');
    if (req.method === 'POST') {
      let b = {};
      try { const t = await req.text(); if (t.length <= 65536) b = JSON.parse(t || '{}') || {}; } catch {}
      if (p === '/room') {
        if ((await this.prune()) >= MAX_ROOMS) return json(503, { error: '방이 너무 많아요' });
        let code;
        do { code = String(1000 + randInt(9000)); } while (await this.room(code));
        const token = hex(12);
        const r = { seats: { A: { token, w: null }, B: null }, state: null, t: Date.now() };
        this.rooms.set(code, r);
        await this.save(code, r);
        return json(200, { code, token, seat: 'A' });
      }
      if (p === '/join') {
        const code = String(b.code || '');
        const r = await this.room(code);
        if (!r) return json(404, { error: '없는 방 번호예요' });
        const again = this.seatOf(r, b.token);
        if (again) return json(200, { token: b.token, seat: again });
        if (r.seats.B) return json(409, { error: '방이 꽉 찼어요' });
        const token = hex(12);
        r.seats.B = { token, w: null };
        r.t = Date.now();
        await this.save(code, r);
        return json(200, { token, seat: 'B' });
      }
      if (p === '/send') {
        const code = String(b.code || '');
        const r = await this.room(code);
        const me = this.seatOf(r, b.token);
        if (!me) return json(404, { error: 'no seat' });
        const msg = b.msg || {};
        r.t = Date.now();
        this.push(r.seats[me === 'A' ? 'B' : 'A'], msg);
        if (msg.state) { r.state = msg.state; await this.save(code, r); }
        return json(200, { ok: true });
      }
      return json(404, { error: 'not found' });
    }

    if (p === '/events') {
      const r = await this.room(u.searchParams.get('code'));
      const me = this.seatOf(r, u.searchParams.get('token'));
      if (!me) return json(404, { error: 'no seat' });
      const otherKey = me === 'A' ? 'B' : 'A';
      const seat = r.seats[me];
      const { readable, writable } = new TransformStream();
      const w = writable.getWriter();
      if (seat.w) { const old = seat.w; seat.w = null; old.close().catch(() => {}); }
      seat.w = w;
      r.t = Date.now();
      this.write(w, 'retry: 1500\n\n');
      const other = r.seats[otherKey];
      this.push(seat, { t: 'hello', seat: me, peer: !!(other && other.w), state: r.state });
      this.push(other, { t: 'peer', on: true });
      const gone = () => { if (seat.w === w) { seat.w = null; this.push(r.seats[otherKey], { t: 'peer', on: false }); } };
      w.closed.then(gone, gone);
      if (!this.timer) this.timer = setInterval(() => this.tick(), 20000);
      return new Response(readable, {
        headers: { 'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache, no-transform', ...CORS },
      });
    }
    return json(404, { error: 'not found' });
  }
}
