'use strict';
/*  GoldSrc Deathmatch – multiplayer server
 *  npm install && node server.js   ->   http://localhost:3000
 *  Put the game file next to this script and name it index.html (static files are served from this folder). */
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 10;
const RESPAWN_MS = 3000;
const TICK_MS = 33;                      // snapshot broadcast rate (~30 Hz)
const ROOM_IDLE_MS = 6 * 3600 * 1000;

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' }, pingInterval: 10000, pingTimeout: 20000 });
const rooms = new Map();                 // code -> room

app.use(express.static(__dirname));
app.get('/health', (req, res) => res.json({ ok: true, rooms: rooms.size }));

/* ---------- helpers ---------- */
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function newCode() {
  let c;
  do { c = ''; for (let i = 0; i < 4; i++) c += CODE_CHARS[(Math.random() * CODE_CHARS.length) | 0]; } while (rooms.has(c));
  return c;
}
const cleanName = n => String(n || 'Player').replace(/[^\w \-.]/g, '').trim().slice(0, 14) || 'Player';
const pub = p => ({ id: p.id, name: p.name, team: p.team, k: p.k, d: p.d, dead: p.dead, hp: p.hp });
const roster = r => [...r.players.values()].map(pub);
function pickTeam(r) {
  let t = 0, c = 0;
  for (const p of r.players.values()) p.team === 'T' ? t++ : c++;
  return t <= c ? 'T' : 'CT';
}
const VALID_MAPS = new Set(['arena', 'wh', 'village', 'test']);
function cleanCfg(c) {
  c = c || {};
  return {
    map: VALID_MAPS.has(c.map) ? c.map : 'arena',
    mode: c.mode === 'tdm' ? 'tdm' : 'dm',
    frag: Math.min(200, Math.max(5, parseInt(c.frag, 10) || 30)),
  };
}
function broadcastScores(r) { io.to(r.code).emit('scores', roster(r)); }
function resetMatch(r) {
  for (const p of r.players.values()) { p.k = p.d = 0; p.hp = 100; p.ar = 100; p.dead = false; }
}

/* ---------- socket handlers ---------- */
io.on('connection', (sock) => {
  let room = null, me = null;

  function leaveRoom() {
    if (!room || !me) return;
    const r = room;
    r.players.delete(me.id);
    sock.leave(r.code);
    io.to(r.code).emit('peer-leave', { id: me.id });
    if (r.players.size === 0) {
      rooms.delete(r.code);
    } else {
      if (r.host === me.id) {                        // host migration
        r.host = r.players.keys().next().value;
        io.to(r.code).emit('host', { id: r.host });
      }
      broadcastScores(r);
    }
    room = null; me = null;
  }

  function enter(r, name, ack) {
    if (r.players.size >= MAX_PLAYERS) return ack && ack({ ok: false, error: 'Room is full' });
    me = { id: sock.id, name: cleanName(name), team: pickTeam(r), hp: 100, ar: 100, k: 0, d: 0, dead: false, st: null };
    r.players.set(me.id, me);
    room = r;
    sock.join(r.code);
    r.last = Date.now();
    const others = roster(r).filter(p => p.id !== me.id);
    sock.to(r.code).emit('peer-join', pub(me));
    broadcastScores(r);
    ack && ack({ ok: true, code: r.code, id: me.id, team: me.team, host: r.host, cfg: r.cfg, started: r.started, players: others });
  }

  sock.on('create', (d, ack) => {
    leaveRoom();
    const r = { code: newCode(), host: sock.id, cfg: cleanCfg(d && d.cfg), started: false, players: new Map(), dirty: false, last: Date.now() };
    rooms.set(r.code, r);
    enter(r, d && d.name, ack);
  });

  sock.on('join', (d, ack) => {
    leaveRoom();
    const r = rooms.get(String((d && d.code) || '').toUpperCase().trim());
    if (!r) return ack && ack({ ok: false, error: 'Room not found' });
    enter(r, d && d.name, ack);
  });

  sock.on('cfg', (c) => {                              // host changes map/mode before start
    if (!room || !me || room.host !== me.id || room.started) return;
    room.cfg = cleanCfg(c);
    io.to(room.code).emit('cfg', room.cfg);
  });

  sock.on('start', () => {
    if (!room || !me || room.host !== me.id) return;
    room.started = true;
    resetMatch(room);
    const teams = {};
    for (const p of room.players.values()) teams[p.id] = p.team;
    io.to(room.code).emit('start', { cfg: room.cfg, teams });
    broadcastScores(room);
  });

  /* position / rotation / movement state (client sends ~30 Hz, we re-broadcast in snapshots) */
  sock.on('state', (s) => {
    if (!room || !me || !s) return;
    const n = v => (Number.isFinite(+v) ? +v : 0);
    me.st = { x: n(s.x), y: n(s.y), z: n(s.z), yaw: n(s.yaw), pitch: n(s.pitch), duck: s.duck ? 1 : 0, w: String(s.w || '').slice(0, 16), dead: me.dead ? 1 : 0 };
    room.dirty = true; room.last = Date.now();
  });

  /* event relays */
  sock.on('shoot', (s) => { if (room && me && !me.dead && s) sock.to(room.code).volatile.emit('shoot', { id: me.id, o: s.o, d: s.d, w: String(s.w || '').slice(0, 16) }); });
  sock.on('act', (a) => { if (room && me && a) sock.to(room.code).emit('act', { id: me.id, a }); });

  /* damage – the server owns hp / armor / deaths */
  sock.on('hit', (d) => {
    const r = room;
    if (!r || !r.started || !me || me.dead || !d) return;
    const t = r.players.get(d.t);
    if (!t || t === me || t.dead) return;
    if (r.cfg.mode === 'tdm' && t.team === me.team) return;             // no friendly fire
    const mult = { h: 4, c: 1, s: 1.25, l: 0.75 }[d.g] || 1;
    let dmg = Math.min(200, Math.max(0, Number(d.d) || 0)) * mult;
    const ratio = Math.min(2, Math.max(0.5, Number(d.ar) || 1));
    if (t.ar > 0 && d.g !== 'l') {                                      // CS 1.6 armor absorption
      const n = dmg * ratio * 0.5, a = (dmg - n) * 0.5;
      if (a > t.ar) { dmg -= t.ar * 2; t.ar = 0; } else { t.ar -= a; dmg = n; }
    }
    t.hp -= Math.round(dmg);
    io.to(r.code).emit('hp', { id: t.id, hp: Math.max(0, t.hp), ar: Math.round(t.ar), by: me.id, w: d.w });
    if (t.hp > 0) return;
    t.dead = true; t.d++; me.k++;
    io.to(r.code).emit('death', { id: t.id, by: me.id, w: d.w, h: d.g === 'h' });
    broadcastScores(r);
    const total = r.cfg.mode === 'tdm'
      ? [...r.players.values()].filter(p => p.team === me.team).reduce((q, p) => q + p.k, 0) : me.k;
    if (total >= r.cfg.frag) {
      io.to(r.code).emit('win', { id: me.id, team: me.team, mode: r.cfg.mode });
      resetMatch(r); broadcastScores(r);
      for (const p of r.players.values()) io.to(r.code).emit('respawn', { id: p.id });
      return;
    }
    setTimeout(() => {
      if (!rooms.has(r.code) || !r.players.has(t.id)) return;
      t.dead = false; t.hp = 100; t.ar = 100;
      io.to(r.code).emit('respawn', { id: t.id });
    }, RESPAWN_MS);
  });

  sock.on('leave', leaveRoom);
  sock.on('disconnect', leaveRoom);
});

/* ---------- snapshot loop + housekeeping ---------- */
setInterval(() => {
  for (const r of rooms.values()) {
    if (!r.dirty) continue;
    r.dirty = false;
    const snap = [];
    for (const p of r.players.values()) if (p.st) snap.push(Object.assign({ id: p.id }, p.st));
    io.to(r.code).volatile.emit('snap', snap);
  }
}, TICK_MS);

setInterval(() => {                                    // drop abandoned rooms
  const now = Date.now();
  for (const [code, r] of rooms) if (r.players.size === 0 || now - r.last > ROOM_IDLE_MS) rooms.delete(code);
}, 60000);

server.listen(PORT, () => console.log('Game server on http://localhost:' + PORT));
