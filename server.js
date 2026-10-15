// ===== JURAMENTO · servidor de salas =====
// Hace dos cosas: (1) sirve el juego (la carpeta superior) por HTTP, (2) gestiona salas por WebSocket.
// El servidor NO simula nada: el anfitrión corre el juego y el servidor solo reenvía mensajes.
//   cliente -> servidor : {t:'create',n} | {t:'join',c,n} | {t:'g',...}  (mensaje de juego)
//   servidor -> cliente : {t:'room',c,id,max} | {t:'err',m} | {t:'peer+',id,n} | {t:'peer-',id} | {t:'closed'} | {t:'g',...}
//   lobby global (plaza social): {t:'hub',n} | {t:'hp',x,y} | {t:'hc',m}  ->  {t:'hub',id,o,r,p} | {t:'hs',o,p} | {t:'rl',r,o} | {t:'hc',id,n,m}
// Un mensaje de juego de un invitado llega al anfitrión con from:<id>. El del anfitrión va a todos, o a uno si trae to:<id>.
const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const ROOT = path.resolve(process.env.ROOT || path.join(__dirname, '..'));
const MAX = Math.min(4, +process.env.MAX_PLAYERS || 4);
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.webp': 'image/webp', '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.mp4': 'video/mp4', '.webm': 'video/webm',
  '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.ttf': 'font/ttf', '.txt': 'text/plain; charset=utf-8', '.json': 'application/json' };

const server = http.createServer((req, res) => {
  let p;
  try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); return res.end(); }
  if (p === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok rooms=' + rooms.size); }
  if (p.endsWith('/')) p += 'index.html';
  const f = path.normalize(path.join(ROOT, p));
  if (f !== ROOT && !f.startsWith(ROOT + path.sep)) { res.writeHead(403); return res.end(); }
  if (f.startsWith(path.join(ROOT, 'server') + path.sep) || f.includes('node_modules')) { res.writeHead(404); return res.end(); }
  fs.stat(f, (e, st) => {
    if (e || !st.isFile()) { res.writeHead(404); return res.end('404'); }
    const type = MIME[path.extname(f).toLowerCase()] || 'application/octet-stream';
    const h = { 'Content-Type': type, 'Accept-Ranges': 'bytes', 'Cache-Control': 'no-cache' };
    const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
    if (m && (m[1] || m[2])) { // Range: necesario para audio/vídeo en Safari
      let a = m[1] ? +m[1] : st.size - +m[2], b = m[1] && m[2] ? +m[2] : st.size - 1;
      b = Math.min(b, st.size - 1);
      if (!(a >= 0) || a > b) { res.writeHead(416, { 'Content-Range': 'bytes */' + st.size }); return res.end(); }
      res.writeHead(206, Object.assign(h, { 'Content-Range': `bytes ${a}-${b}/${st.size}`, 'Content-Length': b - a + 1 }));
      return fs.createReadStream(f, { start: a, end: b }).pipe(res);
    }
    res.writeHead(200, Object.assign(h, { 'Content-Length': st.size }));
    fs.createReadStream(f).pipe(res);
  });
});

const rooms = new Map(); // código -> {host, peers:Map(id->ws), names:Map}
const wss = new WebSocketServer({ server, maxPayload: 256 * 1024 });
const ABC = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
const newCode = () => { for (let k = 0; k < 50; k++) { let c = ''; for (let i = 0; i < 4; i++) c += ABC[Math.random() * ABC.length | 0]; if (!rooms.has(c)) return c; } return null; };
const clean = s => String(s || '').replace(/[<>&"'`]/g, '').trim().slice(0, 14) || 'Sin nombre';
const tx = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(o)); };

// ---- lobby global ----
const hub = new Set(); let hubSeq = 0, hsDirty = false, rlDirty = false;
const online = () => wss.clients.size;
const hubPlayers = () => [...hub].map(w => [w.hid, w.hn, w.hx, w.hy, w.hid]);
const roomList = () => { const a = []; rooms.forEach((r, c) => { if (!r.pub) return; const p = 1 + r.peers.size; a.push({ c, n: r.names.get(0), g: r.reg || '', p, m: MAX }); }); return a.slice(0, 40); };
const toHub = o => { const s = JSON.stringify(o); hub.forEach(w => { if (w.readyState === 1) w.send(s); }); };
const leaveHub = ws => { if (hub.delete(ws)) { hsDirty = true; } };
setInterval(() => { // difusión limitada: posiciones 8/s, salas cuando cambian
  if (hsDirty && hub.size) { hsDirty = false; toHub({ t: 'hs', o: online(), p: hubPlayers() }); }
  if (rlDirty && hub.size) { rlDirty = false; toHub({ t: 'rl', o: online(), r: roomList() }); }
}, 125);

wss.on('connection', ws => {
  ws.alive = true; ws.room = null; ws.pid = -1;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'hub') {
      if (ws.room || hub.has(ws)) return;
      ws.hid = hubSeq++; ws.hn = clean(m.n); ws.hx = 192; ws.hy = 176; ws.lastChat = 0; hub.add(ws); hsDirty = true;
      return tx(ws, { t: 'hub', id: ws.hid, o: online(), r: roomList(), p: hubPlayers() });
    }
    if (m.t === 'hp') {
      if (!hub.has(ws)) return;
      const x = +m.x, y = +m.y; if (!(x >= 0 && x <= 384 && y >= 0 && y <= 256)) return;
      ws.hx = Math.round(x); ws.hy = Math.round(y); hsDirty = true; return;
    }
    if (m.t === 'hc') {
      if (!hub.has(ws)) return;
      const txt = String(m.m || '').replace(/[<>`]/g, '').trim().slice(0, 100); if (!txt) return;
      const now = Date.now(); if (now - ws.lastChat < 700) return; ws.lastChat = now;
      return toHub({ t: 'hc', id: ws.hid, n: ws.hn, m: txt });
    }
    if (m.t === 'create') {
      if (ws.room) return;
      const c = newCode(); if (!c) return tx(ws, { t: 'err', m: 'No hay códigos libres' });
      const r = { host: ws, peers: new Map(), names: new Map(), pub: m.pub === undefined ? 0 : (m.pub ? 1 : 0), reg: String(m.reg || '').replace(/[<>&"'`]/g, '').slice(0, 24) };
      leaveHub(ws); rlDirty = true;
      rooms.set(c, r); ws.room = c; ws.pid = 0; r.names.set(0, clean(m.n));
      return tx(ws, { t: 'room', c, id: 0, max: MAX });
    }
    if (m.t === 'join') {
      if (ws.room) return;
      const c = String(m.c || '').toUpperCase().trim(), r = rooms.get(c);
      if (!r) return tx(ws, { t: 'err', m: 'Esa sala no existe' });
      let id = -1; for (let i = 1; i < MAX; i++) if (!r.peers.has(i)) { id = i; break; }
      if (id < 0) return tx(ws, { t: 'err', m: 'Sala llena' });
      leaveHub(ws); rlDirty = true;
      r.peers.set(id, ws); ws.room = c; ws.pid = id; r.names.set(id, clean(m.n));
      tx(ws, { t: 'room', c, id, max: MAX });
      return tx(r.host, { t: 'peer+', id, n: r.names.get(id) });
    }
    if (m.t !== 'g' || !ws.room) return;
    const r = rooms.get(ws.room); if (!r) return;
    if (ws.pid === 0) { // anfitrión -> invitados
      const to = m.to; delete m.to;
      const s = JSON.stringify(m);
      if (to != null) { const p = r.peers.get(to); if (p && p.readyState === 1) p.send(s); }
      else r.peers.forEach(p => { if (p.readyState === 1) p.send(s); });
    } else { m.from = ws.pid; tx(r.host, m); } // invitado -> anfitrión
  });
  ws.on('close', () => {
    leaveHub(ws); hsDirty = true; rlDirty = true;
    const c = ws.room; if (!c) return; const r = rooms.get(c); if (!r) return;
    if (ws.pid === 0) { r.peers.forEach(p => { tx(p, { t: 'closed' }); p.room = null; try { p.close(); } catch (e) {} }); rooms.delete(c); }
    else if (r.peers.get(ws.pid) === ws) { r.peers.delete(ws.pid); r.names.delete(ws.pid); tx(r.host, { t: 'peer-', id: ws.pid }); }
  });
  ws.on('error', () => {});
});

setInterval(() => { wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; try { ws.ping(); } catch (e) {} }); }, 20000);
server.listen(PORT, () => console.log('Juramento listo en http://localhost:' + PORT + '  (sirviendo ' + ROOT + ')'));
