/**
 * Servidor de Salas & Relay WebSocket para "Juego de Carreras" (Godot 4).
 * 
 * Este servidor permite que jugadores en celulares (con datos móviles 4G/5G o WiFi)
 * jueguen entre sí por Internet sin necesidad de abrir puertos ni saber IPs.
 * 
 * Funcionalidades:
 * 1. Directorio de Salas (Lobby): Lista de salas activas en tiempo real.
 * 2. Identificación por Nombre de Sala y Nombre del Creador.
 * 3. Relay de Paquetes: Retransmite los datos entre el anfitrión y los clientes de cada sala.
 * 4. Panel Web en el puerto 8080: Puedes abrir http://localhost:8080 en tu navegador para ver las salas en vivo.
 */

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

// ===================================================================
// Seguridad de salas
// ===================================================================
// Versión del protocolo de red. Súbela cuando cambie el formato de los paquetes:
// los clientes con otra versión reciben un error claro en vez de fallos raros.
// v2: el servidor antepone 4 bytes (u32 LE) con el peer_id del remitente a cada paquete binario.
// v3: si el anfitrión se va en plena carrera, la carrera continúa con el heredero (no vuelve al lobby).
const PROTOCOL_VERSION = 3;
const MAX_WS_PAYLOAD = 64 * 1024;          // ningún paquete legítimo del juego se acerca a esto
const CREATE_LIMIT_PER_MIN = 6;            // salas creadas por IP y minuto
const JOIN_LIMIT_PER_MIN = 20;             // intentos de unirse por IP y minuto
const PASSWORD_FAILS_MAX = 5;              // fallos de contraseña por IP y sala...
const PASSWORD_LOCK_MS = 60000;            // ...antes de bloquear durante este tiempo
const PASSWORD_MIN_LEN = 3;
const PASSWORD_MAX_LEN = 32;
const CODE_LEN_PUBLIC = 4;
const CODE_LEN_HIDDEN = 6;                 // salas ocultas: código más largo para que no se pueda adivinar
const RECONNECT_GRACE_MS = 30000;          // un jugador que pierde la conexión conserva su hueco este tiempo
const HOST_GRACE_MS = 12000;               // el anfitrión: pasado este tiempo, otro jugador hereda la sala
const HOST_GRACE_RACING_MS = 6000;         // en carrera se espera menos: sin anfitrión la IA está congelada

function newToken() {
    return crypto.randomBytes(18).toString('base64url');
}

/** Contador por ventana de tiempo. Devuelve true si se superó el límite. */
const rateBuckets = new Map();
function overLimit(key, limit, windowMs) {
    const now = Date.now();
    let b = rateBuckets.get(key);
    if (!b || now - b.t > windowMs) { b = { n: 0, t: now }; rateBuckets.set(key, b); }
    b.n++;
    return b.n > limit;
}
setInterval(() => {
    const now = Date.now();
    for (const [k, b] of rateBuckets) if (now - b.t > 5 * 60000) rateBuckets.delete(k);
}, 60000);

function hashPassword(pw) {
    const salt = crypto.randomBytes(16);
    return { salt, hash: crypto.scryptSync(pw, salt, 32) };
}

function checkPassword(room, pw) {
    if (!room.passwordHash) return true;
    if (typeof pw !== 'string' || pw.length === 0 || pw.length > PASSWORD_MAX_LEN) return false;
    const h = crypto.scryptSync(pw, room.passwordHash.salt, 32);
    return crypto.timingSafeEqual(h, room.passwordHash.hash);
}

function clientIp(req) {
    return ((req && req.headers && req.headers['x-forwarded-for']) || (req && req.socket && req.socket.remoteAddress) || '')
        .toString().split(',')[0].trim() || 'unknown';
}

function sendError(ws, code, message) {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'error', code, message }));
}

// ===================================================================
// Clasificación online (Contrarreloj): mejor vuelta por piloto y pista
// ===================================================================
// Se guarda en memoria + JSON local. En Render gratis el disco se borra al
// reiniciar, así que si están definidas UPSTASH_REDIS_REST_URL y
// UPSTASH_REDIS_REST_TOKEN se guarda también en una base de datos Upstash
// Redis (gratis) y se recarga de ahí al arrancar.
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const REDIS_URL = (process.env.UPSTASH_REDIS_REST_URL || '').trim().replace(/\/+$/, '');
const REDIS_TOKEN = (process.env.UPSTASH_REDIS_REST_TOKEN || '').trim();
const REDIS_ON = !!(REDIS_URL && REDIS_TOKEN);
const REDIS_TRACKS_KEY = 'lb:tracks';            // SET con los nombres de pista
const redisTrackKey = (t) => 'lb:track:' + t;     // HASH playerId -> JSON de la entrada
const LB_FILE = path.join(DATA_DIR, 'leaderboard.json');
const LB_MIN_LAP = 3.0;      // segundos: por debajo se considera trampa/error
const LB_MAX_LAP = 1800.0;
const LB_MAX_PER_TRACK = 500;

// leaderboard: { [track]: { [playerId]: { name, lap, total, car, at } } }
let leaderboard = {};
try {
    if (fs.existsSync(LB_FILE)) {
        leaderboard = JSON.parse(fs.readFileSync(LB_FILE, 'utf8')) || {};
    }
} catch (e) {
    console.error('No se pudo leer la clasificación:', e.message);
    leaderboard = {};
}

// Entradas cambiadas pendientes de escribir en Redis: track -> Set(playerId)
const lbDirty = new Map();
function markDirty(track, playerId) {
    if (!REDIS_ON) return;
    if (!lbDirty.has(track)) lbDirty.set(track, new Set());
    lbDirty.get(track).add(playerId);
}

let lbSaveTimer = null;
function scheduleLeaderboardSave(delay = 2000) {
    if (lbSaveTimer) return;
    lbSaveTimer = setTimeout(() => {
        lbSaveTimer = null;
        try {
            fs.mkdirSync(DATA_DIR, { recursive: true });
            const tmp = LB_FILE + '.tmp';
            fs.writeFileSync(tmp, JSON.stringify(leaderboard));
            fs.renameSync(tmp, LB_FILE);
        } catch (e) {
            console.error('No se pudo guardar la clasificación:', e.message);
        }
        flushToRedis();
    }, delay);
}

// --- Upstash Redis (API REST, sin dependencias) ---------------------------

/** Ejecuta varios comandos Redis en una sola petición. Devuelve el array de resultados. */
function redisPipeline(commands) {
    return new Promise((resolve, reject) => {
        const body = JSON.stringify(commands);
        let url;
        try { url = new URL(REDIS_URL + '/pipeline'); } catch (e) { reject(e); return; }
        const lib = url.protocol === 'http:' ? http : https; // http sólo para pruebas locales
        const req = lib.request(url, {
            method: 'POST',
            headers: {
                'Authorization': 'Bearer ' + REDIS_TOKEN,
                'Content-Type': 'application/json',
                'Content-Length': Buffer.byteLength(body)
            },
            timeout: 10000
        }, (res) => {
            const chunks = [];
            res.on('data', (c) => chunks.push(c));
            res.on('end', () => {
                const text = Buffer.concat(chunks).toString('utf8');
                if (res.statusCode !== 200) { reject(new Error(`HTTP ${res.statusCode}: ${text.substring(0, 200)}`)); return; }
                try {
                    const out = JSON.parse(text);
                    const err = Array.isArray(out) && out.find((r) => r && r.error);
                    if (err) { reject(new Error(err.error)); return; }
                    resolve(Array.isArray(out) ? out.map((r) => r.result) : []);
                } catch (e) { reject(e); }
            });
        });
        req.on('timeout', () => req.destroy(new Error('timeout')));
        req.on('error', reject);
        req.end(body);
    });
}

let lbLoaded = !REDIS_ON;   // hasta cargar de Redis no se escribe en él (para no pisar datos)
let lbFlushing = false;

function flushToRedis() {
    if (!REDIS_ON || !lbLoaded || lbFlushing || lbDirty.size === 0) return;
    const batch = new Map(lbDirty);
    lbDirty.clear();
    const cmds = [];
    for (const [track, ids] of batch) {
        const board = leaderboard[track] || {};
        const hset = ['HSET', redisTrackKey(track)];
        const hdel = ['HDEL', redisTrackKey(track)];
        for (const pid of ids) {
            if (board[pid]) hset.push(pid, JSON.stringify(board[pid]));
            else hdel.push(pid);
        }
        cmds.push(['SADD', REDIS_TRACKS_KEY, track]);
        if (hset.length > 2) cmds.push(hset);
        if (hdel.length > 2) cmds.push(hdel);
    }
    lbFlushing = true;
    redisPipeline(cmds).then(() => {
        lbFlushing = false;
        if (lbDirty.size) scheduleLeaderboardSave();
    }).catch((e) => {
        lbFlushing = false;
        console.error('No se pudo guardar la clasificación en Redis (se reintentará):', e.message || e.code || String(e));
        for (const [track, ids] of batch) for (const pid of ids) markDirty(track, pid);
        scheduleLeaderboardSave(15000);
    });
}

/** Carga la clasificación de Redis y la mezcla con la de memoria (gana la mejor vuelta). */
async function loadFromRedis() {
    const [tracks] = await redisPipeline([['SMEMBERS', REDIS_TRACKS_KEY]]);
    const list = Array.isArray(tracks) ? tracks : [];
    const results = list.length ? await redisPipeline(list.map((t) => ['HGETALL', redisTrackKey(t)])) : [];
    let total = 0;
    list.forEach((track, i) => {
        const flat = Array.isArray(results[i]) ? results[i] : [];
        if (!leaderboard[track]) leaderboard[track] = {};
        const board = leaderboard[track];
        const remote = {};
        for (let k = 0; k + 1 < flat.length; k += 2) {
            try { remote[flat[k]] = JSON.parse(flat[k + 1]); } catch (e) { /* entrada corrupta: ignorar */ }
        }
        for (const [pid, e] of Object.entries(remote)) {
            if (!e || !isFinite(e.lap)) continue;
            total++;
            if (!board[pid] || e.lap <= board[pid].lap) board[pid] = e;
        }
        // Entradas locales mejores que las de Redis: subirlas
        for (const pid of Object.keys(board)) {
            if (!remote[pid] || board[pid].lap < remote[pid].lap) markDirty(track, pid);
        }
    });
    for (const track of Object.keys(leaderboard)) {
        if (!list.includes(track)) for (const pid of Object.keys(leaderboard[track])) markDirty(track, pid);
    }
    return { tracks: list.length, total };
}

let lbReady = Promise.resolve();
if (REDIS_ON) {
    lbReady = new Promise((resolve) => {
        let attempt = 0;
        const tryLoad = () => {
            attempt++;
            loadFromRedis().then(({ tracks, total }) => {
                lbLoaded = true;
                console.log(`[⏱] Clasificación cargada de Upstash Redis: ${total} tiempos en ${tracks} pistas`);
                scheduleLeaderboardSave(100);
                resolve();
            }).catch((e) => {
                console.error(`No se pudo cargar la clasificación de Redis (intento ${attempt}):`, e.message || e.code || String(e));
                if (attempt === 2) resolve(); // no bloquear las peticiones para siempre
                setTimeout(tryLoad, Math.min(30000, 3000 * attempt));
            });
        };
        tryLoad();
    });
} else {
    console.log('[⏱] Clasificación sin base de datos (solo archivo local). Define UPSTASH_REDIS_REST_URL y UPSTASH_REDIS_REST_TOKEN para conservarla.');
}

function cleanText(s, max) {
    return String(s == null ? '' : s).replace(/[\u0000-\u001f\u007f]/g, '').trim().substring(0, max);
}

function sortedTrack(track) {
    const entries = leaderboard[track] ? Object.entries(leaderboard[track]) : [];
    entries.sort((a, b) => a[1].lap - b[1].lap || a[1].at - b[1].at);
    return entries;
}

function trackStanding(track, playerId) {
    const sorted = sortedTrack(track);
    const idx = playerId ? sorted.findIndex(([pid]) => pid === playerId) : -1;
    return { sorted, rank: idx >= 0 ? idx + 1 : 0, entry: idx >= 0 ? sorted[idx][1] : null };
}

function publicEntry([pid, e], i, playerId) {
    return { rank: i + 1, name: e.name, lap: e.lap, total: e.total, car: e.car, at: e.at, me: pid === playerId };
}

// Límite simple de envíos por IP (evita spam)
const lbRate = new Map();
function rateLimited(ip) {
    const now = Date.now();
    const r = lbRate.get(ip) || { n: 0, t: now };
    if (now - r.t > 60000) { r.n = 0; r.t = now; }
    r.n++;
    lbRate.set(ip, r);
    return r.n > 20;
}
setInterval(() => {
    const now = Date.now();
    for (const [ip, r] of lbRate) if (now - r.t > 120000) lbRate.delete(ip);
}, 60000);

function sendJson(res, status, obj) {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(obj));
}

function readBody(req, maxBytes, cb) {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
        size += c.length;
        if (size > maxBytes) { req.destroy(); return; }
        chunks.push(c);
    });
    req.on('end', () => cb(Buffer.concat(chunks).toString('utf8')));
}

function handleLeaderboard(req, res, url) {
    if (req.method === 'GET' && url.pathname === '/api/leaderboard/tracks') {
        const tracks = Object.keys(leaderboard).map((t) => {
            const top = sortedTrack(t)[0];
            return { track: t, count: Object.keys(leaderboard[t]).length, best: top ? { name: top[1].name, lap: top[1].lap } : null };
        });
        sendJson(res, 200, { ok: true, tracks });
        return true;
    }

    if (req.method === 'GET' && url.pathname === '/api/leaderboard') {
        const track = cleanText(url.searchParams.get('track'), 64);
        const playerId = cleanText(url.searchParams.get('player_id'), 64);
        const limit = Math.min(Math.max(parseInt(url.searchParams.get('limit') || '20', 10) || 20, 1), 100);
        if (!track) { sendJson(res, 400, { ok: false, error: 'Falta la pista' }); return true; }
        const { sorted, rank, entry } = trackStanding(track, playerId);
        sendJson(res, 200, {
            ok: true,
            track,
            count: sorted.length,
            entries: sorted.slice(0, limit).map((e, i) => publicEntry(e, i, playerId)),
            me: entry ? { rank, name: entry.name, lap: entry.lap, total: entry.total, car: entry.car, at: entry.at, me: true } : null
        });
        return true;
    }

    if (req.method === 'POST' && url.pathname === '/api/leaderboard/submit') {
        const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').toString().split(',')[0].trim();
        if (rateLimited(ip)) { sendJson(res, 429, { ok: false, error: 'Demasiados envíos, espera un minuto' }); return true; }
        readBody(req, 4096, (raw) => {
            let d;
            try { d = JSON.parse(raw); } catch (e) { sendJson(res, 400, { ok: false, error: 'JSON inválido' }); return; }
            const track = cleanText(d.track, 64);
            const playerId = cleanText(d.player_id, 64);
            const name = cleanText(d.name, 24) || 'Piloto';
            const car = cleanText(d.car, 32);
            const lap = Number(d.lap);
            const total = Number(d.total);
            if (!track || !/^[A-Za-z0-9_-]{8,64}$/.test(playerId)) {
                sendJson(res, 400, { ok: false, error: 'Datos incompletos' }); return;
            }
            if (!isFinite(lap) || lap < LB_MIN_LAP || lap > LB_MAX_LAP) {
                sendJson(res, 400, { ok: false, error: 'Tiempo de vuelta no válido' }); return;
            }
            if (isFinite(total) && total > 0 && total + 0.01 < lap) {
                sendJson(res, 400, { ok: false, error: 'Tiempos incoherentes' }); return;
            }
            if (!leaderboard[track]) {
                if (Object.keys(leaderboard).length >= 200) { sendJson(res, 400, { ok: false, error: 'Pista desconocida' }); return; }
                leaderboard[track] = {};
            }
            const board = leaderboard[track];
            const prev = board[playerId];
            const improved = !prev || lap < prev.lap;
            if (improved) {
                board[playerId] = {
                    name, car,
                    lap: Math.round(lap * 1000) / 1000,
                    total: isFinite(total) && total > 0 ? Math.round(total * 1000) / 1000 : 0,
                    at: Date.now()
                };
                markDirty(track, playerId);
            } else if (prev.name !== name) {
                prev.name = name; // el piloto cambió de nombre: actualizarlo igualmente
                markDirty(track, playerId);
            }
            // Recortar pistas enormes conservando los mejores
            const ids = Object.keys(board);
            if (ids.length > LB_MAX_PER_TRACK) {
                for (const [pid] of sortedTrack(track).slice(LB_MAX_PER_TRACK)) {
                    delete board[pid];
                    markDirty(track, pid);
                }
            }
            scheduleLeaderboardSave();
            const { sorted, rank, entry } = trackStanding(track, playerId);
            if (improved) console.log(`[⏱] ${name} marcó ${lap.toFixed(3)}s en "${track}" (puesto ${rank}/${sorted.length})`);
            sendJson(res, 200, {
                ok: true,
                improved,
                rank,
                count: sorted.length,
                best: entry ? entry.lap : lap
            });
        });
        return true;
    }

    return false;
}

// Almacén de salas en memoria
// Clave: roomId -> { id, roomName, hostName, maxPlayers, players: Map<peerId, socket>, isRacing, createdAt, lastHeartbeat }
const rooms = new Map();

// Servidor HTTP básico para monitoreo y lista de salas vía REST
const server = http.createServer((req, res) => {
    // Cabeceras CORS para permitir peticiones desde cualquier origen
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    let parsedUrl;
    try { parsedUrl = new URL(req.url, 'http://localhost'); } catch (e) { parsedUrl = null; }
    if (parsedUrl && parsedUrl.pathname.startsWith('/api/leaderboard')) {
        // Esperar a que se cargue la clasificación de la base de datos (tras despertar)
        lbReady.then(() => {
            if (!handleLeaderboard(req, res, parsedUrl)) sendJson(res, 404, { ok: false, error: 'Not Found' });
        });
        return;
    }

    if (req.url === '/health' || req.url === '/healthz') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('ok');
        return;
    }

    if (req.url === '/api/rooms' || req.url === '/rooms') {
        const roomList = getPublicRoomsList();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ ok: true, count: roomList.length, rooms: roomList }));
        return;
    }

    // Panel web informativo
    if (req.url === '/' || req.url === '') {
        const roomList = getPublicRoomsList();
        const rowsHtml = roomList.map(r => `
            <tr style="border-bottom: 1px solid #2a3447;">
                <td style="padding: 10px; font-weight: bold; color: #ff9800;">${r.locked ? '🔒 ' : ''}${escapeHtml(r.roomName)}</td>
                <td style="padding: 10px; color: #90caf9;">${escapeHtml(r.hostName)}</td>
                <td style="padding: 10px; text-align: center;">${r.playerCount} / ${r.maxPlayers}</td>
                <td style="padding: 10px; text-align: center;">
                    <span style="background: ${r.isRacing ? '#ff5722' : '#4caf50'}; color: white; padding: 3px 8px; border-radius: 4px; font-size: 12px;">
                        ${r.isRacing ? 'EN CARRERA' : 'EN ESPERA'}
                    </span>
                </td>
                <td style="padding: 10px; font-family: monospace; color: #bbb;">${r.id}</td>
            </tr>
        `).join('');

        const html = `<!DOCTYPE html>
        <html lang="es">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Servidor Relay - Juego de Carreras</title>
            <style>
                body { background: #0c1017; color: #e6edf3; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 24px; }
                .card { background: #151b26; border: 1px solid #2b3545; border-radius: 12px; max-width: 900px; margin: 0 auto; padding: 24px; box-shadow: 0 8px 24px rgba(0,0,0,0.5); }
                h1 { color: #ff9800; margin-top: 0; }
                table { width: 100%; border-collapse: collapse; margin-top: 16px; }
                th { text-align: left; background: #1e2638; padding: 10px; color: #aaa; font-size: 14px; }
                .status-badge { display: inline-block; background: #238636; color: white; padding: 4px 10px; border-radius: 20px; font-weight: bold; font-size: 13px; }
            </style>
            <meta http-equiv="refresh" content="5">
        </head>
        <body>
            <div class="card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <h1>🏁 Servidor de Carreras (Online Relay)</h1>
                    <span class="status-badge">● ONLINE (Puerto ${PORT})</span>
                </div>
                <p style="color: #8b949e;">Este servidor retransmite partidas de Godot 4 a través de Internet sin abrir puertos.</p>
                <div style="margin-top: 20px;">
                    <h3>Salas Activas (${roomList.length}):</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Nombre de Sala</th>
                                <th>Creador</th>
                                <th style="text-align: center;">Jugadores</th>
                                <th style="text-align: center;">Estado</th>
                                <th>Código</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${rowsHtml || '<tr><td colspan="5" style="padding: 24px; text-align: center; color: #888;">No hay salas activas en este momento. ¡Crea una desde el juego!</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>
        </body>
        </html>`;

        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(html);
        return;
    }

    res.writeHead(404);
    res.end('Not Found');
});

// Servidor WebSocket integrado (baja latencia para juegos en tiempo real)
const wss = new WebSocket.Server({
    server,
    perMessageDeflate: false, // Desactiva compresión innecesaria para evitar delay en paquetes pequeños
    maxPayload: MAX_WS_PAYLOAD // por defecto ws acepta 100 MB: un solo mensaje enorme tumbaría el servidor
});

let nextPeerId = 2; // El Host siempre es el peer ID 1 en Godot

wss.on('connection', (ws, req) => {
    // Desactivar el algoritmo de Nagle en TCP para enviar paquetes de inmediato (sin buffer latency)
    if (ws._socket && typeof ws._socket.setNoDelay === 'function') {
        ws._socket.setNoDelay(true);
    }

    ws.isAlive = true;
    ws.roomId = null;
    ws.peerId = null;
    ws.isHost = false;
    ws.ip = clientIp(req);

    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message, isBinary) => {
        // Si es binario, es un paquete directo de juego (RPC de Godot)
        if (isBinary) {
            handleGamePacket(ws, message);
            return;
        }

        // Si es texto, es señalización de control (JSON). El texto que no es JSON se descarta.
        let data;
        try {
            data = JSON.parse(message.toString());
        } catch (err) {
            return;
        }
        if (data && typeof data === 'object') handleControlMessage(ws, data);
    });

    ws.on('close', () => {
        handleDisconnect(ws);
    });

    ws.on('error', (err) => {
        console.error('Error en WebSocket:', err.message);
        handleDisconnect(ws);
    });
});

/**
 * Procesa mensajes de control JSON (Crear sala, unirse, listar)
 */
function handleControlMessage(ws, msg) {
    const type = msg.type || msg.action;

    switch (type) {
        case 'leave': {
            // Salida explícita: el cliente avisa antes de cerrar (los proxies tardan ~10 s en propagar el cierre).
            // No se reserva hueco: quien sale a propósito no va a reconectar.
            handleDisconnect(ws, true);
            break;
        }

        case 'rejoin': {
            // Vuelta tras un corte de red (cambio WiFi/4G, túnel...): mismo hueco y mismo peer_id.
            if (Number(msg.protocol) !== PROTOCOL_VERSION) {
                sendError(ws, 'version', 'Tu versión del juego no coincide con la del servidor. Actualiza el juego.');
                return;
            }
            if (overLimit('join|' + ws.ip, JOIN_LIMIT_PER_MIN, 60000)) {
                sendError(ws, 'rate_limited', 'Demasiados intentos. Espera un minuto.');
                return;
            }
            const room = rooms.get(cleanText(msg.room_id, 12).toUpperCase());
            const token = typeof msg.token === 'string' ? msg.token : '';
            const peerId = room && token ? room.sessions.get(token) : undefined;
            if (!room || peerId === undefined) {
                sendError(ws, 'session_expired', 'Ya no se puede volver a la sala.');
                return;
            }
            const away = room.away.get(peerId);
            const current = room.players.get(peerId);
            if (!away && !current) {
                sendError(ws, 'session_expired', 'Ya no se puede volver a la sala.');
                return;
            }
            if (ws.roomId && ws !== current) handleDisconnect(ws, true);
            if (away) {
                clearTimeout(away.timer);
                room.away.delete(peerId);
            } else if (current !== ws) {
                // El servidor aún no notó que la conexión vieja murió (típico al cambiar de red): reemplazarla.
                current.roomId = null;
                try { current.terminate(); } catch (e) { /* ignorar */ }
            }
            ws.roomId = room.id;
            ws.peerId = peerId;
            ws.isHost = peerId === 1;
            ws.playerName = away ? away.name : current.playerName;
            ws.token = token;
            room.players.set(peerId, ws);
            if (ws.isHost) room.hostWs = ws;
            room.lastHeartbeat = Date.now();
            console.log(`[↺] "${ws.playerName}" (peer ${peerId}) volvió a la sala "${room.roomName}"`);

            ws.send(JSON.stringify({
                type: 'room_rejoined',
                room_id: room.id,
                room_name: room.roomName,
                host_name: room.hostName,
                peer_id: peerId,
                is_host: ws.isHost,
                hidden: room.hidden,
                locked: !!room.passwordHash,
                closed: room.closed,
                is_racing: room.isRacing,
                epoch_ms: room.createdAt
            }));
            if (away) {
                if (ws.isHost) notifyClients(room, { type: 'host_back' });
                else notifyHost(room, { type: 'peer_back', peer_id: peerId });
            }
            break;
        }

        case 'kick': {
            // Solo el anfitrión. ban = además bloquear su IP en esta sala.
            if (!ws.roomId || !ws.isHost) return;
            const room = rooms.get(ws.roomId);
            const target = Number(msg.peer_id);
            if (!room || !target || target === 1) return;
            const tws = room.players.get(target);
            const away = room.away.get(target);
            const ban = !!msg.ban;
            if (ban) {
                const ip = tws ? tws.ip : (away ? away.ip : '');
                if (ip) room.banned.add(ip);
            }
            if (tws) {
                if (tws.readyState === WebSocket.OPEN) {
                    tws.send(JSON.stringify({
                        type: 'kicked',
                        banned: ban,
                        message: ban ? 'El anfitrión te ha bloqueado en esta sala.' : 'El anfitrión te ha expulsado de la sala.'
                    }));
                }
                handleDisconnect(tws, true);
                setTimeout(() => { try { tws.close(4001, 'kicked'); } catch (e) { /* ignorar */ } }, 100);
            } else if (away) {
                clearTimeout(away.timer);
                room.away.delete(target);
                finalizeLeave(room, target, away.token, false, away.name);
            }
            console.log(`[✖] Peer ${target} ${ban ? 'bloqueado' : 'expulsado'} de "${room.roomName}"`);
            break;
        }

        case 'set_closed': {
            // El anfitrión cierra/abre la entrada aunque haya sitio
            if (!ws.roomId || !ws.isHost) return;
            const room = rooms.get(ws.roomId);
            if (!room) return;
            room.closed = !!msg.closed;
            ws.send(JSON.stringify({ type: 'room_state', closed: room.closed }));
            break;
        }

        case 'create_room': {
            if (Number(msg.protocol) !== PROTOCOL_VERSION) {
                sendError(ws, 'version', 'Tu versión del juego no coincide con la del servidor. Actualiza el juego.');
                return;
            }
            if (overLimit('create|' + ws.ip, CREATE_LIMIT_PER_MIN, 60000)) {
                sendError(ws, 'rate_limited', 'Has creado demasiadas salas. Espera un minuto.');
                return;
            }
            const password = typeof msg.password === 'string' ? msg.password : '';
            if (password && (password.length < PASSWORD_MIN_LEN || password.length > PASSWORD_MAX_LEN)) {
                sendError(ws, 'bad_password', `La contraseña debe tener entre ${PASSWORD_MIN_LEN} y ${PASSWORD_MAX_LEN} caracteres.`);
                return;
            }
            if (ws.roomId) handleDisconnect(ws, true); // ya estaba en otra sala: salir primero
            const hidden = !!msg.hidden;
            const roomId = generateRoomCode(hidden ? CODE_LEN_HIDDEN : CODE_LEN_PUBLIC);
            const roomName = cleanText(msg.room_name, 40) || 'Sala de Carreras';
            const hostName = cleanText(msg.host_name, 24) || 'Anfitrión';
            const maxPlayers = Math.min(Math.max(parseInt(msg.max_players || 8), 2), 8);

            const room = {
                id: roomId,
                roomName: roomName,
                hostName: hostName,
                maxPlayers: maxPlayers,
                hostWs: ws,
                players: new Map(),
                isRacing: false,
                hidden: hidden,
                passwordHash: password ? hashPassword(password) : null,
                closed: false,               // el anfitrión cerró la entrada
                banned: new Set(),           // IPs bloqueadas en esta sala
                sessions: new Map(),         // token de sesión -> peerId (para reconectar)
                away: new Map(),             // peerId -> { token, name, ip, wasHost, timer } (hueco reservado)
                createdAt: Date.now(),
                lastHeartbeat: Date.now()
            };

            ws.roomId = roomId;
            ws.peerId = 1; // ID 1 para el host en Godot
            ws.isHost = true;
            ws.playerName = hostName;
            ws.token = newToken();
            room.players.set(1, ws);
            room.sessions.set(ws.token, 1);

            rooms.set(roomId, room);
            const tags = [hidden ? 'oculta' : 'pública', room.passwordHash ? 'con contraseña' : 'abierta'].join(', ');
            console.log(`[+] Sala creada: "${roomName}" por ${hostName} [ID: ${roomId}] (${tags})`);

            ws.send(JSON.stringify({
                type: 'room_created',
                room_id: roomId,
                room_name: roomName,
                host_name: hostName,
                peer_id: 1,
                hidden: hidden,
                locked: !!room.passwordHash,
                session_token: ws.token,
                epoch_ms: room.createdAt
            }));
            break;
        }

        case 'list_rooms': {
            ws.send(JSON.stringify({
                type: 'rooms_list',
                rooms: getPublicRoomsList()
            }));
            break;
        }

        case 'join_room': {
            const roomId = cleanText(msg.room_id || msg.code, 12).toUpperCase();
            const clientName = cleanText(msg.player_name, 24) || 'Piloto';

            if (Number(msg.protocol) !== PROTOCOL_VERSION) {
                sendError(ws, 'version', 'Tu versión del juego no coincide con la del servidor. Actualiza el juego.');
                return;
            }
            if (overLimit('join|' + ws.ip, JOIN_LIMIT_PER_MIN, 60000)) {
                sendError(ws, 'rate_limited', 'Demasiados intentos. Espera un minuto.');
                return;
            }

            if (ws.roomId) {
                if (ws.roomId === roomId) return; // join duplicado: ignorar
                handleDisconnect(ws, true);
            }

            const room = rooms.get(roomId);
            if (!room) {
                sendError(ws, 'not_found', 'La sala no existe o ya cerró.');
                return;
            }

            if (room.banned.has(ws.ip)) {
                sendError(ws, 'banned', 'El anfitrión te ha bloqueado en esta sala.');
                return;
            }

            if (room.closed) {
                sendError(ws, 'room_closed', 'El anfitrión ha cerrado la entrada a la sala.');
                return;
            }

            if (room.passwordHash) {
                const failKey = 'pwfail|' + ws.ip + '|' + roomId;
                const fails = rateBuckets.get(failKey);
                if (fails && fails.n >= PASSWORD_FAILS_MAX && Date.now() - fails.t <= PASSWORD_LOCK_MS) {
                    sendError(ws, 'rate_limited', 'Demasiados intentos de contraseña. Espera un minuto.');
                    return;
                }
                const pw = typeof msg.password === 'string' ? msg.password : '';
                if (!pw) {
                    sendError(ws, 'password_required', 'Esta sala tiene contraseña.');
                    return;
                }
                if (!checkPassword(room, pw)) {
                    overLimit(failKey, PASSWORD_FAILS_MAX, PASSWORD_LOCK_MS);
                    sendError(ws, 'wrong_password', 'Contraseña incorrecta.');
                    return;
                }
            }

            if (room.isRacing) {
                sendError(ws, 'racing', 'La carrera ya ha comenzado.');
                return;
            }

            if (room.players.size + room.away.size >= room.maxPlayers) {
                sendError(ws, 'full', 'La sala está llena.');
                return;
            }

            const assignedPeerId = nextPeerId++;
            ws.roomId = roomId;
            ws.peerId = assignedPeerId;
            ws.isHost = false;
            ws.playerName = clientName;
            ws.token = newToken();

            room.players.set(assignedPeerId, ws);
            room.sessions.set(ws.token, assignedPeerId);
            console.log(`[+] Jugador "${clientName}" (peer ${assignedPeerId}) se unió a la sala "${room.roomName}"`);

            // Notificar al nuevo cliente
            ws.send(JSON.stringify({
                type: 'room_joined',
                room_id: roomId,
                room_name: room.roomName,
                host_name: room.hostName,
                peer_id: assignedPeerId,
                session_token: ws.token,
                epoch_ms: room.createdAt
            }));

            // Notificar al anfitrión
            if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
                room.hostWs.send(JSON.stringify({
                    type: 'peer_connected',
                    peer_id: assignedPeerId,
                    player_name: clientName
                }));
            }
            break;
        }

        case 'set_racing_state': {
            if (ws.roomId && ws.isHost) {
                const room = rooms.get(ws.roomId);
                if (room) {
                    room.isRacing = !!msg.is_racing;
                }
            }
            break;
        }

        case 'ping': {
            let epochMs = Date.now();
            if (ws.roomId) {
                const room = rooms.get(ws.roomId);
                if (room) {
                    room.lastHeartbeat = Date.now();
                    epochMs = room.createdAt;
                }
            }
            ws.send(JSON.stringify({
                type: 'pong',
                c_ms: msg.c_ms || 0,
                s_ms: Date.now(),
                epoch_ms: epochMs
            }));
            break;
        }
    }
}

/**
 * Difunde un paquete binario a todos los miembros de la sala excepto senderPeerId.
 */
function broadcastToRoom(room, senderPeerId, packet, droppable) {
    for (const [peerId, clientWs] of room.players) {
        if (peerId !== senderPeerId && clientWs.readyState === WebSocket.OPEN) {
            if (clientWs.bufferedAmount > MAX_BUFFERED_HARD) {
                // Cliente muerto/atascado: expulsarlo en vez de acumular memoria sin límite
                console.warn(`[!] Cliente ${peerId} atascado (${clientWs.bufferedAmount} B), cerrando`);
                clientWs.terminate();
                continue;
            }
            if (droppable && clientWs.bufferedAmount > MAX_BUFFERED_DROPPABLE) {
                continue;
            }
            clientWs.send(packet, { binary: true });
        }
    }
}

/**
 * Retransmite paquetes binarios de la carrera a los miembros de la misma sala
 */
function handleGamePacket(ws, packet) {
    if (!ws.roomId) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;

    room.lastHeartbeat = Date.now();

    // Solo se pueden descartar paquetes "efímeros" (snapshot / car_state / input),
    // porque llegará uno más nuevo enseguida. Los mensajes críticos (lobby, start_race,
    // hit, hit_report, event, join, return_to_lobby, torneo...) NUNCA se descartan: perderlos
    // deja a los clientes desincronizados o atascados.
    const cmd = readGodotCmd(packet);
    const droppable = cmd !== null && DROPPABLE_CMDS.has(cmd);

    // El remitente lo pone el servidor (lo sabe por la conexión), no el paquete:
    // así nadie puede hacerse pasar por otro jugador ni por el anfitrión.
    const framed = stampSender(ws.peerId, packet);

    if (ws.isHost) {
        // El host difunde snapshots o eventos a todos los clientes de su sala
        broadcastToRoom(room, 1, framed, droppable);
    } else if (cmd === 'car_state') {
        // Un solo salto por Render: el estado del coche de cada cliente va directo al host y demás clientes
        broadcastToRoom(room, ws.peerId, framed, true);
    } else {
        // Los demás mensajes del cliente (join, set_car, hit_report, input) van únicamente al host
        if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
            if (droppable && room.hostWs.bufferedAmount > MAX_BUFFERED_DROPPABLE) {
                return;
            }
            room.hostWs.send(framed, { binary: true });
        }
    }
}

/** Antepone 4 bytes (u32 little-endian) con el peer_id del remitente. */
function stampSender(peerId, packet) {
    const out = Buffer.allocUnsafe(packet.length + 4);
    out.writeUInt32LE(peerId >>> 0, 0);
    packet.copy(out, 4);
    return out;
}

const MAX_BUFFERED_DROPPABLE = 65536;     // 64 KB: a partir de aquí se descartan snapshots/inputs viejos
const MAX_BUFFERED_HARD = 4 * 1024 * 1024; // 4 MB: la conexión se considera muerta
const DROPPABLE_CMDS = new Set(['snapshot', 'car_state', 'input']);

/**
 * Lee el valor de la clave "cmd" de un Dictionary serializado con var_to_bytes() de Godot 4
 * (formato: u32 tipo=27, u32 nº elementos, luego pares clave/valor; String = u32 tipo=4, u32 len, bytes con padding a 4).
 * Devuelve null si el paquete no tiene ese formato (en ese caso se trata como crítico).
 */
function readGodotCmd(buf) {
    try {
        if (buf.length < 8) return null;
        if ((buf.readUInt32LE(0) & 0xFFFF) !== 27) return null; // TYPE_DICTIONARY
        let off = 8;
        const readString = () => {
            if ((buf.readUInt32LE(off) & 0xFFFF) !== 4) return null;
            const len = buf.readUInt32LE(off + 4);
            if (len > 64) return null;
            const s = buf.toString('utf8', off + 8, off + 8 + len);
            off += 8 + len + ((4 - (len % 4)) % 4);
            return s;
        };
        if ((buf.readUInt32LE(off) & 0xFFFF) !== 4) return null;
        if (readString() !== 'cmd') return null;
        if ((buf.readUInt32LE(off) & 0xFFFF) !== 4) return null;
        return readString();
    } catch (e) {
        return null;
    }
}

/**
 * Maneja la desconexión de un cliente o anfitrión
 */
/**
 * Maneja la desconexión de un cliente o anfitrión.
 * explicit = salió a propósito (leave, expulsado, cambio de sala): se libera el hueco ya.
 * Si se cortó la red, el hueco queda reservado unos segundos por si vuelve con su token.
 */
function handleDisconnect(ws, explicit = false) {
    if (!ws.roomId) return;
    const roomId = ws.roomId;
    ws.roomId = null; // idempotente: 'leave' + 'close' no deben procesarse dos veces
    const room = rooms.get(roomId);
    if (!room) return;
    if (room.players.get(ws.peerId) !== ws) return; // esta conexión ya fue reemplazada por una reconexión
    room.players.delete(ws.peerId);

    if (!explicit && ws.token && room.sessions.has(ws.token)) {
        const peerId = ws.peerId;
        const grace = ws.isHost ? (room.isRacing ? HOST_GRACE_RACING_MS : HOST_GRACE_MS) : RECONNECT_GRACE_MS;
        const entry = { token: ws.token, name: ws.playerName, ip: ws.ip, wasHost: ws.isHost, timer: null };
        entry.timer = setTimeout(() => {
            if (room.away.get(peerId) !== entry) return;
            room.away.delete(peerId);
            finalizeLeave(room, peerId, entry.token, entry.wasHost, entry.name);
        }, grace);
        room.away.set(peerId, entry);
        console.log(`[…] "${ws.playerName || peerId}" perdió la conexión en "${room.roomName}" (espera ${grace / 1000} s)`);
        if (ws.isHost) {
            room.hostWs = null;
            notifyClients(room, { type: 'host_away', grace_ms: grace });
        } else {
            notifyHost(room, { type: 'peer_away', peer_id: peerId });
        }
        return;
    }

    finalizeLeave(room, ws.peerId, ws.token, ws.isHost, ws.playerName);
}

/** El jugador se fue del todo: liberar su sesión y avisar (o pasar el mando si era el anfitrión). */
function finalizeLeave(room, peerId, token, wasHost, name) {
    if (token) room.sessions.delete(token);
    if (rooms.get(room.id) !== room) return;
    if (wasHost) {
        migrateHost(room, name);
    } else {
        console.log(`[-] Jugador "${name || peerId}" salió de la sala "${room.roomName}"`);
        notifyHost(room, { type: 'peer_disconnected', peer_id: peerId });
    }
    if (rooms.get(room.id) === room && room.players.size === 0 && room.away.size === 0) {
        closeRoom(room, 'vacía');
    }
}

/**
 * El anfitrión se fue: hereda la sala el jugador conectado que lleva más tiempo (peer_id más bajo).
 * El heredero pasa a ser el peer 1, porque todo el juego asume que el anfitrión es el 1.
 * Si había carrera, sigue: el heredero toma el control de la IA, los choques y la meta
 * desde el último estado que tenía (in_race avisa a los clientes para no reordenar la parrilla).
 */
function migrateHost(room, oldName) {
    room.hostWs = null;
    let best = null;
    for (const [pid, w] of room.players) {
        if (w.readyState === WebSocket.OPEN && (best === null || pid < best)) best = pid;
    }
    if (best === null) {
        closeRoom(room, 'sin jugadores conectados para heredarla');
        return;
    }
    const heir = room.players.get(best);
    const inRace = room.isRacing;
    room.players.delete(best);
    heir.peerId = 1;
    heir.isHost = true;
    room.players.set(1, heir);
    if (heir.token) room.sessions.set(heir.token, 1);
    room.hostWs = heir;
    room.hostName = heir.playerName || room.hostName;
    console.log(`[⇄] "${oldName}" dejó "${room.roomName}": nuevo anfitrión "${room.hostName}" (era peer ${best})${inRace ? ' — la carrera continúa' : ''}`);

    heir.send(JSON.stringify({ type: 'you_are_host', old_peer_id: best, host_name: room.hostName, in_race: inRace, race_aborted: false }));
    notifyClients(room, { type: 'host_migrated', new_host_old_peer: best, host_name: room.hostName, in_race: inRace, race_aborted: false });
}

function closeRoom(room, reason) {
    for (const [, entry] of room.away) clearTimeout(entry.timer);
    room.away.clear();
    for (const [peerId, clientWs] of room.players) {
        clientWs.roomId = null;
        if (clientWs.readyState === WebSocket.OPEN) {
            clientWs.send(JSON.stringify({ type: 'host_disconnected', message: 'La sala se ha cerrado.' }));
            clientWs.close();
        }
    }
    room.players.clear();
    rooms.delete(room.id);
    console.log(`[-] Sala "${room.roomName}" [ID: ${room.id}] cerrada (${reason})`);
}

function notifyHost(room, obj) {
    if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) room.hostWs.send(JSON.stringify(obj));
}

function notifyClients(room, obj) {
    const text = JSON.stringify(obj);
    for (const [peerId, w] of room.players) {
        if (peerId !== 1 && w.readyState === WebSocket.OPEN) w.send(text);
    }
}

/**
 * Devuelve la lista pública de salas activas formateada
 */
function getPublicRoomsList() {
    const list = [];
    const now = Date.now();

    for (const [id, r] of rooms) {
        // Si una sala no ha tenido latidos en 30 segundos, considerarla inactiva
        if (now - r.lastHeartbeat > 30000) {
            // Cerrar de verdad al host y clientes (antes quedaban "huérfanos" sin sala)
            closeRoom(r, 'inactiva');
            continue;
        }

        if (r.hidden) continue; // salas ocultas: solo se entra con el código

        list.push({
            id: r.id,
            roomName: r.roomName,
            hostName: r.hostName,
            playerCount: r.players.size + r.away.size,
            maxPlayers: r.maxPlayers,
            isRacing: r.isRacing,
            locked: !!r.passwordHash,
            closed: !!r.closed
        });
    }

    return list;
}

/**
 * Genera un código de sala corto y legible (ej. "RAC-72")
 */
function generateRoomCode(len = CODE_LEN_PUBLIC) {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    for (let attempt = 0; attempt < 50; attempt++) {
        let code = '';
        for (let i = 0; i < len; i++) {
            code += chars.charAt(crypto.randomInt(chars.length)); // aleatorio criptográfico: no predecible
        }
        if (!rooms.has(code)) return code; // evitar pisar una sala existente
    }
    return Date.now().toString(36).toUpperCase().slice(-6); // último recurso
}

function escapeHtml(str) {
    if (!str) return '';
    return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Limpieza periódica de conexiones muertas
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) {
            handleDisconnect(ws);
            return ws.terminate();
        }
        ws.isAlive = false;
        ws.ping();
    });
}, 5000);

// Render apaga el servicio con SIGTERM (al dormir o desplegar): subir lo pendiente antes de salir
let shuttingDown = false;
function shutdown(sig) {
    if (shuttingDown) return;
    shuttingDown = true;
    if (!REDIS_ON || !lbLoaded || (lbDirty.size === 0 && !lbFlushing)) process.exit(0);
    console.log(`[${sig}] Guardando la clasificación antes de salir...`);
    if (lbSaveTimer) { clearTimeout(lbSaveTimer); lbSaveTimer = null; }
    flushToRedis();
    const started = Date.now();
    const wait = setInterval(() => {
        if ((!lbFlushing && lbDirty.size === 0) || Date.now() - started > 8000) {
            clearInterval(wait);
            process.exit(0);
        }
        if (!lbFlushing && lbDirty.size) flushToRedis();
    }, 100);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Iniciar servidor
server.listen(PORT, () => {
    console.log(`=======================================================`);
    console.log(`🏁 SERVIDOR RELAY DE CARRERAS INICIADO EN PUERTO ${PORT}`);
    console.log(`   Panel Web:      http://localhost:${PORT}`);
    console.log(`   Endpoint API:   http://localhost:${PORT}/api/rooms`);
    console.log(`   Clasificación:  http://localhost:${PORT}/api/leaderboard/tracks`);
    console.log(`   WebSocket URL:  ws://localhost:${PORT}`);
    console.log(`=======================================================`);
});
