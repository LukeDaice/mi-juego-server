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
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

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
                <td style="padding: 10px; font-weight: bold; color: #ff9800;">${escapeHtml(r.roomName)}</td>
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
    perMessageDeflate: false // Desactiva compresión innecesaria para evitar delay en paquetes pequeños
});

let nextPeerId = 2; // El Host siempre es el peer ID 1 en Godot

wss.on('connection', (ws) => {
    // Desactivar el algoritmo de Nagle en TCP para enviar paquetes de inmediato (sin buffer latency)
    if (ws._socket && typeof ws._socket.setNoDelay === 'function') {
        ws._socket.setNoDelay(true);
    }

    ws.isAlive = true;
    ws.roomId = null;
    ws.peerId = null;
    ws.isHost = false;

    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (message, isBinary) => {
        // Si es binario, es un paquete directo de juego (RPC de Godot)
        if (isBinary) {
            handleGamePacket(ws, message);
            return;
        }

        // Si es texto, es señalización de control (JSON)
        try {
            const data = JSON.parse(message.toString());
            handleControlMessage(ws, data);
        } catch (err) {
            // Si llega texto no-JSON o buffer, retransmitir
            handleGamePacket(ws, message);
        }
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
            // Salida explícita: el cliente avisa antes de cerrar (los proxies tardan ~10 s en propagar el cierre)
            handleDisconnect(ws);
            break;
        }

        case 'create_room': {
            if (ws.roomId) handleDisconnect(ws); // ya estaba en otra sala: salir primero
            const roomId = generateRoomCode();
            const roomName = (msg.room_name || 'Sala de Carreras').trim().substring(0, 40);
            const hostName = (msg.host_name || 'Anfitrión').trim().substring(0, 24);
            const maxPlayers = Math.min(Math.max(parseInt(msg.max_players || 8), 2), 8);

            const room = {
                id: roomId,
                roomName: roomName,
                hostName: hostName,
                maxPlayers: maxPlayers,
                hostWs: ws,
                players: new Map(),
                isRacing: false,
                createdAt: Date.now(),
                lastHeartbeat: Date.now()
            };

            ws.roomId = roomId;
            ws.peerId = 1; // ID 1 para el host en Godot
            ws.isHost = true;
            ws.playerName = hostName;
            room.players.set(1, ws);

            rooms.set(roomId, room);
            console.log(`[+] Sala creada: "${roomName}" por ${hostName} [ID: ${roomId}]`);

            ws.send(JSON.stringify({
                type: 'room_created',
                room_id: roomId,
                room_name: roomName,
                host_name: hostName,
                peer_id: 1,
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
            const roomId = (msg.room_id || msg.code || '').trim().toUpperCase();
            const clientName = (msg.player_name || 'Piloto').trim().substring(0, 24);

            if (ws.roomId) {
                if (ws.roomId === roomId) return; // join duplicado: ignorar
                handleDisconnect(ws);
            }

            const room = rooms.get(roomId);
            if (!room) {
                ws.send(JSON.stringify({ type: 'error', message: 'La sala no existe o ya cerró.' }));
                return;
            }

            if (room.isRacing) {
                ws.send(JSON.stringify({ type: 'error', message: 'La carrera ya ha comenzado.' }));
                return;
            }

            if (room.players.size >= room.maxPlayers) {
                ws.send(JSON.stringify({ type: 'error', message: 'La sala está llena.' }));
                return;
            }

            const assignedPeerId = nextPeerId++;
            ws.roomId = roomId;
            ws.peerId = assignedPeerId;
            ws.isHost = false;
            ws.playerName = clientName;

            room.players.set(assignedPeerId, ws);
            console.log(`[+] Jugador "${clientName}" (peer ${assignedPeerId}) se unió a la sala "${room.roomName}"`);

            // Notificar al nuevo cliente
            ws.send(JSON.stringify({
                type: 'room_joined',
                room_id: roomId,
                room_name: room.roomName,
                host_name: room.hostName,
                peer_id: assignedPeerId,
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

    if (ws.isHost) {
        // El host difunde snapshots o eventos a todos los clientes de su sala
        broadcastToRoom(room, 1, packet, droppable);
    } else if (cmd === 'car_state') {
        // Un solo salto por Render: el estado del coche de cada cliente va directo al host y demás clientes
        broadcastToRoom(room, ws.peerId, packet, true);
    } else {
        // Los demás mensajes del cliente (join, set_car, hit_report, input) van únicamente al host
        if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
            if (droppable && room.hostWs.bufferedAmount > MAX_BUFFERED_DROPPABLE) {
                return;
            }
            room.hostWs.send(packet, { binary: true });
        }
    }
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
function handleDisconnect(ws) {
    if (!ws.roomId) return;
    const roomId = ws.roomId;
    ws.roomId = null; // idempotente: 'leave' + 'close' no deben procesarse dos veces
    const room = rooms.get(roomId);
    if (!room) return;

    if (ws.isHost) {
        console.log(`[-] El anfitrión cerró la sala "${room.roomName}" [ID: ${roomId}]`);
        // Avisar a todos los clientes que la sala se cerró
        for (const [peerId, clientWs] of room.players) {
            if (peerId !== 1 && clientWs.readyState === WebSocket.OPEN) {
                clientWs.roomId = null;
                clientWs.send(JSON.stringify({ type: 'host_disconnected', message: 'El anfitrión ha cerrado la sala.' }));
                clientWs.close();
            }
        }
        rooms.delete(roomId);
    } else {
        console.log(`[-] Jugador "${ws.playerName || ws.peerId}" salió de la sala "${room.roomName}"`);
        room.players.delete(ws.peerId);
        // Avisar al host
        if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
            room.hostWs.send(JSON.stringify({
                type: 'peer_disconnected',
                peer_id: ws.peerId
            }));
        }
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
            for (const [, pws] of r.players) {
                pws.roomId = null;
                try { pws.close(1001, 'Sala inactiva'); } catch (e) { /* ignorar */ }
            }
            rooms.delete(id);
            continue;
        }

        list.push({
            id: r.id,
            roomName: r.roomName,
            hostName: r.hostName,
            playerCount: r.players.size,
            maxPlayers: r.maxPlayers,
            isRacing: r.isRacing
        });
    }

    return list;
}

/**
 * Genera un código de sala corto y legible (ej. "RAC-72")
 */
function generateRoomCode() {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    for (let attempt = 0; attempt < 50; attempt++) {
        let code = '';
        for (let i = 0; i < 4; i++) {
            code += chars.charAt(Math.floor(Math.random() * chars.length));
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
