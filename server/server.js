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
const WebSocket = require('ws');

const PORT = process.env.PORT || 8080;

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

// Servidor WebSocket integrado
const wss = new WebSocket.Server({ server });

let nextPeerId = 2; // El Host siempre es el peer ID 1 en Godot

wss.on('connection', (ws) => {
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
        case 'create_room': {
            const roomId = generateRoomCode();
            const roomName = (msg.room_name || 'Sala de Carreras').trim().substring(0, 40);
            const hostName = (msg.host_name || 'Anfitrión').trim().substring(0, 24);
            const maxPlayers = Math.min(Math.max(parseInt(msg.max_players || 6), 2), 6);

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
                peer_id: 1
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
                peer_id: assignedPeerId
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
            if (ws.roomId) {
                const room = rooms.get(ws.roomId);
                if (room) room.lastHeartbeat = Date.now();
            }
            ws.send(JSON.stringify({ type: 'pong' }));
            break;
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

    if (ws.isHost) {
        // El host difunde snapshots o eventos a todos los clientes de su sala
        for (const [peerId, clientWs] of room.players) {
            if (peerId !== 1 && clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(packet, { binary: true });
            }
        }
    } else {
        // Los clientes envían sus inputs únicamente al host
        if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
            room.hostWs.send(packet, { binary: true });
        }
    }
}

/**
 * Maneja la desconexión de un cliente o anfitrión
 */
function handleDisconnect(ws) {
    if (!ws.roomId) return;
    const room = rooms.get(ws.roomId);
    if (!room) return;

    if (ws.isHost) {
        console.log(`[-] El anfitrión cerró la sala "${room.roomName}" [ID: ${ws.roomId}]`);
        // Avisar a todos los clientes que la sala se cerró
        for (const [peerId, clientWs] of room.players) {
            if (peerId !== 1 && clientWs.readyState === WebSocket.OPEN) {
                clientWs.send(JSON.stringify({ type: 'host_disconnected', message: 'El anfitrión ha cerrado la sala.' }));
                clientWs.close();
            }
        }
        rooms.delete(ws.roomId);
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
    let code = '';
    for (let i = 0; i < 4; i++) {
        code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
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
}, 10000);

// Iniciar servidor
server.listen(PORT, () => {
    console.log(`=======================================================`);
    console.log(`🏁 SERVIDOR RELAY DE CARRERAS INICIADO EN PUERTO ${PORT}`);
    console.log(`   Panel Web:      http://localhost:${PORT}`);
    console.log(`   Endpoint API:   http://localhost:${PORT}/api/rooms`);
    console.log(`   WebSocket URL:  ws://localhost:${PORT}`);
    console.log(`=======================================================`);
});
