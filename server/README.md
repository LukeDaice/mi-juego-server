# 🏁 Servidor de Salas y Relay Multijugador (Online Real)

Este servidor permite que los jugadores se conecten y compitan en **"Juego de Carreras"** a través de **Internet real** (desde casas distintas, redes Wi-Fi diferentes o datos móviles 4G/5G).

---

## ❓ ¿Por qué se necesita este servidor?
En redes móviles (4G/5G) y routers residenciales, los celulares están detrás de un cortafuegos llamado **NAT / CGNAT**. Esto significa que dos celulares en lugares distintos **no pueden comunicarse directamente entre sí** sin que alguien haga de puente.

Este servidor actúa como:
1. **Directorio de Salas en Vivo (Lobby)**: Registra el nombre de la sala (ej. *"Copa Drifters"*), el creador (ej. *"Miguel"*), cantidad de jugadores (ej. `2/6`) y estado.
2. **Puente de Conexión (Relay)**: Retransmite los datos y controles de los autos entre los jugadores de forma instantánea sin necesidad de abrir puertos ni saber direcciones IP.

---

## 🚀 Opción 1: Probar en tu PC (Rápido)
1. Abre esta carpeta `server/` en tu computadora.
2. Haz doble clic en **`iniciar_servidor.bat`** (o abre una terminal y ejecuta `node server.js`).
3. Verás un mensaje en pantalla indicando que el servidor está corriendo en el puerto `8080`.
4. Abre tu navegador en `http://localhost:8080` para ver el panel de salas en vivo.

---

## 🌐 Opción 2: Subir a Internet 100% GRATIS (Salas Mundiales 24/7)
Para que tus amigos puedan jugar contigo sin que tengas tu computadora encendida, puedes alojar este servidor gratis en **Render.com** (o Railway.app):

1. Sube esta carpeta `server` a un repositorio en **GitHub** (ej. `mi-juego-server`).
2. Entra en **[render.com](https://render.com)** y crea una cuenta gratuita.
3. Haz clic en **New +** ➡️ **Web Service**.
4. Conecta tu repositorio de GitHub.
5. Configura:
   - **Environment**: `Node`
   - **Build Command**: `npm install`
   - **Start Command**: `node server.js`
   - **Plan**: `Free` ($0/mes)
6. Haz clic en **Create Web Service**.
7. Render te dará una URL segura como:
   `https://mi-juego-server-aex7.onrender.com`
8. En el juego, en la pestaña **EN LÍNEA**, introduces esa dirección en formato WebSocket:
   `wss://mi-juego-server-aex7.onrender.com`
¡Listo! Cualquier persona en el mundo podrá ver tus salas y unirse a tus carreras.

---

## 📱 ¿Y si estamos en el mismo lugar o compartimos datos?
Si tú y tus amigos están en la misma red Wi-Fi o compartes el punto de acceso (zona Wi-Fi portátil) de tu celular:
- **No necesitas internet ni este servidor**.
- El juego detecta automáticamente las salas en la pestaña **RED LOCAL (WiFi)** con el nombre de la sala y del creador, permitiendo unirse con un solo toque.
