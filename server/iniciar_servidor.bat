@echo off
title Servidor Relay - Juego de Carreras
echo =======================================================
echo    INICIANDO SERVIDOR RELAY Y SALAS (GODOT 4)
echo =======================================================
echo.
cd /d "%~dp0"
if not exist "node_modules\ws" (
    echo [1/2] Instalando dependencia de red (ws)...
    call npm install ws --no-audit --no-fund
    echo.
)
echo [2/2] Iniciando servidor de salas en el puerto 8080...
node server.js
pause
