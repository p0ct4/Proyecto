import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import List, Dict, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from websocket_manager import manager
from mqtt_client import MQTTClient

# ============================================================
# BASE DE DATOS EN MEMORIA (simulación, sin PostgreSQL/SQLite)
# ============================================================
lecturas_memoria: List[Dict[str, Any]] = []
CONTADOR_ID = 0
_loop_para_mqtt = None  # ← Aquí guardaremos el loop principal de asyncio

def guardar_lectura(payload: dict):
    global CONTADOR_ID
    CONTADOR_ID += 1
    lectura = {
        "id": CONTADOR_ID,
        "temperatura": float(payload.get("temperatura", 0)),
        "humedad": float(payload.get("humedad", 0)),
        "device_id": str(payload.get("device_id", "esp32_01")),
        "timestamp": datetime.now().isoformat()
    }
    lecturas_memoria.append(lectura)
    if len(lecturas_memoria) > 500:
        lecturas_memoria.pop(0)
    return lectura

# ============================================================
# PROCESADOR MQTT (usa el loop principal guardado)
# ============================================================
def process_mqtt_message(payload: dict):
    try:
        lectura = guardar_lectura(payload)
        print(f"📥 Recibido MQTT: {lectura}")

        msg = {"type": "lectura", "data": lectura}

        # Usamos el loop principal que guardamos en lifespan
        if _loop_para_mqtt is not None:
            asyncio.run_coroutine_threadsafe(manager.broadcast(msg), _loop_para_mqtt)
        else:
            print("⚠️ Loop aún no inicializado, mensaje descartado para WS")

    except Exception as e:
        print(f"❌ Error procesando MQTT: {e}")

# ============================================================
# CLIENTE MQTT (ya existe process_mqtt_message arriba)
# ============================================================
mqtt_client = MQTTClient(message_handler=process_mqtt_message)

# ============================================================
# LIFESPAN: guardamos el loop de uvicorn aquí
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _loop_para_mqtt
    _loop_para_mqtt = asyncio.get_running_loop()  # ← Guarda el loop principal
    print("🚀 Iniciando OVOTECH Backend...")
    mqtt_client.start()
    yield
    print("🛑 Apagando OVOTECH Backend...")
    mqtt_client.stop()

# ============================================================
# FASTAPI APP
# ============================================================
app = FastAPI(title="OVOTECH API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def root():
    return {
        "message": "OVOTECH API",
        "modo": "SIMULACION SIN BASE DE DATOS",
        "lecturas_en_memoria": len(lecturas_memoria),
        "endpoints": {
            "lecturas": "/api/lecturas?limit=50",
            "ultima": "/api/lecturas/ultima",
            "websocket": "/ws"
        }
    }

@app.get("/api/lecturas")
async def get_lecturas(limit: int = 50):
    datos = lecturas_memoria[-limit:] if lecturas_memoria else []
    return {"data": datos, "count": len(datos)}

@app.get("/api/lecturas/ultima")
async def get_ultima_lectura():
    if not lecturas_memoria:
        raise HTTPException(status_code=404, detail="No hay lecturas")
    return lecturas_memoria[-1]

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print(f"🌐 WS Cliente conectado. Total: {len(manager.active_connections)}")
    
    # Enviar datos históricos al conectar (para que el gráfico no esté vacío)
    if lecturas_memoria:
        try:
            await websocket.send_json({
                "type": "historico",
                "data": lecturas_memoria[-50:]
            })
        except Exception as e:
            print(f"⚠️ Error enviando histórico: {e}")
    
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text("pong")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print(f"🌐 Cliente desconectado. Total: {len(manager.active_connections)}")
    except Exception as e:
        print(f"⚠️ WS Error: {e}")
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)