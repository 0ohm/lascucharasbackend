from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import io
import random
import math
from datetime import datetime, timedelta
import pandas as pd

app = FastAPI()

# --- CONFIGURACIÓN CORS (CRÍTICO PARA QUE TU FRONTEND SE CONECTE) ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, restringe esto a tu dominio de Vercel/Netlify
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =================================================================
# 1. ENDPOINT RAÍZ: LISTA DE PUENTES (Dashboard Principal)
# =================================================================
@app.get("/")
def get_bridges():
    """
    Devuelve la configuración inicial y estado de los puentes.
    Simula la respuesta de la base de datos de configuración.
    """
    # Fecha actual simulada
    now_iso = datetime.now().isoformat()

    return [
        {
            "id": "br-001",
            "nombre": "Puente 1 — Las Cucharas",
            "ubicacion": { "region": "Valparaíso", "lat": -33.036, "lng": -71.522 },
            "status": "ok",
            "lastUpdate": now_iso,
            "meta": { "tipo": "Arco de Hormigón", "largo": "180m", "imagen": "/puente.png" },
            "kpis": {
                "structuralHealth": { "id": "b1-kpi-health", "score": 98, "trend": "stable", "label": "Integridad Estructural", "unit": "%" },
                "accelX": { "id": "b1-kpi-acc-x", "val": 0.004, "unit": "g", "status": "ok", "label": "Vibración Global (X)", "trend": "flat" },
                "accelY": { "id": "b1-kpi-acc-y", "val": 0.008, "unit": "g", "status": "ok", "label": "Vibración Global (Y)", "trend": "flat" },
                "accelZ": { "id": "b1-kpi-acc-z", "val": 0.045, "unit": "g", "status": "ok", "label": "Vibración Global (Z)", "trend": "stable" },
                "naturalFreq": { "id": "b1-kpi-freq", "val": 3.42, "unit": "Hz", "status": "ok", "label": "Modo Fundamental" },
                "aiAnalysis": { "id": "b1-kpi-ai", "type": "text", "status": "ok", "label": "Diagnóstico IA", "text": "Comportamiento nominal. Las firmas espectrales coinciden con el modelo base.", "confidence": 96, "lastModelUpdate": now_iso }
            },
            "nodes": [
                {
                    "id": "node-a1", "alias": "Pilar Central - Base", "x": 50, "y": 80, "status": "ok",
                    "config": { "odr": 125, "range": 2, "filter": "high-pass" },
                    "health": { "battery": 88, "signalStrength": -65, "boardTemp": 34.2, "lastSeen": now_iso },
                    "telemetry": { "accel_rms": { "x": 0.002, "y": 0.003, "z": 0.045 }, "sensorTemp": 22.5 },
                    "alarms": []
                },
                {
                    "id": "node-a2", "alias": "Tablero - Tramo Norte", "x": 20, "y": 25, "status": "warn",
                    "config": { "odr": 125, "range": 2 },
                    "health": { "battery": 15, "signalStrength": -98, "boardTemp": 32.1, "lastSeen": now_iso },
                    "telemetry": { "accel_rms": { "x": 0.005, "y": 0.005, "z": 0.060 }, "sensorTemp": 23.0 },
                    "alarms": [{ "type": "BATTERY_LOW", "severity": "warn", "msg": "Batería < 20%" }]
                }
            ]
        },
        {
            "id": "br-002",
            "nombre": "Puente 2 — BioBío",
            "ubicacion": { "region": "Biobío", "lat": -36.820, "lng": -73.050 },
            "status": "alert",
            "lastUpdate": now_iso,
            "meta": { "tipo": "Vigas de Acero", "largo": "2200m", "imagen": "/bridges/p2.jpg" },
            "kpis": {
                "structuralHealth": { "id": "b2-kpi-health", "score": 65, "trend": "down", "label": "Integridad Estructural", "unit": "%" },
                "accelX": { "id": "b2-kpi-acc-x", "val": 0.015, "unit": "g", "status": "warn", "label": "Vibración Global (X)", "trend": "up" },
                "accelY": { "id": "b2-kpi-acc-y", "val": 0.020, "unit": "g", "status": "warn", "label": "Vibración Global (Y)", "trend": "flat" },
                "accelZ": { "id": "b2-kpi-acc-z", "val": 0.120, "unit": "g", "status": "alert", "label": "Vibración Global (Z)", "trend": "up" },
                "naturalFreq": { "id": "b2-kpi-freq", "val": 2.10, "unit": "Hz", "status": "warn", "label": "Modo Fundamental" },
                "aiAnalysis": { "id": "b2-kpi-ai", "type": "text", "status": "alert", "label": "Diagnóstico IA", "text": "¡Atención! Impactos de alta energía detectados en juntas de dilatación.", "confidence": 89, "lastModelUpdate": now_iso }
            },
            "nodes": [
                {
                    "id": "node-b1", "alias": "Junta de Dilatación 4", "x": 60, "y": 10, "status": "alert",
                    "config": { "odr": 500, "range": 4 },
                    "health": { "battery": 98, "signalStrength": -55, "boardTemp": 28.5, "lastSeen": now_iso },
                    "telemetry": { "accel_rms": { "x": 0.040, "y": 0.010, "z": 0.120 }, "sensorTemp": 19.4 },
                    "alarms": [{ "type": "SHOCK_DETECTED", "severity": "alert", "msg": "Impacto > 0.8g eje Z" }]
                }
            ]
        },
        {
            "id": "br-003",
            "nombre": "Puente 3 — Canal de Chacao",
            "ubicacion": { "region": "Los Lagos", "lat": -41.793, "lng": -73.526 },
            "status": "warn",
            "lastUpdate": now_iso,
            "meta": { "tipo": "Colgante Multivano", "largo": "2750m", "imagen": "/bridges/chacao_render.jpg" },
            "kpis": {
                "structuralHealth": { "id": "b3-kpi-health", "score": 88, "trend": "stable", "label": "Integridad Estructural", "unit": "%" },
                "accelX": { "id": "b3-kpi-acc-x", "val": 0.010, "unit": "g", "status": "ok", "label": "Vibración Global (X)", "trend": "flat" },
                "accelY": { "id": "b3-kpi-acc-y", "val": 0.075, "unit": "g", "status": "warn", "label": "Vibración Global (Y)", "trend": "up" },
                "accelZ": { "id": "b3-kpi-acc-z", "val": 0.030, "unit": "g", "status": "ok", "label": "Vibración Global (Z)", "trend": "stable" },
                "naturalFreq": { "id": "b3-kpi-freq", "val": 0.15, "unit": "Hz", "status": "ok", "label": "Modo Fundamental" },
                "aiAnalysis": { "id": "b3-kpi-ai", "type": "text", "status": "warn", "label": "Diagnóstico IA", "text": "Oscilaciones laterales moderadas por ráfagas de viento > 60km/h.", "confidence": 92, "lastModelUpdate": now_iso }
            },
            "nodes": [
                {
                    "id": "node-c1", "alias": "Pilono Central - Cima", "x": 45, "y": 15, "status": "warn",
                    "config": { "odr": 100, "range": 2 },
                    "health": { "battery": 92, "signalStrength": -70, "boardTemp": 18.5, "lastSeen": now_iso },
                    "telemetry": { "accel_rms": { "x": 0.015, "y": 0.035, "z": 0.010 }, "sensorTemp": 12.0 },
                    "alarms": [{ "type": "WIND_VIBRATION", "severity": "warn", "msg": "Vibración lateral alta (Viento)" }]
                }
            ]
        }
    ]

# =================================================================
# 2. ENDPOINT: RESUMEN DE TENDENCIA (Para el gráfico pequeño)
# =================================================================
@app.get("/summary/{resource_id}")
def get_trend_summary(resource_id: str):
    """
    Devuelve 144 puntos (1 dato cada 10 min) para el gráfico de detalle.
    Simula perfiles de carga según el ID del recurso.
    """
    data = []
    
    # Perfiles de simulación
    base_val = 0.045
    noise = 0.005
    pattern = 'normal'

    if "node-b1" in resource_id or "b2-" in resource_id:
        base_val = 0.12
        pattern = 'damage'
    elif "node-c1" in resource_id or "b3-" in resource_id:
        base_val = 0.075
        pattern = 'wind'
    elif "ai" in resource_id:
        base_val = 96
        noise = 2
        pattern = 'stable'

    # Generar 24 puntos (uno por hora para simplificar el mock, el front espera {t, v})
    for i in range(24):
        val = base_val
        hour = i
        
        # Lógica de simulación
        if pattern == 'normal': # Tráfico en hora punta
            if (7 <= hour <= 9) or (18 <= hour <= 20):
                val += base_val * 0.5
        elif pattern == 'wind': # Oscilación
            val += math.sin(i / 3.0) * (noise * 3)
        elif pattern == 'damage': # Picos aleatorios
            if random.random() > 0.8:
                val += base_val * 0.8

        # Añadir ruido aleatorio
        val += random.uniform(-noise, noise)
        
        # Formato de hora "HH:00"
        time_str = f"{hour:02d}:00"
        
        data.append({
            "t": time_str,
            "v": round(val, 4)
        })
        
    return data

# =================================================================
# 3. ENDPOINT: DESCARGA CSV (Raw Data Masiva)
# =================================================================
@app.get("/export/csv")
def export_csv(
    id: str, 
    start: str, 
    end: str, 
    type: str = Query("sensor", enum=["sensor", "kpi"])
):
    """
    Genera un CSV masivo simulando 200Hz.
    Columnas: Timestamp, Accel_X(g), Accel_Y(g), Accel_Z(g), Battery(%), RSSI(dBm)
    """
    
    # 1. Parsear fechas (Manejo de errores básico)
    try:
        # Asegurar formato ISO
        if "T" not in start: start += "T00:00:00"
        if "T" not in end: end += "T23:59:59"
        
        start_dt = datetime.fromisoformat(start)
        end_dt = datetime.fromisoformat(end)
        
        # Limitar a 1 hora máximo de simulación para no reventar la RAM del servidor
        if (end_dt - start_dt).total_seconds() > 3600:
            end_dt = start_dt + timedelta(hours=1)
            
    except ValueError:
        return {"error": "Formato de fecha inválido"}

    # 2. Configuración de Simulación
    HZ = 200
    total_seconds = int((end_dt - start_dt).total_seconds())
    total_points = total_seconds * HZ
    
    # 3. Generación Eficiente (Usando generadores para no llenar RAM)
    def iter_csv():
        # Cabecera
        if type == "sensor":
            yield "Timestamp,Accel_X(g),Accel_Y(g),Accel_Z(g),Battery(%),RSSI(dBm)\n"
        else:
            yield "Timestamp,Value,Status,Confidence(%)\n"

        # Estado inicial
        current_time = start_dt
        time_step = timedelta(seconds=1.0/HZ)
        
        batt = 98.5
        rssi = -65.0
        
        base_z = 1.0
        noise = 0.02
        
        # Ajustar patrón según ID
        if "b2" in id or "node-b1" in id: noise = 0.08 # Dañado
        
        # Bucle de datos
        for i in range(total_points):
            ts_str = current_time.isoformat()
            
            if type == "sensor":
                # Física
                x = random.uniform(-noise, noise)
                y = random.uniform(-noise, noise)
                z = base_z + random.uniform(-noise, noise)
                
                # Datos de lote (solo cada 1 segundo / 200 muestras)
                bat_str = ""
                rssi_str = ""
                if i % 200 == 0:
                    bat_str = f"{batt:.1f}"
                    rssi_str = f"{rssi:.0f}"
                    # Degradar batería lentamente
                    if random.random() > 0.99: batt -= 0.1
                
                yield f"{ts_str},{x:.4f},{y:.4f},{z:.4f},{bat_str},{rssi_str}\n"
            else:
                # KPI Simulado
                val = 0.5 + random.uniform(-0.1, 0.1)
                yield f"{ts_str},{val:.3f},ok,98\n"
            
            current_time += time_step

    # 4. Respuesta Streaming (Descarga directa)
    filename = f"export_{id}_{start_dt.strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

if __name__ == "__main__":
    import uvicorn
    import os
    
    # Render asigna el puerto en la variable de entorno 'PORT'
    # Si no existe (en tu PC), usa el 8000
    port = int(os.environ.get("PORT", 8000))
    
    # Escucha en 0.0.0.0 para ser visible externamente
    uvicorn.run(app, host="0.0.0.0", port=port)