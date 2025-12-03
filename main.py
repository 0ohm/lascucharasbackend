import os
import random
import math
import io
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# --- SQLALCHEMY IMPORTS ---
from sqlalchemy import create_engine, Column, String, Integer, Float, ForeignKey, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

app = FastAPI()

# --- CONFIGURACIÓN CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =================================================================
# 1. CONFIGURACIÓN DE BASE DE DATOS
# =================================================================

# Obtener URL de Render y corregir el prefijo si es necesario
DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Si no hay variable (local), usa SQLite temporal
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///./local_test.db"
    print("⚠️ USANDO BASE DE DATOS LOCAL (SQLite) - Configura DATABASE_URL para producción")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- DEFINICIÓN DE TABLAS (MODELOS) ---

class BridgeDB(Base):
    __tablename__ = "bridges"
    id = Column(String, primary_key=True, index=True)
    name = Column(String)
    region = Column(String)
    lat = Column(Float)
    lng = Column(Float)
    # Guardamos la imagen en Base64 directamente (simple para prototipo)
    image_data = Column(Text, nullable=True) 
    
    # Relación con sensores
    sensors = relationship("SensorDB", back_populates="bridge", cascade="all, delete-orphan")

class SensorDB(Base):
    __tablename__ = "sensors"
    id = Column(String, primary_key=True, index=True) # node-a1
    bridge_id = Column(String, ForeignKey("bridges.id"))
    alias = Column(String)
    pos_x = Column(Float)
    pos_y = Column(Float)
    odr = Column(Integer, default=125)
    range_g = Column(Integer, default=2)
    
    bridge = relationship("BridgeDB", back_populates="sensors")

# Crear tablas si no existen
Base.metadata.create_all(bind=engine)

# Dependencia para obtener sesión DB
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =================================================================
# 2. MODELOS PYDANTIC (Para recibir datos del Frontend)
# =================================================================

class BridgeInfo(BaseModel):
    name: str
    location: dict # { lat: float, lng: float, region: str }

class SensorConfig(BaseModel):
    odr: int
    range: int
    filter: Optional[str] = "high-pass"

# Modelo para crear/editar PUENTE
class BridgeCreatePayload(BaseModel):
    name: str
    location: dict # { lat, lng, region }
    image_data: Optional[str] = None
    id: Optional[str] = None # Opcional para edición

# Modelo para crear/editar SENSOR
class SensorCreatePayload(BaseModel):
    id: str
    alias: str
    bridge_info: BridgeInfo # Información del puente (para validación)
    x: float
    y: float
    config: SensorConfig
    image_data: Optional[str] = None # Base64 de la imagen

# =================================================================
# 3. ENDPOINTS DE ADMINISTRACIÓN (CRUD REAL)
# =================================================================

# --- PUENTES ---

@app.post("/admin/bridge")
def create_or_update_bridge(payload: BridgeCreatePayload, db: Session = Depends(get_db)):
    """
    Crea o actualiza un puente (sin sensores).
    """
    # Si viene ID, intentamos buscarlo, si no, generamos uno nuevo
    bridge_id = payload.id
    if not bridge_id:
        clean_name = "".join(e for e in payload.name if e.isalnum()).lower()
        bridge_id = f"br-{clean_name[:8]}"
    
    bridge = db.query(BridgeDB).filter(BridgeDB.id == bridge_id).first()
    
    if not bridge:
        # CREAR
        bridge = BridgeDB(
            id=bridge_id,
            name=payload.name,
            region=payload.location['region'],
            lat=payload.location['lat'],
            lng=payload.location['lng'],
            image_data=payload.image_data
        )
        db.add(bridge)
    else:
        # ACTUALIZAR
        bridge.name = payload.name
        bridge.region = payload.location['region']
        bridge.lat = payload.location['lat']
        bridge.lng = payload.location['lng']
        if payload.image_data: # Solo actualizamos foto si viene nueva
            bridge.image_data = payload.image_data
            
    db.commit()
    db.refresh(bridge)
    return {"status": "success", "bridge_id": bridge.id}

@app.delete("/admin/bridge/{bridge_id}")
def delete_bridge(bridge_id: str, db: Session = Depends(get_db)):
    """
    Elimina un puente y todos sus sensores (Cascada).
    """
    bridge = db.query(BridgeDB).filter(BridgeDB.id == bridge_id).first()
    if not bridge:
        raise HTTPException(status_code=404, detail="Puente no encontrado")
    
    db.delete(bridge)
    db.commit()
    return {"status": "deleted", "id": bridge_id}


# --- SENSORES ---

@app.post("/admin/sensor")
def create_or_update_sensor(payload: SensorCreatePayload, db: Session = Depends(get_db)):
    """
    Crea o actualiza un sensor. 
    Si el puente no existe, lo crea automáticamente.
    """
    
    # 1. GESTIÓN DEL PUENTE (Upsert implícito)
    # Buscamos el puente por nombre para ver si ya existe
    # (Esto es útil si creas el sensor primero que el puente)
    # Nota: Idealmente deberíamos pasar el bridge_id explícito desde el front
    bridge_id = f"br-{payload.bridge_info.name.replace(' ', '').lower()[:5]}"
    
    bridge = db.query(BridgeDB).filter(BridgeDB.id == bridge_id).first()
    
    if not bridge:
        bridge = BridgeDB(
            id=bridge_id,
            name=payload.bridge_info.name,
            region=payload.bridge_info.location['region'],
            lat=payload.bridge_info.location['lat'],
            lng=payload.bridge_info.location['lng'],
            image_data=payload.image_data
        )
        db.add(bridge)
        db.commit() # Commit intermedio para tener el ID listo

    # 2. GESTIÓN DEL SENSOR (Upsert)
    sensor = db.query(SensorDB).filter(SensorDB.id == payload.id).first()
    
    if not sensor:
        sensor = SensorDB(id=payload.id)
        db.add(sensor)
    
    # Actualizar datos
    sensor.bridge_id = bridge_id # Vinculamos al puente encontrado/creado
    sensor.alias = payload.alias
    sensor.pos_x = payload.x
    sensor.pos_y = payload.y
    sensor.odr = payload.config.odr
    sensor.range_g = payload.config.range
    
    db.commit()
    return {"status": "success", "sensor_id": sensor.id, "bridge_id": bridge.id}

@app.delete("/admin/sensor/{sensor_id}")
def delete_sensor(sensor_id: str, db: Session = Depends(get_db)):
    """
    Elimina un sensor específico.
    """
    sensor = db.query(SensorDB).filter(SensorDB.id == sensor_id).first()
    if not sensor:
        raise HTTPException(status_code=404, detail="Sensor no encontrado")
    
    db.delete(sensor)
    db.commit()
    return {"status": "deleted"}

# =================================================================
# 4. ENDPOINT RAÍZ (EL HÍBRIDO: REAL CONFIG + FAKE DATA)
# =================================================================
@app.get("/")
def get_dashboard_data(db: Session = Depends(get_db)):
    """
    Lee Puentes y Sensores REALES de la BD.
    Inyecta Telemetría y KPIs FALSOS para que el dashboard funcione.
    """
    bridges_db = db.query(BridgeDB).all()
    
    dashboard_data = []
    now_iso = datetime.now().isoformat()

    for b in bridges_db:
        # 1. Construir estructura base REAL
        bridge_obj = {
            "id": b.id,
            "nombre": b.name,
            "ubicacion": { "region": b.region, "lat": b.lat, "lng": b.lng },
            "status": "ok", # Fake status global
            "lastUpdate": now_iso,
            # Si hay imagen en BD la usamos, si no, placeholder
            "meta": { 
                "tipo": "Hormigón Armado", 
                "largo": "N/A", 
                "imagen": b.image_data if b.image_data else "/puente.png" 
            },
            "kpis": {}, # Se llenará abajo
            "nodes": [] # Se llenará abajo
        }

        # 2. Simular Estado Global del Puente
        # (Para ver variedad, si el nombre tiene 'Bio' lo ponemos en alerta)
        is_alert = "Bio" in b.name
        bridge_obj["status"] = "alert" if is_alert else "ok"

        # 3. Generar KPIs Falsos
        bridge_obj["kpis"] = {
            "structuralHealth": { "id": f"{b.id}-kpi-h", "score": 65 if is_alert else 98, "trend": "stable", "label": "Integridad Estructural", "unit": "%" },
            "accelZ": { "id": f"{b.id}-kpi-z", "val": 0.120 if is_alert else 0.045, "unit": "g", "status": bridge_obj["status"], "label": "Vibración Global (Z)", "trend": "stable" },
            "aiAnalysis": { "id": f"{b.id}-kpi-ai", "type": "text", "status": bridge_obj["status"], "label": "Diagnóstico IA", "text": "Análisis preliminar completado.", "confidence": 95, "lastModelUpdate": now_iso }
        }

        # 4. Procesar Sensores Reales
        for s in b.sensors:
            node_status = "alert" if is_alert and random.random() > 0.5 else "ok"
            
            node_obj = {
                "id": s.id,
                "alias": s.alias,
                "x": s.pos_x,
                "y": s.pos_y,
                "status": node_status,
                "config": { "odr": s.odr, "range": s.range_g },
                # Datos Falsos Vivos
                "health": { "battery": random.randint(80, 100), "signalStrength": random.randint(-80, -50), "boardTemp": 25.0, "lastSeen": now_iso },
                "telemetry": { 
                    "accel_rms": { 
                        "x": round(random.uniform(0, 0.01), 3), 
                        "y": round(random.uniform(0, 0.01), 3), 
                        "z": round(random.uniform(0.04, 0.1) if node_status == 'alert' else 0.045, 3) 
                    }, 
                    "sensorTemp": 22.0 
                },
                "alarms": []
            }
            if node_status == 'alert':
                node_obj["alarms"].append({ "type": "THRESHOLD", "severity": "alert", "msg": "Vibración Excesiva" })
            
            bridge_obj["nodes"].append(node_obj)

        dashboard_data.append(bridge_obj)

    # Si la BD está vacía, devolvemos al menos un mock para no romper el front
    if not dashboard_data:
        return get_mock_fallback()

    return dashboard_data

def get_mock_fallback():
    # Retorna tu JSON mock original si no hay nada en la BD
    return [{
        "id": "mock-01",
        "nombre": "Puente Demo (Base de Datos Vacía)",
        "ubicacion": { "region": "Demo", "lat": -33, "lng": -70 },
        "status": "ok",
        "lastUpdate": datetime.now().isoformat(),
        "meta": { "imagen": "/puente.png" },
        "kpis": { "structuralHealth": { "id": "m-kpi", "score": 100, "label": "Demo", "unit": "%" } },
        "nodes": []
    }]

# =================================================================
# 5. ENDPOINTS DE DATOS SIMULADOS (SUMMARY / CSV)
# =================================================================

@app.get("/summary/{resource_id}")
def get_trend_summary(resource_id: str):
    """
    Genera resumen de tendencia (24h) con patrones realistas.
    """
    data = []
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

    for i in range(24):
        val = base_val
        hour = i
        
        if pattern == 'normal': # Tráfico
            if (7 <= hour <= 9) or (18 <= hour <= 20): val += base_val * 0.5
        elif pattern == 'wind': # Oscilación
            val += math.sin(i / 3.0) * (noise * 3)
        elif pattern == 'damage': # Picos
            if random.random() > 0.8: val += base_val * 0.8

        val += random.uniform(-noise, noise)
        data.append({ "t": f"{hour:02d}:00", "v": round(val, 4) })
        
    return data

@app.get("/export/csv")
def export_csv(id: str, start: str, end: str, type: str = Query("sensor")):
    """
    Genera un CSV masivo (200Hz) respetando fechas y horas.
    """
    try:
        # Asegurar formato ISO completo si viene cortado del frontend
        if "T" not in start: start += "T00:00:00"
        if "T" not in end: end += "T23:59:59"
        
        # Intentar parsear con o sin segundos
        try:
            start_dt = datetime.fromisoformat(start)
        except ValueError:
            start_dt = datetime.strptime(start, "%Y-%m-%dT%H:%M")
            
        try:
            end_dt = datetime.fromisoformat(end)
        except ValueError:
            end_dt = datetime.strptime(end, "%Y-%m-%dT%H:%M")
        
        # Límite de seguridad (1 hora)
        if (end_dt - start_dt).total_seconds() > 3600:
            end_dt = start_dt + timedelta(hours=1)
            
    except Exception as e:
        return {"error": f"Formato de fecha inválido: {e}"}

    HZ = 200
    total_seconds = int((end_dt - start_dt).total_seconds())
    if total_seconds <= 0: total_seconds = 60 # Minimo 1 min
    
    total_points = total_seconds * HZ
    
    def iter_csv():
        if type == "sensor":
            yield "Timestamp,Accel_X(g),Accel_Y(g),Accel_Z(g),Battery(%),RSSI(dBm)\n"
        else:
            yield "Timestamp,Value,Status,Confidence(%)\n"

        current_time = start_dt
        time_step = timedelta(seconds=1.0/HZ)
        batt = 98.5
        rssi = -65.0
        base_z = 1.0
        noise = 0.02
        
        if "b2" in id or "node-b1" in id: noise = 0.08 

        for i in range(total_points):
            ts_str = current_time.isoformat()
            
            if type == "sensor":
                x = random.uniform(-noise, noise)
                y = random.uniform(-noise, noise)
                z = base_z + random.uniform(-noise, noise)
                
                bat_str = ""
                rssi_str = ""
                if i % 200 == 0:
                    bat_str = f"{batt:.1f}"
                    rssi_str = f"{rssi:.0f}"
                    if random.random() > 0.99: batt -= 0.1
                
                yield f"{ts_str},{x:.4f},{y:.4f},{z:.4f},{bat_str},{rssi_str}\n"
            else:
                val = 0.5 + random.uniform(-0.1, 0.1)
                yield f"{ts_str},{val:.3f},ok,98\n"
            
            current_time += time_step

    filename = f"export_{id}_{start_dt.strftime('%Y%m%d%H%M')}.csv"
    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)