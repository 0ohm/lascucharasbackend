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
from sqlalchemy import create_engine, Column, String, Integer, Float, ForeignKey, Text, DateTime, func, desc
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship

app = FastAPI()

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

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

if not DATABASE_URL:
    # Fallback local solo si no hay variable
    DATABASE_URL = "sqlite:///./local_test.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- MODELOS DB ---

class BridgeDB(Base):
    __tablename__ = "bridges"
    id = Column(String, primary_key=True, index=True)
    name = Column(String)
    region = Column(String)
    lat = Column(Float)
    lng = Column(Float)
    image_data = Column(Text, nullable=True) 
    # Cascada vital para borrar todo de una vez
    sensors = relationship("SensorDB", back_populates="bridge", cascade="all, delete-orphan")

class SensorDB(Base):
    __tablename__ = "sensors"
    id = Column(String, primary_key=True, index=True)
    # Cascada en DB (ON DELETE CASCADE) es mejor gestionada por SQLAlchemy con cascade="..." arriba
    bridge_id = Column(String, ForeignKey("bridges.id")) 
    alias = Column(String)
    pos_x = Column(Float)
    pos_y = Column(Float)
    odr = Column(Integer, default=125)
    range_g = Column(Integer, default=2)
    
    # Campos de estado (Actualizados por DataFlow)
    health_battery = Column(Float, nullable=True)
    health_rssi = Column(Float, nullable=True)
    last_seen = Column(DateTime, nullable=True)
    status = Column(String, default="ok")

    bridge = relationship("BridgeDB", back_populates="sensors")
    # Relación inversa para borrar mediciones si se borra el sensor
    # (Esto permite borrar sensor desde Python y que SQLAlchemy limpie las mediciones)
    measurements = relationship("MeasurementDB", back_populates="sensor", cascade="all, delete-orphan")

class MeasurementDB(Base):
    __tablename__ = "measurements"
    ts = Column(DateTime, primary_key=True) 
    sensor_id = Column(String, ForeignKey("sensors.id"), primary_key=True)
    acc_x = Column(Float)
    acc_y = Column(Float)
    acc_z = Column(Float)
    temp = Column(Float)
    battery = Column(Float, nullable=True)
    rssi = Column(Float, nullable=True)
    
    sensor = relationship("SensorDB", back_populates="measurements")

# Inicializar tablas
Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =================================================================
# 2. MODELOS PYDANTIC
# =================================================================

class BridgeInfo(BaseModel):
    name: str
    location: dict 

class SensorConfig(BaseModel):
    odr: int
    range: int
    filter: Optional[str] = "high-pass"

class BridgeCreatePayload(BaseModel):
    name: str
    location: dict
    image_data: Optional[str] = None
    id: Optional[str] = None 

class SensorCreatePayload(BaseModel):
    id: str
    alias: str
    bridge_info: BridgeInfo 
    bridge_id: Optional[str] = None 
    x: float
    y: float
    config: SensorConfig
    image_data: Optional[str] = None 

def generate_bridge_id(name: str):
    clean_name = "".join(e for e in name if e.isalnum()).lower()
    return f"br-{clean_name[:8]}"

# =================================================================
# 3. ENDPOINTS DE ADMINISTRACIÓN (CRUD)
# =================================================================

@app.post("/admin/bridge")
def create_or_update_bridge(payload: BridgeCreatePayload, db: Session = Depends(get_db)):
    bridge_id = payload.id if payload.id else generate_bridge_id(payload.name)
    bridge = db.query(BridgeDB).filter(BridgeDB.id == bridge_id).first()
    
    if not bridge:
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
        bridge.name = payload.name
        bridge.region = payload.location['region']
        bridge.lat = payload.location['lat']
        bridge.lng = payload.location['lng']
        if payload.image_data:
            bridge.image_data = payload.image_data
            
    db.commit()
    db.refresh(bridge)
    return {"status": "success", "bridge_id": bridge.id}

@app.post("/admin/sensor")
def create_or_update_sensor(payload: SensorCreatePayload, db: Session = Depends(get_db)):
    # Lógica de ID de puente (Prioridad al ID explícito)
    if payload.bridge_id:
        target_bridge_id = payload.bridge_id
    else:
        target_bridge_id = generate_bridge_id(payload.bridge_info.name)
    
    # Asegurar que el puente existe
    bridge = db.query(BridgeDB).filter(BridgeDB.id == target_bridge_id).first()
    
    if not bridge:
        bridge = BridgeDB(
            id=target_bridge_id,
            name=payload.bridge_info.name,
            region=payload.bridge_info.location['region'],
            lat=payload.bridge_info.location['lat'],
            lng=payload.bridge_info.location['lng'],
            image_data=payload.image_data
        )
        db.add(bridge)
        db.commit() 

    # Crear/Actualizar Sensor
    sensor = db.query(SensorDB).filter(SensorDB.id == payload.id).first()
    
    if not sensor:
        sensor = SensorDB(id=payload.id)
        db.add(sensor)
    
    sensor.bridge_id = target_bridge_id 
    sensor.alias = payload.alias
    sensor.pos_x = payload.x
    sensor.pos_y = payload.y
    sensor.odr = payload.config.odr
    sensor.range_g = payload.config.range
    
    db.commit()
    return {"status": "success", "sensor_id": sensor.id, "bridge_id": target_bridge_id}

@app.delete("/admin/bridge/{bridge_id}")
def delete_bridge(bridge_id: str, db: Session = Depends(get_db)):
    bridge = db.query(BridgeDB).filter(BridgeDB.id == bridge_id).first()
    if not bridge:
        raise HTTPException(status_code=404, detail="Puente no encontrado")
    
    # SQLAlchemy borrará sensores y mediciones gracias al 'cascade'
    db.delete(bridge)
    db.commit()
    return {"status": "deleted", "id": bridge_id}

@app.delete("/admin/sensor/{sensor_id}")
def delete_sensor(sensor_id: str, db: Session = Depends(get_db)):
    sensor = db.query(SensorDB).filter(SensorDB.id == sensor_id).first()
    if not sensor:
        raise HTTPException(status_code=404, detail="Sensor no encontrado")
    
    # SQLAlchemy borrará mediciones asociadas gracias al 'cascade' en SensorDB.measurements
    db.delete(sensor)
    db.commit()
    return {"status": "deleted"}

# =================================================================
# 4. ENDPOINT RAÍZ (DATOS REALES)
# =================================================================
@app.get("/")
def get_dashboard_data(db: Session = Depends(get_db)):
    bridges_db = db.query(BridgeDB).all()
    dashboard_data = []
    
    if not bridges_db:
        return []

    for b in bridges_db:
        bridge_obj = {
            "id": b.id,
            "nombre": b.name,
            "ubicacion": { "region": b.region, "lat": b.lat, "lng": b.lng },
            "status": "ok", 
            "lastUpdate": None, 
            "meta": { 
                "tipo": "Estructura Monitorizada", 
                "largo": "N/A", 
                "imagen": b.image_data if b.image_data else "/puente.png" 
            },
            "kpis": {}, 
            "nodes": []
        }

        last_update_global = None
        bridge_status = "ok"

        for s in b.sensors:
            # Buscar último dato real (usando 'ts')
            last_meas = db.query(MeasurementDB).filter(
                MeasurementDB.sensor_id == s.id
            ).order_by(desc(MeasurementDB.ts)).first()

            node_obj = {
                "id": s.id,
                "alias": s.alias,
                "x": s.pos_x,
                "y": s.pos_y,
                "status": s.status if s.status else "ok", 
                "config": { "odr": s.odr, "range": s.range_g },
                # Datos de Salud (Vienen de la tabla sensors, actualizados por DataFlow)
                "health": { 
                    "battery": s.health_battery if s.health_battery is not None else 0, 
                    "signalStrength": s.health_rssi if s.health_rssi is not None else 0, 
                    "boardTemp": 0, 
                    "lastSeen": s.last_seen.isoformat() if s.last_seen else None 
                },
                # Telemetría: Si hay dato, lo mostramos. Si no, ceros.
                "telemetry": { "accel_rms": { "x": 0, "y": 0, "z": 0 }, "sensorTemp": 0 },
                "alarms": []
            }

            if last_meas:
                node_obj["telemetry"] = {
                    "accel_rms": { 
                        "x": last_meas.acc_x, 
                        "y": last_meas.acc_y, 
                        "z": last_meas.acc_z 
                    },
                    "sensorTemp": last_meas.temp
                }
                
                if not last_update_global or last_meas.ts > last_update_global:
                    last_update_global = last_meas.ts

            # Lógica básica de estado (puedes mejorarla con la tabla events después)
            if node_obj["status"] == "alert":
                bridge_status = "alert"
                node_obj["alarms"].append({ "type": "THRESHOLD", "severity": "alert", "msg": "Alerta detectada" })

            bridge_obj["nodes"].append(node_obj)

        # Actualizar estado global y timestamp del puente
        bridge_obj["status"] = bridge_status
        if last_update_global:
            bridge_obj["lastUpdate"] = last_update_global.isoformat()

        # KPIs Globales (Calculados simples por ahora)
        bridge_obj["kpis"] = {
            "structuralHealth": { "id": f"{b.id}-health", "score": 100 if bridge_status == 'ok' else 60, "trend": "stable", "label": "Integridad", "unit": "%" },
            "accelZ": { "id": f"{b.id}-accel", "val": 0.000, "unit": "g", "status": bridge_status, "label": "Vibración (Z)", "trend": "flat" },
             "aiAnalysis": { "id": f"{b.id}-ai", "type": "text", "status": bridge_status, "label": "IA", "text": "Monitorizando...", "confidence": 0 }
        }
        
        dashboard_data.append(bridge_obj)

    return dashboard_data

# =================================================================
# 5. ENDPOINTS DE DATOS (REALES)
# =================================================================

@app.get("/summary/{resource_id}")
def get_trend_summary(resource_id: str, db: Session = Depends(get_db)):
    # Consulta Real: Últimos 144 puntos ordenados por tiempo
    measurements = db.query(MeasurementDB).filter(
        MeasurementDB.sensor_id == resource_id
    ).order_by(desc(MeasurementDB.ts)).limit(144).all()
    
    if not measurements:
        return [] 

    # Convertir a formato gráfico {t, v}
    data = []
    for m in reversed(measurements): # Revertir para que quede cronológico (Izq a Der)
        data.append({
            "t": m.ts.strftime("%H:%M"),
            "v": m.acc_z 
        })
        
    return data

@app.get("/export/csv")
def export_csv(id: str, start: str, end: str, type: str = Query("sensor"), db: Session = Depends(get_db)):
    try:
        # Parseo robusto de fechas (acepta con o sin T)
        start_dt = datetime.fromisoformat(start.replace("T", " "))
        end_dt = datetime.fromisoformat(end.replace("T", " "))
    except:
        start_dt = datetime.now() - timedelta(hours=1)
        end_dt = datetime.now()

    query = db.query(MeasurementDB).filter(
        MeasurementDB.sensor_id == id,
        MeasurementDB.ts >= start_dt,
        MeasurementDB.ts <= end_dt
    ).order_by(MeasurementDB.ts)
    
    def iter_csv():
        yield "Timestamp,Accel_X(g),Accel_Y(g),Accel_Z(g),Battery(%),RSSI(dBm)\n"
        # Yield per 1000 para no saturar memoria
        for row in query.yield_per(1000): 
            ts = row.ts.isoformat()
            bat = row.battery if row.battery is not None else ""
            rssi = row.rssi if row.rssi is not None else ""
            yield f"{ts},{row.acc_x},{row.acc_y},{row.acc_z},{bat},{rssi}\n"

    filename = f"export_{id}.csv"
    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)