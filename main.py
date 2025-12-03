import os
import random
import math
import io
from datetime import datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
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
if not DATABASE_URL:
    raise RuntimeError("CRITICAL: DATABASE_URL no configurada. Este servicio requiere PostgreSQL.")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# =================================================================
# 2. MODELOS DE BASE DE DATOS (ESQUEMA COMPLETO)
# =================================================================

class BridgeDB(Base):
    __tablename__ = "bridges"
    id = Column(String, primary_key=True, index=True)
    name = Column(String)
    region = Column(String)
    lat = Column(Float)
    lng = Column(Float)
    image_data = Column(Text, nullable=True)
    admin_status = Column(String, default="active") 
    sensors = relationship("SensorDB", back_populates="bridge", cascade="all, delete-orphan")
    kpis = relationship("KpiDB", back_populates="bridge", cascade="all, delete-orphan")

class SensorDB(Base):
    __tablename__ = "sensors"
    id = Column(String, primary_key=True, index=True)
    bridge_id = Column(String, ForeignKey("bridges.id", ondelete="CASCADE"))
    alias = Column(String)
    pos_x = Column(Float)
    pos_y = Column(Float)
    odr = Column(Integer, default=125)
    range_g = Column(Integer, default=2)
    filter_type = Column(String, default="high-pass") 
    health_battery = Column(Float, nullable=True)
    health_rssi = Column(Float, nullable=True)
    last_seen = Column(DateTime, nullable=True)
    status = Column(String, default="ok") 
    bridge = relationship("BridgeDB", back_populates="sensors")
    measurements = relationship("MeasurementDB", back_populates="sensor", cascade="all, delete-orphan")
    events = relationship("EventDB", back_populates="sensor", cascade="all, delete-orphan") 

class MeasurementDB(Base):
    __tablename__ = "measurements"
    ts = Column(DateTime, primary_key=True) 
    sensor_id = Column(String, ForeignKey("sensors.id", ondelete="CASCADE"), primary_key=True)
    acc_x = Column(Float)
    acc_y = Column(Float)
    acc_z = Column(Float)
    temp = Column(Float)
    battery = Column(Float, nullable=True)
    rssi = Column(Float, nullable=True)
    sensor = relationship("SensorDB", back_populates="measurements")

class KpiDB(Base):
    __tablename__ = "kpis"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=func.now())
    bridge_id = Column(String, ForeignKey("bridges.id", ondelete="CASCADE"))
    kpi_type = Column(String, nullable=False) 
    value = Column(Float, nullable=True)
    text_value = Column(Text, nullable=True)
    status = Column(String, default="ok") 
    confidence = Column(Float, nullable=True)
    bridge = relationship("BridgeDB", back_populates="kpis")

class EventDB(Base):
    __tablename__ = "events"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime)
    sensor_id = Column(String, ForeignKey("sensors.id", ondelete="CASCADE"))
    type = Column(String) 
    message = Column(String)
    status = Column(String, default="active")
    sensor = relationship("SensorDB", back_populates="events")

try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    print(f"Nota DB: {e}")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# =================================================================
# 3. MODELOS PYDANTIC (PAYLOADS)
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
# 4. ENDPOINTS DE ADMINISTRACIÓN (CRUD)
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
    if payload.bridge_id:
        target_bridge_id = payload.bridge_id
    else:
        target_bridge_id = generate_bridge_id(payload.bridge_info.name)
    
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
    
    db.delete(bridge) # Cascada elimina sensores y KPIs
    db.commit()
    return {"status": "deleted", "id": bridge_id}

@app.delete("/admin/sensor/{sensor_id}")
def delete_sensor(sensor_id: str, db: Session = Depends(get_db)):
    sensor = db.query(SensorDB).filter(SensorDB.id == sensor_id).first()
    if not sensor:
        raise HTTPException(status_code=404, detail="Sensor no encontrado")
    
    db.delete(sensor) # Cascada elimina mediciones y eventos
    db.commit()
    return {"status": "deleted"}

# =================================================================
# 5. ENDPOINT RAÍZ (TRANSFORMACIÓN A JSON ESTRUCTURADO)
# =================================================================

@app.get("/")
def get_dashboard_data(db: Session = Depends(get_db)):
    bridges_db = db.query(BridgeDB).all()
    
    if not bridges_db:
        return []

    dashboard_data = []

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
            "kpis": {
                "structuralHealth": { "id": f"{b.id}-kpi-h", "score": 100, "trend": "stable", "label": "Integridad Estructural", "unit": "%", "status": "ok" },
                "accelZ": { "id": f"{b.id}-kpi-z", "val": 0.000, "unit": "g", "status": "ok", "label": "Vibración Global (Z)", "trend": "flat" },
                "accelGlob": { "id": f"{b.id}-kpi-g", "val": 0.000, "unit": "g", "status": "ok", "label": "Vibración Global", "trend": "flat" },
                "aiAnalysis": { "id": f"{b.id}-kpi-ai", "type": "text", "status": "ok", "label": "Diagnóstico IA", "text": "Esperando datos suficientes...", "confidence": 0, "lastModelUpdate": None }
            },
            "nodes": []
        }

        # 2. Recuperar KPIs Reales y Datos de Telemetría
        last_update_global = None
        bridge_status = "ok"

        # Cargar KPIs de la BD para este puente
        kpis_db = db.query(KpiDB).filter(KpiDB.bridge_id == b.id).all()
        kpis_map = {k.kpi_type: k for k in kpis_db}
        
        # Integrar KPIs reales en el objeto JSON
        for k_type, k_default in bridge_obj["kpis"].items():
            if k_type in kpis_map:
                k_db = kpis_map[k_type]
                bridge_obj["kpis"][k_type] = {
                    "id": k_default["id"],
                    "status": k_db.status,
                    "label": k_default["label"], 
                    "lastModelUpdate": k_db.timestamp.isoformat() if k_db.timestamp else None,
                    "val": k_db.value if k_db.value is not None else 0.0,
                    "score": k_db.value if k_db.value is not None else 0.0,
                    "trend": "stable", 
                    "unit": k_default.get("unit"),
                    # Propiedades IA
                    "type": k_db.text_value and "text",
                    "text": k_db.text_value,
                    "confidence": k_db.confidence
                }


        # Procesar Sensores y Telemetría
        for s in b.sensors:
            last_meas = db.query(MeasurementDB).filter(
                MeasurementDB.sensor_id == s.id
            ).order_by(desc(MeasurementDB.ts)).first()

            node_obj = {
                "id": s.id,
                "alias": s.alias,
                "x": s.pos_x,
                "y": s.pos_y,
                # Estado y Configuración desde la tabla sensors
                "status": s.status if s.status else "ok", 
                "config": { "odr": s.odr, "range": s.range_g },
                "health": { 
                    "battery": s.health_battery if s.health_battery is not None else 0, 
                    "signalStrength": s.health_rssi if s.health_rssi is not None else 0, 
                    "boardTemp": 0, 
                    "lastSeen": s.last_seen.isoformat() if s.last_seen else None 
                },
                "telemetry": { 
                    "accel_rms": { "x": 0.0, "y": 0.0, "z": 0.0 }, 
                    "sensorTemp": 0.0 
                },
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

            # Derivar estado del puente
            if node_obj["status"] == "alert":
                bridge_status = "alert"
                node_obj["alarms"].append({ "type": "THRESHOLD", "severity": "alert", "msg": "Alerta detectada" })
            elif node_obj["status"] == "warn" and bridge_status != "alert":
                bridge_status = "warn"

            bridge_obj["nodes"].append(node_obj)

        # Actualizar estado global
        bridge_obj["status"] = bridge_status
        if last_update_global:
            bridge_obj["lastUpdate"] = last_update_global.isoformat()

        dashboard_data.append(bridge_obj)

    return dashboard_data

# =================================================================
# 6. ENDPOINTS DE DATOS (LECTURA DE TABLAS)
# =================================================================

@app.get("/summary/{resource_id}")
def get_trend_summary(resource_id: str, db: Session = Depends(get_db)):
    # Lógica de tendencia que maneja KPI y Sensor
    
    # 1. ES UN KPI (Ej: br-puentela-structuralHealth)
    known_kpi_types = ["structuralHealth", "accelGlob", "accelZ", "accelX", "accelY", "aiAnalysis", "naturalFreq"]
    
    target_type = next((k_type for k_type in known_kpi_types if resource_id.endswith(f"-{k_type}")), None)
    
    if target_type:
        target_bridge_id = resource_id.replace(f"-{target_type}", "")
        
        kpis = db.query(KpiDB).filter(
            KpiDB.bridge_id == target_bridge_id,
            KpiDB.kpi_type == target_type
        ).order_by(desc(KpiDB.timestamp)).limit(144).all()

        data = []
        for k in reversed(kpis):
            val = k.confidence if target_type == "aiAnalysis" else k.value
            data.append({
                "t": k.timestamp.strftime("%H:%M"),
                "v": val if val is not None else 0
            })
        return data

    # 2. ES UN SENSOR (Ej: node-a1)
    measurements = db.query(MeasurementDB).filter(
        MeasurementDB.sensor_id == resource_id
    ).order_by(desc(MeasurementDB.ts)).limit(144).all()
    
    if measurements:
        data = []
        for m in reversed(measurements): 
            data.append({
                "t": m.ts.strftime("%H:%M"),
                "v": m.acc_z # Por defecto graficamos Z para el sensor
            })
        return data

    return []

@app.get("/export/csv")
def export_csv(id: str, start: str, end: str, type: str = Query("sensor"), db: Session = Depends(get_db)):
    try:
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