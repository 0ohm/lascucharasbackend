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
    DATABASE_URL = "sqlite:///./local_test.db"
    print("⚠️ USANDO BASE DE DATOS LOCAL (SQLite)")

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
    
    # Estado administrativo (ej: 'maintenance', 'active')
    admin_status = Column(String, default="active") 

    # Relaciones con Cascada
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
    
    # Filtro de configuración
    filter_type = Column(String, default="high-pass") 

    # Campos de salud (Snapshot actualizado por DataFlow)
    health_battery = Column(Float, nullable=True)
    health_rssi = Column(Float, nullable=True)
    last_seen = Column(DateTime, nullable=True)
    status = Column(String, default="ok") 
    
    bridge = relationship("BridgeDB", back_populates="sensors")
    measurements = relationship("MeasurementDB", back_populates="sensor", cascade="all, delete-orphan")

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
    
    # Tipo de KPI: 'structuralHealth', 'naturalFreq', 'aiAnalysis', 'accelZ', etc.
    kpi_type = Column(String, nullable=False) 
    
    # Valores
    value = Column(Float, nullable=True)
    text_value = Column(Text, nullable=True) # Para mensajes IA
    
    status = Column(String, default="ok") 
    confidence = Column(Float, nullable=True) # Para IA

    bridge = relationship("BridgeDB", back_populates="kpis")

# Inicializar tablas
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
    
    # Verificar existencia del puente
    bridge = db.query(BridgeDB).filter(BridgeDB.id == target_bridge_id).first()
    if not bridge:
        # Crear puente implícitamente si no existe (Opcional)
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
    
    db.delete(bridge) # SQLAlchemy Cascade borrará sensores, kpis, etc.
    db.commit()
    return {"status": "deleted", "id": bridge_id}

@app.delete("/admin/sensor/{sensor_id}")
def delete_sensor(sensor_id: str, db: Session = Depends(get_db)):
    sensor = db.query(SensorDB).filter(SensorDB.id == sensor_id).first()
    if not sensor:
        raise HTTPException(status_code=404, detail="Sensor no encontrado")
    
    db.delete(sensor) # SQLAlchemy Cascade borrará mediciones
    db.commit()
    return {"status": "deleted"}

# =================================================================
# 5. ENDPOINT RAÍZ (ESTRUCTURA JSON REAL)
# =================================================================
@app.get("/")
def get_dashboard_data(db: Session = Depends(get_db)):
    bridges_db = db.query(BridgeDB).all()
    
    if not bridges_db:
        return []

    dashboard_data = []

    for b in bridges_db:
        # 1. Estructura Base del Puente
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
                # KPIs por defecto si no existen en BD
                "structuralHealth": { "id": f"{b.id}-kpi-h", "score": 100, "trend": "stable", "label": "Integridad Estructural", "unit": "%", "status": "ok" },
                "accelZ": { "id": f"{b.id}-kpi-z", "val": 0.000, "unit": "g", "status": "ok", "label": "Vibración Global (Z)", "trend": "flat" },
                "aiAnalysis": { "id": f"{b.id}-kpi-ai", "type": "text", "status": "ok", "label": "Diagnóstico IA", "text": "Sin datos suficientes para análisis.", "confidence": 0, "lastModelUpdate": None }
            },
            "nodes": []
        }

        # 2. Recuperar KPIs Reales de la BD (si existen)
        # (Aquí buscamos los últimos KPIs generados por la IA)
        for kpi_type in ["structuralHealth", "accelZ", "aiAnalysis"]:
            latest_kpi = db.query(KpiDB).filter(
                KpiDB.bridge_id == b.id, 
                KpiDB.kpi_type == kpi_type
            ).order_by(desc(KpiDB.timestamp)).first()
            
            if latest_kpi:
                kpi_data = {
                    "id": f"{b.id}-{kpi_type}",
                    "status": latest_kpi.status,
                    "label": bridge_obj["kpis"][kpi_type]["label"], # Mantenemos label fijo
                    "lastModelUpdate": latest_kpi.timestamp.isoformat()
                }
                
                if kpi_type == "aiAnalysis":
                    kpi_data["type"] = "text"
                    kpi_data["text"] = latest_kpi.text_value
                    kpi_data["confidence"] = latest_kpi.confidence
                else:
                    kpi_data["val"] = latest_kpi.value
                    kpi_data["unit"] = bridge_obj["kpis"][kpi_type]["unit"]
                    kpi_data["score"] = latest_kpi.value # Algunos usan score
                    kpi_data["trend"] = "stable" # Podría calcularse comparando con el anterior

                bridge_obj["kpis"][kpi_type] = kpi_data

        last_update_global = None
        bridge_status = "ok" # Estado derivado de sensores

        # 3. Procesar Sensores
        for s in b.sensors:
            # Buscar último dato real
            last_meas = db.query(MeasurementDB).filter(
                MeasurementDB.sensor_id == s.id
            ).order_by(desc(MeasurementDB.ts)).first()

            node_obj = {
                "id": s.id,
                "alias": s.alias,
                "x": s.pos_x,
                "y": s.pos_y,
                # Estado actual directo desde la BD
                "status": s.status if s.status else "ok", 
                "config": { "odr": s.odr, "range": s.range_g },
                
                # Salud desde BD (Tabla sensors)
                "health": { 
                    "battery": s.health_battery if s.health_battery is not None else 0, 
                    "signalStrength": s.health_rssi if s.health_rssi is not None else 0, 
                    "boardTemp": 0, 
                    "lastSeen": s.last_seen.isoformat() if s.last_seen else None 
                },
                
                # Telemetría (Inicializar vacío)
                "telemetry": { 
                    "accel_rms": { "x": 0.0, "y": 0.0, "z": 0.0 }, 
                    "sensorTemp": 0.0 
                },
                "alarms": []
            }

            # Llenar telemetría si hay datos
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
            elif node_obj["status"] == "warn" and bridge_status != "alert":
                bridge_status = "warn"

            bridge_obj["nodes"].append(node_obj)

        # Actualizar globales
        if bridge_status != "ok":
            bridge_obj["status"] = bridge_status
            # Actualizamos el KPI visual también para que coincida
            bridge_obj["kpis"]["structuralHealth"]["status"] = bridge_status 
            
        if last_update_global:
            bridge_obj["lastUpdate"] = last_update_global.isoformat()
        
        dashboard_data.append(bridge_obj)

    return dashboard_data

# =================================================================
# 6. ENDPOINTS DE DATOS (REALES)
# =================================================================
@app.get("/summary/{resource_id}")
def get_trend_summary(resource_id: str, db: Session = Depends(get_db)):
    """
    Devuelve la tendencia histórica (gráfico) para un Sensor O para un KPI.
    """
    
    # ---------------------------------------------------------
    # ESCENARIO A: Es un SENSOR (Busca en Measurements)
    # ---------------------------------------------------------
    measurements = db.query(MeasurementDB).filter(
        MeasurementDB.sensor_id == resource_id
    ).order_by(desc(MeasurementDB.ts)).limit(144).all()
    
    if measurements:
        data = []
        for m in reversed(measurements): 
            data.append({
                "t": m.ts.strftime("%H:%M"),
                "v": m.acc_z # Graficamos Z por defecto
            })
        return data

    # ---------------------------------------------------------
    # ESCENARIO B: Es un KPI (Busca en KpiDB)
    # ---------------------------------------------------------
    # El ID viene como "br-puentela-structuralHealth". Hay que separarlo.
    # Definimos los tipos conocidos para detectar cuál es.
    known_kpi_types = ["structuralHealth", "accelZ", "accelX", "accelY", "aiAnalysis", "naturalFreq"]
    
    target_bridge_id = None
    target_type = None

    for k_type in known_kpi_types:
        suffix = f"-{k_type}"
        if resource_id.endswith(suffix):
            target_type = k_type
            # Obtenemos el ID del puente quitándole el sufijo al resource_id
            target_bridge_id = resource_id[:-len(suffix)]
            break
    
    if target_bridge_id and target_type:
        # Consultamos la tabla de KPIs
        kpis = db.query(KpiDB).filter(
            KpiDB.bridge_id == target_bridge_id,
            KpiDB.kpi_type == target_type
        ).order_by(desc(KpiDB.timestamp)).limit(144).all()

        data = []
        for k in reversed(kpis):
            # Si es IA, graficamos la "confianza", si es otro, el "valor"
            val = k.confidence if target_type == "aiAnalysis" else k.value
            
            data.append({
                "t": k.timestamp.strftime("%H:%M"),
                "v": val if val is not None else 0
            })
        return data

    # Si no es ni sensor ni KPI, devolvemos vacío
    return []

@app.get("/export/csv")
def export_csv(id: str, start: str, end: str, type: str = Query("sensor"), db: Session = Depends(get_db)):
    
    # 1. DIAGNÓSTICO: Ver qué llega exactamente
    print(f"📥 CSV REQUEST -> ID: {id} | Start: {start} | End: {end}")

    # 2. PARSEO ROBUSTO (Sin fallback silencioso a now())
    try:
        # Intentamos formato ISO estándar (YYYY-MM-DDTHH:MM:SS)
        # Si viene con espacio en vez de T, lo arreglamos
        clean_start = start.replace(" ", "T")
        clean_end = end.replace(" ", "T")
        
        # Si falta la hora o segundos, fromisoformat suele ser inteligente, 
        # pero a veces datetime-local manda "YYYY-MM-DDTHH:MM" (sin segundos)
        # y python lo acepta bien.
        start_dt = datetime.fromisoformat(clean_start)
        end_dt = datetime.fromisoformat(clean_end)

    except ValueError as e:
        print(f"❌ Error parseando fechas: {e}")
        return JSONResponse(status_code=400, content={"error": f"Formato de fecha inválido: {str(e)}. Use ISO 8601."})

    print(f"🔍 QUERY DB -> Buscando desde {start_dt} hasta {end_dt}")

    # 3. CONSULTA SQL
    query = db.query(MeasurementDB).filter(
        MeasurementDB.sensor_id == id,
        MeasurementDB.ts >= start_dt,
        MeasurementDB.ts <= end_dt
    ).order_by(MeasurementDB.ts)
    
    # 4. GENERADOR CON LOGS
    def iter_csv():
        # Escribir cabecera
        yield "Timestamp,Accel_X(g),Accel_Y(g),Accel_Z(g),Battery(%),RSSI(dBm)\n"
        
        count = 0
        # yield_per trae datos en lotes para no saturar RAM
        for row in query.yield_per(1000): 
            count += 1
            ts = row.ts.isoformat()
            bat = row.battery if row.battery is not None else ""
            rssi = row.rssi if row.rssi is not None else ""
            yield f"{ts},{row.acc_x},{row.acc_y},{row.acc_z},{bat},{rssi}\n"
        
        print(f"✅ CSV Generado: {count} filas exportadas.")

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