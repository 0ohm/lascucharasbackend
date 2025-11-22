from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
import os
import sqlalchemy
from sqlalchemy import text

app = FastAPI()

# --------- DATABASE ---------
DATABASE_URL = os.getenv("DATABASE_URL")
engine = sqlalchemy.create_engine(DATABASE_URL, pool_pre_ping=True)


# --------- CREAR TABLAS AUTOMÁTICAMENTE ---------
def create_tables():
    sql = """
    CREATE TABLE IF NOT EXISTS measurements (
        id SERIAL PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL,
        value DOUBLE PRECISION NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


@app.on_event("startup")
def on_startup():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL no configurada")
    create_tables()   # <<< CREA LA TABLA AUTOMÁTICAMENTE AL ARRANCAR


# --------- MODELOS ---------
class FilterParams(BaseModel):
    since: datetime | None = None
    until: datetime | None = None
    limit: int | None = 100


@app.get("/")
def root():
    return {"status": "ok"}


# --------- POST: subir archivo ---------
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    content = await file.read()

    try:
        with engine.begin() as conn:
            for line in content.decode("utf-8").splitlines():
                if not line.strip():
                    continue
                ts_str, val_str = line.split(";")
                conn.execute(
                    text(
                        "INSERT INTO measurements (ts, value) VALUES (:ts, :value)"
                    ),
                    {"ts": ts_str, "value": float(val_str)},
                )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error insertando en DB: {e}")

    return {"status": "file_processed"}


# --------- POST: obtener datos ---------
@app.post("/data")
def get_data(filters: FilterParams):
    query = "SELECT id, ts, value FROM measurements WHERE 1=1"
    params = {}

    if filters.since:
        query += " AND ts >= :since"
        params["since"] = filters.since

    if filters.until:
        query += " AND ts <= :until"
        params["until"] = filters.until

    if filters.limit:
        query += " ORDER BY ts DESC LIMIT :limit"
        params["limit"] = filters.limit

    with engine.begin() as conn:
        rows = conn.execute(text(query), params).mappings().all()

    return JSONResponse(content=[dict(r) for r in rows])
