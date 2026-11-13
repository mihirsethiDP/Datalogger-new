"""Datalogger provisioning backend.

Run from the repo root:  uvicorn backend.app:app --reload
Dashboard: http://127.0.0.1:8000/   API docs: http://127.0.0.1:8000/docs

Transport is chosen by DL_TRANSPORT: "mock" (default, jobs delivered on heartbeat,
data posted over HTTP) or "aws" (jobs created in IoT Core Jobs; data arrives from the
SQS queue named by DL_SQS_URL). See transport.py.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import sqlite3
import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from .compiler import PlantDraft, compile_config, cycle_estimate, validate
from .transport import make_transport

log = logging.getLogger("backend")
ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("DL_DB", ROOT / "provisioning.db"))
ONLINE_WINDOW = timedelta(seconds=90)
transport = make_transport()


META_KEYS = {"plantId", "plcId", "TIMESTAMP", "serial", "config_version", "quality", "ts", "plant_id", "plc_id", "values"}


def normalize_payload(body: dict) -> dict:
    """Accept the flat site payload ({plantId, plcId, TIMESTAMP, <tag>: value, ...}) the dataloggers publish,
    or the older nested form, and keep one shape for the dashboard."""
    values = body.get("values") if isinstance(body.get("values"), dict) else {k: v for k, v in body.items() if k not in META_KEYS}
    return {"ts": body.get("TIMESTAMP") or body.get("ts"), "serial": body.get("serial"), "plant_id": body.get("plantId") or body.get("plant_id"),
            "plc_id": body.get("plcId") or body.get("plc_id"), "config_version": body.get("config_version"), "values": values, "quality": body.get("quality") or {}}


def store_payload(body: dict) -> None:
    """Record the latest payload for a device, whether it came over HTTP or from the SQS queue."""
    p = normalize_payload(body)
    serial = p["serial"]
    with db() as con:
        if not serial:   # the deployed build does not send the serial: fall back to the plant's assigned device
            r = con.execute("SELECT serial FROM devices WHERE plant_id=? AND status='assigned'", (p["plant_id"],)).fetchone()
            serial = r["serial"] if r else None
        if not serial or not con.execute("SELECT 1 FROM devices WHERE serial=?", (serial,)).fetchone():
            log.warning("payload from unknown device dropped: %s", str(body)[:120])
            return
        p["serial"] = serial
        con.execute("INSERT INTO payloads (serial, payload, received_at) VALUES (?,?,?) ON CONFLICT(serial) DO UPDATE SET payload=excluded.payload, received_at=excluded.received_at",
                    (serial, json.dumps(p), now()))
        con.execute("UPDATE devices SET last_seen=? WHERE serial=?", (now(), serial))


@asynccontextmanager
async def lifespan(_app: FastAPI):
    transport.start_inbound(store_payload)
    log.info("transport: %s", transport.name)
    yield


app = FastAPI(title="Datalogger Provisioning API", version="0.1.0", openapi_url="/api/v1/openapi.json", lifespan=lifespan)


# ---------- storage ----------
SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
  serial TEXT PRIMARY KEY, pairing_code TEXT UNIQUE NOT NULL, status TEXT NOT NULL DEFAULT 'unassigned',
  plant_id TEXT, last_seen TEXT, tailscale INTEGER DEFAULT 0, ip TEXT, image_version TEXT, config_version INTEGER,
  created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS plants (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, plc_id TEXT NOT NULL, draft TEXT NOT NULL,
  config_version INTEGER DEFAULT 0, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS configs (
  plant_id TEXT NOT NULL, version INTEGER NOT NULL, document TEXT NOT NULL, created_at TEXT NOT NULL,
  PRIMARY KEY (plant_id, version));
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, serial TEXT NOT NULL, plant_id TEXT NOT NULL, config_version INTEGER NOT NULL,
  status TEXT NOT NULL, message TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS payloads (
  serial TEXT PRIMARY KEY, payload TEXT NOT NULL, received_at TEXT NOT NULL);
"""


@contextmanager
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


with db() as con:
    con.executescript(SCHEMA)
    if "health" not in [c[1] for c in con.execute("PRAGMA table_info(devices)")]:
        con.execute("ALTER TABLE devices ADD COLUMN health TEXT")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def device_out(r: sqlite3.Row) -> dict:
    seen = datetime.fromisoformat(r["last_seen"]) if r["last_seen"] else None
    return {
        "serial": r["serial"], "pairing_code": r["pairing_code"], "status": r["status"], "plant_id": r["plant_id"],
        "online": bool(seen and datetime.now(timezone.utc) - seen < ONLINE_WINDOW), "last_seen": r["last_seen"],
        "tailscale": bool(r["tailscale"]), "ip": r["ip"], "image_version": r["image_version"], "config_version": r["config_version"],
        "health": json.loads(r["health"]) if r["health"] else None,
    }


def job_out(r: sqlite3.Row) -> dict:
    return dict(r)


def get_device(con: sqlite3.Connection, serial: str) -> sqlite3.Row:
    r = con.execute("SELECT * FROM devices WHERE serial=?", (serial,)).fetchone()
    if not r:
        raise HTTPException(404, "Unknown device")
    return r


# ---------- agent endpoints ----------
class RegisterRequest(BaseModel):
    serial: str
    image_version: str | None = None


class Heartbeat(BaseModel):
    tailscale: bool = False
    ip: str | None = None
    config_version: int | None = None
    health: dict | None = None   # cpu_temp_c, mem_free_frac, buffered, cycle_s


class Ack(BaseModel):
    status: str
    message: str | None = None


class Payload(BaseModel):
    """Flat site payload; any key not listed here is a tag value."""
    model_config = {"extra": "allow"}
    TIMESTAMP: str | None = None
    ts: str | None = None
    plantId: str | None = None
    plcId: str | None = None
    config_version: int | None = None
    values: dict[str, float | bool | None] | None = None
    quality: dict[str, str] | None = None


@app.post("/api/v1/devices/register")
def register(req: RegisterRequest):
    with db() as con:
        r = con.execute("SELECT * FROM devices WHERE serial=?", (req.serial,)).fetchone()
        if r:
            con.execute("UPDATE devices SET last_seen=?, image_version=COALESCE(?, image_version) WHERE serial=?", (now(), req.image_version, req.serial))
        else:
            while True:
                code = f"{secrets.randbelow(900000) + 100000}"
                if not con.execute("SELECT 1 FROM devices WHERE pairing_code=?", (code,)).fetchone():
                    break
            con.execute("INSERT INTO devices (serial, pairing_code, last_seen, image_version, created_at) VALUES (?,?,?,?,?)",
                        (req.serial, code, now(), req.image_version, now()))
        return device_out(get_device(con, req.serial))


@app.post("/api/v1/devices/{serial}/heartbeat")
def heartbeat(serial: str, hb: Heartbeat):
    with db() as con:
        get_device(con, serial)
        con.execute("UPDATE devices SET last_seen=?, tailscale=?, ip=COALESCE(?, ip), config_version=COALESCE(?, config_version), health=COALESCE(?, health) WHERE serial=?",
                    (now(), int(hb.tailscale), hb.ip, hb.config_version, json.dumps(hb.health) if hb.health else None, serial))
        if transport.name != "mock":
            return {"job": None}  # jobs travel through IoT Core; the heartbeat only keeps the online state
        job = con.execute("SELECT * FROM jobs WHERE serial=? AND status IN ('queued','sent') ORDER BY created_at LIMIT 1", (serial,)).fetchone()
        if job and job["status"] == "queued":
            con.execute("UPDATE jobs SET status='sent', updated_at=? WHERE id=?", (now(), job["id"]))
            job = con.execute("SELECT * FROM jobs WHERE id=?", (job["id"],)).fetchone()
        return {"job": job_out(job) if job else None}


@app.get("/api/v1/devices/{serial}/config")
def device_config(serial: str):
    with db() as con:
        d = get_device(con, serial)
        if d["status"] != "assigned" or not d["plant_id"]:
            raise HTTPException(404, "Device is not assigned to a plant")
        p = con.execute("SELECT config_version FROM plants WHERE id=?", (d["plant_id"],)).fetchone()
        c = con.execute("SELECT document FROM configs WHERE plant_id=? AND version=?", (d["plant_id"], p["config_version"])).fetchone()
        return json.loads(c["document"])


@app.post("/api/v1/devices/{serial}/jobs/{job_id}/ack")
def ack(serial: str, job_id: str, a: Ack):
    if a.status not in ("applied", "failed"):
        raise HTTPException(422, "status must be applied or failed")
    with db() as con:
        j = con.execute("SELECT * FROM jobs WHERE id=? AND serial=?", (job_id, serial)).fetchone()
        if not j:
            raise HTTPException(404, "Unknown job")
        con.execute("UPDATE jobs SET status=?, message=?, updated_at=? WHERE id=?", (a.status, a.message, now(), job_id))
        if a.status == "applied":
            con.execute("UPDATE devices SET config_version=? WHERE serial=?", (j["config_version"], serial))
        return job_out(con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())


@app.post("/api/v1/devices/{serial}/data", status_code=204)
def post_data(serial: str, p: Payload):
    with db() as con:
        d = get_device(con, serial)
        if d["status"] != "assigned":
            raise HTTPException(409, "Device is not assigned; payload rejected")
    store_payload({**p.model_dump(exclude_none=True), "serial": serial})
    return Response(status_code=204)


# ---------- dashboard endpoints ----------
class PairRequest(BaseModel):
    code: str


class PushRequest(BaseModel):
    serial: str


@app.get("/api/v1/devices")
def list_devices(status: str = "unassigned"):
    with db() as con:
        q = "SELECT * FROM devices" + ("" if status == "all" else " WHERE status=?") + " ORDER BY last_seen DESC"
        rows = con.execute(q, () if status == "all" else (status,)).fetchall()
        return [device_out(r) for r in rows]


@app.post("/api/v1/devices/pair")
def pair(req: PairRequest):
    with db() as con:
        r = con.execute("SELECT * FROM devices WHERE pairing_code=? AND status='unassigned'", (req.code,)).fetchone()
        if not r:
            raise HTTPException(404, "No waiting device has this code")
        return device_out(r)


@app.get("/api/v1/devices/{serial}")
def one_device(serial: str):
    with db() as con:
        return device_out(get_device(con, serial))


@app.get("/api/v1/devices/{serial}/live")
def live(serial: str):
    with db() as con:
        get_device(con, serial)
        r = con.execute("SELECT * FROM payloads WHERE serial=?", (serial,)).fetchone()
        return {"payload": json.loads(r["payload"]) if r else None, "received_at": r["received_at"] if r else None}


@app.get("/api/v1/plants")
def list_plants():
    with db() as con:
        rows = con.execute("SELECT p.*, d.serial AS device_serial FROM plants p LEFT JOIN devices d ON d.plant_id=p.id AND d.status='assigned' ORDER BY p.name").fetchall()
        return [{"id": r["id"], "name": r["name"], "plc_id": r["plc_id"], "device_serial": r["device_serial"],
                 "config_version": r["config_version"] or None, "tag_count": len(json.loads(r["draft"])["tags"])} for r in rows]


@app.get("/api/v1/plants/{plant_id}")
def get_plant(plant_id: str):
    with db() as con:
        r = con.execute("SELECT draft FROM plants WHERE id=?", (plant_id,)).fetchone()
        if not r:
            raise HTTPException(404, "Unknown plant")
        return json.loads(r["draft"])


@app.put("/api/v1/plants/{plant_id}")
def save_plant(plant_id: str, draft: PlantDraft):
    if draft.id != plant_id:
        raise HTTPException(422, [{"row": None, "msg": "Plant id in the body must match the URL."}])
    errs, warns = validate(draft)
    if errs:
        raise HTTPException(422, [e.model_dump() for e in errs])
    with db() as con:
        r = con.execute("SELECT config_version FROM plants WHERE id=?", (plant_id,)).fetchone()
        version = r["config_version"] if r else 0
        con.execute("INSERT INTO plants (id, name, plc_id, draft, config_version, updated_at) VALUES (?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name, plc_id=excluded.plc_id, draft=excluded.draft, updated_at=excluded.updated_at",
                    (plant_id, draft.name, draft.plc_id, draft.model_dump_json(), version, now()))
        serial_row = con.execute("SELECT serial FROM devices WHERE plant_id=? AND status='assigned'", (plant_id,)).fetchone()
    cfg = compile_config(draft, version + 1, serial_row["serial"] if serial_row else None)
    return {"config": cfg, "warnings": warns, "cycle_estimate_s": round(cycle_estimate(draft, cfg["polls"]), 2)}


@app.post("/api/v1/plants/{plant_id}/push")
def push(plant_id: str, req: PushRequest):
    with db() as con:
        p = con.execute("SELECT * FROM plants WHERE id=?", (plant_id,)).fetchone()
        if not p:
            raise HTTPException(404, "Save the plant before pushing")
        d = get_device(con, req.serial)
        if d["status"] == "assigned" and d["plant_id"] != plant_id:
            raise HTTPException(409, f"Device is assigned to {d['plant_id']}; unassign it first")
        draft = PlantDraft.model_validate_json(p["draft"])
        errs, _ = validate(draft)
        if errs:
            raise HTTPException(422, [e.model_dump() for e in errs])
        version = (p["config_version"] or 0) + 1
        doc = compile_config(draft, version, req.serial)
        try:
            transport.prepare_device(req.serial, doc)   # AWS: attach the per-plant topic policy before anything is pushed
        except RuntimeError as e:
            raise HTTPException(409, str(e)) from None
        con.execute("INSERT INTO configs (plant_id, version, document, created_at) VALUES (?,?,?,?)", (plant_id, version, json.dumps(doc), now()))
        con.execute("UPDATE plants SET config_version=? WHERE id=?", (version, plant_id))
        # retire any other Pi that was serving this plant, then assign this one
        con.execute("UPDATE devices SET status='retired', plant_id=NULL WHERE plant_id=? AND serial<>? AND status='assigned'", (plant_id, req.serial))
        con.execute("UPDATE devices SET status='assigned', plant_id=? WHERE serial=?", (plant_id, req.serial))
        con.execute("UPDATE jobs SET status='failed', message='superseded', updated_at=? WHERE serial=? AND status IN ('queued','sent')", (now(), req.serial))
        job_id = uuid.uuid4().hex[:12]
        try:
            status = transport.deliver_job(job_id, req.serial, doc)
        except Exception as e:  # noqa: BLE001  (boto3 raises many classes; the config version is kept so a retry re-pushes it)
            log.exception("job delivery failed")
            raise HTTPException(502, f"Could not deliver the job: {e}") from None
        con.execute("INSERT INTO jobs (id, serial, plant_id, config_version, status, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                    (job_id, req.serial, plant_id, version, status, now(), now()))
        return job_out(con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone())


@app.get("/api/v1/jobs/{job_id}")
def get_job(job_id: str):
    with db() as con:
        r = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if not r:
            raise HTTPException(404, "Unknown job")
        if r["status"] in ("queued", "sent"):
            try:
                synced = transport.job_status(job_id, r["serial"])
            except Exception:  # noqa: BLE001
                log.exception("job status sync failed")
                synced = None
            if synced and synced != r["status"]:
                con.execute("UPDATE jobs SET status=?, updated_at=? WHERE id=?", (synced, now(), job_id))
                if synced == "applied":
                    con.execute("UPDATE devices SET config_version=? WHERE serial=?", (r["config_version"], r["serial"]))
                r = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return job_out(r)


# ---------- dashboard static files (must be last) ----------
STATIC = ROOT / "static"
if STATIC.exists():
    app.mount("/", StaticFiles(directory=STATIC, html=True), name="static")
