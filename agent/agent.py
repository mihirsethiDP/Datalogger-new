"""Pi base-image agent: the one program every datalogger runs (under pm2 on site).

    python -m agent.agent --server http://127.0.0.1:8000 --serial DP-PI-0F3A21 --mock
    python -m agent.agent --transport aws --endpoint xxx-ats.iot.ap-south-1.amazonaws.com

Start-up, in the order the deployed main.py uses it:
  logging + display → health checks (CPU temperature, RAM, internet) → provisioning
  (AWS, when enabled) and onboarding (device code from the backend, cached) → load
  config.json → background services (jobs, remote control) → the acquisition loop.

The loop keeps a fixed cadence (POLLING_INTERVAL), decodes registers into engineering
values, filters flow noise, integrates flow totals (persisted, never decreasing),
publishes a flat payload, and buffers to a local SQLite file whenever the link is
down, draining one old row per cycle once it is back.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sqlite3
import struct
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from .display import Display
from .modbus_sources import open_sources

log = logging.getLogger("agent")
HERE = Path(__file__).resolve().parent
BAD_READ_LIMIT = 5          # consecutive failed cycles before clients are re-initialised
CPU_TEMP_MAX = 80.0         # °C
MEM_FREE_MIN = 0.10         # fraction


# ---------- hardware / environment ----------
def pi_serial() -> str:
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("Serial"):
                return "DP-PI-" + line.split(":")[1].strip()[-6:].upper()
    except OSError:
        pass
    return "DP-PI-" + hex(abs(hash(socket.gethostname())))[-6:].upper()


def pi_health() -> dict:
    h: dict = {}
    try:
        h["cpu_temp_c"] = int(Path("/sys/class/thermal/thermal_zone0/temp").read_text()) / 1000
    except (OSError, ValueError):
        pass
    try:
        mem = {k: int(v.split()[0]) for k, v in (ln.split(":") for ln in Path("/proc/meminfo").read_text().splitlines() if ":" in ln)}
        h["mem_free_frac"] = round(mem["MemAvailable"] / mem["MemTotal"], 3)
    except (OSError, KeyError, ValueError):
        pass
    return h


def internet_up(host: str = "1.1.1.1", port: int = 53) -> bool:
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def tailscale_up() -> bool:
    try:
        out = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True, timeout=5)
        return out.returncode == 0 and '"Online": true' in out.stdout
    except (OSError, subprocess.TimeoutExpired):
        return False


def local_ip() -> str | None:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return None


# ---------- decoding ----------
def decode(regs: list[int], typ: str, order: str) -> float | int | bool:
    """Recombine 16-bit words into a value. Word orders as the field uses them:
    ABCD high word first (big-endian), CDAB words swapped, DCBA fully reversed, BADC bytes swapped in each word."""
    if typ in ("bool", "bool_di"):
        return bool(regs[0])
    if typ == "int16":
        return struct.unpack(">h", struct.pack(">H", regs[0] & 0xFFFF))[0]
    if typ == "uint16":
        return regs[0] & 0xFFFF
    raw = b"".join(struct.pack(">H", w & 0xFFFF) for w in regs)
    if order == "CDAB":
        raw = b"".join(struct.pack(">H", w & 0xFFFF) for w in reversed(regs))
    elif order == "DCBA":
        raw = raw[::-1]
    elif order == "BADC":
        raw = b"".join(struct.pack("<H", w & 0xFFFF) for w in regs)
    fmt = {"int32": ">i", "uint32": ">I", "float32": ">f", "float64": ">d"}[typ]
    return struct.unpack(fmt, raw[: struct.calcsize(fmt)])[0]


class Buffer:
    """Store-and-forward queue in SQLite (the deployed build uses MariaDB; same table shape: id, timestamp, json_data)."""

    def __init__(self, path: Path):
        self.con = sqlite3.connect(path)
        self.con.execute("CREATE TABLE IF NOT EXISTS localdata (id INTEGER PRIMARY KEY, timestamp TEXT NOT NULL, json_data TEXT NOT NULL)")

    def push(self, body: dict):
        self.con.execute("INSERT INTO localdata (timestamp, json_data) VALUES (?, ?)", (body.get("TIMESTAMP", ""), json.dumps(body)))
        self.con.commit()

    def pending(self, limit: int) -> list[tuple[int, dict]]:
        return [(r[0], json.loads(r[1])) for r in self.con.execute("SELECT id, json_data FROM localdata ORDER BY id LIMIT ?", (limit,))]

    def drop(self, ids: list[int]):
        self.con.executemany("DELETE FROM localdata WHERE id=?", [(i,) for i in ids])
        self.con.commit()

    def size(self) -> int:
        return self.con.execute("SELECT COUNT(*) FROM localdata").fetchone()[0]


class Totals:
    """Running totals integrated from flow rates (rate per hour × interval). Persisted; never decreases."""

    def __init__(self, path: Path):
        self.path = path
        try:
            self.data = json.loads(path.read_text()) if path.exists() else {}
        except ValueError:
            self.data = {}

    def update(self, tag: str, rate: float | None, interval_s: float) -> float:
        total = float(self.data.get(tag, 0.0))
        if isinstance(rate, (int, float)) and rate >= 0:      # misfire guard: skip None / negative readings
            total += rate * interval_s / 3600.0
            self.data[tag] = total
            self.path.write_text(json.dumps(self.data))
        return round(total, 4)


class Agent:
    def __init__(self, server: str, serial: str, mock: bool, state_dir: Path, aws=None, restart_cmd: str | None = None):
        self.api = server.rstrip("/") + "/api/v1"
        self.serial = serial
        self.mock = mock
        self.aws = aws
        self.restart_cmd = restart_cmd
        self.state_dir = state_dir
        self.config_path = state_dir / "config.json"
        self.code_path = state_dir / "device_code.json"
        self.config: dict | None = self._load_config()
        self.http = httpx.Client(timeout=10)
        self.buffer = Buffer(state_dir / "buffer.db")
        self.totals = Totals(state_dir / "totalizer_data.json")
        self.display = Display()
        self.sources = None
        self.bad_reads = 0
        self.last_cycle = 0.0

    def _load_config(self) -> dict | None:
        if not self.config_path.exists():
            return None
        try:
            cfg = json.loads(self.config_path.read_text())
            self._validate(cfg)
            return cfg
        except (ValueError, RuntimeError) as e:
            log.error("config.json unusable (%s); waiting for a new one", e)
            return None

    @staticmethod
    def _validate(cfg: dict):
        for key in ("version", "plant_id", "plc_id", "publish", "sources", "polls", "tags"):
            if key not in cfg:
                raise RuntimeError(f"config is missing {key!r}")

    # ---------- start-up ----------
    def startup_checks(self):
        while True:
            h = pi_health()
            if h.get("cpu_temp_c", 0) > CPU_TEMP_MAX or h.get("mem_free_frac", 1) < MEM_FREE_MIN:
                log.warning("health check: %s · waiting", h)
                self.display.show("Health check", f"CPU {h.get('cpu_temp_c', '?')}C")
                time.sleep(10)
                continue
            break
        host = self.api.split("//")[1].split("/")[0].split(":")[0]
        while not (internet_up() or internet_up(host, int(self.api.split(":")[-1].split("/")[0]) if self.api.count(":") > 1 else 80)):
            log.warning("no internet · waiting")
            self.display.show("Waiting for", "internet")
            time.sleep(5)

    def onboard(self):
        """Get (or reuse) the device code; the same POST the deployed onboarding.py sends."""
        cached = None
        if self.code_path.exists():
            try:
                cached = json.loads(self.code_path.read_text()).get("device_code")
            except ValueError:
                pass
        while True:
            try:
                r = self.http.post(f"{self.api}/devices/register", json={"serial": self.serial, "image_version": "base-2.4.0"})
                r.raise_for_status()
                d = r.json()
                break
            except httpx.HTTPError as e:
                if cached:
                    log.warning("server unreachable (%s); using cached device code", e.__class__.__name__)
                    d = {"pairing_code": cached, "status": "unknown"}
                    break
                log.warning("server unreachable (%s), retrying in 5 s", e.__class__.__name__)
                time.sleep(5)
        code = d["pairing_code"]
        self.code_path.write_text(json.dumps({"device_code": code, "serial": self.serial}))
        log.info("device code %s %s · status=%s", code[:3], code[3:], d.get("status"))
        self.display.show("Device code", f"{code[:3]} {code[3:]}")

    # ---------- server ----------
    def heartbeat(self):
        body = {"tailscale": tailscale_up(), "ip": local_ip(), "config_version": (self.config or {}).get("version"), "health": {**pi_health(), "buffered": self.buffer.size(), "cycle_s": round(self.last_cycle, 2)}}
        r = self.http.post(f"{self.api}/devices/{self.serial}/heartbeat", json=body)
        r.raise_for_status()
        job = r.json().get("job")
        if job and not self.aws:
            self.apply_http_job(job)

    def apply_config(self, cfg: dict) -> str:
        self._validate(cfg)
        self.config_path.write_text(json.dumps(cfg, indent=2))
        self.config = cfg
        logging.getLogger().setLevel(logging.DEBUG if cfg.get("debug") else logging.INFO)   # DEBUG_OPTION
        if self.sources:
            self.sources.close()
        self.sources = open_sources(cfg, mock=self.mock)
        self.bad_reads = 0
        log.info("applied config v%s (%d tags, %d polls)", cfg["version"], len(cfg["tags"]), len(cfg["polls"]))
        self.display.show(f"Config v{cfg['version']}", f"{len(cfg['tags'])} tags")
        if self.restart_cmd:
            log.info("restarting via %r in 3 s", self.restart_cmd)
            subprocess.Popen(f"sleep 3 && {self.restart_cmd}", shell=True)
        return f"v{cfg['version']} · {len(cfg['tags'])} tags · {len(cfg['polls'])} polls"

    def apply_http_job(self, job: dict):
        try:
            cfg = self.http.get(f"{self.api}/devices/{self.serial}/config").raise_for_status().json()
            if cfg["version"] != job["config_version"]:
                raise RuntimeError(f"server config is v{cfg['version']}, job expects v{job['config_version']}")
            summary = self.apply_config(cfg)
            self.http.post(f"{self.api}/devices/{self.serial}/jobs/{job['id']}/ack", json={"status": "applied", "message": summary})
        except Exception as e:  # noqa: BLE001
            log.exception("job %s failed", job["id"])
            self.http.post(f"{self.api}/devices/{self.serial}/jobs/{job['id']}/ack", json={"status": "failed", "message": str(e)[:300]})

    def apply_aws_job(self, job_id: str, document: dict):
        try:
            summary = self.apply_config(document)
            self.aws.job_update(job_id, True, summary)
        except Exception as e:  # noqa: BLE001
            log.exception("job %s failed", job_id)
            self.aws.job_update(job_id, False, str(e))

    def on_control(self, body: dict):
        """Remote-control commands (the deployed remotecontrol.py). Writing to the PLC needs the bypass/command addresses, so for now it is logged."""
        log.info("control command received: %s", json.dumps(body)[:200])

    # ---------- plant ----------
    def read_cycle(self) -> dict:
        cfg = self.config
        if self.sources is None:
            self.sources = open_sources(cfg, mock=self.mock)
        blocks: dict[tuple[str, int, int], list[int] | None] = {}
        failures = 0
        for p in cfg["polls"]:
            data = self.sources.read(p["source"], p["fc"], p["start"], p["count"])
            if data is None:
                failures += 1
            blocks[(p["source"], p["fc"], p["start"])] = data
        if failures == len(cfg["polls"]):
            self.bad_reads += 1
            if self.bad_reads >= BAD_READ_LIMIT:
                log.warning("%d consecutive failed cycles · re-initialising Modbus clients", self.bad_reads)
                self.sources.close()
                self.sources = open_sources(cfg, mock=self.mock)
                self.bad_reads = 0
        else:
            self.bad_reads = 0

        payload: dict = {"plantId": cfg["plant_id"], "plcId": cfg["plc_id"], "TIMESTAMP": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                         "serial": self.serial, "config_version": cfg["version"]}
        quality: dict[str, str] = {}
        interval = cfg["publish"]["interval_s"]
        for t in cfg["tags"]:
            typ = t["type"]
            fc = {"bool": 1, "bool_di": 2}.get(typ, 3)
            span = {"int32": 2, "uint32": 2, "float32": 2, "float64": 4}.get(typ, 1)
            block = next(((k, v) for k, v in blocks.items() if k[0] == t["source"] and k[1] == fc and k[2] <= t["address"] and v is not None and t["address"] + span - 1 < k[2] + len(v)), None)
            if block is None:
                payload[t["name"]] = None
                quality[t["name"]] = "read failed"
                continue
            off = t["address"] - block[0][2]
            regs = block[1][off:off + span]
            if typ == "bool_bit":
                v: float | int | bool = bool((regs[0] >> int(t.get("bit") or 0)) & 1)
            else:
                v = decode(regs, typ, cfg["sources"][t["source"]].get("word_order", "ABCD"))
            if isinstance(v, bool):
                payload[t["name"]] = int(v)
                continue
            v = v * t.get("scale", 1)
            if t.get("zero_below") is not None and v < t["zero_below"]:
                v = 0.0
            v = round(v, 4)
            payload[t["name"]] = v
            if t.get("role") == "flow":
                payload[t["name"] + "_total"] = self.totals.update(t["name"], v, interval)
        if quality:
            payload["quality"] = quality
        return payload

    def _send(self, body: dict) -> str:
        """'ok', 'retry' (keep buffered) or 'drop' (nothing will ever accept it)."""
        if self.aws:
            return "ok" if self.aws.publish(self.config["publish"]["topic"], body) else "retry"
        try:
            r = self.http.post(f"{self.api}/devices/{self.serial}/data", json=body)
            if r.status_code == 409:
                return "drop"
            r.raise_for_status()
            return "ok"
        except httpx.HTTPError:
            return "retry"

    def publish(self, payload: dict):
        self.buffer.push(payload)
        drain = int(self.config["publish"].get("buffer_drain_per_cycle", 1))
        done: list[int] = []
        # the newest row goes first; then at most `drain` old rows, so a long outage replays gradually
        rows = self.buffer.pending(limit=10_000)
        order = rows[-1:] + rows[:-1][:drain]
        for pid, body in order:
            result = self._send(body)
            if result == "retry":
                log.warning("delivery failed; %d payloads buffered", self.buffer.size())
                break
            done.append(pid)
        self.buffer.drop(done)

    def run(self):
        self.display.show("Datalogger", self.serial)
        self.startup_checks()
        if self.aws:
            while True:
                try:
                    self.aws.provision()
                    break
                except Exception as e:  # noqa: BLE001
                    log.warning("provisioning failed (%s), retrying in 30 s", e)
                    self.display.show("Provisioning", "retrying")
                    time.sleep(30)
        self.onboard()
        if self.config:
            logging.getLogger().setLevel(logging.DEBUG if self.config.get("debug") else logging.INFO)
            self.sources = open_sources(self.config, mock=self.mock)
        if self.aws:
            self.aws.connect(self.apply_aws_job, on_control=self.on_control,
                             control_topic=(self.config or {}).get("publish", {}).get("control_topic"),
                             health=lambda: {"serial": self.serial, **pi_health(), "buffered": self.buffer.size()},
                             health_topic=(self.config or {}).get("publish", {}).get("health_topic"))
        next_hb = 0.0
        next_read = time.monotonic()
        try:
            while True:
                now = time.monotonic()
                try:
                    if now >= next_hb:
                        self.heartbeat()
                        next_hb = now + 15
                except httpx.HTTPError as e:
                    log.warning("server unreachable: %s", e.__class__.__name__)
                    next_hb = now + 15
                if self.config and now >= next_read:
                    interval = self.config["publish"]["interval_s"]
                    t0 = time.monotonic()
                    try:
                        self.publish(self.read_cycle())
                    except Exception:  # noqa: BLE001
                        log.exception("cycle failed")
                    self.last_cycle = time.monotonic() - t0
                    log.debug("cycle %.2fs", self.last_cycle)
                    if self.last_cycle > interval:
                        log.warning("cycle overrun: %.1fs > %ss interval · next cycle starts now", self.last_cycle, interval)
                        next_read = time.monotonic()
                    else:
                        next_read = t0 + interval
                time.sleep(0.5)
        finally:
            self.cleanup()

    def cleanup(self):
        if self.sources:
            self.sources.close()
        if self.aws:
            self.aws.close()
        self.display.show("Stopped", "")
        log.info("clean shutdown")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default=os.environ.get("API_URL", "http://127.0.0.1:8000"))
    ap.add_argument("--serial", default=None)
    ap.add_argument("--mock", action="store_true", help="simulate the PLC and serial devices instead of opening Modbus")
    ap.add_argument("--state", default=None, help="directory for config.json, device_code.json, buffer.db, totalizer_data.json")
    ap.add_argument("--transport", choices=["http", "aws"], default=os.environ.get("DL_TRANSPORT_AGENT", "http"))
    ap.add_argument("--endpoint", default=os.environ.get("DL_IOT_ENDPOINT"), help="IoT Core ATS data endpoint (aws)")
    ap.add_argument("--certs", default=os.environ.get("DL_CERTS", str(HERE / "certs")), help="dir with AmazonRootCA1.pem, claim.pem.crt, claim.private.key")
    ap.add_argument("--template", default=os.environ.get("DL_PROVISIONING_TEMPLATE", "dl-datalogger"))
    ap.add_argument("--restart-cmd", default=os.environ.get("DL_RESTART_CMD"), help='run after a config job, e.g. "sudo pm2 restart datalogger"')
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    serial = a.serial or pi_serial()
    state = Path(a.state) if a.state else HERE / "state" / serial
    state.mkdir(parents=True, exist_ok=True)
    aws = None
    if a.transport == "aws":
        if not a.endpoint:
            ap.error("--transport aws needs --endpoint (or DL_IOT_ENDPOINT)")
        from .aws_transport import AwsIot

        aws = AwsIot(a.endpoint, serial, state, Path(a.certs), a.template)
    Agent(a.server, serial, a.mock, state, aws=aws, restart_cmd=a.restart_cmd).run()


if __name__ == "__main__":
    main()
