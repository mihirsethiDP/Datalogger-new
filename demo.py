"""Presentation demo: a seeded fleet on one laptop, no plant and no AWS.

    python demo.py            # resets the local DB, starts everything, opens the dashboard
    python demo.py --no-browser

What you get:
  * DP-PI-DEMO01  assigned to "Welspun STP 1.2 MLD", config v1 running, live values
  * DP-PI-DEMO02  online, waiting to be assigned (its pairing code is printed below)
  * DP-PI-DEMO03  registered, then switched off: shows offline after ~90 s
Ctrl+C stops the backend and the simulated Pis.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent
API = "http://127.0.0.1:8000/api/v1"
PY = sys.executable

WELSPUN = {
    "id": "welspun-stp-12", "name": "Welspun STP 1.2 MLD", "plc_id": "PLC-01", "interval_s": 10,   # 60 on site; 10 keeps the demo moving
    "plc": {"ip": "192.168.10.20", "port": 502, "unit_id": 1, "word_order": "ABCD", "max_address": None},
    "serial": [{"id": "em1", "label": "Energy meter 1", "port": "/dev/ttyUSB0", "baud": 9600, "parity": "N", "slave_id": 1}],
    "tags": [
        {"name": "PH_AXTdd1_1:ADA_ADA", "source": "plc", "address": 1000, "type": "float32", "scale": 1, "unit": "pH"},
        {"name": "DO_ATdd1_1:ADA_ADA", "source": "plc", "address": 1200, "type": "float32", "scale": 1, "unit": "mg/L"},
        {"name": "FMF_EQTdd1ssAXTdd1_1:ADA_ADA", "source": "plc", "address": 1400, "type": "float32", "scale": 1, "unit": "m3/h", "role": "flow", "zero_below": 0.5},
        {"name": "FMT_EQTdd1_1:ADA_ADA", "source": "plc", "address": 1402, "type": "uint32", "scale": 1, "unit": "m3", "role": "counter"},
        {"name": "LT_EQTdd1_1:ADA_ADA", "source": "plc", "address": 1600, "type": "int16", "scale": 0.1, "unit": "m"},
        {"name": "ONOFF_BLW_1:ADA_ADA", "source": "plc", "address": 2001, "type": "bool"},
        {"name": "TRP_BLW_1:ADA_ADA", "source": "plc", "address": 3000, "type": "bool_bit", "bit": 0},
        {"name": "TRP_BLW_2:ADA_ADA", "source": "plc", "address": 3000, "type": "bool_bit", "bit": 1},
        {"name": "EM1_kWh", "source": "em1", "address": 100, "type": "uint32", "scale": 0.1, "unit": "kWh", "role": "counter"},
    ],
}


def wait_for(fn, timeout: float, what: str):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            v = fn()
            if v:
                return v
        except Exception:  # noqa: BLE001
            pass
        time.sleep(0.5)
    sys.exit(f"gave up waiting for {what}")


def agent(serial: str) -> subprocess.Popen:
    return subprocess.Popen([PY, "-m", "agent.agent", "--mock", "--serial", serial, "--server", "http://127.0.0.1:8000"],
                            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-browser", action="store_true")
    a = ap.parse_args()
    c = httpx.Client(timeout=5)
    try:
        c.get(API + "/devices")
        sys.exit("something is already listening on port 8000; stop it first (or run the demo against it by hand)")
    except httpx.HTTPError:
        pass

    for p in (ROOT / "backend" / "provisioning.db", ROOT / "agent" / "state"):
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()

    procs: list[subprocess.Popen] = []
    try:
        procs.append(subprocess.Popen([PY, "-m", "uvicorn", "backend.app:app", "--port", "8000", "--log-level", "warning"], cwd=ROOT))
        wait_for(lambda: c.get(API + "/devices").status_code == 200, 30, "the backend")
        print("backend up at http://127.0.0.1:8000")

        procs += [agent("DP-PI-DEMO01"), agent("DP-PI-DEMO02")]
        devs = wait_for(lambda: {d["serial"]: d for d in c.get(API + "/devices").json()} if len(c.get(API + "/devices").json()) >= 2 else None, 30, "the Pis to register")

        r = c.put(API + "/plants/welspun-stp-12", json=WELSPUN)
        r.raise_for_status()
        job = c.post(API + "/plants/welspun-stp-12/push", json={"serial": "DP-PI-DEMO01"}).json()
        wait_for(lambda: c.get(API + "/jobs/" + job["id"]).json()["status"] == "applied", 40, "DEMO01 to apply its config")
        wait_for(lambda: c.get(API + "/devices/DP-PI-DEMO01/live").json()["payload"], 30, "the first payload")
        print("DP-PI-DEMO01  running Welspun STP 1.2 MLD, config v1, data flowing")

        third = agent("DP-PI-DEMO03")
        wait_for(lambda: any(d["serial"] == "DP-PI-DEMO03" for d in c.get(API + "/devices").json()), 30, "DEMO03 to register")
        third.terminate()
        print("DP-PI-DEMO03  registered and switched off (shows offline after ~90 s)")
        print(f"DP-PI-DEMO02  waiting to be assigned · pairing code {devs['DP-PI-DEMO02']['pairing_code']}")
        print("\nDashboard  http://127.0.0.1:8000/     Guide  http://127.0.0.1:8000/guide.html     API docs  http://127.0.0.1:8000/docs")
        print("Ctrl+C to stop.")
        if not a.no_browser:
            webbrowser.open("http://127.0.0.1:8000/")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                p.kill()
        print("stopped")


if __name__ == "__main__":
    main()
