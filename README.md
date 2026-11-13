# E2E Pilot — zero-touch datalogger onboarding

Replaces hand-built Node-RED flows on Raspberry Pi dataloggers with one base agent plus a
per-plant config that is compiled on the server and pushed to the device. Background and the
pain points this addresses: [extract.md](extract.md) (from the Health Assure 5 recording,
[transcript.txt](transcript.txt)).

## Aligned with the site documentation (13 Nov 2026)

The two architecture docs (*Datalogger Workflow and Architecture*, *Datalogger System
Architecture and Data Flow Documentation*) describe the deployed `main.py` build and the
Node-RED era. This repo now follows them:

| Site practice | Here |
|---|---|
| Flat payload `{plantId, plcId, TIMESTAMP, <tag>…}` to topic `<site>/pub`, IoT rule → SQS | Same shape; topic `<plant_id>/pub`; one rule on `+/pub`; backend also accepts the older nested form |
| `config.json` with `POLLING_INTERVAL`, `ANALOG_SENSORS` (start/end/data_type/word_order/Scaling_Factor), `BOOLEAN_SENSORS`, `DEBUG_OPTION` | Compiled config carries those flat keys next to the structured sections, so the existing `core/config_loader.py` can load it |
| Types float/double/int/uint, word orders ABCD/DCBA/CDAB; booleans as coils, discrete inputs, or packed bits (`bit_index`) | `float32/float64/int16/uint16/int32/uint32`, `ABCD/CDAB/DCBA/BADC`, `bool/bool_di/bool_bit` + `bit` |
| Flow filter (`flow < threshold → 0`), totals integrated on the device and persisted, never decreasing | `zero_below` per tag; `role: flow` → `<tag>_total` in `totalizer_data.json` with the misfire guard |
| Startup health checks, device code on the OLED and cached, `provisioning_state.json`, pm2 restart after a job, bad-read re-init, fixed cadence with overrun warning, one buffered row replayed per cycle | All in `agent/agent.py`; `--restart-cmd "sudo pm2 restart datalogger"` opts into the restart |
| MQTT pre-flight (socket + cert headers), exponential back-off, jobs fetched via `jobs/{id}/get`, remote-control topic | `agent/aws_transport.py`; `<plant>/control` and `<plant>/health` in the per-device policy |
| Local MySQL buffer `localdata(id, timestamp, json_data)` | Same table in SQLite (`buffer.db`); no MariaDB to install on the image |

Not carried over: the Mongo ObjectId plant/PLC ids (the dashboard uses the plant slug; map it downstream), and remote-control writes to the PLC (the topic is wired, commands are logged until the bypass/command addresses are part of the config).

## Usable without help (13 Nov 2026 pass)

The dashboard was walked end to end as a first-time user and reworked so nobody needs a
second person or a manual:

- **Plain words first, PLC words second.** Types read "Decimal (REAL)", "Whole number ≥ 0
  (UINT)", "On/off: bit in a register"; roles read "Use as it is / Already a running total /
  A rate · make a running total". One name for each thing everywhere: *device code*, *number
  format*, *meters and probes*, *USB 1 / USB 2*.
- **Helpers where the mistakes happen.** *Find the number format* decodes two raw registers all
  four ways and, given the reading the operator expects, names the matching format with a
  *Use* button. *Bits inside registers* draws every packed register as 16 cells, red where two
  tags claim the same bit. *Which type?* maps what the PLC programmer says to the row to pick.
- **Tooltips (`?`) and sub-labels** on every term that is not everyday language; a
  "What you need before you start" checklist on the first step; an empty-state message in the
  tag table; error messages that say what to do, not what is wrong.
- **Design laws applied.** Hick: fewer visible choices (word order and register limit behind
  *Advanced*, interval down to four options, cloud details behind *Technical details*).
  Progressive disclosure: every help box is collapsed by default. Jakob: a standard 5-step
  wizard with Back/Continue. Fitts: one primary action per screen in the fixed footer.
  Miller/Gestalt: fields grouped by question (PLC vs meters, numbers vs on/off). Doherty:
  validation and the poll plan update on every keystroke. Error prevention: fields that do
  not apply are disabled, impossible combinations are refused with a reason.

## Live pages (GitHub Pages)

The three pages are published from the `docs/` folder at
https://mihirsethidp.github.io/Datalogger-new/ — the dashboard (`index.html`, in example
mode: no backend, so pushes are simulated), the provisioning guide (`guide.html`) and the
end-to-end walkthrough (`e2e.html`). `docs/` is a copy of `onboarding-ui.html`,
`backend/static/guide.html` and `e2e-walkthrough.html`; copy them again after editing. The
real backend and mock fleet run locally (below), not on Pages.

## Layout

| Path | What |
|---|---|
| `api/openapi.yaml` | The contract. Agent endpoints (register, heartbeat, config, ack, data) and dashboard endpoints (devices, plants, push, jobs, live). |
| `backend/app.py` | FastAPI + SQLite implementation. Serves the dashboard at `/`, docs at `/docs`. |
| `backend/compiler.py` | Draft → config: validation (duplicates, register overlap, address range, serial bus conflicts) and the poll plan (contiguous ranges, 125-register / 2000-coil limits). |
| `backend/static/index.html` | The onboarding dashboard, served by the backend (copy of `onboarding-ui.html`). |
| `onboarding-ui.html` | Same page as a standalone prototype with example data. Published artifact: https://claude.ai/artifact/YXMVNqztEtPjBDZBbTthGb |
| `agent/agent.py` | Pi base-image agent: register → heartbeat → apply job → poll Modbus → decode → post, with an on-disk buffer when the link is down. |
| `agent/modbus_sources.py` | pymodbus TCP/RTU readers (honours the inter-device pause on a shared RS485 converter) and a mock register bank. |
| `transcribe.py` | faster-whisper transcription used for the recording. |

## Presentation demo

```bash
python demo.py
```

Resets the local database, starts the backend and three simulated Pis, seeds one running plant
and opens the dashboard: a Pi waiting with its pairing code, a plant to edit (push v2), and a
Pi that drops offline after ~90 s. The guide for whoever operates it is served at
`/guide.html` (also published: https://claude.ai/artifact/UyAULSVXNuPtCc9PKsBH4a). The
self-contained walkthrough of the whole loop, Pi to cloud, is `e2e-walkthrough.html`.

## Run it locally

```bash
pip install -r requirements.txt
```

Backend (from the repo root):

```bash
python -m uvicorn backend.app:app --port 8000
```

A mock Pi (simulates the PLC and serial devices; any serial works):

```bash
python -m agent.agent --mock --serial DP-PI-0F3A21
```

Open http://127.0.0.1:8000 — the mock Pi appears under *Waiting to be assigned* with its
pairing code. Walk through the five steps and push; the timeline completes when the first
payload arrives and the tiles show decoded values.

On a real Pi, drop `--mock` and pass `--server https://<your-host>`; the serial is read from
`/proc/cpuinfo`.

## Transports

Both sit behind one interface (`backend/transport.py`), so swapping IoT Core for something
else later (Kafka was mentioned) touches one file on each side.

**mock** (default): jobs are collected on the next heartbeat, data is posted over HTTP.

**aws**: the Pi provisions itself in IoT Core on first boot (fleet provisioning by claim, Thing
named after the serial, `agent/aws_transport.py`). Pushing a config attaches a policy limited to
that plant's topics to the device's certificate and creates an IoT Core job whose document is
the config. The agent applies it, reports through the Jobs API, and publishes payloads to
`dp/<plant_id>/<plc_id>/data`; an IoT rule forwards those to SQS and the backend drains the
queue. Register and heartbeat stay on HTTP for the online state.

One-time setup (needs IoT/IAM/SQS admin rights):

```bash
AWS_REGION=ap-south-1 python infra/setup_aws.py --out agent/certs
```

It prints the environment for both sides, roughly:

```bash
DL_TRANSPORT=aws AWS_REGION=ap-south-1 DL_SQS_URL=https://sqs.../dl-plant-data python -m uvicorn backend.app:app --port 8000
```

```bash
python -m agent.agent --transport aws --endpoint <xxx>-ats.iot.ap-south-1.amazonaws.com --certs agent/certs
```

`agent/certs/` holds only the claim certificate and the Amazon root CA and is what the base
image ships. Device certificates are created by IoT Core and stored on the Pi
(`agent/state/<serial>/device.pem.crt`); never copy them anywhere.

Status of the aws path: the backend side is exercised against botocore stubs (policy
create/version/attach, job create, job status mapping, SQS drain) and the agent side against a
fake MQTT client (provisioning request/reply, job routing, buffer-and-flush). It has not yet run
against a live AWS account; `infra/setup_aws.py` in particular is untested.

## Database

`backend/provisioning.db` (override with `DL_DB`). Tables: devices, plants, configs (every
pushed version), jobs, payloads (last payload per device). Delete the file to start clean.
