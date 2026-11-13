# Health Assure 5 — data pipeline extract

Source: `transcript.txt` (64:42, Hindi/Hinglish → English via faster-whisper medium). Timestamps are `[mm:ss]` into the recording. Names and a few terms are Whisper's best guess and are flagged with `(?)`.

---

## 1. Plant-side basics (what the datalogger sits next to)

- **Sensors vs actuators** `[00:34–01:23]` — pH, DO, MLSS, level (LTE/LDX?) are sensors. Motors, blowers, valves are actuators. Valve feedback comes back as **DI** (digital input).
- **PLC I/O cards** `[01:35–02:34]` — AI / DI / AO / DO cards. The PLC runs the automation logic ("if DO below X, start blower") written in ladder logic; vendors supply complex blocks in ST / FBD `[13:36–15:17]`.
- **Three equipment modes** `[02:50–04:11]` — Auto (PLC logic), Manual (HMI button / bypass), Remote (software bypass bit). One equipment is in exactly one mode at a time.
- **Network at site** `[04:15–04:44]` — a router with three Ethernet runs: PLC, HMI, datalogger. Everything must be on the same LAN to talk.

## 2. Modbus: how data physically reaches the Pi

- **Wired only; RS485** `[04:47–05:16]` — RS232 not used. Two flavours: **Modbus RTU** (serial, 2-wire) and **Modbus TCP/IP** (RJ45).
- **TCP to the PLC** `[05:23–05:28]` is the default path.
- **RTU via USB↔RS485 converter** `[05:29–05:48], [10:04–10:14]` for devices that speak Modbus directly — MLSS probes `[07:11–07:15]`, third-party energy meters (SMCT?) `[09:39–10:14]`. They show up as `/dev/ttyUSB…` on the Pi `[41:24–41:28]`.
- **RS485 bus** `[08:35–09:16]` — ~64–128 devices on one pair; the electrical engineer assigns each a slave address; the Pi polls them one by one.
- **Sensor output types** `[06:41–07:08]` — analog 0–10 V or 4–20 mA → PLC analog card; or native Modbus. "98 % of sensors" are one of these `[09:30–09:38]`.

## 3. Registers and the 16-bit / 32-bit problem

- **PLC writes analog values into Modbus registers** `[10:15–11:50]` — the PLC manufacturer publishes usable address ranges (example given: 1000–4000 analog, 4000–5000 boolean). The register's type (integer / boolean) is declared in the PLC program.
- **Contiguous ranges on purpose** `[11:51–12:14], [16:49–17:38]` — e.g. analog in 1–100, boolean in 101–200, so one poll block can read "from here to here". Non-adjacent addresses force extra poll blocks.
- **Internal address standard (Deeksha)** `[29:12–30:14]` — pH sensors start at 1000, DO at 1200, etc., with gaps left between families. Without a standard every PLC programmer picks arbitrary addresses and the IoT side has to rediscover them.
- **Raw value = 32-bit split across two 16-bit registers** `[17:47–18:40]` — one Modbus register is 16 bits; a float (e.g. DO = 2.7) occupies two registers, an MSB half and an LSB half. The datalogger must read both and recombine before the value means anything.
- **Data types seen in Node-RED flows** `[38:40–38:57], [40:20–41:04]` — integer, boolean, and "real" (their name for float/integer). Different devices on the same flow use different types, and it is not written anywhere which block is which device.
- **Ferrules** `[12:36–13:05]` — wire numbering so the electrical engineer can trace I/O during troubleshooting.
- **Address universe is per-PLC and limited** `[01:02:27–01:03:14]` — the current datalogger lets you add unlimited tags, but a PLC has a fixed number of addresses; the tool should not pretend otherwise.

## 4. The current Raspberry Pi + Node-RED pipeline

- **Hardware/OS** `[15:45–16:09]` — Raspberry Pi, SD card, Raspbian (Debian). Code is Python or Node-RED. Pi spec quoted later: 4 GB RAM, 128 GB storage `[49:31–49:35]`. Cost ~₹15K + ₹5K router, up from ₹12K `[46:33–46:51]`.
- **Node-RED** `[16:12–16:32]` — low-code, predefined nodes, custom logic in JavaScript function nodes.
- **The four-block flow** `[16:33–19:16], [36:28–36:44]`:
  1. **Modbus poll** — start address + quantity (e.g. start 32000, count 20/50). Neither is predefined; it depends on what the PLC programmer wrote `[37:40–38:29]`.
  2. **Raw → meaningful** — combine the two 16-bit halves into the real value.
  3. **Tag mapping** — register → tag name, build a JSON payload: plant ID, PLC ID, timestamp, tag, value.
  4. **MQTT publish** to AWS IoT Core.
- **Polling cadence**: speaker polls DO and MLSS in Python within 1 s "without failure" `[42:49–43:01]`; Node-RED is kept only because of legacy `[43:02–43:28]`.

## 5. AWS IoT Core / MQTT

- **MQTT** `[19:23–19:46]` — lightweight pub/sub; IoT Core is the broker `[19:47–20:32]`.
- **Per-plant onboarding steps in AWS** `[21:14–24:19]`:
  1. Create a **Thing**.
  2. Attach a **Policy** — who may connect, which client ID, which topics may be published/subscribed. No wildcard access between plants.
  3. Generate the **certificate** (TLS; public + private).
  4. Create a **Rule** — anything on this plant's topic → **SQS** → stored downstream ("Plycosm"(?) / the data store) `[24:13–25:21]`.
  5. In Node-RED's MQTT node: paste the certificate, set the client ID (the Thing name) and the pub topic `[23:48–24:55]`.
  6. Verify by subscribing to the topic in the AWS MQTT test client `[25:00–25:17]`.
- Pipeline on the AWS side was built by "Satyasar"(?) `[24:38–24:43]`.

## 6. Tailscale

- **What it is for** `[25:26–28:00]` — a VPN (one company tailnet). Every site's Pi and the engineers' laptops are on it, so support can SSH / open the GUI / reconfigure remotely instead of travelling.
- **What it is NOT for** `[26:56–27:15], [28:01–28:05]` — plant data does not flow over Tailscale. Data goes router → internet → IoT Core directly. Tailscale is only for access.
- **Single tailnet, all sites** `[28:06–28:19]` — one Tailscale server/account for the company; every site is added to it (no per-site segregation).
- Needed at onboarding time: the Pi reaches site, then the engineer configures it remotely over Tailscale `[27:33–27:58]`.

## 7. Setup / onboarding steps as they happen today

1. Pi is imaged with OS + dependencies and shipped `[25:50–25:59]`.
2. Site electrical: router, three Ethernet runs, RS485 wiring and slave addresses `[04:15–04:44], [08:51–09:16]`.
3. PLC programmer maps analog inputs to Modbus registers and hands over the address list `[13:17–13:35], [15:35–15:47]`. The IoT team cannot discover these addresses on its own — only the PLC programmer knows them `[58:54–59:11]`.
4. AWS: Thing → Policy → Certificate → Rule → SQS `[21:14–24:19]`.
5. Node-RED: build poll blocks (one per address range / device), conversion, tag mapping, MQTT node with cert + client ID + topic `[16:33–19:16], [36:28–36:44]`.
6. Add the Pi to Tailscale for remote access `[26:51–27:00]`.
7. Verify data in the AWS MQTT test client / dashboard `[25:00–25:17]`.
8. **Remote control (extra)** `[30:15–33:30]` — PLC programmer adds a bypass bit + software on/off address per equipment; the datalogger subscribes to a command topic, maps tag → register and writes 1/0 to the PLC. Without those addresses remote control cannot be written.

## 8. Pain points and failures mentioned

| # | Pain point | Where |
|---|-----------|-------|
| 1 | **Unreadable Node-RED flows** — a site flow (TCS?) is so large that understanding which block polls which sensor "will take me a day". No naming, no standard. | `[33:35–35:11], [44:16–46:00]` |
| 2 | **6-second hop delay between serial devices** on one USB converter, otherwise the poll fails. With many devices a full cycle takes minutes — Welspun / Hinargo(?) ~3 min; a TWM project with 60–70 flow meters ≈ 360 s+. | `[35:34–36:25], [46:11–46:22]` |
| 3 | **Certificates live on a developer's laptop** — security risk when that person leaves. | `[23:24–23:41], [56:15–56:20]` |
| 4 | **No buffering** — if the link breaks, data for that period is simply lost. | `[44:30–44:40]` |
| 5 | **Every plant is hand-configured** — repetitive, slow, and the "time to open" a new site is unpredictable. | `[44:41–44:56], [45:42–45:49]` |
| 6 | **Poll start/quantity and data types not predefined** — depends on each PLC programmer; different devices mixed in one flow with no labels. | `[37:40–38:29], [40:20–41:12]` |
| 7 | **Remote-control code hard-codes one address** — speaker rewrote it as a variable + mapping in three lines. | `[41:44–42:28]` |
| 8 | **Address standard not enforced** across PLC programmers. | `[29:12–30:14]` |
| 9 | **Site changes break data silently** — wire moved from one I/O point to another, PLC program changed, or wrong poll block in Node-RED. Ops only notices "data not coming", then sends an engineer, then checks address-vs-tag in the flow. | `[01:00:32–01:02:26]` |
| 10 | **Unlimited tags in the datalogger vs fixed address space in the PLC** — tool allows invalid configs. | `[01:02:27–01:03:14]` |
| 11 | **PLC vendor software can't run on the Pi** (Siemens ≈ 27 GB; closed, vendor-specific). Remote PLC programming needs a cloud VM + Tailscale forwarded to the edge. | `[48:38–51:33]` |
| 12 | **Hardware cost creep** — Pi datalogger ₹12K → ₹15K + ₹5K router; a ₹3K SIM-based telemetric microcontroller (4 MB, single flow meter, no OS) is an alternative for simple sites. | `[46:29–48:20]` |
| 13 | **Architecture undecided** — IoT Core may be replaced by Kafka; the speaker refuses to write code until an ADR exists. | `[01:03:50–01:04:42]` |

## 9. Proposed replacement (as described by the speaker)

`[51:34–58:04]`

- **Drop Node-RED.** Pi runs one **base code** identical on every device + a **per-plant config file**.
- **Provisioning / zero-touch onboarding**:
  1. SD image contains a **bootstrap certificate** (minimum needed to reach IoT Core) `[53:11–53:30]`.
  2. On power + internet the Pi checks: config file present? provisioned? If not, it calls a **registration API** (Rajan's) with its **serial number**; the device is written to a DB table `[54:27–54:48]`.
  3. The device **orders its real certificate** itself — no developer ever holds it `[53:31–53:44], [56:15–56:20]`.
  4. Ops **assigns the asset** in the dashboard and fills in: PLC IP, integer start address, boolean start address, tag ↔ address, per-analog flags (totalizer, failure) `[54:50–55:37]`.
  5. Server builds the **config JSON**, delivers it via **IoT Core Jobs** / MQTT; Pi saves it and starts polling + publishing `[55:38–56:14]`.
- **6-digit pairing code** per device, stored in the DB, so a cloned SD card cannot push data without being mapped `[57:07–58:04]`.
- **CSV upload of tags** instead of adding one sensor at a time — but the CSV must come from the PLC programmer, who is the only one who knows the Modbus addresses; the electrical IO list / AutoCAD wiring (Nagendra ji) has tags and PLC I/O points, not Modbus addresses `[58:08–01:00:31]`.
- Team's counter-argument: building this takes time now; it pays off at 50–1000 devices `[56:50–57:03]`.
