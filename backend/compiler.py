"""Turn a plant draft into a device config: validate, then build the poll plan.

The same rules the dashboard shows inline live here, so a hand-crafted request
cannot push an invalid config. The compiled document carries both the structured
sections the new agent reads and the flat keys (POLLING_INTERVAL, ANALOG_SENSORS,
BOOLEAN_SENSORS, ...) that the existing main.py / core/config_loader.py expects,
so either runtime can load it.
"""
from __future__ import annotations

import ipaddress
import re
from typing import Literal

from pydantic import BaseModel, Field, model_validator

# type → (registers per value, address space)
#   coil     : FC01 read coils            (one bit per address)
#   discrete : FC02 read discrete inputs  (one bit per address)
#   reg      : FC03 read holding registers (16-bit words; bool_bit picks one bit of a word)
TYPES = {
    "bool": (1, "coil"),
    "bool_di": (1, "discrete"),
    "bool_bit": (1, "reg"),
    "int16": (1, "reg"),
    "uint16": (1, "reg"),
    "int32": (2, "reg"),
    "uint32": (2, "reg"),
    "float32": (2, "reg"),
    "float64": (4, "reg"),
}
FC = {"coil": 1, "discrete": 2, "reg": 3}
WORD_ORDERS = ("ABCD", "CDAB", "DCBA", "BADC")
LIMIT = {"coil": 2000, "discrete": 2000, "reg": 125}   # Modbus per-request limits
GAP_MERGE = 10      # read across small gaps instead of issuing another request
HOP_S = 6           # observed pause needed between serial devices on one converter
TAG_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_:]*$")       # DigitalPaani tags carry ':' (PH_AXTdd1_1:ADA_ADA)

# names the existing config_loader uses for data_type
LEGACY_TYPE = {"int16": "int", "uint16": "uint", "int32": "dint", "uint32": "udint", "float32": "float", "float64": "double"}


class Tag(BaseModel):
    name: str
    source: str = "plc"
    address: int
    type: Literal["bool", "bool_di", "bool_bit", "int16", "uint16", "int32", "uint32", "float32", "float64"]
    bit: int | None = None                     # bool_bit only: 0–15
    scale: float = 1
    unit: str | None = None
    role: Literal["value", "counter", "flow"] = "value"   # counter: register already totals; flow: device integrates rate → <name>_total
    zero_below: float | None = None            # flow-noise filter: values below this are published as 0
    totalizer: bool | None = None              # legacy alias for role == counter

    @model_validator(mode="after")
    def _legacy(self):
        if self.totalizer and self.role == "value":
            self.role = "counter"
        self.totalizer = self.role == "counter"
        return self


class Plc(BaseModel):
    ip: str
    port: int = 502
    unit_id: int = 1
    word_order: Literal["ABCD", "CDAB", "DCBA", "BADC"] = "ABCD"
    max_address: int | None = None


class SerialDevice(BaseModel):
    id: str
    label: str
    port: str
    baud: int = 9600
    parity: Literal["N", "E", "O"] = "N"
    slave_id: int
    word_order: Literal["ABCD", "CDAB", "DCBA", "BADC"] = "ABCD"


class PlantDraft(BaseModel):
    id: str = Field(pattern=r"^[a-z0-9-]{3,40}$")
    name: str = Field(min_length=1)
    plc_id: str = "PLC-01"
    interval_s: int = 60
    topic: str | None = None                   # default <id>/pub, the convention the IoT rules match
    debug: bool = False                        # DEBUG_OPTION on the device
    plc: Plc
    serial: list[SerialDevice] = []
    tags: list[Tag]


class Issue(BaseModel):
    row: int | None = None
    msg: str


def validate(d: PlantDraft) -> tuple[list[Issue], list[str]]:
    errs: list[Issue] = []
    warns: list[str] = []

    try:
        ipaddress.IPv4Address(d.plc.ip)
    except ValueError:
        errs.append(Issue(msg=f"PLC IP address {d.plc.ip!r} is not valid."))
    if not 1 <= d.plc.port <= 65535:
        errs.append(Issue(msg="PLC port must be 1–65535."))
    if not 0 <= d.plc.unit_id <= 255:
        errs.append(Issue(msg="PLC unit ID must be 0–255."))
    if d.interval_s < 1:
        errs.append(Issue(msg="Publish interval must be at least 1 s."))
    if d.topic is not None and not re.match(r"^[A-Za-z0-9_\-/]{1,120}$", d.topic):
        errs.append(Issue(msg="Topic may only contain letters, digits, '-', '_' and '/'."))

    seen_bus: dict[tuple[str, int], str] = {}
    for s in d.serial:
        if not s.label:
            errs.append(Issue(msg=f"Serial device {s.id} needs a name."))
        if not 1 <= s.slave_id <= 247:
            errs.append(Issue(msg=f"{s.label or s.id}: slave ID must be 1–247."))
        key = (s.port, s.slave_id)
        if key in seen_bus:
            errs.append(Issue(msg=f"{s.label or s.id} and {seen_bus[key]} both use slave {s.slave_id} on {s.port}."))
        seen_bus[key] = s.label or s.id
    serial_ids = {s.id for s in d.serial}

    names: dict[str, int] = {}
    occupied: dict[tuple[str, str, int], Tag] = {}
    bits_used: set[tuple[str, int, int]] = set()
    for i, t in enumerate(d.tags):
        label = f"“{t.name}”" if t.name else f"row {i + 1}"
        if not t.name:
            errs.append(Issue(row=i, msg=f"Row {i + 1}: tag name is empty."))
        elif not TAG_RE.match(t.name):
            errs.append(Issue(row=i, msg=f"{label}: letters, digits, '_' and ':' only, starting with a letter."))
        else:
            k = t.name.lower()
            if k in names:
                errs.append(Issue(row=i, msg=f"{label} duplicates row {names[k] + 1}."))
            names.setdefault(k, i)
        if t.source != "plc" and t.source not in serial_ids:
            errs.append(Issue(row=i, msg=f"{label}: source {t.source!r} is not a configured serial device."))
        if t.address < 0:
            errs.append(Issue(row=i, msg=f"{label}: address must be ≥ 0."))
            continue
        span, space = TYPES[t.type]
        if t.type == "bool_bit":
            if t.bit is None or not 0 <= t.bit <= 15:
                errs.append(Issue(row=i, msg=f"{label}: a packed boolean needs a bit number 0–15."))
            elif (t.source, t.address, t.bit) in bits_used:
                errs.append(Issue(row=i, msg=f"{label}: bit {t.bit} of register {t.address} is already used by another tag."))
            bits_used.add((t.source, t.address, t.bit or 0))
        if d.plc.max_address is not None and t.source == "plc" and space == "reg" and t.address + span - 1 > d.plc.max_address:
            errs.append(Issue(row=i, msg=f"{label}: {t.type} at {t.address} needs registers up to {t.address + span - 1}, beyond the PLC's highest usable register ({d.plc.max_address})."))
        for k in range(span):
            key = (t.source, space, t.address + k)
            o = occupied.get(key)
            if o is None:
                occupied[key] = t
            elif o.type == "bool_bit" and t.type == "bool_bit":
                continue  # several bits may share one register
            else:
                errs.append(Issue(row=i, msg=f"{label} at {t.address} overlaps “{o.name}” ({o.type} at {o.address})."))
                break
        if t.scale == 0:
            errs.append(Issue(row=i, msg=f"{label}: scale must be non-zero."))
        if t.role == "counter" and t.type in ("bool", "bool_di", "bool_bit", "float32", "float64"):
            warns.append(f"{label}: counters are normally Int32/UInt32 registers; a {t.type} counter will roll over or lose precision.")
        if t.role == "flow" and t.type.startswith("bool"):
            errs.append(Issue(row=i, msg=f"{label}: a boolean cannot be a flow rate."))
        if t.zero_below is not None and t.type.startswith("bool"):
            warns.append(f"{label}: the zero-below filter has no effect on a boolean.")
    if not d.tags:
        errs.append(Issue(msg="Add at least one tag."))

    serial_used = {t.source for t in d.tags if t.source != "plc"}
    if len(serial_used) > 1:
        secs = (len(serial_used) - 1) * HOP_S
        if secs > d.interval_s:
            warns.append(f"{len(serial_used)} serial devices need about {secs} s per read cycle, longer than the {d.interval_s} s publish interval. Serial values will repeat between reads.")
    return errs, warns


def poll_plan(tags: list[Tag]) -> list[dict]:
    groups: dict[tuple[str, str], list[tuple[int, int]]] = {}
    for t in tags:
        span, space = TYPES[t.type]
        groups.setdefault((t.source, space), []).append((t.address, t.address + span - 1))
    polls: list[dict] = []
    for (src, space), ranges in groups.items():
        limit = LIMIT[space]
        cur: list | None = None
        merged: list[list] = []
        for start, end in sorted(set(ranges)):
            if cur and start - cur[1] - 1 <= GAP_MERGE and end - cur[0] + 1 <= limit:
                cur[1] = max(cur[1], end)
            else:
                cur = [start, end]
                merged.append(cur)
        for start, end in merged:
            s = start
            while end - s + 1 > limit:
                polls.append({"source": src, "fc": FC[space], "start": s, "count": limit})
                s += limit
            polls.append({"source": src, "fc": FC[space], "start": s, "count": end - s + 1})
    return polls


def cycle_estimate(d: PlantDraft, polls: list[dict]) -> float:
    plc_polls = sum(1 for p in polls if p["source"] == "plc")
    serial_devs = {p["source"] for p in polls if p["source"] != "plc"}
    serial_polls = len(polls) - plc_polls
    serial_s = (len(serial_devs) - 1) * HOP_S + serial_polls * 0.3 if serial_devs else 0
    return max(plc_polls * 0.05, serial_s)


def topic_for(d: PlantDraft) -> str:
    return d.topic or f"{d.id}/pub"


def compile_config(d: PlantDraft, version: int, serial: str | None) -> dict:
    sources: dict[str, dict] = {
        "plc": {"type": "modbus_tcp", "host": d.plc.ip, "port": d.plc.port, "unit_id": d.plc.unit_id, "word_order": d.plc.word_order},
    }
    for s in d.serial:
        sources[s.id] = {"type": "modbus_rtu", "label": s.label, "port": s.port, "baud": s.baud, "parity": s.parity,
                         "slave_id": s.slave_id, "word_order": s.word_order, "inter_device_delay_s": HOP_S}
    topic = topic_for(d)
    analog = [{"tag": t.name, "source": t.source, "start": t.address, "end": t.address + TYPES[t.type][0] - 1,
               "data_type": LEGACY_TYPE[t.type], "word_order": sources[t.source]["word_order"], "Scaling_Factor": t.scale,
               "is_flow": t.role == "flow", "is_counter": t.role == "counter", "zero_below": t.zero_below, "unit": t.unit}
              for t in d.tags if not t.type.startswith("bool")]
    boolean = [{"tag": t.name, "source": t.source, "address": t.address, "fc": FC[TYPES[t.type][1]], "bit_index": t.bit}
               for t in d.tags if t.type.startswith("bool")]
    return {
        "version": version,
        "plant_id": d.id,
        "plc_id": d.plc_id,
        "device": {"serial": serial},
        "publish": {"topic": topic, "control_topic": f"{d.id}/control", "health_topic": f"{d.id}/health",
                    "interval_s": d.interval_s, "buffer_on_disconnect": True, "buffer_drain_per_cycle": 1},
        "debug": d.debug,
        "sources": sources,
        "polls": poll_plan(d.tags),
        "tags": [t.model_dump(exclude={"totalizer"}) for t in d.tags],
        # ---- flat keys read by the existing main.py / core/config_loader.py ----
        "PLANT_ID": d.id,
        "PLC_ID": d.plc_id,
        "POLLING_INTERVAL": d.interval_s,
        "MODBUS_HOST": d.plc.ip,
        "MODBUS_PORT": d.plc.port,
        "MODBUS_UNIT_ID": d.plc.unit_id,
        "MQTT_TOPIC": topic,
        "DEBUG_OPTION": d.debug,
        "ANALOG_SENSORS": analog,
        "BOOLEAN_SENSORS": boolean,
    }
