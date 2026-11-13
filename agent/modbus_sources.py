"""Modbus readers for the agent: real pymodbus clients, or a mock register bank.

Real clients follow core/modbus_client.py on the deployed build: one shared TCP client,
a lock around every operation, and reconnection attempts when a read finds the socket
closed. Serial devices on one converter are read with the configured pause between
slaves. The mock encodes plausible values with each source's word order, so the
agent's decode path is exercised exactly as it would be against a PLC.
"""
from __future__ import annotations

import logging
import math
import struct
import threading
import time

log = logging.getLogger("modbus")


def _encode(v: float | int, typ: str, order: str) -> list[int]:
    fmt = {"int32": ">i", "uint32": ">I", "float32": ">f", "float64": ">d"}[typ]
    if typ in ("int32", "uint32"):
        v = int(v)
    raw = struct.pack(fmt, v)
    words = list(struct.unpack(f">{len(raw) // 2}H", raw))
    if order == "CDAB":
        words = words[::-1]
    elif order == "DCBA":
        words = list(struct.unpack(f">{len(raw) // 2}H", raw[::-1]))
    elif order == "BADC":
        words = [struct.unpack("<H", struct.pack(">H", w))[0] for w in words]
    return words


class MockBank:
    """Pretends to be every source in the config. Values drift slowly so dashboards move."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.tags = cfg["tags"]
        self.t0 = time.time()

    def _value(self, tag: dict, t: float) -> float:
        seed = sum(ord(c) for c in tag["name"])
        if tag["type"].startswith("bool"):
            return 1 if math.sin(t / 37 + seed) > -0.3 else 0
        if tag.get("role") == "counter":
            return (t - self.t0) * 0.8 + seed * 10          # climbs like a counter
        base = {"ph": 7.2, "do": 2.4, "orp": 140, "mlss": 3200, "fmf": 185, "flow": 185, "lt": 250, "level": 2.1, "kwh": 1500, "pto": -0.3}
        key = next((k for k in base if k in tag["name"].lower()), None)
        centre = base[key] if key else 50 + seed % 40
        return centre * (1 + 0.04 * math.sin(t / 23 + seed)) / (tag.get("scale", 1) or 1)

    def read(self, source: str, fc: int, start: int, count: int) -> list[int]:
        t = time.time()
        regs = [0] * count
        order = self.cfg["sources"].get(source, {}).get("word_order", "ABCD")
        for tag in self.tags:
            typ = tag["type"]
            tag_fc = {"bool": 1, "bool_di": 2}.get(typ, 3)
            if tag["source"] != source or tag_fc != fc:
                continue
            off = tag["address"] - start
            if off < 0 or off >= count:
                continue
            v = self._value(tag, t)
            if typ in ("bool", "bool_di"):
                regs[off] = int(v)
            elif typ == "bool_bit":
                regs[off] |= int(v) << int(tag.get("bit") or 0)
            elif typ == "int16":
                regs[off] = struct.unpack(">H", struct.pack(">h", max(-32768, min(32767, int(v)))))[0]
            elif typ == "uint16":
                regs[off] = max(0, min(65535, int(v)))
            else:
                words = _encode(v, typ, order)
                if off + len(words) <= count:
                    regs[off:off + len(words)] = words
        return regs

    def close(self):
        pass


class ModbusSources:
    """Real clients. One TCP client for the PLC, one serial client per USB port."""

    def __init__(self, cfg: dict):
        from pymodbus.client import ModbusSerialClient, ModbusTcpClient

        self.cfg = cfg
        self.lock = threading.Lock()
        self.clients: dict[str, object] = {}
        self.slave: dict[str, int] = {}
        self.delay: dict[str, float] = {}
        self.last_serial: tuple[str, int] | None = None
        self.last_serial_at = 0.0
        for sid, s in cfg["sources"].items():
            if s["type"] == "modbus_tcp":
                self.clients[sid] = ModbusTcpClient(s["host"], port=s["port"], timeout=3)
                self.slave[sid] = s["unit_id"]
            else:
                key = "serial:" + s["port"]
                if key not in self.clients:
                    self.clients[key] = ModbusSerialClient(port=s["port"], baudrate=s["baud"], parity=s["parity"], stopbits=1, bytesize=8, timeout=2)
                self.clients[sid] = self.clients[key]
                self.slave[sid] = s["slave_id"]
                self.delay[sid] = s.get("inter_device_delay_s", 0)

    def _pause_for_bus(self, source: str):
        port = self.cfg["sources"][source]["port"]
        if self.last_serial and self.last_serial[0] == port and self.last_serial[1] != self.slave[source]:
            wait = self.delay[source] - (time.monotonic() - self.last_serial_at)
            if wait > 0:
                time.sleep(wait)
        self.last_serial = (port, self.slave[source])
        self.last_serial_at = time.monotonic()

    def read(self, source: str, fc: int, start: int, count: int) -> list[int] | None:
        c = self.clients[source]
        if source in self.delay:
            self._pause_for_bus(source)
        with self.lock:
            try:
                if not c.connected and not c.connect():
                    log.warning("%s: cannot connect", source)
                    return None
                slave = self.slave[source]
                if fc == 1:
                    rr = c.read_coils(start, count=count, slave=slave)
                elif fc == 2:
                    rr = c.read_discrete_inputs(start, count=count, slave=slave)
                else:
                    rr = c.read_holding_registers(start, count=count, slave=slave)
                if rr.isError():
                    log.warning("%s: %s", source, rr)
                    return None
                return [int(b) for b in rr.bits[:count]] if fc in (1, 2) else list(rr.registers)
            except Exception as e:  # noqa: BLE001
                log.warning("%s: %s · reconnecting", source, e)
                try:
                    c.close()
                except Exception:  # noqa: BLE001
                    pass
                return None

    def close(self):
        with self.lock:
            for c in set(self.clients.values()):
                try:
                    c.close()
                except Exception:  # noqa: BLE001
                    pass


def open_sources(cfg: dict, mock: bool):
    return MockBank(cfg) if mock else ModbusSources(cfg)
