"""AWS IoT Core side of the agent: fleet provisioning by claim, Jobs, remote control, MQTT publish.

Mirrors core/provisioning.py, core/aws_jobs_handler.py and core/mqtt_client.py on the
deployed build: pre-flight checks (socket to the endpoint, certificate headers) before
connecting, exponential back-off on retries, jobs fetched through jobs/{id}/get after a
notify-next, and a health-check publish once connected.

Files the base image ships in --certs (never per-plant, never from a developer laptop):
    AmazonRootCA1.pem   claim.pem.crt   claim.private.key
Files the device creates for itself in its state directory on first boot:
    device.pem.crt      device.private.key   provisioning_state.json
"""
from __future__ import annotations

import json
import logging
import socket
import ssl
import threading
import time
from pathlib import Path
from typing import Callable

import paho.mqtt.client as mqtt

log = logging.getLogger("aws")
MAX_RETRY_ATTEMPTS = 6


def preflight(endpoint: str, port: int, files: list[Path]) -> None:
    """Fail early with a clear message instead of a vague TLS error."""
    try:
        with socket.create_connection((endpoint, port), timeout=5):
            pass
    except OSError as e:
        raise RuntimeError(f"cannot reach {endpoint}:{port} ({e}); check internet / firewall") from None
    for f in files:
        if not f.exists() or f.stat().st_size == 0:
            raise RuntimeError(f"missing or empty certificate file {f}")
        head = f.read_text(errors="ignore").lstrip()[:40]
        if "-----BEGIN" not in head:
            raise RuntimeError(f"{f.name} does not look like a PEM file")


class AwsIot:
    def __init__(self, endpoint: str, serial: str, state_dir: Path, certs_dir: Path, template: str, port: int = 8883):
        self.endpoint = endpoint
        self.port = port
        self.serial = serial
        self.state = state_dir
        self.certs = certs_dir
        self.template = template
        self.thing = serial
        self.client: mqtt.Client | None = None
        self.on_job: Callable[[str, dict], None] | None = None
        self.on_control: Callable[[dict], None] | None = None
        self.control_topic: str | None = None
        self.health: Callable[[], dict] | None = None
        self.health_topic: str | None = None
        self.connected = threading.Event()
        self.seen_jobs: set[str] = set()

    # ---------- plumbing ----------
    def _client(self, cert: Path, key: Path, client_id: str) -> mqtt.Client:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv311)
        c.tls_set(ca_certs=str(self.certs / "AmazonRootCA1.pem"), certfile=str(cert), keyfile=str(key), tls_version=ssl.PROTOCOL_TLS_CLIENT)
        return c

    @staticmethod
    def _request(c: mqtt.Client, request_topic: str, body: dict, accepted: str, rejected: str, timeout: float = 15) -> dict:
        """Publish once and wait for the accepted/rejected reply on the paired topics."""
        done = threading.Event()
        out: dict = {}

        def on_msg(client, userdata, msg):
            out["topic"] = msg.topic
            out["body"] = json.loads(msg.payload or b"{}")
            done.set()

        c.message_callback_add(accepted, on_msg)
        c.message_callback_add(rejected, on_msg)
        c.subscribe([(accepted, 1), (rejected, 1)])
        c.publish(request_topic, json.dumps(body), qos=1)
        if not done.wait(timeout):
            raise TimeoutError(f"no reply on {accepted}")
        c.message_callback_remove(accepted)
        c.message_callback_remove(rejected)
        if out["topic"] == rejected:
            raise RuntimeError(f"rejected: {out['body']}")
        return out["body"]

    def _connect_with_retries(self, c: mqtt.Client) -> None:
        ready = threading.Event()
        c.on_connect = lambda *a: ready.set()
        delay = 1.0
        for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
            try:
                c.connect(self.endpoint, self.port, keepalive=30)
                c.loop_start()
                if ready.wait(15):
                    return
                c.loop_stop()
                raise TimeoutError("connect timed out")
            except (OSError, TimeoutError, ssl.SSLError) as e:
                log.warning("connect attempt %d/%d failed: %s · retry in %.0fs", attempt, MAX_RETRY_ATTEMPTS, e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 60)
        raise RuntimeError(f"could not connect to {self.endpoint} after {MAX_RETRY_ATTEMPTS} attempts")

    # ---------- provisioning ----------
    def provisioned(self) -> bool:
        st = self.state / "provisioning_state.json"
        if not st.exists():
            return False
        try:
            return bool(json.loads(st.read_text()).get("complete")) and (self.state / "device.pem.crt").exists()
        except ValueError:
            return False

    def provision(self) -> None:
        if self.provisioned():
            self.thing = json.loads((self.state / "provisioning_state.json").read_text()).get("thingName", self.serial)
            return
        claim_cert, claim_key = self.certs / "claim.pem.crt", self.certs / "claim.private.key"
        preflight(self.endpoint, self.port, [self.certs / "AmazonRootCA1.pem", claim_cert, claim_key])
        log.info("provisioning %s with the claim certificate", self.serial)
        c = self._client(claim_cert, claim_key, f"claim-{self.serial}")
        self._connect_with_retries(c)
        try:
            cert = self._request(c, "$aws/certificates/create/json", {},
                                 "$aws/certificates/create/json/accepted", "$aws/certificates/create/json/rejected")
            reg = self._request(c, f"$aws/provisioning-templates/{self.template}/provision/json",
                                {"certificateOwnershipToken": cert["certificateOwnershipToken"], "parameters": {"SerialNumber": self.serial}},
                                f"$aws/provisioning-templates/{self.template}/provision/json/accepted",
                                f"$aws/provisioning-templates/{self.template}/provision/json/rejected")
            (self.state / "device.pem.crt").write_text(cert["certificatePem"])
            (self.state / "device.private.key").write_text(cert["privateKey"])
            self.thing = reg.get("thingName", self.serial)
            (self.state / "provisioning_state.json").write_text(json.dumps({"complete": True, "thingName": self.thing, "certificateId": cert["certificateId"], "at": time.time()}))
            log.info("provisioned as thing %s", self.thing)
        finally:
            c.loop_stop()
            c.disconnect()

    # ---------- steady state ----------
    def connect(self, on_job: Callable[[str, dict], None], on_control: Callable[[dict], None] | None = None, control_topic: str | None = None,
                health: Callable[[], dict] | None = None, health_topic: str | None = None) -> None:
        self.on_job, self.on_control, self.control_topic, self.health, self.health_topic = on_job, on_control, control_topic, health, health_topic
        cert, key = self.state / "device.pem.crt", self.state / "device.private.key"
        preflight(self.endpoint, self.port, [self.certs / "AmazonRootCA1.pem", cert, key])
        c = self._client(cert, key, self.thing)
        c.on_connect = self._on_connect
        c.on_disconnect = lambda *a: self.connected.clear()
        c.on_message = self._on_message
        c.reconnect_delay_set(1, 60)
        c.connect_async(self.endpoint, self.port, keepalive=60)
        c.loop_start()
        self.client = c

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            log.warning("connect failed: %s", reason_code)
            return
        base = f"$aws/things/{self.thing}/jobs"
        subs = [(f"{base}/notify-next", 1), (f"{base}/$next/get/accepted", 1), (f"{base}/+/get/accepted", 1)]
        if self.control_topic:
            subs.append((self.control_topic, 1))
        client.subscribe(subs)
        client.publish(f"{base}/$next/get", json.dumps({}), qos=1)   # anything queued while we were off?
        if self.health and self.health_topic:
            client.publish(self.health_topic, json.dumps({"event": "connected", **self.health()}), qos=0)
        self.connected.set()
        log.info("connected to IoT Core as %s", self.thing)

    def _on_message(self, client, userdata, msg):
        try:
            body = json.loads(msg.payload or b"{}")
        except ValueError:
            return
        if self.control_topic and msg.topic == self.control_topic:
            if self.on_control:
                self.on_control(body)
            return
        ex = body.get("execution")
        if not ex or ex.get("status") not in ("QUEUED", "IN_PROGRESS"):
            return
        job_id = ex["jobId"]
        if "jobDocument" in ex:
            if job_id in self.seen_jobs:
                return
            self.seen_jobs.add(job_id)
            if self.on_job:
                threading.Thread(target=self.on_job, args=(job_id, ex["jobDocument"]), daemon=True).start()
        else:   # a bare notification: ask for the full document
            client.publish(f"$aws/things/{self.thing}/jobs/{job_id}/get", json.dumps({"includeJobDocument": True}), qos=1)

    def job_update(self, job_id: str, ok: bool, message: str) -> None:
        if self.client:
            self.client.publish(f"$aws/things/{self.thing}/jobs/{job_id}/update",
                                json.dumps({"status": "SUCCEEDED" if ok else "FAILED", "statusDetails": {"message": message[:250]}}), qos=1)

    def publish(self, topic: str, body: dict, timeout: float = 5) -> bool:
        if not self.client or not self.connected.is_set():
            return False
        info = self.client.publish(topic, json.dumps(body), qos=1)
        try:
            info.wait_for_publish(timeout)
        except (ValueError, RuntimeError):
            return False
        return info.is_published()

    def close(self) -> None:
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
