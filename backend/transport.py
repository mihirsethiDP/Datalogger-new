"""How a config job reaches the device and how data comes back.

Two implementations behind one interface so the dashboard and the agent do not care:

* MockTransport  — the device collects jobs on its HTTP heartbeat and posts data over HTTP.
* AwsIotTransport — jobs go through AWS IoT Core Jobs; data arrives on an SQS queue fed by an
  IoT rule on dp/+/+/data. Selected with DL_TRANSPORT=aws.

The IoT Thing is named after the Pi's serial (it provisions itself before it has a plant);
the plant identity lives in the topics, and a per-device policy restricted to that plant's
topics is attached to the device certificate when the config is pushed. Nothing here ever
handles a device certificate.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Callable

log = logging.getLogger("transport")

# IoT job execution status → our job status
STATUS_MAP = {
    "QUEUED": "sent", "IN_PROGRESS": "sent", "SUCCEEDED": "applied",
    "FAILED": "failed", "TIMED_OUT": "failed", "REJECTED": "failed", "REMOVED": "failed", "CANCELED": "failed",
}


def policy_document(serial: str, document: dict, region: str, account: str) -> dict:
    """Least-privilege policy for one device on one plant: its data and health topics,
    its remote-control topic, and its own Jobs topics."""
    arn = f"arn:aws:iot:{region}:{account}"
    jobs = f"$aws/things/{serial}/jobs/*"
    pub = document["publish"]
    return {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "iot:Connect", "Resource": f"{arn}:client/{serial}"},
            {"Effect": "Allow", "Action": "iot:Publish", "Resource": [f"{arn}:topic/{pub['topic']}", f"{arn}:topic/{pub['health_topic']}", f"{arn}:topic/{jobs}"]},
            {"Effect": "Allow", "Action": "iot:Subscribe", "Resource": [f"{arn}:topicfilter/{jobs}", f"{arn}:topicfilter/{pub['control_topic']}"]},
            {"Effect": "Allow", "Action": "iot:Receive", "Resource": [f"{arn}:topic/{jobs}", f"{arn}:topic/{pub['control_topic']}"]},
        ],
    }


class MockTransport:
    name = "mock"

    def prepare_device(self, serial: str, document: dict) -> None:
        return None

    def deliver_job(self, job_id: str, serial: str, document: dict) -> str:
        return "queued"  # the device picks it up on its next heartbeat

    def job_status(self, job_id: str, serial: str) -> str | None:
        return None

    def start_inbound(self, on_payload: Callable[[dict], None]) -> None:
        return None


class AwsIotTransport:
    name = "aws"

    def __init__(self, iot=None, sts=None, sqs=None, region: str | None = None):
        import boto3

        self.region = region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not self.region:
            raise RuntimeError("DL_TRANSPORT=aws needs AWS_REGION (and credentials) in the environment")
        self.iot = iot or boto3.client("iot", region_name=self.region)
        self.sts = sts or boto3.client("sts", region_name=self.region)
        self.sqs = sqs
        self.sqs_url = os.environ.get("DL_SQS_URL")
        self.account = self.sts.get_caller_identity()["Account"]

    # -- helpers --
    def _thing_arn(self, serial: str) -> str:
        try:
            return self.iot.describe_thing(thingName=serial)["thingArn"]
        except self.iot.exceptions.ResourceNotFoundException:
            raise RuntimeError(f"{serial} has not provisioned itself in IoT Core yet. Power it on with internet and the bootstrap certificate, then push again.") from None

    def _upsert_policy(self, name: str, document: dict) -> None:
        body = json.dumps(document)
        try:
            self.iot.create_policy(policyName=name, policyDocument=body)
            return
        except self.iot.exceptions.ResourceAlreadyExistsException:
            pass
        versions = self.iot.list_policy_versions(policyName=name)["policyVersions"]
        if len(versions) >= 5:  # IoT keeps at most five; drop the oldest non-default
            oldest = sorted((v for v in versions if not v["isDefaultVersion"]), key=lambda v: v["createDate"])[0]
            self.iot.delete_policy_version(policyName=name, policyVersionId=oldest["versionId"])
        self.iot.create_policy_version(policyName=name, policyDocument=body, setAsDefault=True)

    # -- interface --
    def prepare_device(self, serial: str, document: dict) -> None:
        """Attach a policy limited to this plant's topics to every certificate the device holds."""
        self._thing_arn(serial)
        principals = self.iot.list_thing_principals(thingName=serial)["principals"]
        if not principals:
            raise RuntimeError(f"{serial} exists in IoT Core but has no certificate attached; provisioning did not finish.")
        name = f"dl-device-{serial}"
        self._upsert_policy(name, policy_document(serial, document, self.region, self.account))
        for p in principals:
            self.iot.attach_policy(policyName=name, target=p)

    def deliver_job(self, job_id: str, serial: str, document: dict) -> str:
        self.iot.create_job(
            jobId=f"cfg-{job_id}", targets=[self._thing_arn(serial)], document=json.dumps(document),
            targetSelection="SNAPSHOT", description=f"config v{document['version']} for {document['plant_id']}",
        )
        return "sent"

    def job_status(self, job_id: str, serial: str) -> str | None:
        try:
            ex = self.iot.describe_job_execution(jobId=f"cfg-{job_id}", thingName=serial)["execution"]
        except self.iot.exceptions.ResourceNotFoundException:
            return None
        return STATUS_MAP.get(ex["status"])

    def start_inbound(self, on_payload: Callable[[dict], None]) -> None:
        if not self.sqs_url:
            log.warning("DL_SQS_URL not set: payloads published over MQTT will not reach the dashboard")
            return
        import boto3

        sqs = self.sqs or boto3.client("sqs", region_name=self.region)

        def loop():
            while True:
                try:
                    msgs = sqs.receive_message(QueueUrl=self.sqs_url, MaxNumberOfMessages=10, WaitTimeSeconds=20).get("Messages", [])
                    for m in msgs:
                        try:
                            on_payload(json.loads(m["Body"]))
                        except Exception:  # noqa: BLE001
                            log.exception("bad payload on queue")
                        sqs.delete_message(QueueUrl=self.sqs_url, ReceiptHandle=m["ReceiptHandle"])
                except Exception:  # noqa: BLE001
                    log.exception("sqs receive failed; retrying")
                    threading.Event().wait(5)

        threading.Thread(target=loop, name="sqs-inbound", daemon=True).start()


def make_transport():
    return AwsIotTransport() if os.environ.get("DL_TRANSPORT", "mock") == "aws" else MockTransport()
