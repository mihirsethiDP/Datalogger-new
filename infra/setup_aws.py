"""One-time AWS setup for the datalogger fleet. Idempotent; safe to re-run.

    AWS_REGION=ap-south-1 python infra/setup_aws.py --out agent/certs

Creates:
  * a claim policy + claim certificate (the bootstrap identity baked into the base image),
  * an IAM role and a fleet-provisioning template that names each Thing after the Pi serial
    and attaches a baseline policy (connect as yourself, use your own Jobs topics),
  * an SQS queue and an IoT rule that forwards every dp/+/+/data message to it,
  * and prints the environment variables the backend and agent need.

Per-plant publish permissions are NOT granted here; the backend attaches them per device when
a config is pushed (see backend/transport.py). Needs admin-level IoT/IAM/SQS rights to run.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import boto3

PREFIX = "dl"
TEMPLATE = f"{PREFIX}-datalogger"
QUEUE = f"{PREFIX}-plant-data"


def already(fn, *a, **kw):
    """Call fn; return None if the resource already exists."""
    try:
        return fn(*a, **kw)
    except Exception as e:  # noqa: BLE001
        if "AlreadyExists" in e.__class__.__name__ or "EntityAlreadyExists" in str(e):
            return None
        raise


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="agent/certs", help="where to write the claim certificate files")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"))
    a = ap.parse_args()
    if not a.region:
        sys.exit("set AWS_REGION")
    iot = boto3.client("iot", region_name=a.region)
    iam = boto3.client("iam")
    sqs = boto3.client("sqs", region_name=a.region)
    account = boto3.client("sts").get_caller_identity()["Account"]
    arn = f"arn:aws:iot:{a.region}:{account}"
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    # 1. Baseline device policy: a provisioned device may connect as itself and talk to its own Jobs topics.
    device_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "iot:Connect", "Resource": f"{arn}:client/${{iot:Connection.Thing.ThingName}}"},
            {"Effect": "Allow", "Action": ["iot:Publish", "iot:Receive"], "Resource": f"{arn}:topic/$aws/things/${{iot:Connection.Thing.ThingName}}/jobs/*"},
            {"Effect": "Allow", "Action": "iot:Subscribe", "Resource": f"{arn}:topicfilter/$aws/things/${{iot:Connection.Thing.ThingName}}/jobs/*"},
        ],
    }
    # Plant topics (<plant>/pub, <plant>/health, <plant>/control) are granted per device by the backend at push time.
    already(iot.create_policy, policyName=f"{PREFIX}-device-baseline", policyDocument=json.dumps(device_policy))

    # 2. Claim policy: only enough to ask for a certificate and register through the template.
    claim_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "iot:Connect", "Resource": "*"},
            {"Effect": "Allow", "Action": ["iot:Publish", "iot:Receive"], "Resource": [
                f"{arn}:topic/$aws/certificates/create/*", f"{arn}:topic/$aws/provisioning-templates/{TEMPLATE}/provision/*"]},
            {"Effect": "Allow", "Action": "iot:Subscribe", "Resource": [
                f"{arn}:topicfilter/$aws/certificates/create/*", f"{arn}:topicfilter/$aws/provisioning-templates/{TEMPLATE}/provision/*"]},
        ],
    }
    already(iot.create_policy, policyName=f"{PREFIX}-claim", policyDocument=json.dumps(claim_policy))

    # 3. Claim certificate for the base image (written once; re-runs keep the existing files).
    if not (out / "claim.pem.crt").exists():
        keys = iot.create_keys_and_certificate(setAsActive=True)
        (out / "claim.pem.crt").write_text(keys["certificatePem"])
        (out / "claim.private.key").write_text(keys["keyPair"]["PrivateKey"])
        iot.attach_policy(policyName=f"{PREFIX}-claim", target=keys["certificateArn"])
        print(f"claim certificate {keys['certificateId']} written to {out}/ — bake these into the base image")
    if not (out / "AmazonRootCA1.pem").exists():
        import urllib.request

        urllib.request.urlretrieve("https://www.amazontrust.com/repository/AmazonRootCA1.pem", out / "AmazonRootCA1.pem")

    # 4. Provisioning role + template.
    trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "iot.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
    role_name = f"{PREFIX}-provisioning-role"
    already(iam.create_role, RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust))
    iam.attach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSIoTThingsRegistration")
    role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    template_body = {
        "Parameters": {"SerialNumber": {"Type": "String"}, "AWS::IoT::Certificate::Id": {"Type": "String"}},
        "Resources": {
            "certificate": {"Type": "AWS::IoT::Certificate", "Properties": {"CertificateId": {"Ref": "AWS::IoT::Certificate::Id"}, "Status": "Active"}},
            "policy": {"Type": "AWS::IoT::Policy", "Properties": {"PolicyName": f"{PREFIX}-device-baseline"}},
            "thing": {"Type": "AWS::IoT::Thing", "Properties": {"ThingName": {"Ref": "SerialNumber"}, "AttributePayload": {"fleet": PREFIX}}},
        },
    }
    if already(iot.create_provisioning_template, templateName=TEMPLATE, templateBody=json.dumps(template_body), enabled=True, provisioningRoleArn=role_arn) is None:
        iot.create_provisioning_template_version(templateName=TEMPLATE, templateBody=json.dumps(template_body), setAsDefault=True)

    # 5. SQS queue + IoT rule for plant data.
    queue_url = sqs.create_queue(QueueName=QUEUE)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    rule_role = f"{PREFIX}-rule-role"
    already(iam.create_role, RoleName=rule_role, AssumeRolePolicyDocument=json.dumps(trust))
    iam.put_role_policy(RoleName=rule_role, PolicyName="sqs-send", PolicyDocument=json.dumps(
        {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sqs:SendMessage", "Resource": queue_arn}]}))
    rule_role_arn = iam.get_role(RoleName=rule_role)["Role"]["Arn"]
    # one rule for every site instead of one per site: topics follow the <plant_id>/pub convention
    rule = {"sql": "SELECT *, topic(1) AS plantId FROM '+/pub'", "awsIotSqlVersion": "2016-03-23", "ruleDisabled": False,
            "actions": [{"sqs": {"queueUrl": queue_url, "roleArn": rule_role_arn, "useBase64": False}}]}
    try:
        iot.create_topic_rule(ruleName=f"{PREFIX}_plant_data_to_sqs", topicRulePayload=rule)
    except iot.exceptions.ResourceAlreadyExistsException:
        iot.replace_topic_rule(ruleName=f"{PREFIX}_plant_data_to_sqs", topicRulePayload=rule)

    endpoint = iot.describe_endpoint(endpointType="iot:Data-ATS")["endpointAddress"]
    print("\nBackend environment:")
    print(f"  DL_TRANSPORT=aws AWS_REGION={a.region} DL_SQS_URL={queue_url}")
    print("Agent:")
    print(f"  python -m agent.agent --transport aws --endpoint {endpoint} --certs {out} --template {TEMPLATE}")


if __name__ == "__main__":
    main()
