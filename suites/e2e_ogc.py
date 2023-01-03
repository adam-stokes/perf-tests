"""Test Steps"""
from __future__ import annotations

import os
import uuid
from typing import Mapping, NotRequired, TypedDict

from assertpy import assert_that
from ogc.deployer import Deployer
from ogc.provision import choose_provisioner

from framework.api import ApiRequestor
from framework.artifacts import Artifacts, ArtifactsApi
from framework.harness import Harness as H
from framework.log import get_logger
from framework.step import Step
from perf_lib import dict2json, msg
import layouts.ubuntu
from framework.workspace import WorkspaceCluster

log = get_logger("oblt-perf")


class Step00(Step):
    """Bring up machines via OGC

    1. Reads the defined `ogc.models.Layout`
    2. Initializes provisioner (GCE)
    3. Provisions machines based on `ogc.models.Layout` (ie how many machines, OS, sizes)
    4. Downloads and installs elastic-agent on all machines
    5. Enroll agents using a new policy id or existing one"""

    name = "Step00"
    description = "test step 0"
    iterations = 1

    def execute(self):
        """Execute Step00: Horde Depolyment Cleanup"""
        policy_id = self.context.get("policyId", str(uuid.uuid4()))
        assert_that(policy_id).is_not_none()

        policy = self.fleet.get_policy_by_id(policy_id)
        if len(policy) > 0:
            policy = policy[0]
        else:
            new_policy = self.fleet.create_policy(name=policy_id)
            policy = self.fleet.get_policy_by_name(new_policy[0].name)
            assert_that(len(policy)).described_as(
                msg("New Policy created")
            ).is_equal_to(1)
            policy = policy[0]

        log.info(f"Vms will be under policy: {dict2json(policy)}")
        enrollment = self.fleet.create_enrollment_keys(policy, self.cluster.directory)

        # Get latest agent and expose the URL via environment
        artifact = Artifacts(api_r=ApiRequestor(endpoint=ArtifactsApi()))
        os.environ["OGC_ELASTIC_AGENT_URL"] = artifact.latest_agent("8.5")
        os.environ["OGC_FLEET_ENROLLMENT_TOKEN"] = enrollment.api_key
        os.environ["OGC_FLEET_URL"] = self.cluster.get("fleet.url")

        layout = layouts.ubuntu.layout

        provisioner = choose_provisioner(layout=layout)
        self.context["ogc"] = Deployer.from_provisioner(provisioner=provisioner)

        log.info(f"Deploying machines")

        self.context["ogc"].up()
        log.info(
            f"Fleet Status: {dict2json(self.fleet.get_agent_status('local_metadata.host.hostname like ogc*'))}"
        )

        log.info("Registering Elastic Agents")
        self.context["ogc"].exec_scripts()


class Step01(Step):
    """Teardown machines via OGC"""

    name = "Step01"
    description = "Teardown ogc"
    iterations = 1

    def execute(self):
        """Execute OGC teardown"""
        if self.context["ogc"]:
            log.info(self.context)
            log.info(
                f"Fleet Status: {dict2json(self.fleet.get_agent_status('local_metadata.host.hostname like ogc*'))}"
            )
            log.info("Teardown")
            self.context["ogc"].down()


class StepContext(TypedDict):
    """Context"""

    policyId: NotRequired[str]
    ogc: NotRequired[Deployer]


def run(cluster: WorkspaceCluster, harness_context: Mapping[str, str] | None) -> int:
    """Test executing of steps"""

    ctx_default = StepContext()
    if harness_context:
        ctx_default.update(harness_context)

    hrns = H(
        name="A test harness",
        description="Test harness",
        iterations=1,
        suite="ogc",
        cluster=cluster,
        context=harness_context,
    )
    hrns.setup()
    hrns.add(Step00())
    hrns.add(Step01())
    hrns.execute()
    return hrns.teardown()
