"""Test Steps"""
from __future__ import annotations

import os
import time
import typing as t
import uuid
from pathlib import Path

from assertpy import assert_that

from framework.fleet import Inputs, get_integration_packages
from framework.harness import Harness as H
from framework.log import get_logger
from framework.step import Step
from framework.swarm import SwarmFactory
from framework.workspace import WorkspaceCluster
from perf_lib import (
    add_integration_to_policy,
    add_policy_if_missing,
    change_config_in_policy,
    cluster_report,
    get_integrations,
    move_swarm_to_policy,
    msg,
    poll_action_id_status,
    poll_agents_status,
    prepare_fleet_server_inputs,
    update_policy_config,
)

log = get_logger("oblt-perf")


class StepInitializeEnv(Step):
    """Initialization"""

    name = "Initialize"
    description = "test step 0: Initialize run"

    def execute(self):
        max_agents = self.context["maxAgents"]
        fleet = self.fleet
        log.info(fleet.get_agent_status("local_metadata.host.hostname like eh*"))
        policy = fleet.get_policy_by_id("policy-elastic-agent-on-cloud")[0]
        log.debug(policy)
        log.info(f"{policy.name} at revision {policy.revision}")
        log.info(f"set max_agents in {policy.name} to {max_agents}")
        inputs_file = prepare_fleet_server_inputs(self.cluster_directory, max_agents)
        update_policy_config(
            fleet,
            "policy-elastic-agent-on-cloud",
            "fleet_server",
            inputs_file,
        )


class Step01(Step):
    """Initial step to create policy and add integration"""

    name = "Step01"
    description = "test step 1: Create initial policies"
    iterations = 1

    def execute(self) -> None:
        """Creates the initial policies"""
        fleet = self.fleet
        policy_name = self.context["policies.0"]

        if len(fleet.get_policy_by_name(policy_name)) == 0:
            log.info(f"Adding policy {policy_name}")

            policy = fleet.create_policy(name=policy_name, unique=False)

            assert_that(policy).described_as(msg("Policy Created")).is_length(1)

            inputs_to_process = [
                ("system", Path("fixtures/package_policies/inputs-system.json")),
                ("endpoint", Path("fixtures/package_policies/inputs-endpoint.json")),
                (
                    "elastic_agent",
                    Path("fixtures/package_policies/inputs-elastic_agent.json"),
                ),
            ]
            for integration, inputs_json in inputs_to_process:
                fleet.create_package_policy(
                    policy=policy[0],
                    integration=integration,
                    inputs=Inputs.from_json(inputs_json),
                )

            assert_that(
                get_integration_packages(fleet.get_policy_by_name(policy_name)[0])
            ).described_as(f"Not all integrations added to {policy_name}").is_length(3)

        for policy in self.context["policies"][1:]:
            add_policy_if_missing(fleet, policy, policy_name)

        for policy in self.context["policies"][1:]:
            assert_that(
                get_integration_packages(fleet.get_policy_by_name(policy)[0])
            ).described_as(msg(f"Not all integrations added to {policy}")).is_length(3)


class Step02(Step):
    """Bring up swarm"""

    name = "Step02"
    description = "test step 2: AC-13 - Bring up swarm"
    iterations = 1

    def execute(self) -> None:
        total_agents = self.context["totalAgents"]
        enroll_rate = self.cluster.get("horde.enrollRate")
        streams = self.cluster.get("horde.stream")
        policies = self.context["policies"]
        fleet = self.fleet

        self.context["swarms"] += SwarmFactory(
            fleet=fleet,
            horde=self.horde,
            cluster_directory=self.cluster_directory,
            name="SwarmL",
            total=total_agents,
            size=total_agents,
            enroll_rate=enroll_rate,
            streams=streams,
            os="linux",
            policy_name=policies[1],
        ).build_swarms()

        if any(obj.deployment is None for obj in self.context["swarms"]):
            raise RuntimeError("Failed building swarms")

        kuery = f"policy_id : {self.context['swarms.0'].policy[0].id}"
        result = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=kuery,
            count=total_agents,
            max_iterations=30,
        )
        status = fleet.get_agent_status(kuery)
        log.info(status)

        assert_that(result).described_as(msg("Swarm online")).is_true()

        log.info(self.context["swarms.0"].restore_file)

        for swarm in self.context["swarms"]:
            self.horde.snapshot(swarm.deployment, swarm.restore_file)

        assert_that(int(status.online)).described_as(
            msg("Number of agents online")
        ).is_equal_to(int(total_agents))


class Step03(Step):
    """Editing a policy that is already in use by multiple agents.

    - Agent policy revision is expected to increment
    - All selected agents to be 'Healthy' (status=online)."""

    name = "Step03"
    description = "test step 3: AC-8 - Change policy config"
    iterations = 1

    def execute(self) -> None:
        """change config"""
        total_agents = self.context["totalAgents"]
        fleet = self.fleet
        policy = fleet.get_policy_full_by_id(self.context["swarms"][0].policy[0].id)[0]
        # check count before
        rev_count = policy.revision
        rev_kuery = "fleet-agents.policy_id : {} and policy_revision_idx : {}"
        status = fleet.get_agent_status(rev_kuery.format(policy.id, rev_count))
        log.info(f"Agent status for {policy.name}: {status}")

        # Change config in policy from inputs-system.json to inputs-system1.json
        config_changed = change_config_in_policy(fleet, policy)
        assert_that(config_changed).described_as(msg("Config changed")).is_true()

        result = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=rev_kuery.format(policy.id, rev_count + 1),
            count=total_agents,
            max_iterations=30,
        )
        assert_that(result).described_as(msg("Drones online")).is_true()

        policy = fleet.get_policy_full_by_id(self.context["swarms"][0].policy[0].id)[0]
        upd_status = fleet.get_agent_status(rev_kuery.format(policy.id, rev_count + 1))
        log.info(
            f"Agent updated status for {policy.name} at rev {policy.revision}: {upd_status}"
        )
        assert_that(status.online).described_as(msg("Updated drones")).is_equal_to(
            total_agents
        )

        updated_policy = fleet.get_policy_full_by_id(policy.id)[0]
        log.info(f"{updated_policy.name} at rev {updated_policy.revision}")
        assert_that(updated_policy.revision).described_as(
            msg("Revision number changed")
        ).is_equal_to(rev_count + 1)


class Step04(Step):
    """Adding a new integration to a policy that is already in use by multiple agents.

    - Osquery_manager integration is expected to be added to a policy
    - Agent policy revision to be bumped
    - All selected agents should remain "Healthy" (online)."""

    name = "Step04"
    description = "test step 4: AC-4 - Add integration to policy"
    iterations = 1

    def execute(self) -> None:
        total_agents = self.context["totalAgents"]
        fleet = self.fleet
        policy = fleet.get_policy_full_by_id(self.context["swarms"][0].policy[0].id)[0]
        rev_count = policy.revision
        rev_kuery = f"fleet-agents.policy_id : {policy.id} and policy_revision_idx : {rev_count + 1}"

        integrations_before = get_integration_packages(policy)
        log.info(
            f"Current list of integrations in {policy.name}: {integrations_before}"
        )

        integration_added = add_integration_to_policy(
            "nginx",
            "fixtures/package_policies/inputs-nginx.json",
            fleet,
            policy,
        )
        assert_that(integration_added).described_as(
            msg("Integration added")
        ).is_not_none()

        log.info("Checking if the changes were applied to all agents...")
        result = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=rev_kuery,
            count=total_agents,
            max_iterations=30,
        )
        assert_that(result).described_as(msg("Drones online")).is_true()
        status = fleet.get_agent_status(rev_kuery)
        log.info(status)
        assert_that(status.online).described_as(msg("Updated drones")).is_equal_to(
            total_agents
        )

        updated_policy = fleet.get_policy_full_by_id(policy.id)[0]
        integrations_after = get_integrations(fleet, updated_policy)
        assert_that(updated_policy.revision).described_as(
            msg("Revision number changed")
        ).is_equal_to(rev_count + 1)
        assert_that(integrations_after).described_as(
            msg("Policy integrations")
        ).is_length(len(integrations_before) + 1)


class Step05(Step):
    """Deleting integration from a policy that is already in use by multiple agents.

    - Osquery_manager integration is expected to be removed from the policy
    - Agent policy revision to be bumped
    - All selected agents should remain "Healthy" (online)."""

    name = "Step05"
    description = "test step 5: AC-9 - Remove integration from policy"
    iterations = 1

    def execute(self) -> None:
        total_agents = self.context["totalAgents"]
        fleet = self.fleet
        policy = fleet.get_policy_full_by_id(self.context["swarms"][0].policy[0].id)[0]
        rev_count = policy.revision
        rev_kuery = f"fleet-agents.policy_id : {policy.id} and policy_revision_idx : {rev_count + 1}"

        integrations_before = get_integrations(fleet, policy)

        policy_deleted = fleet.delete_package_policy_by_name(policy.id, "nginx")
        assert_that(policy_deleted).described_as(
            msg("Integration deleted")
        ).contains_key("id")

        log.info("Checking if the changes were applied to all agents...")
        result = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=rev_kuery,
            count=total_agents,
            max_iterations=30,
        )
        assert_that(result).described_as(msg("Drones online")).is_true()

        status = fleet.get_agent_status(rev_kuery)
        log.info(status)
        assert_that(status.online).described_as(msg("Updated drones")).is_equal_to(
            total_agents
        )

        updated_policy = fleet.get_policy_full_by_id(policy.id)[0]
        integrations_after = get_integrations(fleet, updated_policy)
        assert_that(updated_policy.revision).described_as(
            msg("Policy revision")
        ).is_equal_to(rev_count + 1)
        assert_that(integrations_after).described_as(
            msg("Policy integrations")
        ).is_length(len(integrations_before) - 1)


class Step06(Step):
    """Attempting to delete a policy that is already in use by multiple agents.

    - Response should be 500 and contain specific message.
    - Policy should not be deleted."""

    name = "Step06"
    description = "test step 06: AC-12 - Delete a policy that is already in use by multiple agents"
    iterations = 1

    def execute(self) -> None:
        fleet = self.fleet
        policy = fleet.get_policy_full_by_id(self.context["swarms"][0].policy[0].id)[0]

        response = fleet.delete_policy_by_id(policy.id)
        log.error(response)

        assert_that(response).described_as(
            msg("Expected to receive 500 response")
        ).is_not_empty()
        assert_that(response.get("statusCode")).described_as(
            msg("Expected to receive status code 500")
        ).is_equal_to(500)
        assert_that(response.get("message")).described_as(
            msg("Expected to receive 500 response")
        ).is_equal_to("Cannot delete agent policy that is assigned to agent(s)")


class Step07(Step):
    """Reassigning a large number of agents to a new policy.

    - Selected agents are expected to be reassigned to a new policy in a timely manner (test will be failed otherwise)."""

    name = "Step07"
    description = "test step 7: AC-1 - Move entire swarm to a new policy"
    iterations = 1
    active = True

    def execute(self) -> None:
        total_agents = self.context["totalAgents"]
        fleet = self.fleet
        kuery = "policy_id : {!s}"

        # source policy
        src_policy = self.context["swarms"][0].policy[0]

        # destination policy
        policy_name = self.context["policies"][2]
        dest_policy = fleet.get_policy_by_name(policy_name)[0]

        aid = move_swarm_to_policy(fleet, src_policy, dest_policy)
        swarm_moved = poll_action_id_status(aid, fleet, 20, 10)
        assert_that(swarm_moved).described_as(msg("Swarm moved")).is_true()

        result = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=kuery.format(dest_policy.id),
            count=total_agents,
            max_iterations=30,
        )
        assert_that(result).described_as(msg("Drones online")).is_true()

        src_status = fleet.get_agent_status(kuery.format(src_policy.id))
        log.info(f"Agent status for {src_policy.name}: {src_status}")
        dest_status = fleet.get_agent_status(kuery.format(dest_policy.id))
        log.info(f"Agent status for {policy_name}: {dest_status}")


class Step08(Step):
    """Upgrading multiple agents.

    - Selected agents are expected to be upgraded and healthy in a timely manner."""

    name = "Step08"
    description = "test step 8: AC-2 - Upgrade drones"
    iterations = 1
    active = True  # min 600 rollout time

    def execute(self) -> None:
        """upgrade drones"""
        old_version = "8.2.0"
        new_version = "8.2.1"
        total_agents = self.context["totalAgents"]
        mrds: int = int(600)  # minimum rollout_duration_seconds
        crds = total_agents * 0.03  # calculated rollout_duration_seconds
        duration = mrds if mrds >= crds else crds
        log.info(f"Rollout duration is {duration}")

        fleet = self.fleet
        upgrade_kuery = (
            f"local_metadata.elastic.agent.version : {old_version} "
            f"and local_metadata.elastic.agent.upgradeable : true"
        )
        log.info(fleet.get_agent_status(upgrade_kuery))

        aid = fleet.bulk_upgrade(
            version=new_version, kuery=upgrade_kuery, rollout_duration_seconds=duration
        )
        log.info(f"Upgrade Action ID is {aid}")
        result = poll_action_id_status(aid, fleet, 10, 10)
        assert_that(result).described_as(msg("Fleet Bulk Upgrade")).is_true()

        status_after = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=f"local_metadata.elastic.agent.version : {new_version}",
            count=total_agents,
            max_iterations=30,
        )

        assert_that(status_after).described_as(msg(f"Agent status online")).is_true()


class Step09(Step):
    """Aborting agents upgrade.

    - At first, upgrade action is expected to be created for all agents (nbAgentsActionCreated == number of agents).
    - After cancel action is called
      - upgrade is expected to be cancelled for agents that have not started upgrading yet
      - upgrade is expected to finish for agents that have already started upgrading.
    - Total number of agents in both versions should be equal to the number of agents before upgrade.
    - Should be run togather with Step07 due to dependency on version number"""

    name = "Step09"
    description = "test step 9: AC-11 - Abort agent upgrade"
    iterations = 1

    def execute(self) -> None:
        fleet = self.fleet
        total_agents = self.context["totalAgents"]
        old_version = "8.2.1"
        new_version = "8.2.2"
        upgrade_kuery = (
            "local_metadata.elastic.agent.version : {} "
            "and local_metadata.elastic.agent.upgradeable : true"
        )
        status_begin = fleet.get_agent_status(upgrade_kuery.format(old_version))
        log.info(status_begin)
        log.info("Starting upgrade")
        aid = fleet.bulk_upgrade(
            version=new_version,
            kuery=upgrade_kuery.format(old_version),
            rollout_duration_seconds=600,
        )

        status_interim = fleet.get_agent_status(upgrade_kuery.format(old_version))
        log.info(status_interim)
        assert_that(status_interim.updating).described_as(
            msg("Agents started updating")
        ).is_equal_to(total_agents)

        action_status = fleet.action_id_status(aid)
        log.info(action_status)
        assert_that(action_status).described_as(
            msg("Something went wrong. Action id of the upgrade not found")
        ).is_not_equal_to(None)
        if action_status:
            assert_that(action_status.nbAgentsActionCreated).described_as(
                msg("nbAgentsActionCreated")
            ).is_equal_to(total_agents)
            assert_that(action_status.type).described_as(
                msg("Action type")
            ).is_equal_to("UPGRADE")
            assert_that(action_status.status).described_as(
                msg("Action status")
            ).is_equal_to("IN_PROGRESS")

            log.info("Aborting upgrade...")
            cancel_status = fleet.cancel_action(aid.actionId)
            action_status_cancelled = fleet.action_id_status(aid)
            log.info(f"Upgrade status after cancellation: {action_status_cancelled}")
            assert_that(cancel_status).described_as(msg("Cancel action")).is_true()
            # for small numbers of agents cancel might not work since action is completed immediately
            assert_that(action_status_cancelled.status).described_as(
                msg("Action status")
            ).is_in("CANCELLED", "COMPLETE")

            time.sleep(max(total_agents / 1000, 5))
            status_after = fleet.get_agent_status(upgrade_kuery.format(old_version))
            status_on_new_ver = fleet.get_agent_status(
                upgrade_kuery.format(new_version)
            )
            total = status_after.online + status_on_new_ver.online
            log.info(
                f"Result: {status_after.online} agents canceled, "
                f"{status_on_new_ver.online} upgraded to a new version. Total {total}"
            )
            assert_that(total).described_as(msg("Agents after upgrade")).is_equal_to(
                total_agents
            )


class Step10(Step):
    """Adding/removing tags.

    - Tag "test_tag_{policy.id}" should be created successfully and added to all agents.
    - Then the same tag is expected to be removed from all agents.
    - Tag should not exist on fleet server anymore.
    - Tags can only exist if assigned to at least one agent.
    - All agents should remain "Healthy" (online)"""

    name = "Step10"
    description = "test step 10: AC-14 - Adding/removing tags"
    iterations = 1

    def execute(self) -> None:
        """add/remove tags"""
        total_agents = self.context["totalAgents"]
        fleet = self.fleet
        policy_name = self.context["policies"][2]
        policy = fleet.get_policy_by_name(policy_name)[0]
        kuery = f"policy_id : {policy.id}"
        tag = f"test_tag_{policy.id}"

        status = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=kuery,
            count=total_agents,
            max_iterations=30,
        )
        assert_that(status).described_as(msg(f"Agent status online")).is_true()
        # Add
        tag_added_action_id = fleet.bulk_update_agent_tags(kuery, add_tags=[tag])
        assert_that(tag_added_action_id).described_as(
            msg("Tag added action id created")
        ).is_not_none()
        log.info(f"Add tag: {tag}")
        status_add = poll_action_id_status(tag_added_action_id, fleet, 10, 10)
        assert_that(status_add).described_as(
            msg("Tag adding process finished")
        ).is_true()
        log.info(f"Tags: {fleet.tags()}")
        tag_created = fleet.tag_exists(tag)
        assert_that(tag_created).described_as(
            msg("Tag exist on the FleetServer")
        ).is_true()
        if tag_created:
            log.info(f"Tag {tag} was created successfully")

        # Remove
        tag_removed_action_id = fleet.bulk_update_agent_tags(kuery, remove_tags=[tag])
        assert_that(tag_removed_action_id).described_as(
            msg("Tag removed action id")
        ).is_not_none()
        log.info(f"Remove tag: {tag}")
        status_remove = poll_action_id_status(tag_added_action_id, fleet, 10, 10)
        assert_that(status_remove).described_as(
            msg("Tag removing process finished")
        ).is_true()
        log.info(f"Tags: {fleet.tags()}")
        tag_removed = not fleet.tag_exists(tag)
        assert_that(tag_removed).described_as(
            msg("Tag removed from FleetServer")
        ).is_true()
        if tag_removed:
            log.info(f"Tag {tag} was removed successfully")

        status = poll_agents_status(
            fleet=fleet,
            method="online",
            period=10,
            kuery=kuery,
            count=total_agents,
            max_iterations=30,
        )
        assert_that(status).described_as(msg(f"Agent status online")).is_true()


class Step11(Step):
    """Unenrolling all agents.

    - All agents are expected to be unenrolled (except cloud-managed).
    - Number of "Healthy" (online) agents should be 0.
    - All agents should now have status "Inactive".
    - Swarm snapshots are deleted"""

    name = "Step11"
    description = "test step 11: AC-3 - Unenroll all agents"
    iterations = 1

    def execute(self) -> None:
        fleet = self.fleet
        unenroll_kuery = "local_metadata.host.hostname like eh*"
        log.info(fleet.get_agent_status(unenroll_kuery))

        aid = fleet.bulk_unenroll(kuery=unenroll_kuery)
        result = poll_action_id_status(aid, fleet, 20, 10)
        assert_that(result).described_as(msg("Unenroll completed")).is_true()
        log.info(fleet.get_agent_status(unenroll_kuery))

        for swarm in self.context["swarms"]:
            swarm_kuery = f"policy_id : {swarm.policy[0].id}"
            if os.path.exists(swarm.restore_file):
                os.remove(swarm.restore_file)
                log.info(f"Removed {swarm.restore_file} due to force unenroll")
            log.info(fleet.get_agent_status(swarm_kuery))
            result = poll_agents_status(
                fleet=fleet,
                method="online",
                period=10,
                kuery=swarm_kuery,
                count=0,
                max_iterations=30,
                operator="lessThanOrEqualTo",
            )
            assert_that(result).described_as(msg("Drone Unenrollment")).is_true()
            log.info(fleet.get_agent_status(swarm_kuery))
            fleet.delete_policy_by_id(swarm.policy[0].id)
            log.info(f"Delete {swarm.policy[0].name}")


class StepTeardown(Step):
    name = "Teardown"
    description = "test step 12: Destroy Horde Deployment"

    def execute(self) -> None:
        """Execute teardown"""
        log.info("Calling StepTeardown")
        for swarm in self.context["swarms"]:
            self.horde.halt(swarm.deployment)
            time.sleep(5)
        fleet = self.fleet
        unenroll_kuery = "local_metadata.host.hostname like eh*"
        log.info(fleet.get_agent_status(unenroll_kuery))
        aid = fleet.bulk_unenroll(kuery=unenroll_kuery)
        poll_action_id_status(aid, fleet, 20, 10)
        time.sleep(5)
        if self.context["policies"]:
            for p in self.context["policies"]:
                policy = self.fleet.get_policy_by_name(p)[0]
                fleet.delete_policy_by_id(policy.id)
                log.info(f"Delete {policy.name}")


class StepContext(t.TypedDict):
    """Context"""

    swarms: list[SwarmFactory]
    policies: list[str]
    maxAgents: t.NotRequired[int]
    totalAgents: t.NotRequired[int]
    fleetInstances: t.NotRequired[int]


def run(cluster: WorkspaceCluster, harness_context: t.Mapping[str, str] | None) -> int:
    """Test executing of steps"""

    steps = [
        StepInitializeEnv(),
        Step01(),
        Step02(),
        Step03(),
        Step04(),
        Step05(),
        Step06(),
        Step07(),
        Step08(),
        Step09(),
        Step10(),
        Step11(),
        StepTeardown(),
    ]
    ph = str(uuid.uuid4())[:6]
    _ph = cluster.get("cluster.perf.policy_hash")
    if _ph:
        ph = _ph
        print(f"Using a fixed policy_hash({ph})")

    ctx_default = StepContext(
        swarms=[],
        policies=[f"fp-{ph}", f"pp1-{ph}", f"pp2-{ph}"],
        totalAgents=int(cluster.get("cluster.perf.totalAgents")),
        fleetInstances=int(cluster.get("cluster.perf.fleetServerInstances")),
    )
    ctx_default["maxAgents"] = (
        ctx_default["totalAgents"] / ctx_default["fleetInstances"]
    )
    if harness_context:
        ctx_default.update(harness_context)

    hrns = H(
        name="A test harness",
        description="Test harness",
        iterations=1,
        suite="perf02",
        cluster=cluster,
        context=ctx_default,
    )
    hrns.setup()
    for step in steps:
        hrns.add(step)
    hrns.execute()
    cluster_report(hrns)
    return hrns.teardown()
