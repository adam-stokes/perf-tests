""" General use perf library for steps """
from __future__ import annotations

import inspect
import json
import time
import typing as t
from pathlib import Path
from re import search

from attr import asdict
from attr.exceptions import NotAnAttrsClassError
from elasticsearch.exceptions import ApiError, TransportError
from rich.console import Console
from rich.padding import Padding

from framework.fleet import (
    FleetActionId,
    FleetActionStatus,
    FleetAgentStatus,
    FleetIntegrationPackage,
    FleetPackageDataStream,
    FleetPolicy,
    FleetServer,
    Inputs,
    get_integration_packages,
)
from framework.harness import Harness
from framework.horde import Horde, HordeDeployment
from framework.log import CONSOLE as con
from framework.log import get_logger
from framework.search import get_es_client
from framework.workspace import WorkspaceCluster

log = get_logger("oblt-perf")


def etime(start: float, end: float) -> str:
    minutes, seconds = divmod(round(end - start), 60)
    return f"{minutes:0.0f}m{seconds:02d}s"


def action_stats(a: FleetActionStatus) -> str:
    return (f"type={a.type} status={a.status} actionCreated={a.nbAgentsActionCreated} "
            f"actioned={a.nbAgentsActioned} agentsAck={a.nbAgentsAck} agentsFailed={a.nbAgentsFailed}")


def horde_deployment_halt(horde: Horde, match: str) -> None:
    """
    Wildcard halt of horde deployments by name match
    """
    stats = json.loads(horde.stats())
    deployments = stats["deployments"]
    for item in deployments:
        # if search("Swarm",match):
        if search(match, item["name"]):
            log.info(f'Horde Halt {item["namespace"]} : {item["name"]}({item["id"]})')
            deployment = HordeDeployment(
                id=item["id"],
                name=item["name"],
                start_time="",
                namespace=item["namespace"],
                total=0,
            )
            horde.halt(deployment)


def horde_restore_prep(filename: str, cluster: WorkspaceCluster) -> None:
    """
    Given a filename of a Swarm snaphot from fixtures
    manipulate it such that it can be used to do a restore
    on the current cluster
    """
    url = cluster.get("kibana.url")
    ess_name = cluster.get("ess.name")
    out_file = cluster.directory / Path(filename).name
    log.info(f"{ess_name} Restore prep {out_file} : {url}")
    out_handle = open(out_file, "w+")
    with open(filename) as swarm_file:
        for line in swarm_file:
            line = json.loads(line)
            line["fleet_url"] = url
            out_handle.write(json.dumps(line) + "\n")
    out_handle.close()


def add_policy_if_missing(
    fleet: FleetServer, policy1_name: str, policy2_name: str
) -> t.Optional[list]:
    """
    Add a policy1_name if it doesn't already exist
    by duplicating policy2_name
    """
    log.info(f"try to add {policy1_name} by duplicating {policy2_name}")
    policy1 = fleet.get_policy_by_name(policy1_name)
    if len(policy1) != 0:
        log.info(f"confirmed a policy named {policy1_name} already exists")
        return policy1
    policy2 = fleet.get_policy_by_name(policy2_name)
    if len(policy2) != 0:
        log.info(f"confirmed a policy named {policy2_name} exists")
        if len(policy2) == 1:
            log.info(f"found 1 policy named {policy2_name}")
            description = f"a copy of policy {policy2_name}"
            policy1 = fleet.copy_policy_by_id(policy2[0].id, policy1_name, description)
            log.info(f"add {policy1_name} by duplicating {policy2_name}")
            return policy1
    else:
        log.info(f"a policy named {policy2_name} does not exist")
        return None


def prepare_fleet_server_inputs(
    cluster_dir: Path, max_agents: int | str, yml: t.Optional[str] = None
) -> str:
    """Create fleetserver inputs file from template, store it to cluster dir. Return file path"""
    if yml is None:
        yml = "server.runtime:\n  gc_percent: 20\n"

    if max_agents >= 25000:
        yml = "server.runtime:\n  gc_percent: 20\ncache:\n  num_counters: 640000\n  max_cost: 419430400\nserver.limits:\n  checkin_limit:\n    interval: 250us\n    burst: 8000\n    max: 55000\n  artifact_limit:\n    interval: 250us\n    burst: 8000\n    max: 16000\n  ack_limit:\n    interval: 250us\n    burst: 8000\n    max: 16000\n  enroll_limit:\n    interval: 10ms\n    burst: 200\n    max: 400\n\nserver.gc_percent: 20"

    log.info(f"Fleet server inputs {yml}")
    with open("fixtures/package_policies/inputs-fleet_server.json") as f:
        output = f.read()
    output = output % (
        max_agents,
        yml.encode("unicode_escape").decode("utf-8"),
        max_agents,
    )

    pp_dir = Path(f"{cluster_dir}/package_policies/")
    if not pp_dir.exists():
        pp_dir.mkdir(parents=True, exist_ok=True)

    output_file = str(pp_dir / "inputs-fleet_server.json")
    with open(output_file, "w") as g:
        g.write(output)

    return output_file


def update_policy_config(
    fleet: FleetServer, policy_id: str, package_name: str, update_file: str
) -> list:
    """
    Change the config a policy by updating package config
    from a file
    """
    log.info(f'Update policy({policy_id}) config for "{package_name}" package policy')
    policy = fleet.get_policy_full_by_id(policy_id)[0]
    package_policies = policy.package_policies
    updated_package_policy = None

    for package in package_policies:
        if package.package.name == package_name:
            log.info(
                f'Found "{package.package.name}" package policy, updating from {update_file}'
            )
            new_inst = package.merge(Path(update_file))
            updated_package_policy = fleet.update_package_policy(new_inst)
            if len(updated_package_policy) > 0:
                log.info(
                    f'"{package.package.name}" package policy updated from {update_file}'
                )
                updated_package_policy = updated_package_policy[0]
            else:
                log.error(f"Updating {policy_id}({package.package.name}) failed")
    return updated_package_policy


def update_package_policy_from_dict(
    integration_id: str, policy_id: str, fleet: FleetServer, updates: dict
) -> list:
    """
    Change the config a policy by updating package config
    from a dict
    """

    new_inst = FleetPackageDataStream.from_dict(updates)
    new_inst.id = integration_id
    new_inst.policy_id = policy_id
    updated_package_policy = fleet.update_package_policy(new_inst)

    if len(updated_package_policy) > 0:
        log.info(f'"{new_inst.package.name}" package policy successfully updated')
    else:
        log.error(f"Updating {new_inst.package.name} failed")
    return updated_package_policy


def poll_agents_status(
    *,
    fleet: FleetServer,
    method: str,
    period: int | float | str,
    kuery: str,
    count: int | str,
    max_iterations: int | str,
    operator="greaterThanOrEqualTo",
) -> bool:
    """
    Poll agent status for condition
    """
    start = time.time()
    log.info(f'Looking for "{method}" {operator} {count}')
    prev_status = None
    x = 0
    trend = "undetermined"
    while x < int(max_iterations):
        x += 1
        status = fleet.get_agent_status(kuery)
        if status is None:
            prev_status = status
            continue  # bad status, keep trying
        if hasattr(FleetAgentStatus, method):
            value = getattr(status, method)
            if operator == "lessThanOrEqualTo":
                if prev_status:
                    if getattr(prev_status, method) > value:
                        trend = "descending"
                        x -= 1
                    else:
                        trend = "broken"
                if value <= count:
                    end = time.time()
                    log.info(
                        f'"{method}" {value}/{count} (current/desired) {etime(start,end)}',
                    )
                    return True
            else:
                if prev_status:
                    if getattr(prev_status, method) < value:
                        trend = "ascending"
                        x -= 1
                    else:
                        trend = "broken"
                if value >= count:
                    end = time.time()
                    log.info(
                        f'"{method}" {value}/{count} (current/desired) {etime(start,end)}',
                    )
                    return True
        else:
            log.info(f"poll_agents_status no method [red]{method}[/red]")
            return False
        if status != prev_status:
            log.info({"trend": method, "status": status})
        prev_status = status
        time.sleep(t.cast(float, period))
    end = time.time()
    log.error(f"poll_agents_status reached max_iterations {etime(start,end)}")
    return False


def poll_action_id_status(
    actionId: FleetActionId,
    fleet: FleetServer,
    period: int | str,
    max_iterations: int | str,
) -> bool:
    """
    Poll an actionId to follow it's progress
    """
    start = time.time()
    x = 0
    prev_done = 0
    log.info(f"Polling action_id: {actionId}")
    if actionId == None:
        return False
    log.info(
        "type status nbAgentsActionCreated/nbAgentsActioned/nbAgentsAck/nbAgentsFailed",
    )
    while x < int(max_iterations):
        x += 1
        action_status = fleet.action_id_status(actionId)
        if action_status:
            log.info(action_stats(action_status))
            if action_status.status == "IN_PROGRESS":
                if prev_done < action_status.nbAgentsAck:
                    x -= 1
            if action_status.status == "EXPIRED":
                end = time.time()
                log.info(f"{action_stats(action_status)} {etime(start,end)}")
                return False
            if action_status.status == "COMPLETE":
                end = time.time()
                log.info(f"{action_stats(action_status)} {etime(start,end)}")
                return True
            prev_done = action_status.nbAgentsAck
        else:
            log.info(f"action_status: {action_status}")
        time.sleep(period)
    return False


def cluster_report(hrns: Harness) -> t.Optional[str]:
    """
    Build a report about cluster
    """
    client = get_es_client(hrns.cluster)
    cluster_name = client.cluster.get_settings()["persistent"]["cluster"]["metadata"][
        "display_name"
    ]
    reports_dir = hrns.cluster_directory() / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report_file = reports_dir / f"{cluster_name}.txt"

    def jstr(o):
        return f"{json.dumps(o.body, indent=4, sort_keys=True)}\n"

    try:
        with open(report_file, "w+") as report_file:
            report_file.write(jstr(client.info()))
            report_file.write(jstr(client.cluster.health()))
            report_file.write(jstr(client.cluster.get_settings()))
            # report_file.write(jstr(client.cluster.state()))
            report_file.write(jstr(client.cluster.stats()))
            report_file.write(str(client.cat.nodes(v=True)))
            report_file.write(str(client.cat.nodeattrs(v=True)))
            report_file.write(str(client.cat.health(v=True)))
            report_file.write(str(client.cat.allocation(v=True)))
    except (ApiError, TransportError):
        return None
    return None


def change_config_in_policy(fleet: FleetServer, policy: FleetPolicy) -> bool:
    """
    Change config in policy
    """
    log.info(f"Change config in {policy.name} at rev {policy.revision}")
    try:
        update_policy_config(
            fleet, policy.id, "system", "fixtures/package_policies/inputs-system1.json"
        )
    except Exception as e:
        log.error(f"Error while updating policy config: {e}")
        return False
    finally:
        status = fleet.get_agent_status(f"policy_id : {policy.id}")
        log.info(f"Agent status for {policy.name} at rev {policy.revision}: {status}")
    return True


def add_integration_to_policy(
    name: str, location: Path | str, fleet: FleetServer, policy: FleetPolicy
) -> list[FleetPackageDataStream] | None:
    """
    Add integration to policy
    """
    log.info(f"Adding {name} to {policy.name}")
    try:
        response = fleet.create_package_policy(
            policy=policy,
            integration=name,
            inputs=Inputs.from_json(Path(location)),
        )
        return response
    except Exception as e:
        log.error(f"Exception in add_integration_to_policy: {e}")
        return None


def get_integrations(
    fleet: FleetServer, policy: FleetPolicy
) -> list[FleetIntegrationPackage, str]:
    """
    Wrapper for get_integration_packages that give us some context
    """
    integrations = get_integration_packages(policy)
    log.info(
        f"Updated list of integrations in {policy.name} at rev {policy.revision}: {integrations}"
    )
    return integrations


def move_swarm_to_policy(
    fleet: FleetServer, src_policy: FleetPolicy, dest_policy: FleetPolicy
) -> t.Optional[FleetActionId]:
    """
    Move entire swarm to new policy
    """
    log.info(f"Move swarm from {src_policy.name} to {dest_policy.name}")
    status = fleet.get_agent_status(f"policy_id : {src_policy.id}")
    log.info(f"Agent status for {src_policy.name}: {status}")
    log.info(f"Reassign from {src_policy.name} to {dest_policy.name}")
    result = fleet.bulk_reassign(
        policy_id=dest_policy.id, kuery=f"policy_id : {src_policy.id}"
    )
    log.info(result)
    return result


def msg(message: str) -> str:
    """Format message for console"""
    frame = inspect.currentframe().f_back
    filename = frame.f_code.co_filename.split("/")[-1]
    lineno = frame.f_lineno
    return f"{filename}::{lineno} {message}"


def dict2json(obj: t.Any) -> str | None:
    """Convert attrs class to dict and pretty print json"""
    try:
        return json.dumps(asdict(obj), indent=4, sort_keys=True)
    except NotAnAttrsClassError:
        return json.dumps(obj.__dict__, indent=4, sort_keys=True, skipkeys=True)
    except json.JSONDecodeError:
        return None
