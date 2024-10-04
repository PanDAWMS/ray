from collections.abc import Mapping
from typing import Any, List

import ray

from raythena.utils.config import Config


def build_nodes_resource_list(
    config: Config, run_actor_on_head: bool = False
) -> List[Mapping[str, Any]]:
    """
    Build and setup ray custom resources.
    Actors should then be instantiated by requiring one of the resource in the returned list.

    Args:
        config: application config
        run_actor_on_head: True if a worker should be allowed to run on the ray head node

    Returns:
        A list of available custom resources in the cluster. If a resource is available n times in the cluster, it is
        present n times in the list.
    """
    nodes = ray.nodes()
    if len(nodes) == 1:  # only a head node
        run_actor_on_head = True
    head_ip = config.ray["headip"]
    resource_list = list()
    for node in nodes:
        naddr = node["NodeManagerAddress"]
        if not node["alive"] or (not run_actor_on_head and naddr == head_ip):
            continue
        else:
            resource_list.extend([node])
    return resource_list


def cluster_size() -> int:
    """
    Number of nodes in ray

    Returns:
        Number of nodes in the ray cluster or 0 if not initialized
    """
    if not ray.is_initialized():
        return 0
    return len(ray.nodes())


def is_external_cluster(config: Config) -> bool:
    """
    Checks if an external ray cluster is used

    Args:
        config: application config

    Returns:
        True if raythena is connecting to an existing cluster, False otherwise
    """
    return (
        config.ray["headip"] is not None and config.ray["redisport"] is not None
    )


def setup_ray(config: Config) -> Any:
    """
    Connect to / Setup the ray cluster

    Args:
        config: application config

    Returns:
        dict of cluster params
    """
    log_to_driver = (
        True if not config.logging.get("workerlogfile", None) else False
    )
    if is_external_cluster(config):
        ray_url = f"{config.ray['headip']}:{config.ray['redisport']}"
        return ray.init(
            address=ray_url,
            _redis_password=config.ray["redispassword"],
            log_to_driver=log_to_driver,
        )
    else:
        return ray.init(log_to_driver=log_to_driver)


def shutdown_ray(config: Config) -> None:
    """
    Disconnect from / Stop the ray cluster

    Args:
        config: application config

    Returns:
        None
    """
    if ray.is_initialized():
        ray.shutdown()


def get_node_ip() -> str:
    """
    Retrieve current node ip address

    Returns:
        Node ip address
    """
    return ray._private.services.get_node_ip_address()
