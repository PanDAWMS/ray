import ray
from typing import List
from Raythena.utils.config import Config


def build_nodes_resource_list(config: Config,
                              run_actor_on_head: bool = False) -> List[str]:
    nodes = ray.nodes()
    if len(nodes) == 1:  # only a head node
        run_actor_on_head = True
    head_ip = config.ray['headip']
    resource_list = list()
    custom_resource = "payload_slot"
    workerpernode = config.resources.get('workerpernode', 1)
    for node in nodes:
        naddr = node['NodeManagerAddress']
        if not node['alive'] or (not run_actor_on_head and naddr == head_ip):
            continue
        node_custom_resource = f"{custom_resource}@{naddr}"
        ray.experimental.set_resource(node_custom_resource, workerpernode,
                                      node['NodeID'])
        resource_list.extend([node_custom_resource] * workerpernode)
    return resource_list


def cluster_size() -> int:
    if not ray.is_initialized():
        return 0
    return len(ray.nodes())


def is_external_cluster(config: Config) -> bool:
    return config.ray['headip'] is not None and config.ray[
        'redisport'] is not None


def setup_ray(config: Config) -> None:
    if is_external_cluster(config):
        ray_url = f"{config.ray['headip']}:{config.ray['redisport']}"
        ray.init(address=ray_url, redis_password=config.ray['redispassword'])
    else:
        ray.init()


def shutdown_ray(config: Config) -> None:
    if ray.is_initialized():
        ray.shutdown()


def get_node_ip() -> str:
    return ray.services.get_node_ip_address()
