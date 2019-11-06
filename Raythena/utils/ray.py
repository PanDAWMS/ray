import logging
import ray

logger = logging.getLogger(__name__)


def build_nodes_resource_list(config, run_actor_on_head=False):
    nodes = ray.nodes()
    if len(nodes) == 1:  # only a head node
        run_actor_on_head = True
    head_ip = config.ray_head_ip
    resource_list = set()
    for node in nodes:
        naddr = node['NodeManagerAddress']
        if not run_actor_on_head and naddr == head_ip:
            continue
        resource_list.add(f"node:{naddr}")
    return resource_list


def is_external_cluster(config):
    return config.ray_head_ip is not None and config.ray_redis_port is not None


def setup_ray(config):
    if is_external_cluster(config):
        ray_url = f"{config.ray_head_ip}:{config.ray_redis_port}"
        logger.debug(f"Connection to ray cluster {ray_url}")
        ray.init(address=ray_url,
                 redis_password=config.ray_redis_password)
    else:
        logger.info('No ray cluster provided, starting local cluster...')
        ray.init()


def shutdown_ray(config):
    logger.info('Stopping ray...')
    if is_external_cluster(config) and ray.is_initialized():
        ray.shutdown()


def get_node_ip():
    return ray.services.get_node_ip_address()
