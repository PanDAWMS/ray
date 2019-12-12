import ray


def build_nodes_resource_list(config, run_actor_on_head=False):
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
        ray.experimental.set_resource(node_custom_resource, workerpernode, node['NodeID'])
        resource_list.extend([node_custom_resource] * workerpernode)
    return resource_list


def cluster_size():
    if not ray.is_initialized():
        return 0
    return len(ray.nodes())


def is_external_cluster(config):
    return config.ray['headip'] is not None and config.ray['redisport'] is not None


def setup_ray(config):
    if is_external_cluster(config):
        ray_url = f"{config.ray['headip']}:{config.ray['redisport']}"
        ray.init(address=ray_url,
                 redis_password=config.ray['redispassword'])
    else:
        ray.init()


def shutdown_ray(config):
    if ray.is_initialized():
        ray.shutdown()


def get_node_ip():
    return ray.services.get_node_ip_address()
