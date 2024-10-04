#!/usr/bin/env python
"""
A simple test of Ray

This example uses placement_group API to spread work around

"""

import argparse
import os
import platform
import time
from pprint import pprint
import ray


def build_nodes_resource_list(redis_ip: str):
    nodes = ray.nodes()
    resource_list = list()
    for node in nodes:
        naddr = node["NodeManagerAddress"]
        if naddr == redis_ip:
            continue
        else:
            resource_list.append(node)
    return resource_list


@ray.remote
class actor:
    def __init__(self) -> None:
        self.pid = os.getpid()
        self.hostname = platform.node()
        self.ip = ray._private.services.get_node_ip_address()
        print(f"Initial message from PID - {self.pid} Running on host - {self.hostname} {self.ip}")

    def ping(self):
        print(f"{self.pid} {self.hostname} {self.ip} - ping")
        time.sleep(5)
        return f"{self.pid} {self.hostname} {self.ip} - pong"


def main(redis_ip: str, redis_port: str, redis_password: str):
    redis_address = f"{redis_ip}:{redis_port}"
    ray.init(
        ignore_reinit_error=True,
        address=f"{redis_address}",
        _redis_password=f"{redis_password}",
    )

    # show the ray cluster
    print(f"Ray Cluster resources : {ray.cluster_resources()}")

    # Get list of nodes to use
    nodes = build_nodes_resource_list(redis_ip)
    print(f"Found {len(nodes)} Worker nodes in the Ray Cluster:")
    pprint(nodes)
    print(" ")

    # Setup one Actor per node
    print(f"Setting up {len(nodes)} Actors...")
    actors = []
    for node in nodes:
        node_ip_str = f"node:{node['NodeManagerAddress']}"
        actors.append(actor.options(resources={f"{node_ip_str}": 1}).remote())
    time.sleep(1)

    # Ping-Pong test
    for _ in range(5):
        messages = [actor.ping.remote() for actor in actors]
        for msg in ray.get(messages):
            print(f"Received back message {msg}")

    # now terminate the actors
    [ray.kill(actor) for actor in actors]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wait on ray head node or workers to connect")
    parser.add_argument("--redis-ip", default="{}".format(os.environ["RAYTHENA_RAY_HEAD_IP"]))
    parser.add_argument("--redis-port", default="{}".format(os.environ["RAYTHENA_RAY_REDIS_PORT"]))
    parser.add_argument("--redis-password", default=os.environ["RAYTHENA_RAY_REDIS_PASSWORD"])
    args = parser.parse_args()
    print(f"args : {args}")
    main(args.redis_ip, args.redis_port, args.redis_password)
