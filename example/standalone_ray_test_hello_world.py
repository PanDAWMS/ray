#!/usr/bin/env python
"""
A simple test of Ray
"""

import argparse
import os
import ray
import time


def build_nodes_resource_list(args):
    nodes = ray.nodes()
    resource_list = list()
    custom_resource = "payload_slot"
    for node in nodes:
        naddr = node['NodeManagerAddress']
        node_custom_resource = f"{custom_resource}@{naddr}"
        ray.experimental.set_resource(node_custom_resource, 1, node['NodeID'])
        resource_list.extend([node_custom_resource])
    return resource_list


@ray.remote
class actor():
    def __init__(self, name):
        self.name = name
        print(f"Initial message from {self.name}")

    def ping(self):
        print("ping")
        time.sleep(5)
        return "pong"


def main(args):
    ray.init(ignore_reinit_error=True,
             address="%s" % args.redis_address, redis_password="%s" % args.redis_password)

    # Setup one Actor per ndoe
    nodes = build_nodes_resource_list(args)
    print(f"Found {len(nodes)} nodes in the Ray Cluster:\n\t{nodes}")
    print(f"Setting up {len(nodes)} Actors...")
    actors = [actor.options(resources={node: 1}).remote(f"Actor_{node.split('@')[1]}") for node in nodes]
    time.sleep(1)

    # Ping-Pong test
    for _ in range(10):
        messages = [actor.ping.remote() for actor in actors]
        for msg in ray.get(messages):
            print(f"Received back message {msg}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Wait on ray head node or workers to connect')
    parser.add_argument('--redis-address', default="%s:%s" % (os.environ["RAYTHENA_RAY_HEAD_IP"], os.environ["RAYTHENA_RAY_REDIS_PORT"]))
    parser.add_argument('--redis-password', default=os.environ["RAYTHENA_RAY_REDIS_PASSWORD"])
    parser.add_argument('--worker-per-node', default=os.environ["RAYTHENA_RAY_NWORKERS"])
    args = parser.parse_args()
    main(args)
