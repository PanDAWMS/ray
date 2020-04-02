#!/usr/bin/env python
"""
A simple test of Ray
"""

import argparse
import os
import ray
import time


@ray.remote
def f():
    time.sleep(0.01)
    return ray.services.get_node_ip_address()


def main(ip, password):
    ray.init(ignore_reinit_error=True,
             address="%s" % ip, redis_password="%s" % password)

    # Get a list of the IP addresses of the nodes that have joined the cluster.
    node_ip_addresses = [f.remote() for _ in range(10000)]
    print(set(ray.get(node_ip_addresses)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Wait on ray head node or workers to connect')
    parser.add_argument('--redis-address', default="%s:%s" % (os.environ["RAYTHENA_RAY_HEAD_IP"], os.environ["RAYTHENA_RAY_REDIS_PORT"]))
    parser.add_argument('--redis-password', default=os.environ["RAYTHENA_RAY_REDIS_PASSWORD"])
    args = parser.parse_args()
    main(args.redis_address, args.redis_password)
