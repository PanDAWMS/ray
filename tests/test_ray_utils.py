import socket

import pytest

from Raythena.utils.ray import cluster_size, build_nodes_resource_list, get_node_ip


@pytest.mark.usefixtures("requires_ray")
class TestRayUtils:

    def test_build_nodes_resource_list(self, config):
        constraints = build_nodes_resource_list(config)
        assert len(
            constraints) == cluster_size() * config.resources['workerpernode']

    def test_cluster_size(self):
        assert cluster_size() > 0

    def test_get_node_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        assert get_node_ip() == s.getsockname()[0]
        s.close()
