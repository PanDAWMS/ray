import pytest
import os
from Raythena.utils.config import Config


@pytest.fixture
def config_path():
    return "tests/testconf.yaml"


@pytest.fixture
def config(config_path):
    print(f"cswd {os.getcwd()}")
    return Config(
        config_path, config=None, debug=False, payload_bindir=None,
        ray_driver=None, ray_head_ip=None, ray_redis_password=None, ray_redis_port=None,
        ray_workdir=None, harvester_endpoint=None, panda_queue=None, core_per_node=None)
