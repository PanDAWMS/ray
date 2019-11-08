#!/usr/bin/env python
import click
import signal
import functools

from Raythena.utils.ray import setup_ray, shutdown_ray
from Raythena.utils.config import Config
from Raythena.utils.importUtils import import_from_string


@click.command()
@click.option(
    '--conda-bin',
    help='Path to conda bin directory which contains activate.'
)
@click.option(
    '--ray-venv',
    help='Name of Ray virtualenv.'
)
@click.option(
    '--pilot-venv',
    help='Name of pilot virtualenv.'
)
@click.option(
    '--pilot-dir',
    help='Directory where pilot source code is located. Raythena expects to find pilot.py in this directory.'
)
@click.option(
    '--config',
    required=True,
    help='Raythena configuration file.'
)
@click.option(
    '-d', '--debug',
    is_flag=True,
    help='Debug log level'
)
@click.option(
    '--ray-head-ip',
    help='IP address of ray head node'
)
@click.option(
    '--ray-redis-port',
    help='Port of redis instance used by the ray cluster'
)
@click.option(
    '--ray-redis-password',
    help='Redis password setup in the ray cluster'
)
@click.option(
    '--ray-driver',
    help='Ray driver to start as <moduleName>:<ClassName>. The module should be placed in Raythena.drivers'
)
def cli(*args, **kwargs):

    config = Config(kwargs['config'], *args, **kwargs)

    setup_ray(config)

    driver_class = import_from_string(f"Raythena.drivers.{config.ray['driver']}")
    driver = driver_class(config)

    signal.signal(signal.SIGINT, functools.partial(cleanup, config, driver))
    signal.signal(signal.SIGTERM, functools.partial(cleanup, config, driver))
    signal.signal(signal.SIGQUIT, functools.partial(cleanup, config, driver))
    signal.signal(signal.SIGSEGV, functools.partial(cleanup, config, driver))
    signal.signal(signal.SIGXCPU, functools.partial(cleanup, config, driver))
    signal.signal(signal.SIGUSR1, functools.partial(cleanup, config, driver))
    signal.signal(signal.SIGBUS, functools.partial(cleanup, config, driver))

    driver.run()

    shutdown_ray(config)


def cleanup(config, driver, signum, frame):
    driver.stop()


if __name__ == "__main__":
    cli(auto_envvar_prefix='RAYTHENA')
