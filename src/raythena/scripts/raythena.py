#!/usr/bin/env python
import functools
import signal
import traceback
import types

import click

from raythena.drivers.baseDriver import BaseDriver
from raythena.drivers.esdriver import ESDriver
from raythena.utils.config import Config
from raythena.utils.ray import setup_ray, shutdown_ray


@click.command()
@click.option(
    '--config',
    required=True,
    help='raythena configuration file.'
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
    '--ray-workdir',
    help='Workdirectory for ray actors'
)
@click.option(
    '--harvester-endpoint',
    help='Directory to use to communicate with harvester'
)
@click.option(
    '--panda-queue',
    help='Panda queue provided to the payload'
)
@click.option(
    '--core-per-node',
    help='Used to determine how many events should be buffered by ray actors'
)
def cli(*args, **kwargs):
    """
    Starts the application by initializing the config object, connecting or starting the ray cluster, loading the driver
    and starting it.

    Returns:
        None
    """
    config = Config(kwargs['config'], *args, **kwargs)

    cluster_config = setup_ray(config)
    try:
        driver = ESDriver(config, cluster_config['session_dir'])

        signal.signal(signal.SIGINT, functools.partial(cleanup, config, driver))
        signal.signal(signal.SIGTERM, functools.partial(cleanup, config, driver))
        signal.signal(signal.SIGQUIT, functools.partial(cleanup, config, driver))
        signal.signal(signal.SIGSEGV, functools.partial(cleanup, config, driver))
        signal.signal(signal.SIGXCPU, functools.partial(cleanup, config, driver))
        signal.signal(signal.SIGUSR1, functools.partial(cleanup, config, driver))
        signal.signal(signal.SIGBUS, functools.partial(cleanup, config, driver))
        driver.run()
    except Exception as e:
        print(f"Caught exception in driver process {e}")
        print(traceback.format_exc())
    finally:
        driver.stop()
        shutdown_ray(config)


def cleanup(config: Config, driver: BaseDriver, signum: signal.Signals, frame: types.FrameType) -> None:
    """
    Signal handler, notify the ray driver to stop

    Args:
        config: app config
        driver: driver instance
        signum: signal received
        frame: current program frame

    Returns:
        None
    """
    driver.stop()


def main():
    cli(auto_envvar_prefix='RAYTHENA')


if __name__ == "__main__":
    main()
