from setuptools import setup
from raythena import __version__

setup(
    name='raythena',
    version=__version__,
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=['raythena'],
    scripts=[
        'bin/ray_start_head',
        'bin/ray_start_worker',
        'bin/ray_sync',
        'raythena.py',
        'example/setup_ray_cluster_slurm.sh',
        'example/standalone_ray_test_hello_world.py',
    ],
    data_files=[
        ('conf', ['conf/cori.yaml'])
    ],
    install_requires=[
        'ray',
        'psutil',
        'uvloop',
        'aiohttp',
        'click',
        'setproctitle'
    ]
)
