from setuptools import setup, find_namespace_packages

setup(
    name='raythena',
    version="0.1",
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=find_namespace_packages(include=['raythena.*']),
    scripts=[
        'bin/ray_start_head',
        'bin/ray_start_worker',
        'bin/ray_sync',
        'app.py',
        'example/setup_ray_cluster_slurm.sh',
        'example/standalone_ray_test_hello_world.py',
    ],
    data_files=[
        ('conf', ['conf/cori.yaml'])
    ],
    install_requires=[
        'ray[default]',
        'psutil',
        'uvloop',
        'aiohttp',
        'tox',
        'click',
        'setproctitle'
    ]
)
