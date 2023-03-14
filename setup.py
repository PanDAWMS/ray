from setuptools import setup, find_packages
import re


def find_version(path):
    with open(path) as fp:
        version_match = re.search(
            r"^__version__ = ['\"]([^'\"]*)['\"]", fp.read(), re.M
        )
        if version_match:
            return version_match.group(1)


setup(
    name='raythena',
    version=find_version('src/raythena/__init__.py'),
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    scripts=[
        'bin/ray_start_head',
        'bin/ray_start_worker',
        'bin/ray_sync',
        'example/setup_ray_cluster_slurm.sh',
        'example/standalone_ray_test_hello_world.py',
    ],
    entry_points={
        'console_scripts': ['raythena=raythena.scripts.raythena:main']
    },
    data_files=[
        ('conf', ['conf/cori.yaml'])
    ],
    install_requires=[
        'ray',
        'psutil',
        'uvloop',
        'aiohttp',
        'click',
        'setproctitle',
        'protobuf',
    ]
)
