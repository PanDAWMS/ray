from setuptools import setup, find_namespace_packages

setup(
    name='raythena',
    version='1.0',
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=find_namespace_packages(include=['raythena.*']),
    scripts=[
        'bin/ray_start_head',
        'bin/ray_start_worker',
        'app.py'
    ],
    data_files=[
        ('conf', ['conf/cori.yaml', 'conf/incontainer.yaml'])
    ],
    install_requires=[
        'ray',
        'psutil',
        'uvloop',
        'aiohttp',
        'tox',
        'click',
        'setproctitle'
    ]
)
