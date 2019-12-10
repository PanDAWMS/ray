from setuptools import setup, find_namespace_packages
setup(
    name='Raythena',
    version='1.0',
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=find_namespace_packages(include=['Raythena.*']),
    scripts=[
        'bin/ray_start_head',
        'bin/ray_start_worker',
        'raythena.py'
    ],
    data_files=[
        ('conf', ['conf/cori.yaml'])
    ],
    install_requires=[
        'ray',
        'psutil',
        'uvloop',
        'aiohttp',
        'pytest'
    ],
)
