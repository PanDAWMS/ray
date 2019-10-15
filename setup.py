from setuptools import setup, find_packages

setup(
    name='Raythena',
    version='1.0',
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=find_packages(),
    scripts=[
        'bin/raythena-event-service',
        'bin/ray_start_head',
        'bin/ray_start_worker',
        'bin/run_raythena'
    ],
    data_files=[
        ('conf', ['conf/cori.yaml'])
    ],
    install_requires=[
        'ray',
        'psutil'
    ],
)
