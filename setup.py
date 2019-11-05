from setuptools import setup, find_packages

setup(
    name='Raythena',
    version='1.0',
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=find_packages(),
    scripts=[
        'bin/ray_start_head',
        'bin/ray_start_worker',
        'bin/raythena'
    ],
    data_files=[
        ('conf', ['conf/cori.yaml'])
    ],
    install_requires=[
        'ray',
        'psutil',
        'uvloop',
        'aiohttp'
    ],
)
