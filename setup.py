from setuptools import setup, find_packages

setup(
    name='Raythena',
    version='1.0',
    author='Miha Muskinja',
    author_email='MihaMuskinja@lbl.gov',
    packages=find_packages(),
    scripts=[
        'bin/raythena-event-service',
        'bin/start-ray',
    ],
    install_requires=[
        'ray',
    ],
)
