#coding:utf-8
from setuptools import setup, find_packages


setup(
    name="sesame",
    version="1.0",
    scripts=[
        "bin/sesame",
    ],
    install_requires=[
        "pika==0.10.0",
    ],
    data_files=[
        ('/etc/init.d/', ['etc/sesame',]),
    ],
    description="ops agent",
    packages=find_packages(),
)
