# -*- coding: utf-8 -*-
import io
from setuptools import setup, find_packages

with io.open('README.rst', 'rt', encoding='utf8') as f:
    readme = f.read()

setup(
    name='blsconnect',
    packages=find_packages(),
    maintainer='Daniel Reeves',
    include_package_data=True,
    install_requires=[
    ],
    setup_requires=[
        'pytest-runner',
    ],
    tests_require=[
        'pytest',
    ],
    description="Integration of BLS's API built for Python.",
    long_description=readme
)
