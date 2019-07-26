# -*- coding: utf-8 -*-
import io
from setuptools import setup, find_packages

with io.open('README.rst', 'rt', encoding='utf8') as f:
    readme = f.read()

setup(
    name='blsconnect',
    version='0.9.0',
    packages=find_packages(),
    author='Daniel Reeves',
    maintainer='Daniel Reeves',
    include_package_data=True,
    tests_require=[
        'pytest',
    ],
    url='https://github.com/dwreeves/blsconnect',
    description="Integration of BLS's API built for Python.",
    long_description=readme
)
