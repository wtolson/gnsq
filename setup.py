#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


setup(
    name='gnsq',
    version='0.0.1',
    description='A gevent based NSQ driver for Python.',
    long_description=open('README.md').read(),
    author='William Trevor Olson',
    author_email='trevor@heytrevor.com',
    url='https://github.com/wtolson/gnsq',
    packages=['gnsq'],
    include_package_data=True,
    install_requires=[
        'gevent',
        'blinker',
        'requests',
    ],
    license="MIT",
    zip_safe=False,
    classifiers=[
        'License :: OSI Approved :: MIT License',
    ],
    test_suite='tests',
)
