#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


from setuptools import setup
from setuptools.command.test import test as TestCommand

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = None

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args or 'tests')
        sys.exit(errno)


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
    tests_require=['pytest'],
    cmdclass={'test': PyTest},
)
