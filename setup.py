#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')


# gevent has dropped support for py26 in versions 1.2 and later, so
# install the last 1.1 series if we're on py26.
if sys.version_info[0] == 2 and sys.version_info[1] == 6:
    gevent_version = ' < 1.2.0'
else:
    gevent_version = ''


setup(
    name='gnsq',
    version='0.4.0',
    description='A gevent based python client for NSQ.',
    long_description=readme + '\n\n' + history,
    author='Trevor Olson',
    author_email='trevor@heytrevor.com',
    url='https://github.com/wtolson/gnsq',
    packages=[
        'gnsq',
        'gnsq.contrib',
        'gnsq.stream',
    ],
    package_dir={'gnsq': 'gnsq'},
    include_package_data=True,
    install_requires=[
        'blinker',
        'gevent' + gevent_version,
        'six',
        'urllib3',
    ],
    extras_require={
        'snappy': ['python-snappy'],
    },
    license="BSD",
    zip_safe=False,
    keywords='gnsq',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ]
)
