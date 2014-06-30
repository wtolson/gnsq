#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

requirements = [
    'gevent',
    'blinker',
    'requests',
]


setup(
    name='gnsq',
    version='0.1.0',
    description='A gevent based NSQ driver for Python.',
    long_description=readme + '\n\n' + history,
    author='Trevor Olson',
    author_email='trevor@heytrevor.com',
    url='https://github.com/wtolson/gnsq',
    packages=[
        'gnsq',
    ],
    package_dir={'gnsq':
                 'gnsq'},
    include_package_data=True,
    install_requires=requirements,
    license="BSD",
    zip_safe=False,
    keywords='gnsq',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
    ]
)
