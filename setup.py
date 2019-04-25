#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()


setup(
    name='gnsq',
    version='1.0.1',
    description='A gevent based python client for NSQ.',
    long_description=readme,
    long_description_content_type='text/x-rst',
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
        'gevent',
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
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]
)
