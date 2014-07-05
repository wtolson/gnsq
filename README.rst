===============================
gnsq
===============================

.. image:: https://badge.fury.io/py/gnsq.svg
    :target: http://badge.fury.io/py/gnsq

.. image:: https://travis-ci.org/wtolson/gnsq.svg?branch=master
        :target: https://travis-ci.org/wtolson/gnsq

.. image:: https://pypip.in/d/gnsq/badge.png
        :target: https://pypi.python.org/pypi/gnsq


A `gevent`_ based `NSQ`_ driver for Python.

* Free software: BSD license
* Documentation: http://gnsq.readthedocs.org.

Installation
------------

At the command line::

    $ easy_install gnsq

Or even better, if you have virtualenvwrapper installed::

    $ mkvirtualenv gnsq
    $ pip install gnsq

Usage
-----

To use gnsq in a project::

    import gnsq
    reader = gnsq.Reader('topic', 'channel', 'localhost:4150')

    @reader.on_message.connect
    def handler(reader, message):
        do_work(message.body)

    reader.start()


.. _gevent: http://gevent.org/
.. _NSQ: http://nsq.io/
