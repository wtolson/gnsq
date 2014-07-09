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

Features include:

* Free software: BSD license
* Documentation: http://gnsq.readthedocs.org
* Battle tested on billions and billions of messages `</sagan>`
* Based on `gevent`_ for fast concurrent networking
* Fast and flexible signals with `Blinker`_
* Automatic nsqlookupd discovery and back-off
* Support for TLS, DEFLATE, and Snappy
* Full HTTP clients for both nsqd and nsqlookupd

Installation
------------

At the command line::

    $ easy_install gnsq

Or even better, if you have virtualenvwrapper installed::

    $ mkvirtualenv gnsq
    $ pip install gnsq

Currently there is support for Python 2.6 and Python 2.7. Support for Python 3
is dependent on `gevent support <https://github.com/surfly/gevent/issues/38>`_.

Dependencies
~~~~~~~~~~~~

Snappy support depends on the `python-snappy` package which in turn depends on
libsnappy::

    # Debian
    $ sudo apt-get install libsnappy-dev

    # Or OS X
    $ brew install snappy

    # And then install python-snappy
    $ pip install python-snappy

Usage
-----

To use gnsq in a project::

    import gnsq
    reader = gnsq.Reader('topic', 'channel', 'localhost:4150')

    @reader.on_message.connect
    def handler(reader, message):
        do_work(message.body)

    reader.start()

Contributing
------------

Feedback, issues, and contributions are always gratefully welcomed. See the
`contributing guidelines`_ for details on how to help.


.. _gevent: http://gevent.org/
.. _NSQ: http://nsq.io/
.. _Blinker: http://pythonhosted.org/blinker/
.. _contributing guidelines: https://github.com/wtolson/gnsq/blob/master/CONTRIBUTING.rst
