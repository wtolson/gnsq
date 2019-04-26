===============================
gnsq
===============================

.. image:: https://img.shields.io/pypi/v/gnsq.svg
        :target: https://pypi.python.org/pypi/gnsq

.. image:: https://img.shields.io/travis/wtolson/gnsq.svg
        :target: https://travis-ci.org/wtolson/gnsq

.. image:: https://readthedocs.org/projects/gnsq/badge/?version=latest
        :target: https://gnsq.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


A `gevent`_ based python client for `NSQ`_ distributed messaging platform.

Features include:

* Free software: BSD license
* Documentation: https://gnsq.readthedocs.org
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

Currently there is support for Python 2.7+, Python 3.4+ and PyPy.

Usage
-----

First make sure nsq is `installed and running`_. Next create a producer and
publish some messages to your topic::

    import gnsq

    producer = gnsq.Producer('localhost:4150')
    producer.start()

    producer.publish('topic', 'hello gevent!')
    producer.publish('topic', 'hello nsq!')

Then create a Consumer to consume messages from your topic::

    consumer = gnsq.Consumer('topic', 'channel', 'localhost:4150')

    @consumer.on_message.connect
    def handler(consumer, message):
        print 'got message:', message.body

    consumer.start()

Compatibility
-------------

For **NSQ 1.0** and later, use the major version 1 (``1.x.y``) of gnsq.

For **NSQ 0.3.8** and earlier, use the major version 0 (``0.x.y``) of the
library.

The recommended way to set your requirements in your `setup.py` or
`requirements.txt` is::

    # NSQ 1.x.y
    gnsq>=1.0.0

    # NSQ 0.x.y
    gnsq<1.0.0

Dependencies
------------

Optional snappy support depends on the `python-snappy` package which in turn
depends on libsnappy::

    # Debian
    $ sudo apt-get install libsnappy-dev

    # Or OS X
    $ brew install snappy

    # And then install python-snappy
    $ pip install python-snappy

Contributing
------------

Feedback, issues, and contributions are always gratefully welcomed. See the
`contributing guide`_ for details on how to help and setup a development
environment.


.. _gevent: http://gevent.org/
.. _NSQ: http://nsq.io/
.. _Blinker: http://pythonhosted.org/blinker/
.. _installed and running: http://nsq.io/overview/quick_start.html
.. _contributing guide: https://github.com/wtolson/gnsq/blob/master/CONTRIBUTING.rst
