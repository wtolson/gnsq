===============================
gnsq
===============================

.. image:: https://img.shields.io/pypi/v/gnsq.svg?style=flat
    :target: https://pypi.python.org/pypi/gnsq

.. image:: https://img.shields.io/travis/wtolson/gnsq/master.svg?style=flat
        :target: https://travis-ci.org/wtolson/gnsq

.. image:: https://img.shields.io/pypi/dm/gnsq.svg?style=flat
        :target: https://pypi.python.org/pypi/gnsq


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

Currently there is support for Python 2.6 and Python 2.7. Support for Python 3
is dependent on `gevent support`_.

Usage
-----

First make sure nsq is `installed and running`_. Next create a nsqd connection
and publish some messages to your topic::

    import gnsq
    conn = gnsq.Nsqd(address='localhost', http_port=4151)

    conn.publish('topic', 'hello gevent!')
    conn.publish('topic', 'hello nsq!')

Then create a Reader to consume messages from your topic::

    reader = gnsq.Reader('topic', 'channel', 'localhost:4150')

    @reader.on_message.connect
    def handler(reader, message):
        print 'got message:', message.body

    reader.start()

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
.. _gevent support: https://github.com/surfly/gevent/issues/38
.. _installed and running: http://nsq.io/overview/quick_start.html
.. _contributing guide: https://github.com/wtolson/gnsq/blob/master/CONTRIBUTING.rst
