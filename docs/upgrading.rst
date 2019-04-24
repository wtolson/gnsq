Upgrading to Newer Releases
===========================

This section of the documentation enumerates all the changes in gnsq from
release to release and how you can change your code to have a painless
updating experience.

Use the :command:`pip` command to upgrade your existing Flask installation by
providing the ``--upgrade`` parameter::

    $ pip install --upgrade gnsq


.. _upgrading-to-100:

Version 1.0.0
-------------

While there are no breaking changes in version 1.0.0, much of the interface has
been deprecated to both simplify the api and bring it into better compliance
with the recommended naming schemes for nsq clients. Existing code should work
as is and deprecation warnings will be emitted for any code paths that need to
be changed.

Deprecated Reader
~~~~~~~~~~~~~~~~~

The main interface has been renamed from :class:`~gnsq.Reader` to
:class:`~gnsq.Consumer`. The api remains largely the same and can be swapped out
directly in most cases.

Async messages
``````````````

The ``async`` flag has been removed from the :class:`~gnsq.Consumer`. Instead
:class:`messages <gnsq.Message>` has a
:meth:`message.enable_async() <gnsq.Message.enable_async>`
method that may be used to indicate that a message will be handled
asynchronous.

Max concurrency
```````````````

The ``max_concurrency`` parameter has been removed from
:class:`~gnsq.Consumer`. If you wish to replicate this behavior, you should use
the :class:`gnsq.contrib.QueueHandler` in conjunction with a worker pool::

    from gevent.pool import Pool
    from gnsq import Consumer
    from gnsq.contrib.queue import QueueHandler

    MAX_CONCURRENCY = 4

    # Create your consumer as usual
    consumer = Consumer(
        'topic', 'worker', 'localhost:4150', max_in_flight=16)

    # Connect a queue handler to the on message signal
    queue = QueueHandler()
    consumer.on_message.connect(queue)

    # Start your consumer without blocking or in a separate greenlet
    consumer.start(block=False)

    # If you want to limit your concurrency to a single greenlet, simply loop
    # over the queue in a for loop, or you can use a worker pool to distribute
    # the work.
    pool = Pool(MAX_CONCURRENCY)
    results = pool.imap_unordered(queue, my_handler)

    # Consume the results from the pool
    for result in results:
        pass

Deprecated Nsqd
~~~~~~~~~~~~~~~

The :class:`~gnsq.Nsqd` client has been split into two classes, corresponding
to the tcp and http APIs. The new classes are :class:`~gnsq.NsqdTCPClient` and
:class:`~gnsq.NsqdHTTPClient` respectively.

The methods `publish_tcp`, `publish_http`, `multipublish_tcp`, and
`multipublish_http` have been removed from the new classes.

Deprecated Lookupd
~~~~~~~~~~~~~~~~~~

The :class:`~gnsq.Lookupd` class has been replaced by
:class:`~gnsq.LookupdClient`. :class:`~gnsq.LookupdClient` can be constructed
using the ``host`` and ``port`` or by passing the url to
:meth:`LookupdClient.from_url() <gnsq.LookupdClient.from_url>` instead.

The method :meth:`~gnsq.Lookupd.tombstone_topic_producer`
has been renamed to :func:`~gnsq.LookupdClient.tombstone_topic`.
