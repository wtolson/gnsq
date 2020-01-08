.. :changelog:

History
-------


1.0.2 (2020-01-08)
~~~~~~~~~~~~~~~~~~

* Fix python3 bug in the giveup handler
* Fix bug returning json data from older nsq servers
* Batch handler now checks if messages have been responded to before requeuing


1.0.1 (2019-04-24)
~~~~~~~~~~~~~~~~~~

* Fix long description in packaging


1.0.0 (2019-04-24)
~~~~~~~~~~~~~~~~~~

* Drop support for python 2.6 and python 3.3, add support for python 3.7
* Drop support for nsq < 1.0.0
* Handle changing connections during redistribute ready
* Add create topic and create channel to LookupdClient
* Add pause and unpause topic to NsqdHTTPClient
* Add ability to filter NsqdHTTPClient stats by topic/channel
* Add text format for NsqdHTTPClient stats
* Add binary multipublish over http
* Add queue handler to the contrib package
* Add Producer class, a high level tcp message writer
* Fixed detecting if consumer is starved
* Optimizations to better distribute ready state among the nsqd connections
* Detect starved consumers when batching messages
* [DEPRECATED] :class:`~gnsq.Nsqd` is deprecated. Use
  :class:`~gnsq.NsqdTCPClient` or :class:`~gnsq.NsqdHTTPClient` instead. See
  :ref:`upgrading-to-100` for more information.
* [DEPRECATED] :class:`~gnsq.Lookupd` is deprecated. Use
  :class:`~gnsq.LookupdClient` instead. See :ref:`upgrading-to-100` for more
  information.
* [DEPRECATED] :class:`~gnsq.Reader` is deprecated. Use :class:`~gnsq.Consumer`
  instead.  See :ref:`upgrading-to-100` for more information.


0.4.0 (2017-06-13)
~~~~~~~~~~~~~~~~~~

* #13 - Allow use with nsq v1.0.0 (thanks @daroot)
* Add contrib package with utilities.


0.3.3 (2016-09-25)
~~~~~~~~~~~~~~~~~~

* #11 - Make sure all socket data is sent.
* #5 - Add support for DPUB (defered publish).


0.3.2 (2016-04-10)
~~~~~~~~~~~~~~~~~~

* Add support for Python 3 and PyPy.
* #7 - Fix undeclared variable in compression socket.


0.3.1 (2015-11-06)
~~~~~~~~~~~~~~~~~~

* Fix negative in flight causing not throttling after backoff.


0.3.0 (2015-06-14)
~~~~~~~~~~~~~~~~~~

* Fix extra backoff success/failures during backoff period.
* Fix case where handle_backoff is never called.
* Add backoff parameter to message.requeue().
* Allow overriding backoff on NSQRequeueMessage error.
* Handle connection failures while starting/completing backoff.


0.2.3 (2015-02-16)
~~~~~~~~~~~~~~~~~~

* Remove disconnected nsqd messages from the worker queue.
* #4 - Fix crash in Reader.random_ready_conn (thanks @ianpreston).


0.2.2 (2015-01-12)
~~~~~~~~~~~~~~~~~~

* Allow finishing and requeuing in sync handlers.


0.2.1 (2015-01-12)
~~~~~~~~~~~~~~~~~~

* Topics and channels are now valid to 64 characters.
* Ephemeral topics are now valid.
* Adjustable backoff behavior.


0.2.0 (2014-08-03)
~~~~~~~~~~~~~~~~~~

* Warn on connection failure.
* Add extra requires for snappy.
* Add support for nsq auth protocol.


0.1.4 (2014-07-24)
~~~~~~~~~~~~~~~~~~

* Preemptively update ready count.
* Dependency and contributing documentation.
* Support for nsq back to 0.2.24.


0.1.3 (2014-07-08)
~~~~~~~~~~~~~~~~~~

* Block as expected on start, even if already started.
* Raise runtime error if starting the reader without a message handler.
* Add on_close signal to the reader.
* Allow upgrading to tls+snappy or tls+deflate.


0.1.2 (2014-07-08)
~~~~~~~~~~~~~~~~~~

* Flush delfate buffer for each message.


0.1.1 (2014-07-07)
~~~~~~~~~~~~~~~~~~

* Fix packaging stream submodule.
* Send queued messages before closing socket.
* Continue to read from socket on EAGAIN


0.1.0 (2014-07-07)
~~~~~~~~~~~~~~~~~~

* First release on PyPI.
