.. :changelog:

History
-------

0.2.3 (TBD)
~~~~~~~~~~~
* Remove disconnected nsqd messages from the worker queue.

0.2.2 (2015-01-12)
~~~~~~~~~~~~~~~~~~
* Allow finishing and requeuing in sync handlers.

0.2.1 (2015-01-12)
~~~~~~~~~~~~~~~~~~
* Topics and channels are now valid to 64 characters.
* Ephemeral topics are now valid.
* Adjustable backoff behaviour.

0.2.0 (2014-08-03)
~~~~~~~~~~~~~~~~~~
* Warn on connection failure.
* Add extra requires for snappy.
* Add support for nsq auth protocal.

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
