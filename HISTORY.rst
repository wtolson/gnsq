.. :changelog:

History
-------

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
