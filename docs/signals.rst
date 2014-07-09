Signals
-------

Both :doc:`Reader <reader>` and :doc:`Nsqd <nsqd>` classes expose various
signals provided by the `Blinker`_ library.

Subscribing to signals
~~~~~~~~~~~~~~~~~~~~~~

To subscribe to a signal, you can use the
:meth:`~blinker.base.Signal.connect` method of a signal.  The first
argument is the function that should be called when the signal is emitted,
the optional second argument specifies a sender.  To unsubscribe from a
signal, you can use the :meth:`~blinker.base.Signal.disconnect` method. ::

    def error_handler(reader, error):
        print 'Got an error:', error

    reader.on_error.connect(error_handler)

You can also easily subscribe to signals by using
:meth:`~blinker.base.NamedSignal.connect` as a decorator::

    @reader.on_giving_up.connect
    def handle_giving_up(reader, message):
        print 'Giving up on:', message.id

.. _Blinker: https://pypi.python.org/pypi/blinker
