Contrib modules
---------------

Patterns and best practices for gnsq made code.


Batching messages
~~~~~~~~~~~~~~~~~


.. autoclass:: gnsq.contrib.batch.BatchHandler
  :members:
  :inherited-members:


Giveup handlers
~~~~~~~~~~~~~~~


.. autoclass:: gnsq.contrib.giveup.LogGiveupHandler
  :members:
  :inherited-members:


.. autoclass:: gnsq.contrib.giveup.JSONLogGiveupHandler
  :members:
  :inherited-members:


.. autoclass:: gnsq.contrib.giveup.NsqdGiveupHandler
  :members:
  :inherited-members:


Concurrency
~~~~~~~~~~~


.. autoclass:: gnsq.contrib.queue.QueueHandler
  :members:
  :inherited-members:
  :exclude-members: copy, put, put_nowait


.. autoclass:: gnsq.contrib.queue.ChannelHandler


Error logging
~~~~~~~~~~~~~


.. autoclass:: gnsq.contrib.sentry.SentryExceptionHandler
  :members:
  :inherited-members:
