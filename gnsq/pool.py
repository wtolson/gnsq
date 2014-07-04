import multiprocessing
import gevent
from gevent.queue import Queue
from .reader import Reader


class ReaderPool(Reader):
    def __init__(self, topic, channel, pool_size=None, **kwargs):
        super(ReaderPool, self).__init__(topic, channel, **kwargs)

        if pool_size is None:
            pool_size = 2 * multiprocessing.cpu_count() + 1

        self.queue = Queue()
        self.pool_size = pool_size

    def _run(self):
        for conn, message in self.queue:
            super(ReaderPool, self).handle_message(conn, message)

    def handle_message(self, conn, message):
        self.queue.put((conn, message))

    def start(self, block=True):
        super(ReaderPool, self).start(block=False)

        workers = [gevent.spawn(self._run) for _ in xrange(self.pool_size)]
        self.workers.extend(workers)

        if block:
            self.join()
