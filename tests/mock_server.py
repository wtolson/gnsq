from gevent.event import AsyncResult
from gevent.server import StreamServer


class mock_server(object):
    def __init__(self, handler):
        self.handler = handler
        self.result = AsyncResult()
        self.server = StreamServer(('127.0.0.1', 0), self)

    def __call__(self, socket, address):
        try:
            self.result.set(self.handler(socket, address))
        except Exception as error:
            self.result.set_exception(error)
        finally:
            socket.close()

    def __enter__(self):
        self.server.start()
        return self.server

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.result.get()
        self.server.stop()
