class Message(object):
    def __init__(self, conn, timestamp, attempts, id, body):
        self.conn      = conn
        self.timestamp = timestamp
        self.attempts  = attempts
        self.id        = id
        self.body      = body

    def finish(self):
        self.conn.finish(self.id)

    def requeue(self, time_ms=0):
        self.conn.requeue(self.id, time_ms)
