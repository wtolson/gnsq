# -*- coding: utf-8 -*-


class SentryExceptionHandler(object):
    """Log gnsq exceptions to sentry.

    Example usage:
    >>> from raven import Sentry
    >>> sentry = Sentry()
    >>> reader.on_exception.connect(SentryExceptionHandler(sentry), weak=False)
    """

    def __init__(self, client):
        self.client = client

    def message_extra(self, message):
        return {
            'id': message.id,
            'timestamp': message.timestamp,
            'attempts': message.attempts,
            'body': message.body,
        }

    def __call__(self, reader, message, error):
        extra = {}

        if message:
            extra['message'] = self.message_extra(message)

        self.client.captureException(extra=extra)
