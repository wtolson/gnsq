# -*- coding: utf-8 -*-
import itertools
import json
import sys

import gnsq


class LogGiveupHandler(object):
    """Log messages on giveup.

    Writes the message body to the log. This can be customized by subclassing
    and implementing `format_message`. Assuming messages do not requeued using
    the to_nsq utility.

    Example usage:
    >>> fp = open('topic.__BURY__.log', 'w')
    >>> reader.on_giving_up.connect(LogGiveupHandler(fp.write), weak=False)
    """
    def __init__(self, log=sys.stdout.write, newline='\n'):
        self.log = log
        self.newline = newline

    def format_message(self, message):
        return message.body

    def __call__(self, reader, message):
        self.log(self.format_message(message) + self.newline)


class JSONLogGiveupHandler(LogGiveupHandler):
    """Log messages as json on giveup.

    Works like `LogGiveupHandler` but serializes the message details as json
    before writing to the log.

    Example usage:
    >>> fp = open('topic.__BURY__.log', 'w')
    >>> reader.on_giving_up.connect(JSONLogGiveupHandler(fp.write), weak=False)
    """
    def format_message(self, message):
        return json.dumps({
            'timestamp': message.timestamp,
            'attempts': message.attempts,
            'id': message.id,
            'body': message.body,
        })


class NsqdGiveupHandler(object):
    """Send messages by to nsq on giveup.

    Forwards the message body to the given topic where it can be inspected and
    requeued. This can be customized by subclassing and implementing
    `format_message`. Messages can be requeued with the nsq_to_nsq utility.

    Example usage:
    >>> giveup_handler = NsqdGiveupHandler('topic.__BURY__')
    >>> reader.on_giving_up.connect(giveup_handler)
    """

    def __init__(self, topic, nsqd_hosts=['localhost']):
        if not nsqd_hosts:
            raise ValueError('at least one nsqd host is required')
        self.topic = topic
        self.nsqds = itertools.cycle([gnsq.Nsqd(host) for host in nsqd_hosts])

    def format_message(self, message):
        return message.body

    def __call__(self, reader, message):
        nsq = self.nsqds.next()
        nsq.publish(self.topic, self.format_message(message))
