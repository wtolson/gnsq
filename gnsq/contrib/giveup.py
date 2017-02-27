# -*- coding: utf-8 -*-
import itertools
import json
import sys

import gnsq


class LogGiveupHandler(object):
    """Log messages on giveup.

    Example usage:
    >>> fp = open('topic.__BURY__.log', 'w')
    >>> reader.on_giving_up.connect(LogGiveupHandler(fp.write), weak=False)
    """
    def __init__(self, log=sys.stdout.write, newline='\n'):
        self.log = log
        self.newline = newline

    def format_message(self, message):
        data = json.dumps({
            'timestamp': message.timestamp,
            'attempts': message.attempts,
            'id': message.id,
            'body': message.body,
        })

        if self.newline:
            return data + self.newline

        return data

    def __call__(self, reader, message):
        self.log(self.format_message(message))


class NsqdGiveupHandler(object):
    """Send messages by to nsq on giveup.

    Example usage:
    >>> reader.on_giving_up.connect(NsqdGiveupHandler('topic.__BURY__'), weak=False)
    """

    def __init__(self, topic, nsqd_hosts=['localhost']):
        if not nsqd_hosts:
            raise ValueError('at least one nsqd host is required')
        self.topic = topic
        self.nsqds = itertools.cycle([gnsq.Nsqd(host) for host in nsqd_hosts])

    def __call__(self, reader, message):
        nsq = self.nsqds.next()
        nsq.publish(self.topic, message.body)
