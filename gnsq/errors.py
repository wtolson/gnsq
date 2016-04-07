# -*- coding: utf-8 -*-
from __future__ import absolute_import
import socket


class NSQException(Exception):
    pass


class NSQRequeueMessage(NSQException):
    def __init__(self, backoff=None):
        self.backoff = backoff
        super(NSQRequeueMessage, self).__init__()


class NSQNoConnections(NSQException):
    pass


class NSQHttpError(NSQException):
    pass


class NSQSocketError(socket.error, NSQException):
    pass


class NSQFrameError(NSQException):
    pass


class NSQErrorCode(NSQException):
    fatal = True


class NSQInvalid(NSQErrorCode):
    """E_INVALID"""
    pass


class NSQBadBody(NSQErrorCode):
    """E_BAD_BODY"""
    pass


class NSQBadTopic(NSQErrorCode):
    """E_BAD_TOPIC"""
    pass


class NSQBadChannel(NSQErrorCode):
    """E_BAD_CHANNEL"""
    pass


class NSQBadMessage(NSQErrorCode):
    """E_BAD_MESSAGE"""
    pass


class NSQPutFailed(NSQErrorCode):
    """E_PUT_FAILED"""
    pass


class NSQPubFailed(NSQErrorCode):
    """E_PUB_FAILED"""


class NSQMPubFailed(NSQErrorCode):
    """E_MPUB_FAILED"""


class NSQAuthDisabled(NSQErrorCode):
    """E_AUTH_DISABLED"""


class NSQAuthFailed(NSQErrorCode):
    """E_AUTH_FAILED"""


class NSQUnauthorized(NSQErrorCode):
    """E_UNAUTHORIZED"""


class NSQFinishFailed(NSQErrorCode):
    """E_FIN_FAILED"""
    fatal = False


class NSQRequeueFailed(NSQErrorCode):
    """E_REQ_FAILED"""
    fatal = False


class NSQTouchFailed(NSQErrorCode):
    """E_TOUCH_FAILED"""
    fatal = False


ERROR_CODES = {
    b'E_INVALID': NSQInvalid,
    b'E_BAD_BODY': NSQBadBody,
    b'E_BAD_TOPIC': NSQBadTopic,
    b'E_BAD_CHANNEL': NSQBadChannel,
    b'E_BAD_MESSAGE': NSQBadMessage,
    b'E_PUT_FAILED': NSQPutFailed,
    b'E_PUB_FAILED': NSQPubFailed,
    b'E_MPUB_FAILED': NSQMPubFailed,
    b'E_FINISH_FAILED': NSQFinishFailed,
    b'E_AUTH_DISABLED': NSQAuthDisabled,
    b'E_AUTH_FAILED': NSQAuthFailed,
    b'E_UNAUTHORIZED': NSQUnauthorized,
    b'E_FIN_FAILED': NSQFinishFailed,
    b'E_REQUEUE_FAILED': NSQRequeueFailed,
    b'E_REQ_FAILED': NSQRequeueFailed,
    b'E_TOUCH_FAILED': NSQTouchFailed
}


def make_error(error_code):
    parts = error_code.split(None, 1)
    return ERROR_CODES.get(parts[0], NSQErrorCode)(parts[-1])
