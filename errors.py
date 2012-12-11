import socket

class NSQException(Exception):
    pass

class NSQRequeueMessage(NSQException):
    pass

class NSQNoConnections(NSQException):
    pass

class NSQSocketError(socket.error, NSQException):
    pass

class NSQFrameError(NSQException):
    pass

class NSQInvalid(NSQException):
    """E_INVALID"""
    pass

class NSQBadTopic(NSQException):
    """E_BAD_TOPIC"""
    pass

class NSQBadChannel(NSQException):
    """E_BAD_CHANNEL"""
    pass

class NSQBadMessage(NSQException):
    """E_BAD_MESSAGE"""
    pass

class NSQPutFailed(NSQException):
    """E_PUT_FAILED"""
    pass

class NSQFinishFailed(NSQException):
    """E_FINISH_FAILED"""
    pass

class NSQRequeueFailed(NSQException):
    """E_REQUEUE_FAILED"""
    pass

ERROR_CODES = {
    'E_INVALID':        NSQInvalid,
    'E_BAD_TOPIC':      NSQBadTopic,
    'E_BAD_CHANNEL':    NSQBadChannel,
    'E_BAD_MESSAGE':    NSQBadMessage,
    'E_PUT_FAILED':     NSQPutFailed,
    'E_FINISH_FAILED':  NSQFinishFailed,
    'E_REQUEUE_FAILED': NSQRequeueFailed
}

def make_error(error_code):
    assert error_code in ERROR_CODES
    return ERROR_CODES[error_code](error_code)
