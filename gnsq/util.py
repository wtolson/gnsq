from .errors import NSQException


def assert_list(item):
    if isinstance(item, basestring):
        item = [item]

    elif not isinstance(item, (list, set, tuple)):
        raise NSQException('must be a list, set or tuple')

    return item
