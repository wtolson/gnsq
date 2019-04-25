# -*- coding: utf-8 -*-
import functools
import warnings


class cached_property(object):
    """A decorator that converts a function into a lazy property."""

    def __init__(self, func, name=None, doc=None):
        self.__name__ = name or func.__name__
        self.__module__ = func.__module__
        self.__doc__ = doc or func.__doc__
        self.func = func

    def __get__(self, obj, type=None):
        if obj is None:
            return self

        if self.__name__ in obj.__dict__:
            return obj.__dict__[self.__name__]

        value = obj.__dict__[self.__name__] = self.func(obj)
        return value


def deprecated(fn):
    """Mark a function as deprecated and warn the user on use."""
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        warnings.warn(fn.__doc__.split('\n')[0],
                      category=DeprecationWarning, stacklevel=2)
        return fn(*args, **kwargs)
    return wrapper
