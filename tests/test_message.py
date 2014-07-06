import pytest
import gnsq


class MockConnection(object):
    def __init__(self, operations):
        self.operations = iter(operations)

    def __getattr__(self, name):
        def check_args(*args):
            expected_name, expected_args = self.operations.next()
            assert name == expected_name
            assert args == expected_args
        return check_args

    def assert_finished(self):
        with pytest.raises(StopIteration):
            self.operations.next()

    def connect_message(self, message):
        message.on_finish.connect(self.finish)
        message.on_requeue.connect(self.requeue)
        message.on_touch.connect(self.touch)


def test_basic():
    message = gnsq.Message(0, 42, '1234', 'sup')
    assert message.timestamp == 0
    assert message.attempts == 42
    assert message.id == '1234'
    assert message.body == 'sup'
    assert message.has_responded() is False


def test_finish():
    mock_conn = MockConnection([
        ('finish', ('1234',)),
    ])

    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn.connect_message(message)
    assert message.has_responded() is False

    message.finish()
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.finish()


def test_requeue():
    mock_conn = MockConnection([
        ('requeue', ('1234', 0)),
    ])

    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn.connect_message(message)
    assert message.has_responded() is False

    message.requeue()
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.requeue()

    mock_conn.assert_finished()


def test_requeue_timeout():
    mock_conn = MockConnection([
        ('requeue', ('1234', 1000)),
    ])

    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn.connect_message(message)
    assert message.has_responded() is False

    message.requeue(1000)
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.requeue(1000)

    mock_conn.assert_finished()


def test_touch():
    mock_conn = MockConnection([
        ('touch', ('1234',)),
        ('touch', ('1234',)),
        ('touch', ('1234',)),
        ('finish', ('1234',)),
    ])

    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn.connect_message(message)
    assert message.has_responded() is False

    message.touch()
    message.touch()
    message.touch()
    assert message.has_responded() is False

    message.finish()
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.touch()

    mock_conn.assert_finished()
