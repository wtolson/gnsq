import pytest
import gnsq


class MockConnection(object):
    def __init__(self, message, operations):
        message.on_finish.connect(self.finish)
        message.on_requeue.connect(self.requeue)
        message.on_touch.connect(self.touch)
        self.operations = iter(operations)

    def finish(self, message):
        exp_name, exp_args = next(self.operations)
        assert exp_name == 'finish'
        assert exp_args == (message,)

    def requeue(self, message, timeout, backoff):
        exp_name, exp_args = next(self.operations)
        assert exp_name == 'requeue'
        assert exp_args == (message, timeout, backoff)

    def touch(self, message):
        exp_name, exp_args = next(self.operations)
        assert exp_name == 'touch'
        assert exp_args == (message,)

    def assert_finished(self):
        with pytest.raises(StopIteration):
            next(self.operations)


def test_basic():
    message = gnsq.Message(0, 42, '1234', 'sup')
    assert message.timestamp == 0
    assert message.attempts == 42
    assert message.id == '1234'
    assert message.body == 'sup'
    assert message.has_responded() is False


def test_finish():
    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn = MockConnection(message, [
        ('finish', (message,)),
    ])
    assert message.has_responded() is False

    message.finish()
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.finish()

    mock_conn.assert_finished()


def test_requeue():
    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn = MockConnection(message, [
        ('requeue', (message, 0, True)),
    ])
    assert message.has_responded() is False

    message.requeue()
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.requeue()

    mock_conn.assert_finished()


def test_requeue_timeout():
    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn = MockConnection(message, [
        ('requeue', (message, 1000, True)),
    ])
    assert message.has_responded() is False

    message.requeue(1000)
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.requeue(1000)

    mock_conn.assert_finished()


def test_backoff():
    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn = MockConnection(message, [
        ('requeue', (message, 0, False)),
    ])
    assert message.has_responded() is False

    message.requeue(backoff=False)
    assert message.has_responded() is True

    with pytest.raises(gnsq.errors.NSQException):
        message.requeue()

    mock_conn.assert_finished()


def test_touch():
    message = gnsq.Message(0, 42, '1234', 'sup')
    mock_conn = MockConnection(message, [
        ('touch', (message,)),
        ('touch', (message,)),
        ('touch', (message,)),
        ('finish', (message,)),
    ])
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
