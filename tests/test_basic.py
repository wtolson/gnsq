import pytest

from six.moves import range
from gnsq import BackoffTimer
from gnsq import protocol as nsq


@pytest.mark.parametrize('name,good', [
    ('valid_name', True),
    ('invalid name with space', False),
    ('invalid_name_due_to_length_this_is_' + (4 * 'really_') + 'long', False),
    ('test-with_period.', True),
    ('test#ephemeral', True),
    ('test:ephemeral', False),
])
def test_topic_names(name, good):
    assert nsq.valid_topic_name(name) == good


@pytest.mark.parametrize('name,good', [
    ('test', True),
    ('test-with_period.', True),
    ('test#ephemeral', True),
    ('invalid_name_due_to_length_this_is_' + (4 * 'really_') + 'long', False),
    ('invalid name with space', False),
])
def test_channel_names(name, good):
    assert nsq.valid_channel_name(name) == good


def test_assert_topic():
    assert nsq.assert_valid_topic_name('topic') is None

    with pytest.raises(ValueError):
        nsq.assert_valid_topic_name('invalid name with space')


def test_assert_channel():
    assert nsq.assert_valid_channel_name('channel') is None

    with pytest.raises(ValueError):
        nsq.assert_valid_channel_name('invalid name with space')


def test_invalid_commands():
    with pytest.raises(TypeError):
        nsq.requeue('1234', None)

    with pytest.raises(TypeError):
        nsq.ready(None)

    with pytest.raises(ValueError):
        nsq.ready(-1)


def test_backoff_timer():
    timer = BackoffTimer(max_interval=1000)
    assert timer.get_interval() == 0
    assert timer.is_reset()

    timer.success()
    assert timer.get_interval() == 0
    assert timer.is_reset()

    timer.failure()
    assert timer.c == 1
    assert not timer.is_reset()

    for _ in range(100):
        interval = timer.get_interval()
        assert interval > 0 and interval < 2

    timer.failure()
    assert timer.c == 2

    for _ in range(100):
        interval = timer.get_interval()
        assert interval > 0 and interval < 4

    timer.success().success()
    assert timer.get_interval() == 0
    assert timer.is_reset()

    for _ in range(100):
        timer.failure()

    assert timer.c == 100
    assert timer.get_interval() <= 1000

    timer.reset()
    assert timer.c == 0
    assert timer.get_interval() == 0

    timer = BackoffTimer(min_interval=1000)
    assert timer.c == 0
    assert timer.get_interval() == 1000
