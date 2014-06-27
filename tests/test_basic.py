import pytest

from gnsq import BackoffTimer
from gnsq import protocal as nsq


@pytest.mark.parametrize('name,good', [
    ('valid_name', True),
    ('invalid name with space', False),
    ('invalid_name_due_to_length_this_is_really_really_really_long', False),
    ('test-with_period.', True),
    ('test#ephemeral', False),
    ('test:ephemeral', False),
])
def test_topic_names(name, good):
    assert nsq.valid_topic_name(name) == good


@pytest.mark.parametrize('name,good', [
    ('test', True),
    ('test-with_period.', True),
    ('test#ephemeral', True),
    ('invalid_name_due_to_length_this_is_really_really_really_long', False),
    ('invalid name with space', False),
])
def test_channel_names(name, good):
    assert nsq.valid_channel_name(name) == good


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

    for _ in xrange(100):
        interval = timer.get_interval()
        assert interval > 0 and interval < 2

    timer.failure()
    assert timer.c == 2

    for _ in xrange(100):
        interval = timer.get_interval()
        assert interval > 0 and interval < 4

    timer.success().success()
    assert timer.get_interval() == 0
    assert timer.is_reset()

    for _ in xrange(100):
        timer.failure()

    assert timer.c == 100
    assert timer.get_interval() == 1000
