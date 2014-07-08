import pytest


def pytest_addoption(parser):
    parser.addoption(
        '--fast',
        action='store_true',
        help='do not run slow tests'
    )


def pytest_runtest_setup(item):
    if 'slow' in item.keywords and item.config.getoption('--fast'):
        pytest.skip('skiping because of --fast')
