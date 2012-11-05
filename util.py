
def assert_list(item):
    if isinstance(item, basestring):
        item = [item]

    assert isinstance(item, (list, set, tuple))
    return item
