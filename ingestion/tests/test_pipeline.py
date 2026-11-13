from wdi.client import WDIFetchError
from wdi.loader import IncompleteLoadError
from wdi.pipeline import _refresh


class FakeConn:
    def __init__(self):
        self.rolled_back = False

    def rollback(self):
        self.rolled_back = True


def raise_(exc):
    raise exc


def test_success_lands_records():
    landed = []
    assert _refresh(FakeConn(), lambda: [1, 2], lambda c, r: landed.extend(r))
    assert landed == [1, 2]


def test_api_failure_skips_without_landing():
    landed = []
    assert not _refresh(FakeConn(), lambda: raise_(WDIFetchError("timeout")), lambda c, r: landed.append(r))
    assert landed == []


def test_incomplete_load_rolls_back():
    conn = FakeConn()
    assert not _refresh(conn, lambda: [1], lambda c, r: raise_(IncompleteLoadError("short")))
    assert conn.rolled_back
