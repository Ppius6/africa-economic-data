import pytest

from datetime import date
from comtrade.pipeline import REFRESH_WINDOW_YEARS, default_years

from comtrade.client import fetch_trade, m49_codes_for, RecordCapError, fetch_in_batches, hs_chapters
from comtrade.loader import IncompleteLoadError, check_slice_size

from comtrade.product_pipeline import PRODUCT_START_YEAR
from comtrade.product_pipeline import default_years as product_default_years

REPORTERS = [
    {"reporterCode": 230, "reporterCodeIsoAlpha3": "ETH",
        "isGroup": False},  # Ethiopia (...1992)
    {"reporterCode": 231, "reporterCodeIsoAlpha3": "ETH", "isGroup": False},
    {"reporterCode": 404, "reporterCodeIsoAlpha3": "KEN", "isGroup": False},
    {"reporterCode": 251, "reporterCodeIsoAlpha3": "FRA", "isGroup": False},
    {"reporterCode": 97, "reporterCodeIsoAlpha3": "EUR", "isGroup": True},
]


def test_m49_codes_include_every_historical_code():
    assert m49_codes_for(REPORTERS, {"ETH", "KEN"}) == [230, 231, 404, 711]


def test_m49_codes_skip_groups_and_other_countries():
    assert m49_codes_for(REPORTERS, {"EUR", "FRA"}) == [251, 711]


def test_m49_codes_always_include_customs_union():
    # SACU (711) has ISO3 "ZA1", so it is never matched by country; it must still be queried.
    assert 711 in m49_codes_for(REPORTERS, set())


def test_fetch_trade_needs_exactly_one_side():
    with pytest.raises(ValueError):
        fetch_trade(2022)
    with pytest.raises(ValueError):
        fetch_trade(2022, reporter_codes=[404], partner_codes=[404])


def test_first_load_of_zero_rows_allowed():
    check_slice_size("reporter 1990", 0, 0)


def test_empty_refresh_rejected():
    with pytest.raises(IncompleteLoadError, match="0 records"):
        check_slice_size("reporter 2022", 0, 15000)


def test_sharp_drop_rejected():
    with pytest.raises(IncompleteLoadError, match="down from"):
        check_slice_size("partner 2022", 12000, 15000)


def test_small_drop_and_growth_allowed():
    check_slice_size("partner 2022", 14000, 15000)
    check_slice_size("partner 2022", 16000, 15000)


def test_default_years_is_the_last_five_complete_years():
    assert default_years(date(2026, 11, 14)) == range(2021, 2026)


def test_default_years_never_starts_before_start_year():
    from config import START_YEAR
    early = date(START_YEAR + 2, 1, 1)
    assert default_years(early).start == START_YEAR


def test_hs_chapters_keeps_two_digit_codes_including_99():
    reference = [
        {"id": "TOTAL", "aggrLevel": 0},
        {"id": "01", "aggrLevel": 2},
        {"id": "0101", "aggrLevel": 4},
        {"id": "99", "aggrLevel": 2},
    ]
    assert hs_chapters(reference) == ["01", "99"]


def test_capped_batches_are_halved_until_they_fit(monkeypatch):
    monkeypatch.setattr("comtrade.client.time.sleep", lambda seconds: None)

    def fetch_batch(batch):
        if len(batch) > 2:
            raise RecordCapError("too big")
        return list(batch)

    assert sorted(fetch_in_batches(
        fetch_batch, list(range(10)), 5)) == list(range(10))


def test_single_item_over_cap_is_raised(monkeypatch):
    monkeypatch.setattr("comtrade.client.time.sleep", lambda seconds: None)

    def fetch_batch(batch):
        raise RecordCapError("too big")

    with pytest.raises(RecordCapError):
        fetch_in_batches(fetch_batch, ["01", "02"], 2)


def test_product_years_never_start_before_2000():
    assert product_default_years(date(2003, 6, 1)).start == PRODUCT_START_YEAR
    assert product_default_years(date(2026, 11, 15)) == range(2021, 2026)
