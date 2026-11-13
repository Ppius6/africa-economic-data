import pytest

from wdi.loader import IncompleteLoadError, check_ids_complete, check_indicator_grid

COUNTRIES = {"KEN", "NGA"}


def grid(countries=COUNTRIES, start=1990, end=1992):
    return [(c, y) for c in sorted(countries) for y in range(start, end + 1)]


def check(keys, previous_max_year=None):
    check_indicator_grid("TEST", keys, COUNTRIES, 1990, previous_max_year)


def test_full_grid_accepted():
    check(grid())


def test_new_year_published_accepted():
    check(grid(end=1993), previous_max_year=1992)


def test_empty_rejected():
    with pytest.raises(IncompleteLoadError, match="0 records"):
        check([])


def test_missing_country_rejected():
    with pytest.raises(IncompleteLoadError, match="missing countries"):
        check(grid(countries={"KEN"}))


def test_unexpected_country_rejected():
    with pytest.raises(IncompleteLoadError, match="unexpected"):
        check(grid(countries=COUNTRIES | {"FRA"}))


def test_duplicate_rows_rejected():
    with pytest.raises(IncompleteLoadError, match="duplicate"):
        check(grid() + [("KEN", 1990)])


def test_one_country_short_a_year_rejected():
    keys = [k for k in grid() if k != ("NGA", 1992)]
    with pytest.raises(IncompleteLoadError, match="expected 2 countries x 3 years"):
        check(keys)


def test_year_gap_rejected():
    keys = [k for k in grid() if k[1] != 1991]
    with pytest.raises(IncompleteLoadError, match="continuous"):
        check(keys)


def test_wrong_start_year_rejected():
    with pytest.raises(IncompleteLoadError, match="continuous"):
        check(grid(start=1991))


def test_latest_year_shrinking_rejected():
    with pytest.raises(IncompleteLoadError, match="fell from 1993"):
        check(grid(end=1992), previous_max_year=1993)


def test_metadata_complete_accepted():
    check_ids_complete("countries", ["KEN", "NGA"], COUNTRIES)


def test_metadata_missing_rejected():
    with pytest.raises(IncompleteLoadError, match="missing"):
        check_ids_complete("countries", ["KEN"], COUNTRIES)


def test_metadata_duplicate_rejected():
    with pytest.raises(IncompleteLoadError, match="1 duplicates"):
        check_ids_complete("countries", ["KEN", "NGA", "KEN"], COUNTRIES)
