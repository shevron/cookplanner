from datetime import datetime

from cookplanner import utils


def test_get_dates_in_range():
    start = datetime.strptime("2021-08-01", "%Y-%m-%d")
    end = datetime.strptime("2021-08-05", "%Y-%m-%d")

    actual = list(utils.get_dates_in_range(start, end))
    assert len(actual) == 5
    expected = ["2021-08-01", "2021-08-02", "2021-08-03", "2021-08-04", "2021-08-05"]
    assert [d.strftime("%Y-%m-%d") for d in actual] == expected
