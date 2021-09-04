from datetime import datetime, timedelta
from typing import Iterable, Optional

from dateutil.tz import UTC


def get_dates_in_range(start: datetime, end: datetime) -> Iterable[datetime]:
    day = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
    while day <= end:
        yield day
        day = day + timedelta(days=1)


def get_year_start_date(config_date: str, year: Optional[int] = None) -> datetime:
    """Get the start date of the current year"""
    if year is not None:
        return datetime.strptime(config_date, "%m-%d").replace(year=year, tzinfo=UTC)
    now = datetime.now(tz=UTC)
    start = datetime.strptime(config_date, "%m-%d").replace(year=now.year, tzinfo=UTC)
    if start > now:
        start = start.replace(year=start.year - 1)
    return start


def get_year_end_date(config_date: str, year: Optional[int] = None) -> datetime:
    """Get the end date of the current year"""
    if year is not None:
        return datetime.strptime(config_date, "%m-%d").replace(year=year, tzinfo=UTC)
    now = datetime.now(tz=UTC)
    end = datetime.strptime(config_date, "%m-%d").replace(year=now.year, tzinfo=UTC)
    if end < now:
        end = end.replace(year=end.year + 1)
    return end.replace(hour=23, minute=59, second=59, microsecond=999999)
