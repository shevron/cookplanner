"""Scheduler

Run a series of scheduling algorithms. If an algorithm fails to schedule,
the next one will try.
"""
import logging
import math
import random
from collections import defaultdict
from datetime import datetime
from dateutil.tz import UTC
from math import floor
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Tuple, Union

from .sturcts import WEEKDAYS, WEEKDAYS_REV, ScheduledTask, TaskOwner
from .utils import filter_weekdays, get_dates_in_range

_log = logging.getLogger(__name__)


class Schedule:
    def __init__(self) -> None:
        self._schedule: MutableMapping[str, ScheduledTask] = {}
        self._last_scheduled: MutableMapping[str, datetime] = {}
        self._total_tasks: MutableMapping[str, int] = defaultdict(int)

    def __iter__(self) -> Iterable[Tuple[str, ScheduledTask]]:
        return iter(self._schedule.items())

    def __len__(self) -> int:
        return len(self._schedule)

    def add(self, task: ScheduledTask) -> None:
        self._total_tasks[task.owner.name] += 1
        if task.owner.name in self._last_scheduled:
            self._last_scheduled[task.owner.name] = max(
                self._last_scheduled[task.owner.name], task.date
            )
        else:
            self._last_scheduled[task.owner.name] = task.date
        self._schedule[task.date_str] = task

    def get(self, date: Union[datetime, str]) -> Optional[ScheduledTask]:
        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")
        return self._schedule.get(date)

    def get_last_task_date(self, cook: str) -> Optional[datetime]:
        return self._last_scheduled.get(cook)

    def get_total_tasks(self, cook: str) -> int:
        return self._total_tasks.get(cook, 0)


class Scheduler:
    def __init__(
        self, owners: List[TaskOwner], week_days: Optional[Iterable[str]] = None
    ):
        self.owners = owners
        if week_days is None:
            self.week_days = set(WEEKDAYS.keys())
        else:
            self.week_days = set(week_days)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"

    def schedule(self, date: datetime, schedule: Schedule) -> bool:
        """Schedule an owner for task on the given date and update schedule in accordance

        Each implementation will do this based on it's own algorithm

        A scheduler should return True if scheduling was successful, or False otherwise
        """
        raise NotImplementedError("Must implement this")

    # def _get_last_cooked(
    #     self, cook: str, existing_schedule: Schedule
    # ) -> Optional[datetime]:
    #     last_cooked_historic = self.history.get_last_cook_date(cook)
    #     last_cooked_planned = existing_schedule.get_last_cook_date(cook)
    #     if last_cooked_historic:
    #         if last_cooked_planned:
    #             return max(last_cooked_historic, last_cooked_planned)
    #         return last_cooked_historic
    #     elif last_cooked_planned:
    #         return last_cooked_planned
    #     return None


class PreferredDayBasedScheduler(Scheduler):
    """Primary algorithm: based on preferred day + minimal cook cycle for each cook
    - Calculate a global MCS (Minimal Cook Cycle)
    - Take all cooks that have set this day as their preferred day of week
    - For each cook:
      - Calculate the cooks personal minimal cook cycle (PMCS = MCS / weight)
      - Find out the cook's last cooking date (PLCD)
      - If NOW - PLCD < PMCS remove cook off list
    - Pick the cook with the earliest PLCD
    - If no cooks match the criteria, give up
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.preferred_days = get_preferred_days(self.cooks)
        self.global_mcs = get_cooking_cycle(
            self.cooks, 5
        )  # FIXME: get actual count of cooking weekdays

    def schedule(self, date: datetime, existing_schedule: Schedule) -> Optional[str]:
        cooks = self.preferred_days[WEEKDAYS_REV[date.weekday()]]
        if not cooks:
            return None

        best_match = None
        for last_cooked, cook in self._get_potential_cooks(
            date, cooks, existing_schedule
        ):
            if (
                best_match is None
                or best_match[0] is None
                or last_cooked is None
                or best_match[0] > last_cooked
            ):
                best_match = (last_cooked, cook)
        if best_match:
            return best_match[1]["name"]
        return None

    def _get_potential_cooks(
        self, date: datetime, cooks: List[Dict[str, Any]], existing_schedule
    ) -> Iterable[Tuple[datetime, Dict[str, Any]]]:
        """Get iterator for cooks that didn't cook for more than their MCS"""
        for cook in cooks:
            cook_mcs = self.global_mcs / cook.get("weight", 1.0)
            last_cooked = self._get_last_cooked(cook["name"], existing_schedule)
            if last_cooked:
                days_since_cook = (date - last_cooked).days
                if days_since_cook < cook_mcs:
                    _log.debug(
                        f"Eliminating {cook['name']}, last cooked on {last_cooked}, {days_since_cook} days ago"
                    )
                    continue
            yield last_cooked, cook


class HistoricCooksCountScheduler(Scheduler):
    """Secondary algorithm: based on past actual cookings + distance from MCS edges
    - Calculate a global MCS (Minimal Cook Cycle)
    - For each cook:
      - Calculate number of past actual cookings (multiplied by weight, floored)
    - Pick the cook(s) with the smallest number of past actual cookings
    - If only one cook is picked - pick them
    - If more than one cook is picked (have the same # of past cookings), pick the cook
      with shortest distance from PMCS / 2
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.global_mcs = get_cooking_cycle(
            self.cooks, 5
        )  # FIXME: get actual count of cooking weekdays

    def schedule(self, date: datetime, existing_schedule: Schedule) -> Optional[str]:
        past_cookings = list(
            sorted(
                (
                    (self._get_total_past_cookings(c, existing_schedule), c)
                    for c in self.cooks
                ),
                key=lambda c: (c[0], c[1]["name"]),
            )
        )
        least_cookings = past_cookings[0][0]
        least_cooked = []

        # Get cooks with least number of past cookings
        for cooking in past_cookings:
            if cooking[0] == least_cookings:
                least_cooked.append(cooking[1])

        if len(least_cooked) == 1:
            return least_cooked[0]["name"]
        # We have some people who never cooked - just return the 1st one
        elif least_cookings == 0:
            return least_cooked[0]["name"]

        # This shouldn't happen
        elif len(least_cooked) == 0:
            _log.warning(
                "Didn't find any cooks with least number of cookings, this shouldn't happen"
            )
            return None

        # We have several cooks with the same number. Find the one farthest from cooking again
        distances = list(
            sorted(
                (
                    (self._get_cooking_distance(date, c, existing_schedule), c)
                    for c in least_cooked
                ),
                key=lambda c: (c[0], c[1]["name"]),
            )
        )
        return distances[0][1]["name"]

    def _get_total_past_cookings(
        self, cook: Dict[str, Any], existing_schedule: Schedule
    ) -> int:
        name = cook["name"]
        cookings = existing_schedule.get_total_cookings(
            name
        ) + self.history.get_total_cookings(name)
        return math.floor(cookings * cook.get("weight", 1.0))

    def _get_cooking_distance(self, date, cook: Dict[str, Any], schedule) -> int:
        pmcs = self.global_mcs * cook.get("weight", 1.0)
        last_cooked = self._get_last_cooked(cook["name"], schedule)
        days_since_cook = (date - last_cooked).days
        return math.floor(abs(days_since_cook - (pmcs / 2)))


class SomeoneRandom(Scheduler):
    def schedule(self, date: datetime, schedule: Schedule) -> bool:
        owner = random.choice(self.owners)
        task = ScheduledTask(owner, date, metadata={
            "scheduler": self.__class__.__name__
        })
        schedule.add(task)
        return True


def get_schedulers(
    owners_config: List[Dict[str, Any]], weekdays: List[str]
) -> Iterable[Scheduler]:
    owners = [TaskOwner(**o) for o in owners_config]
    schedulers = [
        # PreferredDayBasedScheduler(owners, weekdays),
        # HistoricCooksCountScheduler(owners, weekdays),
        SomeoneRandom(owners, weekdays),
    ]
    return schedulers


def get_preferred_days(config: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Get all cooks by their preferred days"""
    days = defaultdict(list)
    for cook in config:
        if "preferred_day" not in cook:
            _log.warning("Cook has no preferred day: %s", cook)
            continue
        day = cook.pop("preferred_day")
        days[day].append(cook)
    return days


def get_cooking_cycle(cooks: List[Dict[str, Any]], cooking_days: int):
    """Get global cooking cycle length"""
    total_cooks = sum((cook.get("weight", 1.0) for cook in cooks))
    return floor(total_cooks / cooking_days * 7)


def create_existing_schedule(owners_config: List[Dict[str, Any]], existing: Dict[str, str]) -> Schedule:
    """Load date->owner key value pair list into a new Schedule object"""
    owners = {o["name"]: TaskOwner(**o) for o in owners_config}
    schedule = Schedule()
    for date_str, owner_name in existing:
        date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
        schedule.add(ScheduledTask(
            owner=owners.get(owner_name, TaskOwner(name=owner_name)),
            date=date,
            status="saved",
        ))
    return schedule


def update_schedule(
    schedule: Schedule,
    schedulers: Iterable[Scheduler],
    start: datetime,
    end: datetime,
    cooking_weekdays: List[str],
    holidays: Dict[str, str],
) -> Schedule:
    """Create schedule for given period"""
    all_scheduled = False

    for scheduler in schedulers:
        all_scheduled = True
        for day in filter_weekdays(cooking_weekdays, get_dates_in_range(start, end)):
            if schedule.get(day):
                continue

            day_str = day.strftime("%Y-%m-%d")
            if day_str in holidays:
                continue

            _log.debug(f"Attempting to schedule cook for {day.strftime('%a, %Y-%m-%d')} using {scheduler}")
            if not scheduler.schedule(day, schedule):
                all_scheduled = False

        if all_scheduled:
            _log.info("All days in range have been scheduled, we're done")
            break

    if not all_scheduled:
        _log.warning("Didn't manage scheduling all days in range")

    return schedule
