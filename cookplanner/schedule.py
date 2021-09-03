"""Scheduler

Run a series of scheduling algorithms. If an algorithm fails to schedule,
the next one will try.
"""
import logging
import math
import random
from collections import defaultdict
from datetime import datetime, timedelta
from math import floor
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

from dateutil.tz import UTC

from .sturcts import WEEKDAYS, WEEKDAYS_REV, ScheduledTask, TaskOwner
from .utils import filter_weekdays, get_dates_in_range

_log = logging.getLogger(__name__)


class Schedule:
    def __init__(self) -> None:
        self._schedule: MutableMapping[str, ScheduledTask] = {}
        self._last_scheduled: MutableMapping[str, datetime] = {}
        self._first_scheduled: MutableMapping[str, datetime] = {}
        self._total_tasks: MutableMapping[str, int] = defaultdict(int)

    def __iter__(self) -> Iterator[Tuple[str, ScheduledTask]]:
        return iter(sorted(self._schedule.items(), key=lambda t: t[0]))

    def __len__(self) -> int:
        return len(self._schedule)

    def add(self, task: ScheduledTask) -> None:
        self._total_tasks[task.owner.name] += 1
        date = task.date.replace(hour=0, minute=0, second=0, microsecond=0)
        if task.owner.name not in self._last_scheduled or (self._last_scheduled[task.owner.name] < date):
            self._last_scheduled[task.owner.name] = date

        if task.owner.name not in self._first_scheduled or (self._first_scheduled[task.owner.name] > date):
            self._first_scheduled[task.owner.name] = date

        self._schedule[task.date_str] = task

    def get(self, date: Union[datetime, str]) -> Optional[ScheduledTask]:
        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")
        return self._schedule.get(date)

    def get_last_task_date(self, owner_name: str) -> Optional[datetime]:
        return self._last_scheduled.get(owner_name)

    def get_total_tasks(self, owner_name: str) -> int:
        return self._total_tasks.get(owner_name, 0)

    def get_nearest_task_date(self, owner_name: str, date: datetime) -> Optional[datetime]:
        """Get date of scheduled task for given owner closest to given date

        TODO: Optimize?
        """
        if owner_name not in self._first_scheduled or owner_name not in self._last_scheduled:
            return None

        distance = 0
        date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        while True:
            past_date = date - timedelta(days=distance)
            future_date = date + timedelta(days=distance)

            if past_date < self._first_scheduled[owner_name] and future_date > self._last_scheduled[owner_name]:
                return None

            if past_date >= self._first_scheduled[owner_name]:
                task = self._schedule.get(past_date.strftime("%Y-%m-%d"))
                if task and task.owner.name == owner_name:
                    return past_date

            if future_date <= self._last_scheduled[owner_name]:
                task = self._schedule.get(future_date.strftime("%Y-%m-%d"))
                if task and task.owner.name == owner_name:
                    return future_date

            distance += 1


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

    def _update_schedule(self, schedule: Schedule, owner: TaskOwner, date: datetime):
        task = ScheduledTask(
            owner, date, metadata={"scheduler": self.__class__.__name__}
        )
        _log.debug(f"Chose {owner.name} as owner for {task.date_str}")
        schedule.add(task)


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
        self.preferred_days = get_preferred_days(self.owners)
        self.global_mts = get_normal_task_cycle(self.owners, len(self.week_days))

    def schedule(self, date: datetime, schedule: Schedule) -> bool:
        preferred_owners = self.preferred_days[WEEKDAYS_REV[date.weekday()]]
        if not preferred_owners:
            return False

        best_match = None
        for last_scheduled, owner in self._filter_potential_owners(
            date, preferred_owners, schedule
        ):
            if (
                best_match is None
                or best_match[0] is None
                or last_scheduled is None
                or best_match[0] > last_scheduled
            ):
                best_match = (last_scheduled, owner)

        if best_match:
            self._update_schedule(schedule, best_match[1], date)
            return True
        return False

    def _filter_potential_owners(
        self, date: datetime, owners: List[TaskOwner], schedule: Schedule
    ) -> Iterable[Tuple[datetime, TaskOwner]]:
        """Get iterator for owners that weren't scheduled for more than their MCS"""
        for owner in owners:
            owner_mts = self.global_mts / owner.weight
            last_scheduled = schedule.get_last_task_date(owner.name)
            if last_scheduled:
                days_since = (date - last_scheduled).days
                if days_since < owner_mts:
                    _log.debug(
                        f"Eliminating {owner.name}, last scheduled on {last_scheduled}, {days_since} days ago"
                    )
                    continue
            yield last_scheduled, owner


class HistoricCooksCountScheduler(Scheduler):
    """Secondary algorithm: based on past actual cookings + distance from MCS edges

    - Calculate a global MCS (Minimal Cook Cycle)
    - For each cook:
      - Calculate number of past actual cookings (multiplied by weight, floored)
    - Pick the cook(s) with the smallest number of past actual cookings
    - If only one cook is picked - pick them
    - If more than one cook is picked (have the same # of past cookings), pick the cook
      with shortest distance from PMCS / 2

    FIXME BUG: we strictly prefer historic task count over distance from last / next cook.
      This probably causes that a few of the people end up having very close schedules.
      A smarter approach might be:
        - Pick the person with least cooks
        - Find the best dates for them
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self.global_mts = get_normal_task_cycle(self.owners, len(self.week_days))
        self.owners_by_name = {o.name: o for o in self.owners}

    def schedule(self, date: datetime, schedule: Schedule) -> bool:
        past_ownerships = list(
            sorted(
                ((self._get_total_past_tasks(o, schedule), o.name) for o in self.owners)
            )
        )
        least_scheduled_val = past_ownerships[0][0]
        least_scheduled = []

        # Get owners with least number of past tasks scheduled
        for ownership in past_ownerships:
            if ownership[0] <= least_scheduled_val + 1:
                least_scheduled.append(ownership[1])

        _log.debug(f"Least scheduled group size: {len(least_scheduled)}; Least scheduled value: {least_scheduled_val}")
        if least_scheduled_val == 0:
            owner = self.owners_by_name[least_scheduled[0]]
            self._update_schedule(schedule, owner, date)
            return True
        # This shouldn't happen
        elif len(least_scheduled) == 0:
            _log.warning(
                "Didn't find any owners with least number of scheduled tasks, this shouldn't happen"
            )
            return False

        # We have several owner with the same number. Find the one farthest from being scheduled again
        min_distance = 7
        distances = list(
            sorted(
                filter(
                    lambda d: d[0] > min_distance,
                    (
                        (self._get_scheduled_distance(date, o, schedule), o)
                        for o in least_scheduled
                    )
                ),
                reverse=True
            )
        )

        if not distances:
            return False

        owner = self.owners_by_name[distances[0][1]]
        self._update_schedule(schedule, owner, date)
        return True

    def _get_scheduled_distance(
        self, date: datetime, owner_name: str, schedule: Schedule
    ) -> Union[int, str]:
        nearest_task = schedule.get_nearest_task_date(owner_name, date)
        if nearest_task is None:
            _log.warning(f"Looks like {owner_name} was never scheduled...")
            return "A"  # Hack: "A" is always sorted after integers

        owner = self.owners_by_name[owner_name]
        distance = abs((date - nearest_task).days)
        _log.debug(f"Days to nearest scheduled task for {owner_name} from {date}: {distance}")
        return math.floor(distance / owner.weight)

    @staticmethod
    def _get_total_past_tasks(owner: TaskOwner, schedule: Schedule) -> int:
        return math.floor(schedule.get_total_tasks(owner.name) / owner.weight)


class SomeoneRandom(Scheduler):
    def schedule(self, date: datetime, schedule: Schedule) -> bool:
        owner = random.choice(self.owners)
        self._update_schedule(schedule, owner, date)
        return True


def set_random_seed(seed: float) -> None:
    """Initialize a pre-defined random seed

    This can optionally done to enforce repeatability between runs. If not
    done, each run will yield different results if random functions are used.
    """
    _log.info(f"Setting random seed to {seed}")
    random.seed(seed)


def get_owner_list(owners_config: List[Dict[str, Any]]) -> List[TaskOwner]:
    return [TaskOwner(**o) for o in owners_config]


def get_schedulers(owners: List[TaskOwner], weekdays: List[str]) -> Iterable[Scheduler]:
    schedulers = [
        PreferredDayBasedScheduler(owners, weekdays),
        HistoricCooksCountScheduler(owners, weekdays),
        # SomeoneRandom(owners, weekdays),
    ]
    return schedulers


def get_preferred_days(owners: Iterable[TaskOwner]) -> Dict[str, List[TaskOwner]]:
    """Get all owners by their preferred days"""
    days = defaultdict(list)
    for owner in owners:
        if not owner.preferred_day:
            _log.warning("Owner has no preferred day: %s", owner.name)
            continue
        days[owner.preferred_day].append(owner)
    return days


def get_normal_task_cycle(owners: List[TaskOwner], scheduled_weekdays: int):
    """Get global normal task cycle length"""
    total_owners = sum((owner.weight for owner in owners))
    return floor(total_owners / scheduled_weekdays * 7)


def create_existing_schedule(
    owners_list: List[TaskOwner], existing: Dict[str, str]
) -> Schedule:
    """Load date->owner key value pair list into a new Schedule object"""
    owners = {o.name: o for o in owners_list}
    schedule = Schedule()
    for date_str, owner_name in existing:
        date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
        schedule.add(
            ScheduledTask(
                owner=owners.get(owner_name, TaskOwner(name=owner_name)),
                date=date,
                status="saved",
            )
        )
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

            _log.debug(
                f"Attempting to schedule cook for {day.strftime('%a, %Y-%m-%d')} using {scheduler}"
            )
            if not scheduler.schedule(day, schedule):
                all_scheduled = False

        if all_scheduled:
            _log.info("All days in range have been scheduled, we're done")
            break

    if not all_scheduled:
        _log.warning("Didn't manage scheduling all days in range")

    return schedule
