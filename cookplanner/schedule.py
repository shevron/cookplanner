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
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Union,
)

from dateutil.tz import UTC

from .sturcts import WEEKDAYS, WEEKDAYS_REV, ScheduledTask, TaskOwner
from .utils import get_dates_in_range

_log = logging.getLogger(__name__)


class Schedule:
    def __init__(self) -> None:
        self._schedule: MutableMapping[str, ScheduledTask] = {}
        self._last_scheduled: MutableMapping[str, datetime] = {}
        self._first_scheduled: MutableMapping[str, datetime] = {}
        self._total_tasks: MutableMapping[str, int] = defaultdict(int)

    def __iter__(self) -> Iterator[ScheduledTask]:
        for date_str in sorted(self._schedule.keys()):
            yield self._schedule[date_str]
        # return iter(sorted(self._schedule.items(), key=lambda t: t[0]))

    def __len__(self) -> int:
        return len(self._schedule)

    def __contains__(self, item: str) -> bool:
        return item in self._schedule

    def add(self, task: ScheduledTask) -> None:
        self._total_tasks[task.owner.name] += 1
        date = task.date.replace(hour=0, minute=0, second=0, microsecond=0)
        if task.owner.name not in self._last_scheduled or (
            self._last_scheduled[task.owner.name] < date
        ):
            self._last_scheduled[task.owner.name] = date

        if task.owner.name not in self._first_scheduled or (
            self._first_scheduled[task.owner.name] > date
        ):
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

    def get_nearest_task_date(
        self, owner_name: str, date: datetime
    ) -> Optional[datetime]:
        """Get date of scheduled task for given owner closest to given date

        TODO: Optimize?
        """
        if (
            owner_name not in self._first_scheduled
            or owner_name not in self._last_scheduled
        ):
            return None

        distance = 0
        date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        while True:
            past_date = date - timedelta(days=distance)
            future_date = date + timedelta(days=distance)

            if (
                past_date < self._first_scheduled[owner_name]
                and future_date > self._last_scheduled[owner_name]
            ):
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
        self, owners: Mapping[str, TaskOwner], week_days: Optional[Iterable[str]] = None
    ):
        self.owners = owners
        if week_days is None:
            self.week_days = set(WEEKDAYS.keys())
        else:
            self.week_days = set(week_days)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"

    def schedule(self, dates: Iterable[datetime], schedule: Schedule) -> bool:
        """Schedule an owner for tasks on the given dates and update schedule in accordance

        Each implementation will do this based on it's own algorithm
        A scheduler should return True if scheduling *all* dates was successful, or False otherwise
        """
        raise NotImplementedError("Must implement this")

    def _update_schedule(
        self, schedule: Schedule, owner: TaskOwner, date: datetime
    ) -> None:
        task = ScheduledTask(
            owner, date, metadata={"scheduler": self.__class__.__name__}
        )
        _log.debug(f"Chose {owner.name} as owner for {task.date_str}")
        schedule.add(task)


class _IteratingScheduler(Scheduler):
    """Base scheduler class for schedulers that work day by day"""

    def schedule(self, dates: Iterable[datetime], schedule: Schedule) -> bool:
        all_scheduled = True
        for date in dates:
            if not self._should_schedule_date(date, schedule):
                continue

            _log.debug(
                f"Attempting to schedule cook for {date.strftime('%a, %Y-%m-%d')} using {self}"
            )

            all_scheduled = self._schedule_date(date, schedule) and all_scheduled

        return all_scheduled

    def _should_schedule_date(self, date: datetime, schedule: Schedule) -> bool:
        """Decide if the scheduler should modify today's schedule;

        By default, will skip days that already have been scheduled
        """
        return not bool(schedule.get(date))

    def _schedule_date(self, date: datetime, schedule: Schedule) -> bool:
        raise NotImplementedError("Subclasses should implmenet this")


class PreferredDayBasedScheduler(_IteratingScheduler):
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

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.preferred_days = get_preferred_days(self.owners.values())
        self.global_mts = get_normal_task_cycle(
            self.owners.values(), len(self.week_days)
        )

    def _schedule_date(self, date: datetime, schedule: Schedule) -> bool:
        preferred_owners = self.preferred_days[WEEKDAYS_REV[date.weekday()]]
        if not preferred_owners:
            return False

        best_match = None
        for last_scheduled, owner in self._filter_potential_owners(
            date, preferred_owners, schedule
        ):
            if best_match is None:
                best_match = (last_scheduled, owner)
            elif (
                best_match[0] is None
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
    ) -> Iterable[Tuple[Optional[datetime], TaskOwner]]:
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


class HistoricCooksCountScheduler(_IteratingScheduler):
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

    # def __init__(self, *args: Any, **kwargs: Any):
    #     super().__init__(*args, **kwargs)
    #     self.global_mts = get_normal_task_cycle(self.owners, len(self.week_days))

    def _schedule_date(self, date: datetime, schedule: Schedule) -> bool:
        past_ownerships = list(
            sorted(
                (
                    (self._get_total_past_tasks(o, schedule), o.name)
                    for o in self.owners.values()
                )
            )
        )
        least_scheduled_val = past_ownerships[0][0]
        least_scheduled = []

        # Get owners with least number of past tasks scheduled
        for ownership in past_ownerships:
            if ownership[0] <= least_scheduled_val + 1:
                least_scheduled.append(ownership[1])

        _log.debug(
            f"Least scheduled group size: {len(least_scheduled)}; Least scheduled value: {least_scheduled_val}"
        )
        if least_scheduled_val == 0:
            owner = self.owners[least_scheduled[0]]
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
                    ),
                ),
                reverse=True,
            )
        )

        if not distances:
            return False

        owner = self.owners[distances[0][1]]
        self._update_schedule(schedule, owner, date)
        return True

    def _get_scheduled_distance(
        self, date: datetime, owner_name: str, schedule: Schedule
    ) -> Union[int, float]:
        nearest_task = schedule.get_nearest_task_date(owner_name, date)
        if nearest_task is None:
            _log.warning(f"Looks like {owner_name} was never scheduled...")
            return float("inf")  # infinity!

        owner = self.owners[owner_name]
        distance = abs((date - nearest_task).days)
        _log.debug(
            f"Days to nearest scheduled task for {owner_name} from {date}: {distance}"
        )
        return math.floor(distance / owner.weight)

    @staticmethod
    def _get_total_past_tasks(owner: TaskOwner, schedule: Schedule) -> int:
        return math.floor(schedule.get_total_tasks(owner.name) / owner.weight)


class SomeoneRandom(_IteratingScheduler):
    def _schedule_date(self, date: datetime, schedule: Schedule) -> bool:
        owner = random.choice(list(self.owners.values()))
        self._update_schedule(schedule, owner, date)
        return True


def set_random_seed(seed: float) -> None:
    """Initialize a pre-defined random seed

    This can optionally done to enforce repeatability between runs. If not
    done, each run will yield different results if random functions are used.
    """
    _log.info(f"Setting random seed to {seed}")
    random.seed(seed)


def get_owner_map(
    owners_config: List[Dict[str, Any]], randomize: bool = False
) -> Mapping[str, TaskOwner]:
    if randomize:
        _log.info("Randomizing owner map")
        owners_config = list(owners_config)
        random.shuffle(owners_config)
    return {o.name: o for v in owners_config if (o := TaskOwner(**v))}


def get_schedulers(
    owners: Mapping[str, TaskOwner], weekdays: List[str]
) -> Iterable[Scheduler]:
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


def get_normal_task_cycle(owners: Iterable[TaskOwner], scheduled_weekdays: int) -> int:
    """Get global normal task cycle length"""
    total_owners = sum((owner.weight for owner in owners))
    return floor(total_owners / scheduled_weekdays * 7)


def create_existing_schedule(
    owners_list: List[TaskOwner], existing: Dict[str, str]
) -> Schedule:
    """Load date->owner key value pair list into a new Schedule object"""
    owners = {o.name: o for o in owners_list}
    schedule = Schedule()
    for date_str, owner_name in existing.items():
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
    weekdays_to_schedule: List[str],
    holidays: Dict[str, str],
) -> Schedule:
    """Create schedule for given period"""
    include_weekdays = set((WEEKDAYS[d] for d in weekdays_to_schedule))
    exclude_dates = set(holidays.keys())
    for scheduler in schedulers:
        dates_iter = _get_dates_iter(start, end, include_weekdays, exclude_dates)
        if scheduler.schedule(dates_iter, schedule):
            _log.info("All days in range have been scheduled, we're done")
            break
    else:
        _log.warning("Didn't manage scheduling all days in range")

    return schedule


def _get_dates_iter(
    start: datetime, end: datetime, include_weekdays: Set[int], exclude_dates: Set[str]
) -> Iterable[datetime]:
    """Create an iterable that yields dates that should be scheduled"""

    def f(d: datetime) -> bool:
        return (
            d.weekday() in include_weekdays
            and d.strftime("%Y-%m-%d") not in exclude_dates
        )

    return filter(f, get_dates_in_range(start, end))
