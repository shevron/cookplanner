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

from .sturcts import WEEKDAYS, WEEKDAYS_REV, ScheduledTask, TaskOwner, TaskStatus
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
        self._total_tasks[task.owner.id] += 1
        date = task.date.replace(hour=0, minute=0, second=0, microsecond=0)
        if task.owner.id not in self._last_scheduled or (
            self._last_scheduled[task.owner.id] < date
        ):
            self._last_scheduled[task.owner.id] = date

        if task.owner.id not in self._first_scheduled or (
            self._first_scheduled[task.owner.id] > date
        ):
            self._first_scheduled[task.owner.id] = date

        self._schedule[task.date_str] = task

    def get(self, date: Union[datetime, str]) -> Optional[ScheduledTask]:
        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")
        return self._schedule.get(date)

    def get_last_task_date(self, owner: TaskOwner) -> Optional[datetime]:
        return self._last_scheduled.get(owner.id)

    def get_total_tasks(self, owner: TaskOwner) -> int:
        return self._total_tasks.get(owner.id, 0)

    def get_nearest_task_date(
        self, owner: TaskOwner, date: datetime
    ) -> Optional[datetime]:
        """Get date of scheduled task for given owner closest to given date"""
        if (
            owner.id not in self._first_scheduled
            or owner.id not in self._last_scheduled
        ):
            return None

        distance = 0
        date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        while True:
            past_date = date - timedelta(days=distance)
            future_date = date + timedelta(days=distance)

            if (
                past_date < self._first_scheduled[owner.id]
                and future_date > self._last_scheduled[owner.id]
            ):
                _log.debug(
                    "Went out of bounds for %s @ %s: %s is < %s and %s > %s",
                    owner.name,
                    date.strftime("%Y-%m-%d"),
                    past_date.strftime("%Y-%m-%d"),
                    self._first_scheduled[owner.id].strftime("%Y-%m-%d"),
                    future_date.strftime("%Y-%m-%d"),
                    self._last_scheduled[owner.id].strftime("%Y-%m-%d")
                )
                return None

            if past_date >= self._first_scheduled[owner.id]:
                task = self._schedule.get(past_date.strftime("%Y-%m-%d"))
                if task and task.owner.id == owner.id:
                    return past_date

            if future_date <= self._last_scheduled[owner.id]:
                task = self._schedule.get(future_date.strftime("%Y-%m-%d"))
                if task and task.owner.id == owner.id:
                    return future_date

            distance += 1


class Scheduler:
    def __init__(
        self, owners: Mapping[str, TaskOwner], week_days: Optional[Iterable[str]] = None
    ):
        self.owners = {k: o for k, o in owners.items() if o.active}
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

        best_match: Optional[Tuple[Optional[datetime], TaskOwner]] = None
        for last_scheduled, owner in self._filter_potential_owners(
            date, preferred_owners, schedule
        ):
            if (
                best_match is None
                or last_scheduled is None
                or (best_match[0] is not None and best_match[0] > last_scheduled)
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
            last_scheduled = schedule.get_last_task_date(owner)
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

    LEAST_SCHEDULED_GROUP_TOLERANCE = 1
    MIN_SCHEDULE_DISTANCE = 5

    # def __init__(self, *args: Any, **kwargs: Any):
    #     super().__init__(*args, **kwargs)
    #     self.global_mts = get_normal_task_cycle(self.owners, len(self.week_days))

    def _schedule_date(self, date: datetime, schedule: Schedule) -> bool:
        blocked_weekday = {
            o.id
            for o in self.owners.values()
            if WEEKDAYS_REV[date.weekday()] in o.blocked_days
        }
        if blocked_weekday:
            _log.debug(
                "Owners that have %s blocked so are eliminated: %s",
                WEEKDAYS_REV[date.weekday()],
                blocked_weekday,
            )

        past_ownerships = list(
            sorted(
                (
                    (self._get_total_past_tasks(o, schedule), o.id)
                    for o in self.owners.values()
                    if o.id not in blocked_weekday
                )
            )
        )
        least_scheduled_val = past_ownerships[0][0]
        least_scheduled = []

        # Get owners with least number of past tasks scheduled
        for ownership in past_ownerships:
            if (
                ownership[0]
                <= least_scheduled_val + self.LEAST_SCHEDULED_GROUP_TOLERANCE
            ):
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

        # We have several owners with the same number of past schedulings.
        # Find the one farthest from being scheduled again
        distances = list(
            sorted(
                filter(
                    lambda d: d[0] >= self.MIN_SCHEDULE_DISTANCE,
                    (
                        (self._get_scheduled_distance(date, self.owners[o_id], schedule), o_id)
                        for o_id in least_scheduled
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

    @staticmethod
    def _get_scheduled_distance(
        date: datetime, owner: TaskOwner, schedule: Schedule
    ) -> Union[int, float]:
        nearest_task = schedule.get_nearest_task_date(owner, date)
        if nearest_task is None:
            _log.warning(f"Looks like {owner.name} was never scheduled...")
            return float("inf")  # infinity!

        distance = abs((date - nearest_task).days)
        _log.debug(
            f"Days to nearest scheduled task for {owner.name} from {date}: {distance}"
        )
        return math.ceil(distance / owner.weight)

    @staticmethod
    def _get_total_past_tasks(owner: TaskOwner, schedule: Schedule) -> int:
        return math.floor(schedule.get_total_tasks(owner) / owner.weight) + owner.start_counter_from


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
    return {
        o.id: o
        for v in owners_config
        if (
            o := TaskOwner(
                id=str(v.get("id", v["name"])),
                name=v["name"],
                preferred_day=v.get("preferred_day"),
                blocked_days=set(v.get("blocked_days", [])),
                weight=v.get("weight", 1.0),
                active=v.get("active", True),
                start_counter_from=v.get("start_counter_from", 0)
            )
        )
    }


def set_owner(schedule: Schedule, owners: Mapping[str, TaskOwner], date: datetime, owner_id: str) -> None:
    """Set owner for task date manually"""
    owner = owners[owner_id]
    _log.debug("Manually setting owner for %s to %s", date.strftime("%Y-%m-%d"), owner.name)
    schedule.add(ScheduledTask(
        owner=owner,
        date=date,
        metadata={"scheduler": "manual"},
    ))


def get_schedulers(
    owners: Mapping[str, TaskOwner], weekdays: List[str]
) -> Iterable[Scheduler]:
    schedulers = [
        PreferredDayBasedScheduler(owners, weekdays),
        HistoricCooksCountScheduler(owners, weekdays),
        # SomeoneRandom(owners, weekdays),
    ]
    return schedulers


def get_preferred_days(owners: Iterable[TaskOwner], include_inactive: bool = False) -> Dict[str, List[TaskOwner]]:
    """Get all owners by their preferred days"""
    days = defaultdict(list)
    for owner in owners:
        if not include_inactive and not owner.active:
            continue
        if not owner.preferred_day:
            _log.warning("Owner has no preferred day: %s", owner.name)
            continue
        days[owner.preferred_day].append(owner)
    return days


def get_normal_task_cycle(owners: Iterable[TaskOwner], scheduled_weekdays: int) -> int:
    """Get global normal task cycle length"""
    total_owners = sum((owner.weight for owner in owners))
    return floor(total_owners / scheduled_weekdays * 7)


def create_schedule(
    owners: Mapping[str, TaskOwner],
    existing: Dict[str, str],
    status: TaskStatus = "saved",
) -> Schedule:
    """Load date->owner key value pair list into a new Schedule object"""
    schedule = Schedule()
    for date_str, owner_id in existing.items():
        date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
        schedule.add(
            ScheduledTask(
                owner=owners[owner_id],
                date=date,
                status=status,
                metadata={"scheduler": "manual"},
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
    exclude_dates = set(holidays.keys())
    for scheduler in schedulers:
        _log.info("Starting to schedule tasks using %s", scheduler)
        dates_iter = _get_dates_iter(start, end, weekdays_to_schedule, exclude_dates)
        if scheduler.schedule(dates_iter, schedule):
            _log.info("All days in range have been scheduled, we're done")
            break
    else:
        _log.warning("Didn't manage scheduling all days in range")

    return schedule


def _get_dates_iter(
    start: datetime,
    end: datetime,
    include_weekdays: Iterable[str],
    exclude_dates: Set[str],
) -> Iterable[datetime]:
    """Create an iterable that yields dates that should be scheduled"""
    include_weekdays = set(include_weekdays)

    def f(d: datetime) -> bool:
        return (
            WEEKDAYS_REV[d.weekday()] in include_weekdays
            and d.strftime("%Y-%m-%d") not in exclude_dates
        )

    return filter(f, get_dates_in_range(start, end))
