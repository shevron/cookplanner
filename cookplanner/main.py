import logging
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import click
import dateutil.tz
import yaml

from . import schedule, utils
from .backend import GoogleCalendarBackend

LOG_FORMAT = "%(asctime)-15s %(levelname)s %(message)s"

_log = logging.getLogger(__name__)


@click.group()
@click.option(
    "-c",
    "--config-file",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=True,
    help="Config file path",
)
@click.pass_context
def main(ctx: Any, config_file: str) -> None:
    level = logging.DEBUG
    logging.basicConfig(format=LOG_FORMAT, level=level)
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    ctx.obj = {
        "config": config,
        "backend": GoogleCalendarBackend(**config["backend"]["google_calendar"]),
    }


@main.command("info")
@click.pass_obj
def print_info(obj: Dict[str, Any]) -> None:
    """Print some information based on config and exit"""
    owner_map = schedule.get_owner_map(obj["config"]["owners"])
    owners = schedule.get_preferred_days(owner_map.values())
    days = obj["config"]["schedule"]["weekdays_to_schedule"]

    print("Task Owners:")
    for owner_id, owner in owner_map.items():
        print(f"  {owner_id}: {owner.name}{' [inactive]' if not owner.active else ''}")

    print()
    print("Preferred weekdays:")
    for day in days:
        dc = owners.get(day, [])
        print(f"  {day}: {', '.join((c.name for c in dc))}")

    print()
    print(
        "Normal task cycle: ",
        schedule.get_normal_task_cycle(owner_map.values(), len(days)),
        "days",
    )


@main.command("auth")
@click.pass_obj
def authorize(obj: Dict[str, Any]) -> None:
    """Authorize app with Google Cloud"""
    backend: GoogleCalendarBackend = obj["backend"]
    creds = backend.authorize()
    if creds:
        print("You have authorized the app access to your calendars")


@main.command("list-holidays")
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("-s", "--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.pass_obj
def get_holidays(obj: Dict[str, Any], end: datetime, start: Optional[datetime]) -> None:
    """Get list of known holidays"""
    backend: GoogleCalendarBackend = obj["backend"]
    holidays = backend.get_holidays(end=end, start=start)
    for date, desc in holidays.items():
        print(date, f" - {desc}")


@main.command("list-tasks")
@click.option("-s", "--start", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.pass_obj
def get_tasks(obj: Dict[str, Any], start: datetime, end: Optional[datetime]) -> None:
    """List current scheduled tasks"""
    backend: GoogleCalendarBackend = obj["backend"]
    history = backend.get_scheduled_tasks(start, end)
    print(history)


@main.command("clear-tasks")
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("-s", "--start", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.confirmation_option(prompt="Are you sure you want to delete tasks?")
@click.pass_obj
def clear_schedule(obj: Dict[str, Any], end: datetime, start: datetime) -> None:
    backend: GoogleCalendarBackend = obj["backend"]
    backend.clear_all_tasks(start, end)


@main.command("set-owner")
@click.option("-d", "--date", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("-o", "--owner", type=str, required=True, help="Owner name")
@click.confirmation_option(
    prompt="Are you sure you want to manually set owner for this date?"
)
@click.pass_obj
def set_owner(obj: Dict[str, Any], date: datetime, owner: str) -> None:
    """Manually set owner for a specific date"""
    config = obj["config"]
    backend: GoogleCalendarBackend = obj["backend"]
    owners = schedule.get_owner_map(config["owners"])
    if owner not in owners:
        raise click.ClickException(f"Unknown owner: {owner}")

    sched = schedule.create_schedule(
        owners, {date.strftime("%Y-%m-%d"): owner}, status="new"
    )
    backend.save_schedule(sched)


@main.command("switch-owners")
@click.argument("date1", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.argument("date2", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.confirmation_option(
    prompt="Are you sure you want to manually switch owners for these dates?"
)
@click.pass_obj
def switch_owners(obj: Dict[str, Any], date1: datetime, date2: datetime) -> None:
    """Manually switch owners between two dates"""
    config = obj["config"]
    backend: GoogleCalendarBackend = obj["backend"]

    owners = schedule.get_owner_map(config["owners"])
    if date1 == date2:
        raise click.ClickException(f"Given dates are the same: {date1} == {date2}")
    elif date1 < date2:
        current = backend.get_scheduled_tasks(date1, date2, owners=owners)
    else:
        current = backend.get_scheduled_tasks(date2, date1, owners=owners)

    date1_current = current.get(date1)
    date2_current = current.get(date2)

    if date1_current is None or date2_current is None:
        raise click.ClickException("One of the given dates has no task scheduled")

    if date1_current.owner.id == date2_current.owner.id:
        return

    updated_schedule = schedule.create_schedule(
        owners,
        {
            date1.strftime("%Y-%m-%d"): date2_current.owner.id,
            date2.strftime("%Y-%m-%d"): date1_current.owner.id,
        },
        status="new",
    )
    backend.save_schedule(updated_schedule)


@main.command("update-names")
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("-s", "--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.pass_obj
def update_names(
    obj: Dict[str, Any],
    start: Optional[datetime],
    end: Optional[datetime],
) -> None:
    config = obj["config"]
    backend: GoogleCalendarBackend = obj["backend"]

    if start is None:
        start = datetime.now(tz=dateutil.tz.UTC)
    else:
        start = start.replace(tzinfo=dateutil.tz.UTC)

    if end is None:
        end = utils.get_year_end_date(config["schedule"].get("year_end", "06-30"))
    else:
        end = end.replace(tzinfo=dateutil.tz.UTC)

    owners = schedule.get_owner_map(config["owners"])
    current_schedule = backend.get_scheduled_tasks(start, end, owners)
    _log.info("Existing schedule loaded with %d scheduled tasks", len(current_schedule))

    for task in current_schedule:
        task.status = "modified"

    backend.save_schedule(current_schedule)


@main.command("schedule")
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("-s", "--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("-h", "--history-starts-at", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option(
    "-f",
    "--fixed-owner",
    type=(click.DateTime(formats=["%Y-%m-%d"]), str),
    multiple=True,
)
@click.option("--simulate", is_flag=True, help="Simulation mode")
@click.pass_obj
def create_schedule(
    obj: Dict[str, Any],
    start: Optional[datetime],
    end: Optional[datetime],
    history_starts_at: Optional[datetime],
    fixed_owner: Optional[List[Tuple[datetime, str]]],
    simulate: bool = False,
) -> None:
    """Create schedule"""
    config = obj["config"]
    backend: GoogleCalendarBackend = obj["backend"]

    if history_starts_at is None:
        history_starts_at = utils.get_year_start_date(
            config["schedule"].get("year_start", "09-01")
        )
    else:
        history_starts_at = history_starts_at.replace(tzinfo=dateutil.tz.UTC)

    if start is None:
        start = datetime.now(tz=dateutil.tz.UTC)
    else:
        start = start.replace(tzinfo=dateutil.tz.UTC)

    if end is None:
        end = utils.get_year_end_date(config["schedule"].get("year_end", "06-30"))
    else:
        end = end.replace(tzinfo=dateutil.tz.UTC)

    holidays = backend.get_holidays(end=end, start=start)

    if config["schedule"].get("random_seed"):
        schedule.set_random_seed(config["schedule"]["random_seed"])

    owners = schedule.get_owner_map(
        config["owners"],
        randomize=config["schedule"].get("randomize_owners_list", False),
    )
    current_schedule = backend.get_scheduled_tasks(history_starts_at, end, owners)
    _log.info(
        "Existing schedule loaded starting from %s with %d scheduled tasks",
        history_starts_at.strftime("%Y-%m-%d"),
        len(current_schedule),
    )

    # Fix pre-set owners
    for date, owner in fixed_owner:
        schedule.set_owner(
            current_schedule, owners, date.replace(tzinfo=dateutil.tz.UTC), owner
        )

    schedulers = schedule.get_schedulers(
        owners, config["schedule"]["weekdays_to_schedule"]
    )
    schedule.update_schedule(
        current_schedule,
        schedulers,
        start,
        end,
        config["schedule"]["weekdays_to_schedule"],
        holidays,
    )

    sim_data: Dict[str, Any] = defaultdict(
        lambda: {"count": 0, "min_gap": None, "last_sched": None, "sched": []}
    )
    for scheduled_task in current_schedule:
        # if scheduled_task.date < start:
        #     continue

        print(f"{scheduled_task.date_str}\t=>\t{scheduled_task.owner.name}")
        if simulate:
            if not scheduled_task.owner.active:
                continue

            owner_metrics = sim_data[scheduled_task.owner.name]
            owner_metrics["count"] += 1
            if owner_metrics["last_sched"]:
                gap = abs((scheduled_task.date - owner_metrics["last_sched"]).days)
                if owner_metrics["min_gap"] is None or gap < owner_metrics["min_gap"]:
                    owner_metrics["min_gap"] = gap
            owner_metrics["last_sched"] = scheduled_task.date
            owner_metrics["sched"].append(scheduled_task.date)

    if simulate:
        print("----- Simulation Results -----")
        for owner, owner_metrics in sim_data.items():
            weekdays = Counter([d.strftime("%a") for d in owner_metrics["sched"]])
            scheduled_on = ", ".join(
                [d.strftime("%Y-%m-%d") for d in owner_metrics["sched"]]
            )
            print(f"{owner}:")
            print(f"  Scheduled tasks: {owner_metrics['count']}")
            print(f"  Minimal gap between tasks: {owner_metrics['min_gap']}")
            print(f"  Weekdays: {weekdays}")
            print(f"  Scheduled on: {scheduled_on}")

    else:
        backend.save_schedule(current_schedule)


if __name__ == "__main__":
    main()
