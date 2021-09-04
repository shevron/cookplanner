import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, Optional

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
    """Create schedule"""
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


@main.command("schedule")
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("-s", "--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("-h", "--history-starts-at", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--simulate", is_flag=True, help="Simulation mode")
@click.pass_obj
def create_schedule(
    obj: Dict[str, Any],
    start: Optional[datetime],
    end: Optional[datetime],
    history_starts_at: Optional[datetime],
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
    _log.info("Existing schedule loaded with %d scheduled tasks", len(current_schedule))

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
        lambda: {"count": 0, "min_gap": None, "last_sched": None}
    )
    for scheduled_task in current_schedule:
        print(f"{scheduled_task.date_str}\t=>\t{scheduled_task.owner.name}")
        if simulate:
            owner_metrics = sim_data[scheduled_task.owner.name]
            owner_metrics["count"] += 1
            if owner_metrics["last_sched"]:
                gap = abs((scheduled_task.date - owner_metrics["last_sched"]).days)
                if owner_metrics["min_gap"] is None or gap < owner_metrics["min_gap"]:
                    owner_metrics["min_gap"] = gap
            owner_metrics["last_sched"] = scheduled_task.date

    if simulate:
        print("----- Simulation Results -----")
        for owner, owner_metrics in sim_data.items():
            print(f"{owner}:")
            print(f"  Scheduled tasks: {owner_metrics['count']}")
            print(f"  Minimal gap between tasks: {owner_metrics['min_gap']}")

    else:
        backend.save_schedule(current_schedule)


if __name__ == "__main__":
    main()
