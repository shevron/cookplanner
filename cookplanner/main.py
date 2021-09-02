import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import click
import dateutil.tz
import yaml

from . import backend, schedule

LOG_FORMAT = "%(asctime)-15s %(levelname)s %(message)s"

_log = logging.getLogger(__name__)


@click.group()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    required=True,
    help="Config file path",
)
@click.pass_context
def main(ctx, config):
    level = logging.DEBUG
    logging.basicConfig(format=LOG_FORMAT, level=level)
    with open(config, "r") as f:
        ctx.obj = {"config": yaml.safe_load(f)}


@main.command("info")
@click.pass_obj
def print_info(obj):
    """Print some information based on config and exit"""
    cooks = schedule.get_preferred_days(obj["config"]["cooks"])
    days = obj["config"]["schedule"]["daysToPlan"]
    print("Preferred weekdays:")
    for day in days:
        dc = cooks.get(day, [])
        print(f"  {day}: {', '.join((c['name'] for c in dc))}")
    print()

    print(
        "Cooking Cycle: ", schedule.get_cooking_cycle(obj["config"]["cooks"], len(days))
    )


@main.command("auth")
@click.pass_obj
def authorize(obj):
    """Authorize app with Google Cloud"""
    creds = backend.authorize(obj["config"]["auth"]["google_client_secret_file"])
    if creds:
        print("You have authorized the app access to your calendars")


@main.command("list-holidays")
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("-s", "--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.pass_obj
def get_holidays(obj, end: datetime, start: Optional[datetime]):
    """Get list of known holidays"""
    config = obj["config"]
    creds = backend.authorize(config["auth"]["google_client_secret_file"])
    holidays = backend.get_holidays(
        creds, config["calendars"]["holidayCalendarIds"], end=end, start=start
    )
    for date, desc in holidays.items():
        print(date, f" - {desc}")


@main.command("get-history")
@click.option("-s", "--start", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.pass_obj
def get_history(obj, start, end):
    """Create schedule"""
    config = obj["config"]
    creds = backend.authorize(config["auth"]["google_client_secret_file"])
    history = backend.get_cooking_history(
        creds, config["calendars"]["scheduleCalendarId"], start, end
    )
    print(history)


@main.command("schedule")
@click.option("-e", "--end", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("-s", "--start", default=None, type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("-h", "--history-starts-at", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.pass_obj
def create_schedule(
    obj,
    start: Optional[datetime],
    end: Optional[datetime],
    history_starts_at: Optional[datetime],
):
    """Create schedule"""
    config = obj["config"]

    if history_starts_at is None:
        history_starts_at = schedule.get_year_start_date(
            config["schedule"].get("yearStart", "09-01")
        )
    else:
        history_starts_at = history_starts_at.replace(tzinfo=dateutil.tz.UTC)
    if start is None:
        start = datetime.now(tz=dateutil.tz.UTC)
    else:
        start = start.replace(tzinfo=dateutil.tz.UTC)
    if end is None:
        end = schedule.get_year_end_date(config["schedule"].get("yearEnd", "06-30"))
    else:
        end = end.replace(tzinfo=dateutil.tz.UTC)

    creds = backend.authorize(config["auth"]["google_client_secret_file"])
    holidays = backend.get_holidays(
        creds, config["calendars"]["holidayCalendarIds"], end=end, start=start
    )
    history = backend.get_cooking_history(
        creds, config["calendars"]["scheduleCalendarId"], history_starts_at, end=start
    )
    schedulers = schedule.get_schedulers(config["cooks"], history)
    cooking_plan = schedule.create_schedule(
        schedulers, start, end, config["schedule"]["daysToPlan"], holidays
    )

    for day, cook_info in cooking_plan.items():
        if "holiday" in cook_info:
            print(f"{day}\tHoliday: {cook_info['holiday']}")
        else:
            print(f"{day}\t{cook_info['cook']}")


if __name__ == "__main__":
    main()
