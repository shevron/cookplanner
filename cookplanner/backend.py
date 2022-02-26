import logging
import os.path
from datetime import datetime, timedelta
from typing import Dict, Iterable, Mapping, Optional

from dateutil import tz
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError

from .schedule import Schedule
from .sturcts import ScheduledTask, TaskOwner
from .utils import get_dates_in_range

_log = logging.getLogger(__name__)


class GoogleCalendarBackend:
    """Google Calendar backend

    For now this is the only backend we have, but maybe one day..."""

    SCOPES = [
        "https://www.googleapis.com/auth/calendar.readonly",
        "https://www.googleapis.com/auth/calendar.events",
    ]

    def __init__(
        self,
        client_secret_file: str,
        calendar_id: str,
        holiday_calendar_ids: Iterable[str],
        token_file_path: str = "gc-token.json",
    ):
        self._client_secret_file = client_secret_file
        self._calendar_id = calendar_id
        self._holiday_calendars = holiday_calendar_ids
        self._token_file_path = token_file_path
        self._creds = None

    def authorize(self) -> Credentials:
        """Authorize access to Google Calendar using consent screen or pre-existing token"""
        creds = None
        if self._creds is not None:
            creds = self._creds
        elif os.path.exists(self._token_file_path):
            creds = Credentials.from_authorized_user_file(
                self._token_file_path, self.SCOPES
            )

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self._client_secret_file, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self._token_file_path, "w") as token:
                token.write(creds.to_json())

        self._creds = creds
        return creds

    def get_scheduled_tasks(
        self,
        start_at: datetime,
        end: Optional[datetime],
        owners: Optional[Mapping[str, TaskOwner]] = None,
    ) -> Schedule:
        """Get all scheduled tasks in a given date range"""
        if not end:
            end = datetime.now(tz=tz.UTC)
        end = end.replace(hour=23, minute=59, second=59, microsecond=999999)
        events = (
            self._service.events()
            .list(
                calendarId=self._calendar_id,
                timeMin=start_at.replace(tzinfo=tz.UTC).isoformat(),
                timeMax=end.replace(tzinfo=tz.UTC).isoformat(),
                maxResults=1000,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
            .get("items", [])
        )

        schedule = Schedule()
        for event in events:
            event_props = event.get("extendedProperties", {}).get("private", {})
            if event_props.get("generator") != "cookplanner":
                continue

            start = datetime.fromisoformat(
                event["start"].get("dateTime", event["start"].get("date")),
            ).replace(tzinfo=tz.UTC)
            if start.strftime("%Y-%m-%d") in schedule:
                _log.warning("Date already recorded in cooking history: %s", start)
                continue

            owner_id = event_props.get("owner_id", event["summary"])
            if owners and owner_id in owners:
                owner = owners[owner_id]
            else:
                owner = TaskOwner(
                    id=owner_id,
                    name=event["summary"],
                    active=False,
                )

            task = ScheduledTask(
                owner=owner,
                date=start,
                status="saved",
                metadata={"calendar_event_id": event["id"]},
            )
            schedule.add(task)

        return schedule

    def get_holidays(
        self,
        end: datetime,
        start: Optional[datetime] = None,
    ) -> Dict[str, str]:
        """Get list of dates which are holidays and do not require cooking"""
        holidays = {}
        if start is None:
            start = datetime.now(tz=tz.UTC)

        for calendar_id in self._holiday_calendars:
            events = (
                self._service.events()
                .list(
                    calendarId=calendar_id,
                    timeMin=start.replace(tzinfo=tz.UTC).isoformat(),
                    timeMax=end.replace(tzinfo=tz.UTC).isoformat(),
                    maxResults=1000,
                    singleEvents=True,
                    orderBy="startTime",
                )
                .execute()
                .get("items", [])
            )

            for event in events:
                start = datetime.fromisoformat(
                    event["start"].get("dateTime", event["start"].get("date"))
                )
                end = datetime.fromisoformat(
                    event["end"].get("dateTime", event["end"].get("date"))
                )
                # Google Calendar end time is exclusive, we need it inclusive
                for date in get_dates_in_range(start, end - timedelta(days=1)):
                    holidays[date.strftime("%Y-%m-%d")] = event["summary"]

        return holidays

    def clear_all_tasks(self, start_at: datetime, end: datetime) -> None:
        """Clear all tasks in calendar in a given period"""
        tasks = self.get_scheduled_tasks(start_at, end)
        deleted = 0
        for task in tasks:
            task_id = task.metadata.get("calendar_event_id")
            if task_id:
                self._service.events().delete(
                    calendarId=self._calendar_id, eventId=task_id
                ).execute()
                deleted += 1
        _log.info("Deleted %d events from Google Calendar backend", deleted)

    def save_schedule(self, schedule: Schedule) -> int:
        """Save schedule to Google Calendar"""
        saved = 0
        for task in schedule:
            if task.status in {"new", "modified"}:
                self._save_task(task)
                saved += 1
        _log.info("Saved %d events to Google Calendar backend", saved)
        return saved

    def _save_task(self, task: ScheduledTask) -> None:
        """Save a single task"""
        task_id = f"cookplanner{task.date.strftime('%Y%m%d')}"
        _log.debug("Saving task %s", task_id)
        event_body = {
            "id": task_id,
            "summary": task.owner.name,
            "description": task.owner.name,
            "extendedProperties": {
                "private": {"generator": "cookplanner", "owner_id": task.owner.id}
            },
            "source": {
                "title": "cookplanner",
                "url": "https://github.com/shevron/cookplanner",
            },
            "start": {
                "date": task.date_str,
                "timezone": "UTC",
            },
            "end": {
                "date": (task.date + timedelta(days=1)).strftime("%Y-%m-%d"),
                "timezone": "UTC",
            },
        }

        try:
            # Try inserting
            event = (
                self._service.events()
                .insert(
                    calendarId=self._calendar_id,
                    body=event_body,
                )
                .execute()
            )
        except HttpError as e:
            if e.status_code == 409:
                _log.debug("Event ID already exists, updating")
                event = (
                    self._service.events()
                    .update(
                        calendarId=self._calendar_id,
                        eventId=task_id,
                        body=event_body,
                    )
                    .execute()
                )
            else:
                raise

        _log.debug("Created event: %s", event)

    @property
    def _service(self) -> Resource:
        creds = self.authorize()
        return build("calendar", "v3", credentials=creds)
