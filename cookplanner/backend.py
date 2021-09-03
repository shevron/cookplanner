import logging
import os.path
from datetime import datetime
from typing import Dict, Iterable, Optional

from dateutil import tz
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from .schedule import Schedule
from .utils import get_dates_in_range

_log = logging.getLogger(__name__)


class GoogleCalendarBackend:
    """Google Calendar backend

    For now this is the only backend we have, but maybe one day..."""

    SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]

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

    def get_scheduled_task_history(
        self, start_at: datetime, end: Optional[datetime]
    ) -> Dict[str, str]:
        """Get all scheduled tasks in a given date range"""
        creds = self.authorize()
        service = build("calendar", "v3", credentials=creds)

        if not end:
            end = datetime.now(tz=tz.UTC)
        events = (
            service.events()
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

        history = {}
        for event in events:
            start = datetime.fromisoformat(
                event["start"].get("dateTime", event["start"].get("date"))
            ).strftime("%Y-%m-%d")
            if start in history:
                _log.warning("Date already recorded in cooking history: %s", start)
                continue

            history[start] = event["summary"]

        return history

    def get_holidays(
        self,
        end: datetime,
        start: Optional[datetime] = None,
    ) -> Dict[str, str]:
        """Get list of dates which are holidays and do not require cooking"""
        creds = self.authorize()
        service = build("calendar", "v3", credentials=creds)
        holidays = {}
        if start is None:
            start = datetime.now(tz=tz.UTC)

        for calendar_id in self._holiday_calendars:
            events = (
                service.events()
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
                for date in get_dates_in_range(start, end):
                    holidays[date.strftime("%Y-%m-%d")] = event["summary"]

        return holidays

    def save_schedule(self, schedule: Schedule, calendar_id: str):
        """Save schedule to Google Calendar"""
        pass
