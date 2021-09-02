import logging
import os.path
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dateutil import tz
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from .schedule import get_dates_in_range

_log = logging.getLogger(__name__)

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/calendar.readonly"]


def authorize(client_secret_file: str) -> Credentials:
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds


def get_cooking_history(
    creds: Credentials, calendar_id: str, start_at: datetime, end: Optional[datetime]
) -> Dict[str, str]:
    """Get all past cooks between start_at and now"""
    service = build("calendar", "v3", credentials=creds)

    if not end:
        end = datetime.now(tz=tz.UTC)
    events = (
        service.events()
        .list(
            calendarId=calendar_id,
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
    creds: Credentials,
    calendars: List[str],
    end: datetime,
    start: Optional[datetime] = None,
) -> Dict[str, str]:
    """Get list of dates which are holidays and do not require cooking"""
    service = build("calendar", "v3", credentials=creds)
    holidays = {}
    if start is None:
        start = datetime.now(tz=tz.UTC)

    for calendar_id in calendars:
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
