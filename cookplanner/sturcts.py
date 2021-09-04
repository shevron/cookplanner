import dataclasses
from datetime import datetime
from typing import Any, Dict, Literal, Optional

WEEKDAYS = {"Sun": 6, "Mon": 0, "Tue": 1, "Wed": 2, "Thu": 3, "Fri": 4, "Sat": 5}
WEEKDAYS_REV = {v: k for k, v in WEEKDAYS.items()}

TaskStatus = Literal["new", "saved", "modified"]


@dataclasses.dataclass
class TaskOwner:
    name: str
    preferred_day: Optional[str] = None
    weight: float = 1.0

    def __str__(self) -> str:
        return self.name


@dataclasses.dataclass
class ScheduledTask:
    """A scheduled task, placed inside Schedule"""

    owner: TaskOwner
    date: datetime
    description: Optional[str] = None
    status: TaskStatus = "new"
    metadata: Dict[str, Any] = dataclasses.field(default_factory=dict)

    @property
    def date_str(self) -> str:
        return self.date.strftime("%Y-%m-%d")
