from __future__ import annotations

from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


class AutomationScheduleError(ValueError):
    pass


def next_occurrence(
    schedule_type: str,
    after_ts: float,
    timezone_name: str = "UTC",
    local_time: str = "",
    weekdays: list[int] | None = None,
    interval_minutes: int | None = None,
    run_at: float | None = None,
) -> float | None:
    """Return the next scheduled instant after after_ts, or None for a completed one-shot."""
    if schedule_type == "once":
        return run_at if run_at is not None and run_at > after_ts else None
    if schedule_type == "interval":
        if interval_minutes is None or interval_minutes < 5 or interval_minutes > 10080:
            raise AutomationScheduleError("Interval must be between 5 minutes and 7 days")
        return after_ts + interval_minutes * 60

    try:
        zone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise AutomationScheduleError("Unknown IANA time zone") from exc

    try:
        hour_text, minute_text = local_time.split(":", 1)
        clock = time(int(hour_text), int(minute_text))
    except (ValueError, TypeError) as exc:
        raise AutomationScheduleError("Local time must use HH:MM format") from exc

    local_after = datetime.fromtimestamp(after_ts, zone)
    if schedule_type == "daily":
        candidate = datetime.combine(local_after.date(), clock, zone)
        if candidate.timestamp() <= after_ts:
            candidate = datetime.combine(local_after.date() + timedelta(days=1), clock, zone)
        return candidate.timestamp()

    if schedule_type == "weekly":
        selected = sorted(set(weekdays or []))
        if not selected or any(day < 1 or day > 7 for day in selected):
            raise AutomationScheduleError("Weekly schedules require weekdays from 1 (Monday) to 7 (Sunday)")
        for offset in range(8):
            day = local_after.date() + timedelta(days=offset)
            if day.isoweekday() not in selected:
                continue
            candidate = datetime.combine(day, clock, zone)
            if candidate.timestamp() > after_ts:
                return candidate.timestamp()
        raise AutomationScheduleError("Could not calculate the next weekly occurrence")

    raise AutomationScheduleError("Unsupported schedule type")
