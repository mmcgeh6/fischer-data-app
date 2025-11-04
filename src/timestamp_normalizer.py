"""
Timestamp Normalization Module
Handles messy BMS timestamps and outputs consistent MM/DD/YYYY HH:MM:SS format
"""

import re
from datetime import datetime, timezone
from typing import Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from dateutil import parser as dateutil_parser

# Time zone abbreviation map -> IANA zone
TZ_ABBR_TO_IANA = {
    "EST": "America/New_York",
    "EDT": "America/New_York",
    "CST": "America/Chicago",
    "CDT": "America/Chicago",
    "MST": "America/Denver",
    "MDT": "America/Denver",
    "PST": "America/Los_Angeles",
    "PDT": "America/Los_Angeles",
    "UTC": "UTC",
    "GMT": "Etc/UTC",
    "Z": "UTC",
}

_ABBR_REGEX = re.compile(r"\b([A-Z]{2,4}|Z)\b")
_AMPM_FIX = re.compile(r"\b(a\.m\.|p\.m\.)\b", re.IGNORECASE)
_SPACE_NORM = re.compile(r"\s+")

# Explicit format attempts (order matters)
EXPLICIT_FORMATS = (
    # Common US with seconds
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%m-%d-%Y %I:%M:%S %p",
    "%m-%d-%Y %H:%M:%S",
    # Common US without seconds
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%Y %H:%M",
    "%m-%d-%Y %I:%M %p",
    "%m-%d-%Y %H:%M",
    # Text month with/without comma
    "%B %d, %Y %I:%M:%S %p",
    "%B %d, %Y %I:%M %p",
    "%b %d, %Y %I:%M %p",
    "%B %d %Y %H:%M:%S",
    "%B %d %Y %H:%M",
    # ISO-ish variants
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    # Oddball YYYY-DD-MM
    "%Y-%d-%m %H:%M",
    "%Y-%d-%m %H:%M:%S",
)


def _norm_spaces(s: str) -> str:
    """Normalize whitespace."""
    s = s.replace("\u00A0", " ")  # non-breaking space
    s = _SPACE_NORM.sub(" ", s.strip())
    return s


def _fix_ampm(s: str) -> str:
    """Convert a.m./p.m. -> AM/PM."""
    return _AMPM_FIX.sub(lambda m: m.group(0).replace(".", "").upper(), s)


def _extract_abbr_tz(s: str) -> Tuple[str, Optional[str]]:
    """Extract timezone abbreviation and return (cleaned string, IANA tz or None)."""
    match = _ABBR_REGEX.search(s)
    if not match:
        return s, None
    abbr = match.group(1)
    if abbr in TZ_ABBR_TO_IANA:
        s_wo = (s[:match.start()] + s[match.end():]).strip()
        return s_wo, TZ_ABBR_TO_IANA[abbr]
    return s, None


def _try_strptime(s: str, tz: Optional[str]) -> Optional[datetime]:
    """Try explicit formats with strptime."""
    for fmt in EXPLICIT_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if tz:
                dt = dt.replace(tzinfo=ZoneInfo(tz))
            return dt
        except Exception:
            continue
    return None


def _dateutil_parse(s: str, tz: Optional[str]) -> datetime:
    """Fallback to dateutil parser."""
    dt = dateutil_parser.parse(s, fuzzy=True)
    if dt.tzinfo is None and tz:
        dt = dt.replace(tzinfo=ZoneInfo(tz))
    return dt


def normalize_timestamp(
    value: str,
    assume_tz: str = "America/New_York",
) -> datetime:
    """
    Normalize a timestamp string to a timezone-aware datetime object.

    Args:
        value: Input timestamp string (can be messy)
        assume_tz: Timezone to assume if none detected

    Returns:
        Timezone-aware datetime object
    """
    s = _norm_spaces(value)
    s = _fix_ampm(s)

    s_wo_abbr, abbr_tz = _extract_abbr_tz(s)
    tz_to_use = abbr_tz or assume_tz

    # Try explicit formats first
    dt = _try_strptime(s_wo_abbr, abbr_tz)
    if dt is None:
        dt = _try_strptime(s, abbr_tz)

    # Fallback to dateutil
    if dt is None:
        dt = _dateutil_parse(s, abbr_tz)

    # If still naive, assume configured tz
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo(tz_to_use))

    return dt


def format_timestamp_mdy_hms(
    value: str,
    assume_tz: str = "America/New_York",
    output_tz: str = "America/New_York",
) -> str:
    """
    Normalize and format timestamp to MM/DD/YYYY HH:MM:SS.

    Args:
        value: Input timestamp string
        assume_tz: Timezone to assume if none detected
        output_tz: Timezone for output formatting

    Returns:
        Formatted string in MM/DD/YYYY HH:MM:SS format (24-hour)
    """
    dt = normalize_timestamp(value, assume_tz=assume_tz)
    dt_out = dt.astimezone(ZoneInfo(output_tz))
    return dt_out.strftime("%m/%d/%Y %H:%M:%S")


def detect_timestamp_format(value: str) -> str:
    """
    Detect the format of a timestamp string (for preview purposes).

    Returns:
        A human-readable description of the detected format
    """
    s = _norm_spaces(value)

    # Check for timezone abbreviation
    _, abbr_tz = _extract_abbr_tz(s)
    tz_info = f" ({abbr_tz})" if abbr_tz else " (no TZ)"

    # Check for AM/PM
    has_ampm = "AM" in s.upper() or "PM" in s.upper() or "A.M." in s.upper() or "P.M." in s.upper()
    time_format = "12-hour" if has_ampm else "24-hour"

    # Check for seconds
    has_seconds = s.count(":") >= 2
    seconds_info = " with seconds" if has_seconds else " no seconds"

    # Try to identify date format
    if re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", s):
        date_format = "ISO-style (YYYY-MM-DD)"
    elif re.search(r"\d{1,2}[-/]\d{1,2}[-/]\d{4}", s):
        date_format = "US-style (MM/DD/YYYY)"
    elif re.search(r"[A-Za-z]{3,}", s):
        date_format = "Text month"
    else:
        date_format = "Unknown"

    return f"{date_format}, {time_format}{seconds_info}{tz_info}"
