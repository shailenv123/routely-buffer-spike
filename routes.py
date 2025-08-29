from datetime import datetime
from typing import List, Tuple


# Primary routes of interest
ROUTES = [
    ("VIC", "GTW"),        # Victoria -> Gatwick
    ("PAD", "HXX"),        # Paddington -> Heathrow T2-3
    ("PAD", "HWX"),        # Paddington -> Heathrow T4
    ("PAD", "HWY"),        # Paddington -> Heathrow T5
    ("EXD", "PAD"),        # Exeter -> Paddington
    ("PLY", "PAD"),        # Plymouth -> Paddington
    ("PAD", "BRI"),        # Paddington -> Bristol
]

# Optional feeder routes for chaining later
FEEDERS = [("EXD", "PAD")]  # Exeter -> Paddington

# Time buckets covering the full day (HHMM format)
TIME_BUCKETS = [
    ("0000", "0259"), ("0300", "0559"),
    ("0600", "0859"), ("0900", "1159"),
    ("1200", "1459"), ("1500", "1759"),
    ("1800", "2059"), ("2100", "2359"),
]


def get_days_tag(date: datetime) -> str:
    """Convert a datetime to HSP days tag format."""
    weekday = date.weekday()  # 0=Monday, 6=Sunday
    
    if weekday <= 4:  # Monday-Friday
        return "WEEKDAY"
    elif weekday == 5:  # Saturday
        return "SATURDAY"
    else:  # Sunday
        return "SUNDAY"
