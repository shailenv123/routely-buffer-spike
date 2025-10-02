"""
Lightweight ingest logger for audit trails and performance monitoring.
"""

import json
import os
from datetime import datetime
from typing import Dict, Tuple, Optional


def write_ingest_log(
    date: str,
    origin: str, 
    dest: str,
    window: Optional[Tuple[str, str]],
    metrics_secs: float,
    services_count: int,
    rids_pre: int,
    rids_post: int,
    details_secs: float,
    details_ok: int,
    details_fail: int,
    retry_counts: Dict[str, int]
) -> None:
    """
    Write ingestion audit log entry.
    
    Args:
        date: ISO date being processed (YYYY-MM-DD)
        origin: Origin CRS code
        dest: Destination CRS code  
        window: Time window tuple (from_time, to_time) or None for full day
        metrics_secs: Time spent on metrics API call
        services_count: Number of services returned by metrics call
        rids_pre: Number of RIDs before deduplication
        rids_post: Number of RIDs after deduplication and capping
        details_secs: Time spent on details API calls
        details_ok: Number of successful detail calls
        details_fail: Number of failed detail calls
        retry_counts: Dict of retry counts by operation type
    """
    # Ensure logs directory exists
    log_dir = "data/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    # Build log entry
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "date": date,
        "route": {
            "origin": origin,
            "dest": dest
        },
        "time_window": {
            "from_time": window[0] if window else None,
            "to_time": window[1] if window else None,
            "is_full_day": window is None
        },
        "metrics_phase": {
            "duration_secs": round(metrics_secs, 2),
            "services_count": services_count,
            "rids_collected": rids_pre
        },
        "processing": {
            "rids_pre_dedup": rids_pre,
            "rids_post_cap": rids_post,
            "dedup_ratio": round(rids_post / rids_pre, 3) if rids_pre > 0 else 0
        },
        "details_phase": {
            "duration_secs": round(details_secs, 2),
            "requests_ok": details_ok,
            "requests_fail": details_fail,
            "success_rate": round(details_ok / (details_ok + details_fail), 3) if (details_ok + details_fail) > 0 else 0
        },
        "retries": retry_counts,
        "performance": {
            "total_duration_secs": round(metrics_secs + details_secs, 2),
            "rids_per_sec": round(rids_post / (metrics_secs + details_secs), 2) if (metrics_secs + details_secs) > 0 else 0
        }
    }
    
    # Write to log file (append mode)
    log_filename = f"ingest_{date}_{origin}_{dest}.json"
    log_path = os.path.join(log_dir, log_filename)
    
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(log_entry) + "\n")
    
    # Optional: Print summary to console
    window_str = f"{window[0]}-{window[1]}" if window else "full-day"
    print(f"📝 Logged: {origin}→{dest} {window_str} | "
          f"{metrics_secs:.1f}s metrics → {services_count} services → "
          f"{rids_pre}→{rids_post} RIDs → {details_secs:.1f}s details → "
          f"{details_ok}/{details_ok + details_fail} success")
