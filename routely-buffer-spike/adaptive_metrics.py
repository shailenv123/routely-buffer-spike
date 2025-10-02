"""
Adaptive metrics fetching with recursive time-chunking for high-frequency routes.

Handles gateway timeouts by automatically splitting time windows into smaller chunks.
Includes QPS throttling to prevent gateway policy trips.
"""

import time
import random
from typing import Generator, Dict, Any, List, Optional
from rdm_client import RDMClient

# Global QPS throttling
_last_metrics_call_time = 0.0
METRICS_MIN_INTERVAL = 0.5  # 2 req/s max


def parse_hhmm_to_minutes(time_str: str) -> int:
    """Convert HHMM string to minutes since midnight."""
    if not time_str or len(time_str) < 4:
        return 0
    hours = int(time_str[:2])
    minutes = int(time_str[2:4])
    return hours * 60 + minutes


def minutes_to_hhmm(minutes: int) -> str:
    """Convert minutes since midnight to HHMM string."""
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours:02d}{mins:02d}"


def throttle_metrics_call():
    """Enforce minimum interval between metrics API calls to prevent gateway policy trips."""
    global _last_metrics_call_time
    
    current_time = time.perf_counter()
    time_since_last = current_time - _last_metrics_call_time
    
    if time_since_last < METRICS_MIN_INTERVAL:
        sleep_time = METRICS_MIN_INTERVAL - time_since_last
        print(f"    🚦 QPS throttle: {sleep_time:.2f}s")
        time.sleep(sleep_time)
    
    _last_metrics_call_time = time.perf_counter()


def split_time_window(from_time: str, to_time: str) -> tuple[str, str, str]:
    """Split a time window in half and return (mid_time, first_half_end, second_half_start)."""
    from_min = parse_hhmm_to_minutes(from_time)
    to_min = parse_hhmm_to_minutes(to_time)
    
    mid_min = (from_min + to_min) // 2
    
    # First half: from_time to mid_time
    first_end = minutes_to_hhmm(mid_min)
    
    # Second half: mid_time+1 to to_time  
    second_start = minutes_to_hhmm(mid_min + 1)
    
    return first_end, second_start


def fetch_metrics_adaptive(
    client: RDMClient,
    from_loc: str,
    to_loc: str,
    from_date: str,
    to_date: str,
    days: str,
    from_time: str,
    to_time: str,
    toc_filter: Optional[List[str]] = None,
    max_depth: int = 4,
    metrics_timeout: int = 25,
    min_window_minutes: int = 15
) -> Generator[Dict[str, Any], None, None]:
    """
    Fetch service metrics with adaptive time window splitting.
    
    Args:
        client: RDMClient instance
        from_loc, to_loc: Route endpoints
        from_date, to_date: Date range
        days: Day type (WEEKDAY, SATURDAY, SUNDAY)
        from_time, to_time: Time window in HHMM format
        toc_filter: Optional list of TOC codes to filter
        max_depth: Maximum recursion depth for splitting
        metrics_timeout: Timeout for metrics API calls in seconds
        min_window_minutes: Minimum window size before giving up
    
    Yields:
        Dict: Service metrics JSON response
        
    Raises:
        RuntimeError: If all attempts fail at minimum granularity
    """
    window_duration = parse_hhmm_to_minutes(to_time) - parse_hhmm_to_minutes(from_time)
    
    print(f"    🔄 Adaptive fetch: {from_time}-{to_time} ({window_duration}min) depth={max_depth}")
    
    # Add jittered backoff for retries
    retry_delays = [1.0, 2.0, 4.0]
    
    for attempt in range(len(retry_delays) + 1):
        try:
            # Enforce QPS throttling before API call
            throttle_metrics_call()
            
            # Try the metrics call for this window with timeout
            metrics_data = client.get_metrics(
                from_loc=from_loc,
                to_loc=to_loc,
                from_time=from_time,
                to_time=to_time,
                from_date=from_date,
                to_date=to_date,
                days=days,
                toc_filter=toc_filter,
                timeout=metrics_timeout
            )
            
            # Success - yield the result
            services_count = len(metrics_data.get("Services", []))
            print(f"    ✅ Success: {from_time}-{to_time} → {services_count} services")
            yield metrics_data
            return
            
        except RuntimeError as e:
            error_str = str(e)
            
            # Check if this is a 504, 403 (gateway protection), or other 5xx error
            if "504" in error_str or "403" in error_str or "timeout" in error_str.lower() or any(f"{code}" in error_str for code in [500, 502, 503]):
                print(f"    ⚠️  Gateway protection/Timeout: {from_time}-{to_time} (attempt {attempt + 1})")
                
                # If we can split further and have depth remaining and window is large enough
                if max_depth > 0 and window_duration > min_window_minutes:
                    print(f"    🔀 Splitting window: {from_time}-{to_time}")
                    
                    # Split the time window in half
                    first_end, second_start = split_time_window(from_time, to_time)
                    
                    # Recursively fetch each half
                    try:
                        # First half
                        yield from fetch_metrics_adaptive(
                            client, from_loc, to_loc, from_date, to_date, days,
                            from_time, first_end, toc_filter, max_depth - 1,
                            metrics_timeout, min_window_minutes
                        )
                        
                        # Second half  
                        yield from fetch_metrics_adaptive(
                            client, from_loc, to_loc, from_date, to_date, days,
                            second_start, to_time, toc_filter, max_depth - 1,
                            metrics_timeout, min_window_minutes
                        )
                        
                        return  # Successfully split and processed
                        
                    except RuntimeError as split_error:
                        print(f"    ❌ Split failed: {split_error}")
                        # Continue to retry logic below
                
                # If we can't split or splitting failed, try backoff
                if attempt < len(retry_delays):
                    # For 30-15 min windows, use gentler backoff (2-5s with jitter)
                    if 15 <= window_duration <= 30:
                        jitter = random.uniform(2.0, 5.0)
                        print(f"    ⏱️  Gentle backoff (small window): {jitter:.1f}s")
                        time.sleep(jitter)
                    else:
                        # Standard backoff for larger windows
                        jitter = random.uniform(0, 0.4)
                        sleep_time = retry_delays[attempt] + jitter
                        print(f"    ⏱️  Backoff: {sleep_time:.1f}s")
                        time.sleep(sleep_time)
                    continue
                else:
                    # Final attempt failed - bubble up for circuit breaker
                    if window_duration <= min_window_minutes:
                        print(f"    ❌ Min window failed: {from_time}-{to_time} ({window_duration}min) - bubbling up")
                        raise RuntimeError(f"Minimum window failed: {from_time}-{to_time}")
                    else:
                        print(f"    ❌ Final timeout: {from_time}-{to_time}")
                        raise RuntimeError(f"Gateway timeout after {len(retry_delays) + 1} attempts: {from_time}-{to_time}")
            
            else:
                # Non-timeout error - re-raise immediately
                print(f"    ❌ Non-timeout error: {error_str}")
                raise
    
    # Should never reach here
    raise RuntimeError(f"Unexpected failure in adaptive fetch: {from_time}-{to_time}")
