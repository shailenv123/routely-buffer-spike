#!/usr/bin/env python3
"""
Pipeline orchestrator for rail delay data collection and analysis.

Usage:
    python pipeline.py --days 1   # Validation run (1 day)
    python pipeline.py --days 30  # Full batch run (30 days)
"""

import argparse
import sys
import time
from datetime import datetime, timedelta
from typing import List

from ingest_day import ingest_one_day
from build_percentiles import build_percentiles
from routes import ROUTES
from rdm_client import RDMClient


def get_recent_dates(num_days: int) -> List[str]:
    """
    Get list of recent completed calendar days.
    
    Args:
        num_days: Number of days to include
    
    Returns:
        List of ISO date strings (YYYY-MM-DD), most recent first
    """
    dates = []
    # Start from yesterday (most recent completed day)
    current_date = datetime.now().date() - timedelta(days=1)
    
    for i in range(num_days):
        dates.append(current_date.isoformat())
        current_date -= timedelta(days=1)
    
    return dates


def iter_hour_windows(from_time: str, to_time: str):
    """
    Yield (from_time, to_time) pairs of 60 minutes each.
    
    Args:
        from_time: HHMM format, e.g. '0600'
        to_time: HHMM format, e.g. '1159'
    
    Yields:
        Tuples of (hour_from, hour_to) like ('0600', '0659'), ('0700', '0759'), etc.
    """
    # Parse start and end times
    start_hhmm = int(from_time)
    end_hhmm = int(to_time)
    
    start_hour = start_hhmm // 100
    start_min = start_hhmm % 100
    end_hour = end_hhmm // 100
    end_min = end_hhmm % 100
    
    # Convert to total minutes from midnight
    start_minutes = start_hour * 60 + start_min
    end_minutes = end_hour * 60 + end_min
    
    # Generate hour windows
    current_minutes = start_minutes
    while current_minutes <= end_minutes:
        # Calculate current hour boundaries
        current_hour = current_minutes // 60
        hour_start_minutes = current_hour * 60
        hour_end_minutes = min(hour_start_minutes + 59, end_minutes)
        
        # Convert back to HHMM format
        start_h, start_m = divmod(hour_start_minutes, 60)
        end_h, end_m = divmod(hour_end_minutes, 60)
        
        hour_from = f"{start_h:02d}{start_m:02d}"
        hour_to = f"{end_h:02d}{end_m:02d}"
        
        yield (hour_from, hour_to)
        
        # Move to next hour
        current_minutes = hour_start_minutes + 60


def resolve_tocs(mode: str) -> list[str]:
    """
    Resolve TOC mode to list of TOC codes.
    Sequential processing only - never unfiltered calls.
    
    Args:
        mode: One of 'gw', 'xr', 'both', 'auto'
    
    Returns:
        List of TOC codes like ['GW'] or ['GW', 'XR'] for SEQUENTIAL processing
    """
    if mode == "gw": 
        return ["GW"]
    if mode == "xr": 
        return ["XR"]
    if mode == "both": 
        return ["GW", "XR"]  # Sequential: GW first, then XR
    if mode == "auto": 
        return ["GW", "XR"]  # Sequential: GW first, then XR - NEVER unfiltered
    raise ValueError("toc must be gw|xr|both|auto")


def main():
    """Main pipeline orchestrator."""
    parser = argparse.ArgumentParser(
        description="Rail delay data collection and analysis pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py --days 1                                           # Validation run (1 day)
  python pipeline.py --days 30                                          # Full batch run (30 days)
  python pipeline.py --date 2025-08-27                                  # Specific date
  python pipeline.py --days 1 --route PLY,PAD                          # Single route filter
  python pipeline.py --days 1 --from_time 0600 --to_time 1159          # Morning time window
  python pipeline.py --date 2025-08-27 --route VIC,GTW --from_time 0700 --to_time 0900 --rid-cap 50  # Full example
        """
    )
    
    parser.add_argument(
        "--days", 
        type=int, 
        default=1,
        help="Number of recent days to process (default: 1)"
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="ISO date, e.g. 2025-07-05"
    )
    
    parser.add_argument(
        "--client",
        type=str,
        default="rdm",
        choices=["rdm"],
        help="Data client source (default: rdm)"
    )
    
    parser.add_argument(
        "--route",
        type=str,
        help="CRS pair like 'PLY,PAD'"
    )
    
    parser.add_argument(
        "--from_time",
        type=str,
        help="HHMM, e.g. 0600"
    )
    
    parser.add_argument(
        "--to_time",
        type=str,
        help="HHMM, e.g. 1159"
    )
    
    parser.add_argument(
        "--rid-cap",
        type=int,
        default=150,
        help="Max details per (route,hour)"
    )
    
    parser.add_argument(
        "--metrics-timeout",
        type=int,
        default=120,
        help="Timeout for metrics API calls in seconds"
    )
    
    parser.add_argument(
        "--details-timeout",
        type=int,
        default=30,
        help="Timeout for details API calls in seconds"
    )
    
    parser.add_argument(
        "--toc",
        type=str,
        choices=["gw", "xr", "both", "auto"],
        default="auto",
        help="TOC filter: gw=GWR, xr=Elizabeth, both=both, auto=intelligent"
    )
    
    parser.add_argument(
        "--max-chunk-depth",
        type=int,
        default=3,
        help="Maximum recursion depth for adaptive time chunking"
    )
    
    args = parser.parse_args()
    
    # Build dates list
    if args.date:
        dates = [args.date]
        # Validate ISO date format
        try:
            datetime.fromisoformat(args.date)
        except ValueError:
            raise ValueError(f"Invalid date format: {args.date}. Use ISO format YYYY-MM-DD")
    else:
        # Validate arguments
        if args.days < 1:
            print("Error: --days must be at least 1")
            sys.exit(1)
        dates = get_recent_dates(args.days)
    
    # Process route filter
    routes_filter = None
    if args.route:
        if "," not in args.route:
            raise ValueError("Route must contain a comma (format: 'ORIGIN,DEST')")
        routes_filter = tuple(s.strip().upper() for s in args.route.split(","))
        if len(routes_filter) != 2:
            raise ValueError("Route must have exactly 2 parts (format: 'ORIGIN,DEST')")
    
    # Process time window
    time_window = None
    if args.from_time is not None and args.to_time is not None:
        # Validate time formats
        for time_str, name in [(args.from_time, "from_time"), (args.to_time, "to_time")]:
            if not time_str.isdigit() or len(time_str) != 4:
                raise ValueError(f"{name} must be 4 digits (HHMM format)")
            hhmm = int(time_str)
            if not (0 <= hhmm <= 2359):
                raise ValueError(f"{name} must be between 0000 and 2359")
            # Additional validation for valid time
            hh, mm = divmod(hhmm, 100)
            if hh >= 24 or mm >= 60:
                raise ValueError(f"{name} contains invalid hour or minute")
        
        time_window = (args.from_time, args.to_time)
    elif args.from_time is not None or args.to_time is not None:
        raise ValueError("Both --from_time and --to_time must be provided together")
    
    # Resolve TOC codes
    toc_codes = resolve_tocs(args.toc)
    
    # Print run header
    print(f"[RUN] dates={dates} route={routes_filter} window={time_window} rid_cap={args.rid_cap} "
          f"toc={args.toc} depth={args.max_chunk_depth} mt={args.metrics_timeout}s dt={args.details_timeout}s")
    print(f"🚄 Rail Delay Pipeline - Processing {len(dates)} date(s)")
    print(f"📅 Client: {args.client}")
    
    if routes_filter:
        print(f"🗺️  Route filter: {routes_filter[0]}→{routes_filter[1]}")
    else:
        print(f"🗺️  Processing all {len(ROUTES)} routes")
    
    if time_window:
        print(f"⏰ Time window: {time_window[0]}-{time_window[1]}")
    else:
        print("⏰ Processing full day")
    
    print(f"🔢 RID cap: {args.rid_cap} per (route,hour)")
    print(f"⏱️  Timeouts: metrics={args.metrics_timeout}s, details={args.details_timeout}s")
    
    print("=" * 60)
    
    print(f"📋 Processing dates: {dates[0]} to {dates[-1] if len(dates) > 1 else dates[0]}")
    print()
    
    # Configure client timeouts
    client = RDMClient()
    client.set_metrics_timeout(args.metrics_timeout)
    client.set_details_timeout(args.details_timeout)
    
    # Track totals
    total_rids = 0
    total_rows = 0
    successful_days = 0
    
    # Process each day
    for i, date in enumerate(dates, 1):
        print(f"📅 Day {i}/{len(dates)}: {date}")
        print("-" * 40)
        
        try:
            # If no time window specified, process full day
            if time_window is None:
                result = ingest_one_day(
                    date,
                    routes_filter=routes_filter,
                    time_window=None,
                    client=client,
                    rid_cap=args.rid_cap,
                    toc_filters=[[toc] for toc in toc_codes],  # Convert to old format
                    max_chunk_depth=args.max_chunk_depth
                )
                
                # Update totals
                total_rids += result["rids"]
                total_rows += result["rows"]
                successful_days += 1
                
                # Print summary
                print(f"✅ {date}: {result['rids']} RIDs → {result['rows']} delay records")
            else:
                # Process hour by hour, TOC by TOC
                day_rids = 0
                day_rows = 0
                
                # Get all hour windows
                hour_windows = list(iter_hour_windows(time_window[0], time_window[1]))
                total_chunks = len(hour_windows) * len(toc_codes)
                
                print(f"Processing {len(hour_windows)} hour(s) × {len(toc_codes)} TOC(s) = {total_chunks} chunks")
                
                # Circuit breaker tracking
                consecutive_failures = {}  # Key: (hour, toc), Value: failure_count
                
                chunk_num = 0
                for hour_from, hour_to in hour_windows:
                    for toc in toc_codes:
                        chunk_num += 1
                        chunk_key = (hour_from, toc)
                        
                        print(f"[{chunk_num}/{total_chunks}] {routes_filter[0] if routes_filter else 'all'}→{routes_filter[1] if routes_filter else 'routes'} {hour_from}-{hour_to} TOC={toc}")
                        
                        # Check circuit breaker
                        if consecutive_failures.get(chunk_key, 0) >= 3:
                            print(f"🔴 [breaker] Cooling 90s for {hour_from}-{hour_to} TOC={toc}")
                            time.sleep(90)
                            consecutive_failures[chunk_key] = 0  # Reset after cooling
                        
                        try:
                            # Process this specific hour/TOC combination
                            chunk_result = ingest_one_day(
                                date,
                                routes_filter=routes_filter,
                                time_window=(hour_from, hour_to),
                                client=client,
                                rid_cap=args.rid_cap,
                                toc_filters=[[toc]],  # Single TOC filter
                                max_chunk_depth=args.max_chunk_depth
                            )
                            
                            # Success - reset failure counter
                            consecutive_failures[chunk_key] = 0
                            day_rids += chunk_result["rids"]
                            day_rows += chunk_result["rows"]
                            
                        except Exception as chunk_error:
                            error_str = str(chunk_error)
                            
                            # Track consecutive failures for circuit breaker
                            if "403" in error_str or "5" in error_str:
                                consecutive_failures[chunk_key] = consecutive_failures.get(chunk_key, 0) + 1
                                print(f"⚠️  Chunk failed ({consecutive_failures[chunk_key]}/3): {hour_from}-{hour_to} TOC={toc}")
                            
                            # Continue with next chunk instead of failing entire day
                            print(f"⏭️  Skipping chunk: {chunk_error}")
                            continue
                
                # Update totals
                total_rids += day_rids
                total_rows += day_rows
                successful_days += 1
                
                # Print summary
                print(f"✅ {date}: {day_rids} RIDs → {day_rows} delay records")
            
        except Exception as e:
            print(f"❌ {date}: Error - {e}")
            continue
        
        print()
    
    # Build percentiles from all collected data
    print("=" * 60)
    print("📊 Building percentile analysis...")
    print("-" * 40)
    
    try:
        percentiles_df = build_percentiles(dates)
        total_groups = len(percentiles_df)
        
        print(f"✅ Generated {total_groups} percentile groups")
        print(f"💾 Saved to data/route_hour_p80_p90_p95.csv")
        
    except Exception as e:
        print(f"❌ Percentile analysis failed: {e}")
        total_groups = 0
    
    # Final summary
    print()
    print("=" * 60)
    print("🎯 PIPELINE SUMMARY")
    print("=" * 60)
    print(f"📅 Days processed: {successful_days}/{len(dates)}")
    print(f"🆔 Total RIDs fetched: {total_rids:,}")
    print(f"📊 Total delay rows: {total_rows:,}")
    print(f"📈 Total percentile groups: {total_groups:,}")
    
    if successful_days == 0:
        print("❌ No days processed successfully")
        sys.exit(1)
    elif successful_days < len(dates):
        print(f"⚠️  {len(dates) - successful_days} day(s) failed")
        sys.exit(1)
    else:
        print("✅ All days processed successfully")
    
    print()
    print("🎉 Pipeline complete!")


if __name__ == "__main__":
    main()
