import time
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm
import os

from rdm_client import RDMClient
from routes import get_days_tag, ROUTES
from utils.ingest_log import write_ingest_log
from adaptive_metrics import fetch_metrics_adaptive


def parse_hhmm_to_minutes(time_str: str) -> int:
    """Convert HHMM string to minutes since midnight."""
    if not time_str or len(time_str) < 4:
        return 0
    hours = int(time_str[:2])
    minutes = int(time_str[2:4])
    return hours * 60 + minutes


def ingest_one_day(iso_date: str, routes_filter=None, time_window=None, client=None, rid_cap=None, toc_filters=None, max_chunk_depth=3) -> Dict:
    """
    Ingest delay data for one day with comprehensive timing and logging.
    
    Args:
        iso_date: Date in YYYY-MM-DD format
        routes_filter: Optional single route tuple (origin, dest) to filter
        time_window: Optional time window tuple (from_time, to_time) to filter
        client: Pre-configured RDMClient instance
        rid_cap: Optional cap on RIDs per (origin, dest, hour) group
    
    Returns:
        Dict with summary stats: {"date": str, "rids": int, "rows": int}
    """
    print(f"Ingesting data for {iso_date}...")
    
    # Parse date and get days tag
    date_obj = datetime.fromisoformat(iso_date)
    days_tag = get_days_tag(date_obj)
    
    # Apply route filter
    routes = [routes_filter] if routes_filter else ROUTES
    
    # Initialize client if not provided
    if client is None:
        client = RDMClient()
    
    # Determine time window for API call
    if time_window:
        t0, t1 = time_window
        print(f"Fetching metrics for {len(routes)} route(s) with time window {t0}-{t1}...")
    else:
        t0, t1 = "0000", "2359"
        print(f"Fetching metrics for {len(routes)} route(s) with full day range...")
    
    # Process each route
    for origin, dest in tqdm(routes, desc="Routes"):
        print(f"\n🚄 Processing route {origin}→{dest}")
        
        # Metrics phase timing
        metrics_start = time.perf_counter()
        all_rids = []
        services_count = 0
        
        try:
            # Process each TOC filter
            toc_list = toc_filters or [None]  # Default to no filter
            
            for toc_filter in toc_list:
                toc_name = f"TOC={toc_filter}" if toc_filter else "all-TOCs"
                print(f"  🎯 Fetching {toc_name}...")
                
                # Use adaptive metrics fetching
                for metrics_data in fetch_metrics_adaptive(
                    client=client,
                    from_loc=origin,
                    to_loc=dest,
                    from_date=iso_date,
                    to_date=iso_date,
                    days=days_tag,
                    from_time=t0,
                    to_time=t1,
                    toc_filter=toc_filter,
                    max_depth=max_chunk_depth,
                    metrics_timeout=client.metrics_timeout,
                    min_window_minutes=30
                ):
                    # Extract services and RIDs
                    services = metrics_data.get("Services", [])
                    services_count += len(services)
                    
                    for service in services:
                        attrs = service.get("serviceAttributesMetrics", {})
                        service_rids = attrs.get("rids", [])
                        
                        # Extract service metadata
                        service_origin = attrs.get("origin_location", origin)
                        service_dest = attrs.get("destination_location", dest)
                        gbtt_pta = attrs.get("gbtt_pta", "")
                        gbtt_ptd = attrs.get("gbtt_ptd", "")
                        toc = attrs.get("toc_code", "")
                        
                        # Compute hour from gbtt_pta
                        hour = None
                        if gbtt_pta and len(gbtt_pta) >= 2:
                            try:
                                hour = int(gbtt_pta[:2])
                            except (ValueError, TypeError):
                                hour = None
                        
                        # Add each RID with metadata (pin to query origin/dest, not service terminus)
                        for rid in service_rids:
                            if isinstance(rid, str):
                                all_rids.append({
                                    "rid": rid,
                                    "origin": origin,  # Use query origin, not service origin
                                    "dest": dest,      # Use query dest, not service terminus
                                    "gbtt_pta": gbtt_pta,
                                    "gbtt_ptd": gbtt_ptd,
                                    "toc": toc,
                                    "date": iso_date,
                                    "hour": hour,
                                    "days_tag": days_tag
                                })
            
            metrics_secs = time.perf_counter() - metrics_start
            print(f"📊 Metrics: {metrics_secs:.1f}s → {services_count} services → {len(all_rids)} RIDs")
            
        except Exception as e:
            metrics_secs = time.perf_counter() - metrics_start
            print(f"❌ Error fetching metrics for {origin}→{dest}: {e}")
            
            # Log failed metrics call
            write_ingest_log(
                date=iso_date,
                origin=origin,
                dest=dest,
                window=time_window,
                metrics_secs=metrics_secs,
                services_count=0,
                rids_pre=0,
                rids_post=0,
                details_secs=0,
                details_ok=0,
                details_fail=0,
                retry_counts={"metrics_failed": 1}
            )
            continue
        
        # Process RIDs: deduplication and capping
        if all_rids:
            df_rids = pd.DataFrame(all_rids)
            
            # Deduplicate by RID
            rids_pre = len(df_rids)
            df_rids = df_rids.drop_duplicates(subset=['rid'])
            
            # Apply RID cap per (origin, dest, hour) group if provided
            # Note: With hour-sliced processing, this effectively caps per (hour, TOC) chunk
            if rid_cap is not None:
                df_rids = (df_rids.groupby(["origin", "dest", "hour"], group_keys=False)
                          .apply(lambda g: g.sample(n=min(len(g), rid_cap), random_state=42)))
                df_rids = df_rids.reset_index(drop=True)
            
            rids_post = len(df_rids)
            all_rids = df_rids.to_dict('records')
            
            print(f"🔄 Processing: {rids_pre}→{rids_post} RIDs (after dedup + cap)")
        else:
            rids_pre = rids_post = 0
        
        if not all_rids:
            print(f"⚠️  No RIDs to process for {origin}→{dest}")
            
            # Log empty result
            write_ingest_log(
                date=iso_date,
                origin=origin,
                dest=dest,
                window=time_window,
                metrics_secs=metrics_secs,
                services_count=services_count,
                rids_pre=0,
                rids_post=0,
                details_secs=0,
                details_ok=0,
                details_fail=0,
                retry_counts={}
            )
            continue
        
        # Details phase timing
        details_start = time.perf_counter()
        delay_rows = []
        details_ok = 0
        details_fail = 0
        
        def fetch_details(rid_data):
            """Fetch details for a single RID and return processed row."""
            rid = rid_data["rid"]
            try:
                details = client.get_details(rid)
                
                # Extract service details
                service_attrs = details.get("serviceAttributesDetails", {})
                locations = service_attrs.get("locations", [])
                
                # Find the destination location (tolerant matching for intermediate stops)
                target_dest = rid_data.get("dest", dest)  # Use dest from closure if not in rid_data
                dest_location = None
                for location in locations:
                    # Match location or crs, case-insensitive
                    loc_code = (location.get("location") or location.get("crs") or "").upper()
                    if loc_code == target_dest.upper():
                        dest_location = location
                        break
                
                if not dest_location:
                    # Log available stops for debugging
                    available_stops = [loc.get("location") or loc.get("crs") or "N/A" for loc in locations]
                    print(f"⚠️  {target_dest} not found in stops: {available_stops}")
                    return None, False
                
                # Extract timing data (flexible arrival/departure)
                gbtt_pta = dest_location.get("gbtt_pta") or dest_location.get("gbtt_ptd") or ""
                actual_ta = dest_location.get("actual_ta") or dest_location.get("actual_td") or ""
                
                # Skip if no actual arrival time
                if not actual_ta:
                    return None, False
                
                # Calculate delay in minutes (non-negative)
                if gbtt_pta and actual_ta:
                    planned_min = parse_hhmm_to_minutes(gbtt_pta)
                    actual_min = parse_hhmm_to_minutes(actual_ta)
                    delay_min = max(0, actual_min - planned_min)
                else:
                    delay_min = 0
                
                result = {
                    "origin": rid_data["origin"],
                    "dest": rid_data["dest"],
                    "rid": rid,
                    "date": iso_date,
                    "gbtt_pta": gbtt_pta,
                    "actual_ta": actual_ta,
                    "delay_min": delay_min,
                    "hour": rid_data["hour"],
                    "days_tag": rid_data["days_tag"]
                }
                return result, True
                
            except Exception as e:
                return None, False
        
        # Process RIDs concurrently with micro-throttling
        request_count = 0
        
        with ThreadPoolExecutor(max_workers=12) as executor:
            # Submit all RID detail requests
            future_to_rid = {executor.submit(fetch_details, rid_data): rid_data for rid_data in all_rids}
            
            # Collect results with progress bar and throttling
            for future in tqdm(future_to_rid, desc=f"Details {origin}→{dest}"):
                result, success = future.result()
                
                if success:
                    details_ok += 1
                    if result:  # Should always be true if success=True
                        delay_rows.append(result)
                else:
                    details_fail += 1
                
                request_count += 1
                
                # Micro-throttle: pause briefly every ~200 requests
                if request_count % 200 == 0:
                    time.sleep(0.05)
        
        details_secs = time.perf_counter() - details_start
        
        print(f"🔍 Details: {details_secs:.1f}s → {details_ok}/{details_ok + details_fail} success → {len(delay_rows)} records")
        
        # Write comprehensive log entry
        write_ingest_log(
            date=iso_date,
            origin=origin,
            dest=dest,
            window=time_window,
            metrics_secs=metrics_secs,
            services_count=services_count,
            rids_pre=rids_pre,
            rids_post=rids_post,
            details_secs=details_secs,
            details_ok=details_ok,
            details_fail=details_fail,
            retry_counts={}  # TODO: Could be enhanced to track actual retries
        )
        
        # Save route-specific data immediately
        if delay_rows:
            df_route = pd.DataFrame(delay_rows)
            
            # Ensure output directory exists
            os.makedirs("data/raw_delays", exist_ok=True)
            
            # Write to compressed CSV
            output_path = f"data/raw_delays/delays_{iso_date}.csv.gz"
            
            # Append if file exists, otherwise create new
            if os.path.exists(output_path):
                existing_df = pd.read_csv(output_path, compression="gzip")
                combined_df = pd.concat([existing_df, df_route], ignore_index=True)
                combined_df.to_csv(output_path, index=False, compression="gzip")
            else:
                df_route.to_csv(output_path, index=False, compression="gzip")
            
            print(f"💾 Saved {len(df_route)} records to {output_path}")
    
    # Final summary
    final_df = pd.read_csv(f"data/raw_delays/delays_{iso_date}.csv.gz", compression="gzip")
    total_rows = len(final_df)
    total_rids = final_df['rid'].nunique() if not final_df.empty else 0
    
    print(f"\n✅ Day complete: {total_rids} unique RIDs → {total_rows} delay records")
    
    return {
        "date": iso_date,
        "rids": total_rids,
        "rows": total_rows
    }
