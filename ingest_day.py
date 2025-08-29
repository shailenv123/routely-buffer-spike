import time
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Set
from tqdm import tqdm
import os

from rdm_client import RDMClient
from routes import get_days_tag


def parse_hhmm_to_minutes(time_str: str) -> int:
    """Convert HHMM string to minutes since midnight."""
    if not time_str or len(time_str) < 4:
        return 0
    hours = int(time_str[:2])
    minutes = int(time_str[2:4])
    return hours * 60 + minutes


def ingest_one_day(iso_date: str, routes_filter=None, time_window=None, client_source: str = "rdm", rid_cap: int = None, client=None) -> Dict:
    """
    Ingest delay data for one day: single metrics call → RID list → details → CSV.
    
    Args:
        iso_date: Date in YYYY-MM-DD format
        routes_filter: Optional single route tuple (origin, dest) to filter
        time_window: Optional time window tuple (from_time, to_time) to filter
        client_source: Data source ("rdm")
        rid_cap: Optional cap on RIDs per (origin, dest, hour) group
    
    Returns:
        Dict with summary stats: {"date": str, "rids": int, "rows": int}
    """
    print(f"Ingesting data for {iso_date}...")
    
    # Parse date and get days tag
    date_obj = datetime.fromisoformat(iso_date)
    days_tag = get_days_tag(date_obj)
    
    # Apply route filter
    from routes import ROUTES
    routes = [routes_filter] if routes_filter else ROUTES
    
    # Initialize client
    if client is None:
        if client_source == "rdm":
            client = RDMClient()
        else:
            raise ValueError(f"Unknown client source: {client_source}")
    
    # Collect all RIDs with metadata
    all_rids = []  # List of RID metadata dicts
    
    # Determine time window for API call
    if time_window:
        t0, t1 = time_window
        print(f"Fetching metrics for {len(routes)} route(s) with time window {t0}-{t1}...")
    else:
        t0, t1 = "0000", "2359"
        print(f"Fetching metrics for {len(routes)} route(s) with full day range...")
    
    for origin, dest in tqdm(routes, desc="Routes"):
        try:
            # Metrics call for specified time window
            metrics_data = client.get_metrics(
                from_loc=origin,
                to_loc=dest,
                from_time=t0,
                to_time=t1,
                from_date=iso_date,
                to_date=iso_date,
                days=days_tag
            )
            
            # Extract services and RIDs
            services = metrics_data.get("Services", [])
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
                
                # Add each RID with metadata
                for rid in service_rids:
                    if isinstance(rid, str):
                        all_rids.append({
                            "rid": rid,
                            "origin": service_origin,
                            "dest": service_dest,
                            "gbtt_pta": gbtt_pta,
                            "gbtt_ptd": gbtt_ptd,
                            "toc": toc,
                            "date": iso_date,
                            "hour": hour,
                            "days_tag": days_tag
                        })
                        
        except Exception as e:
            print(f"Error fetching metrics for {origin}->{dest}: {e}")
            continue
    
    print(f"Found {len(all_rids)} RID records")
    
    # Convert to DataFrame for deduplication and capping
    if all_rids:
        df_rids = pd.DataFrame(all_rids)
        
        # Deduplicate by RID
        initial_count = len(df_rids)
        df_rids = df_rids.drop_duplicates(subset=['rid'])
        print(f"Deduplicated: {initial_count} → {len(df_rids)} unique RIDs")
        
        # Apply RID cap per (origin, dest, hour) group if provided
        if rid_cap is not None:
            df_rids = (df_rids.groupby(["origin", "dest", "hour"], group_keys=False)
                      .apply(lambda g: g.sample(n=min(len(g), rid_cap), random_state=42)))
            df_rids = df_rids.reset_index(drop=True)
            print(f"Applied RID cap of {rid_cap}: {len(df_rids)} RIDs remaining")
        
        all_rids = df_rids.to_dict('records')
    
    print(f"Processing {len(all_rids)} RIDs for details")
    
    if not all_rids:
        print("No RIDs found, creating empty CSV")
        empty_df = pd.DataFrame(columns=["origin", "dest", "rid", "date", "gbtt_pta", "actual_ta", "delay_min", "hour", "days_tag"])
        os.makedirs("data/raw_delays", exist_ok=True)
        empty_df.to_csv(f"data/raw_delays/delays_{iso_date}.csv.gz", index=False, compression="gzip")
        return {"date": iso_date, "rids": 0, "rows": 0}
    
    # Fetch details concurrently
    print("Fetching service details...")
    delay_rows = []
    
    def fetch_details(rid_data):
        """Fetch details for a single RID and return processed row."""
        rid = rid_data["rid"]
        try:
            details = client.get_details(rid)
            
            # Extract service details
            service_attrs = details.get("serviceAttributesDetails", {})
            locations = service_attrs.get("locations", [])
            
            # Find the destination location
            dest_location = None
            for location in locations:
                if location.get("location") == rid_data["dest"]:
                    dest_location = location
                    break
            
            if not dest_location:
                return None
            
            # Extract timing data
            gbtt_pta = dest_location.get("gbtt_pta", "")
            actual_ta = dest_location.get("actual_ta", "")
            
            # Skip if no actual arrival time
            if not actual_ta:
                return None
            
            # Calculate delay in minutes (non-negative)
            if gbtt_pta and actual_ta:
                planned_min = parse_hhmm_to_minutes(gbtt_pta)
                actual_min = parse_hhmm_to_minutes(actual_ta)
                delay_min = max(0, actual_min - planned_min)
            else:
                delay_min = 0
            
            return {
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
            
        except Exception as e:
            print(f"Error fetching details for RID {rid}: {e}")
            return None
    
    # Process RIDs concurrently with lighter rate limiting
    request_count = 0
    
    with ThreadPoolExecutor(max_workers=12) as executor:
        # Submit all RID detail requests
        future_to_rid = {executor.submit(fetch_details, rid_data): rid_data for rid_data in all_rids}
        
        # Collect results with progress bar
        for future in tqdm(future_to_rid, desc="Processing RID details"):
            result = future.result()
            if result:
                delay_rows.append(result)
            
            request_count += 1
            
            # Light rate limiting: pause briefly every ~200 requests
            if request_count % 200 == 0:
                time.sleep(0.05)
    
    print(f"Processed {len(delay_rows)} valid delay records")
    
    # Create DataFrame and save
    df = pd.DataFrame(delay_rows)
    
    # Ensure output directory exists
    os.makedirs("data/raw_delays", exist_ok=True)
    
    # Write to compressed CSV with new schema
    output_path = f"data/raw_delays/delays_{iso_date}.csv.gz"
    df.to_csv(output_path, index=False, compression="gzip")
    
    print(f"Saved {len(df)} rows to {output_path}")
    
    return {
        "date": iso_date,
        "rids": len(all_rids),
        "rows": len(df)
    }
