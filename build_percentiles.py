import pandas as pd
import os
from typing import List
from tqdm import tqdm


def build_percentiles(dates: List[str]) -> pd.DataFrame:
    """
    Build percentile analysis from concatenated delay data file.
    
    Args:
        dates: List of dates in YYYY-MM-DD format (for compatibility, but reads from concatenated file)
    
    Returns:
        DataFrame with route/hour/dow percentile analysis
    """
    print(f"Building percentiles from concatenated data file...")
    
    # Read from concatenated file instead of individual daily files
    concatenated_path = "data/delays_raw.csv.gz"
    
    if not os.path.exists(concatenated_path):
        print(f"Concatenated file not found: {concatenated_path}")
        print("Creating empty percentiles")
        empty_df = pd.DataFrame(columns=[
            "origin", "dest", "hour", "dow", "p80", "p90", "p95", "obs_count"
        ])
        return empty_df
    
    try:
        combined_df = pd.read_csv(concatenated_path, compression="gzip")
        print(f"Loaded {len(combined_df)} observations from concatenated file")
    except Exception as e:
        print(f"Error reading concatenated file: {e}")
        empty_df = pd.DataFrame(columns=[
            "origin", "dest", "hour", "dow", "p80", "p90", "p95", "obs_count"
        ])
        return empty_df
    
    if combined_df.empty:
        print("Concatenated file is empty, creating empty percentiles")
        empty_df = pd.DataFrame(columns=[
            "origin", "dest", "hour", "dow", "p80", "p90", "p95", "obs_count"
        ])
        return empty_df
    
    print(f"Combined dataset: {len(combined_df)} total rows")
    
    # Ensure required columns exist
    required_cols = ["origin", "dest", "date", "gbtt_pta", "delay_min"]
    missing_cols = [col for col in required_cols if col not in combined_df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Derive additional columns
    print("Deriving hour and day-of-week...")
    
    # Use stored hour if available, otherwise extract from gbtt_pta
    if "hour" not in combined_df.columns:
        # Extract hour from gbtt_pta (HHMM format) - ensure it's string first
        combined_df["hour"] = combined_df["gbtt_pta"].astype(str).str[:2].astype(int)
    
    # Convert date to datetime and extract day of week (0=Monday, 6=Sunday)
    combined_df["date_dt"] = pd.to_datetime(combined_df["date"])
    combined_df["dow"] = combined_df["date_dt"].dt.dayofweek
    
    # Filter out invalid data
    initial_rows = len(combined_df)
    combined_df = combined_df.dropna(subset=["origin", "dest", "hour", "dow", "delay_min"])
    combined_df = combined_df[combined_df["delay_min"] >= 0]  # Non-negative delays only
    filtered_rows = len(combined_df)
    
    print(f"Filtered dataset: {filtered_rows} rows (removed {initial_rows - filtered_rows} invalid rows)")
    
    if combined_df.empty:
        print("No valid data after filtering")
        empty_df = pd.DataFrame(columns=[
            "origin", "dest", "hour", "dow", "p80", "p90", "p95", "obs_count"
        ])
        return empty_df
    
    # Group by route, hour, and day of week
    print("Computing percentiles by origin/dest/hour/dow...")
    
    grouped = combined_df.groupby(["origin", "dest", "hour", "dow"])["delay_min"]
    
    # Calculate percentiles and observation counts
    percentiles_df = grouped.agg([
        ("p80", lambda x: x.quantile(0.80)),
        ("p90", lambda x: x.quantile(0.90)),
        ("p95", lambda x: x.quantile(0.95)),
        ("obs_count", "count")
    ]).reset_index()
    
    # Flatten column names
    percentiles_df.columns = ["origin", "dest", "hour", "dow", "p80", "p90", "p95", "obs_count"]
    
    # Sort by origin, dest, hour, dow
    percentiles_df = percentiles_df.sort_values(["origin", "dest", "hour", "dow"]).reset_index(drop=True)
    
    # Round percentiles to reasonable precision
    for col in ["p80", "p90", "p95"]:
        percentiles_df[col] = percentiles_df[col].round(2)
    
    print(f"Generated {len(percentiles_df)} percentile groups")
    
    # Save to CSV
    output_path = "data/route_hour_p80_p90_p95.csv"
    os.makedirs("data", exist_ok=True)
    percentiles_df.to_csv(output_path, index=False)
    
    print(f"Saved percentiles to {output_path}")
    
    # Print summary statistics
    print("\nPercentile Summary:")
    print(f"Total groups: {len(percentiles_df)}")
    print(f"Unique routes: {percentiles_df[['origin', 'dest']].drop_duplicates().shape[0]}")
    print(f"Total observations: {percentiles_df['obs_count'].sum()}")
    print(f"Average observations per group: {percentiles_df['obs_count'].mean():.1f}")
    
    return percentiles_df
