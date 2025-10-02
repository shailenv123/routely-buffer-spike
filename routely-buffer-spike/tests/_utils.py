import os
import glob
import pandas as pd


def load_delays_df():
    """Load delay data from either combined file or individual daily files."""
    # Check for combined file first
    p1 = "data/delays_raw.csv.gz"
    if os.path.exists(p1):
        return pd.read_csv(p1, compression="gzip")
    
    # Fall back to individual daily files
    files = sorted(glob.glob("data/raw_delays/delays_*.csv.gz"))
    assert files, "No delay files found. Run the pipeline to produce delays."
    
    # Combine all daily files
    dfs = []
    for f in files:
        df = pd.read_csv(f, compression="gzip")
        dfs.append(df)
    
    return pd.concat(dfs, ignore_index=True)


def load_percentiles_df():
    """Load percentiles data from available file."""
    # Check both possible locations/names
    for p in ["data/leg_percentiles.csv", "data/route_hour_p80_p90_p95.csv"]:
        if os.path.exists(p):
            return pd.read_csv(p)
    
    raise AssertionError("No percentiles file found. Run build_percentiles.py")







