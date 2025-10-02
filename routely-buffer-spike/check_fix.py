#!/usr/bin/env python3
"""Check if the PAD→RDG fix worked."""

import pandas as pd
import os

def main():
    print("=== CHECKING PAD→RDG FIX ===")
    
    # Check latest delay file
    delay_files = [f for f in os.listdir('data/raw_delays/') if f.endswith('.csv.gz')]
    latest = sorted(delay_files)[-1]
    print(f"Latest delay file: {latest}")
    
    d = pd.read_csv(f'data/raw_delays/{latest}', compression='gzip')
    print(f"Total rows: {len(d)}")
    
    # Check PAD→RDG specifically
    pad_rdg = d[(d['origin']=='PAD') & (d['dest']=='RDG')]
    print(f"PAD→RDG rows: {len(pad_rdg)}")
    
    if len(pad_rdg) > 0:
        print("✅ SUCCESS: PAD→RDG data found!")
        print(f"Sample PAD→RDG data:")
        print(pad_rdg.head(3).to_string(index=False))
        
        # Check delay values
        delays = pad_rdg['delay_min']
        print(f"Delay range: {delays.min()}-{delays.max()} minutes")
        print(f"Average delay: {delays.mean():.1f} minutes")
    else:
        print("❌ FAILED: No PAD→RDG data found")
        
        # Show what we do have for PAD
        pad_dests = d[d['origin']=='PAD']['dest'].value_counts()
        print(f"PAD destinations found: {list(pad_dests.index)}")
        
        # Check if there were any debug messages in recent run
        print("\nChecking for any logged missing stops...")

if __name__ == '__main__':
    main()
