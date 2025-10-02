#!/usr/bin/env python3
"""Comprehensive verification of PAD→RDG fix."""

import pandas as pd
import os

def main():
    print("=== PAD→RDG FIX VERIFICATION ===")
    
    # Step 1: Check delay data
    print("\n1. Checking delay data...")
    try:
        delay_files = [f for f in os.listdir('data/raw_delays/') if f.endswith('.csv.gz')]
        latest = sorted(delay_files)[-1]
        print(f"Latest delay file: {latest}")
        
        d = pd.read_csv(f'data/raw_delays/{latest}', compression='gzip')
        print(f"Total rows: {len(d)}")
        
        # Check PAD→RDG specifically  
        pad_rdg = d[(d['origin']=='PAD') & (d['dest']=='RDG')]
        print(f"PAD→RDG delay rows: {len(pad_rdg)}")
        
        if len(pad_rdg) > 0:
            print("✅ SUCCESS: PAD→RDG delay data found!")
            print(f"Unique RIDs: {pad_rdg['rid'].nunique()}")
            print("\nSample data:")
            print(pad_rdg.head(8).to_string(index=False))
            
            # Check delay statistics
            delays = pad_rdg['delay_min']
            print(f"\nDelay statistics:")
            print(f"  Range: {delays.min()}-{delays.max()} minutes")
            print(f"  Average: {delays.mean():.1f} minutes")
            print(f"  Non-negative: {(delays >= 0).all()}")
        else:
            print("❌ No PAD→RDG delay data found")
            pad_dests = d[d['origin']=='PAD']['dest'].value_counts()
            print(f"Available PAD destinations: {list(pad_dests.index)}")
            
    except Exception as e:
        print(f"Error checking delay data: {e}")
    
    # Step 2: Check percentiles
    print("\n2. Checking percentiles...")
    try:
        if os.path.exists('data/route_hour_p80_p90_p95.csv'):
            p = pd.read_csv('data/route_hour_p80_p90_p95.csv')
            print(f"Total percentile groups: {len(p)}")
            
            # Check PAD→RDG percentiles
            pad_rdg_p = p[(p['origin']=='PAD') & (p['dest']=='RDG')]
            print(f"PAD→RDG percentile groups: {len(pad_rdg_p)}")
            
            if len(pad_rdg_p) > 0:
                # Check monotonicity
                monotonic_80_90 = (pad_rdg_p['p80'] <= pad_rdg_p['p90']).all()
                monotonic_90_95 = (pad_rdg_p['p90'] <= pad_rdg_p['p95']).all()
                monotonic = monotonic_80_90 and monotonic_90_95
                
                print(f"✅ SUCCESS: PAD→RDG percentiles found!")
                print(f"Monotonic (p80≤p90≤p95): {monotonic}")
                print(f"Hours covered: {pad_rdg_p['hour'].nunique()}")
                
                print("\nSample percentiles:")
                sample = pad_rdg_p.sort_values(['dow', 'hour']).head(10)
                print(sample.to_string(index=False))
                
                if not monotonic:
                    print("❌ Monotonicity violation detected!")
                    violations = pad_rdg_p[~((pad_rdg_p['p80'] <= pad_rdg_p['p90']) & (pad_rdg_p['p90'] <= pad_rdg_p['p95']))]
                    print("Violations:")
                    print(violations[['hour', 'dow', 'p80', 'p90', 'p95']].to_string(index=False))
            else:
                print("❌ No PAD→RDG percentiles found")
                # Show what routes we do have
                routes = p[['origin', 'dest']].drop_duplicates()
                pad_routes = routes[routes['origin']=='PAD']['dest'].tolist()
                print(f"Available PAD routes in percentiles: {pad_routes}")
        else:
            print("❌ No percentiles file found")
            
    except Exception as e:
        print(f"Error checking percentiles: {e}")
    
    # Step 3: Summary
    print("\n=== VERIFICATION SUMMARY ===")
    try:
        has_delays = len(pd.read_csv('data/raw_delays/delays_2025-08-29.csv.gz', compression='gzip')[(pd.read_csv('data/raw_delays/delays_2025-08-29.csv.gz', compression='gzip')['origin']=='PAD') & (pd.read_csv('data/raw_delays/delays_2025-08-29.csv.gz', compression='gzip')['dest']=='RDG')]) > 0
        
        if os.path.exists('data/route_hour_p80_p90_p95.csv'):
            p = pd.read_csv('data/route_hour_p80_p90_p95.csv')
            pad_rdg_p = p[(p['origin']=='PAD') & (p['dest']=='RDG')]
            has_percentiles = len(pad_rdg_p) > 0
            is_monotonic = (pad_rdg_p['p80'] <= pad_rdg_p['p90']).all() and (pad_rdg_p['p90'] <= pad_rdg_p['p95']).all() if has_percentiles else False
        else:
            has_percentiles = False
            is_monotonic = False
        
        print(f"✅ Delay data present: {has_delays}")
        print(f"✅ Percentiles present: {has_percentiles}")  
        print(f"✅ Monotonic percentiles: {is_monotonic}")
        
        if has_delays and has_percentiles and is_monotonic:
            print("\n🎉 FIX VERIFICATION PASSED!")
            print("PAD→RDG destination extraction is working correctly.")
        else:
            print("\n❌ FIX VERIFICATION FAILED")
            print("Need to investigate further or try different time window.")
            
    except Exception as e:
        print(f"Error in summary: {e}")

if __name__ == '__main__':
    main()
