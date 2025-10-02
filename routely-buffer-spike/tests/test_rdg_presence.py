"""Test PAD→RDG presence - core regression test to prevent reintroduction of destination extraction bug."""

import pandas as pd
from ._utils import load_delays_df


def test_pad_rdg_has_rows():
    """Ensure PAD→RDG delay rows are present in the dataset."""
    d = load_delays_df()
    ex = d[(d['origin'] == "PAD") & (d['dest'] == "RDG")]
    
    assert len(ex) > 0, (
        "No PAD→RDG rows found; check that:\n"
        "1. RID capture is pinned to query origin/dest (not service terminus)\n"
        "2. Destination extraction matches intermediate stops (not just final destination)\n"
        "3. Location matching is case-insensitive and handles both 'location' and 'crs' fields"
    )
    
    # Additional validation: ensure we have realistic data
    assert ex['rid'].nunique() > 0, "PAD→RDG rows exist but have no unique RIDs"
    assert ex['delay_min'].notna().any(), "PAD→RDG rows exist but have no delay data"


def test_pad_rdg_has_reasonable_coverage():
    """Ensure PAD→RDG has reasonable coverage across hours."""
    d = load_delays_df()
    ex = d[(d['origin'] == "PAD") & (d['dest'] == "RDG")]
    
    if len(ex) == 0:
        # This will be caught by test_pad_rdg_has_rows, but let's be explicit
        assert False, "No PAD→RDG data found"
    
    # Should have data across multiple hours (AM period)
    hours = ex['hour'].unique()
    assert len(hours) >= 3, f"PAD→RDG data spans only {len(hours)} hours, expected at least 3 for AM coverage"
    
    # Should have reasonable observation count
    total_obs = len(ex)
    assert total_obs >= 10, f"PAD→RDG has only {total_obs} observations, expected at least 10 for statistical validity"







