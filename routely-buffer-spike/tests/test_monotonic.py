"""Test percentile monotonicity - core regression test to prevent broken delay parsing/quantiles."""

from ._utils import load_percentiles_df


def test_percentiles_monotonic():
    """Ensure all percentiles maintain p80 ≤ p90 ≤ p95 ordering."""
    p = load_percentiles_df()
    
    assert len(p) > 0, "Percentiles file is empty"
    
    # Check p80 ≤ p90
    p80_violations = p[p["p80"] > p["p90"]]
    assert len(p80_violations) == 0, (
        f"Found {len(p80_violations)} cases where p80 > p90:\n"
        f"{p80_violations[['origin', 'dest', 'hour', 'dow', 'p80', 'p90']].to_string()}"
    )
    
    # Check p90 ≤ p95
    p90_violations = p[p["p90"] > p["p95"]]
    assert len(p90_violations) == 0, (
        f"Found {len(p90_violations)} cases where p90 > p95:\n"
        f"{p90_violations[['origin', 'dest', 'hour', 'dow', 'p90', 'p95']].to_string()}"
    )


def test_percentiles_have_pad_rdg():
    """Ensure PAD→RDG is present in percentiles (not just delays)."""
    p = load_percentiles_df()
    
    pad_rdg = p[(p['origin'] == "PAD") & (p['dest'] == "RDG")]
    assert len(pad_rdg) > 0, (
        "PAD→RDG missing from percentiles; check that:\n"
        "1. PAD→RDG delay data exists\n"
        "2. Percentile computation includes PAD→RDG routes\n"
        "3. build_percentiles.py was run after fixing destination extraction"
    )
    
    # Should have reasonable coverage
    assert len(pad_rdg) >= 5, f"PAD→RDG has only {len(pad_rdg)} percentile groups, expected at least 5"
    
    # Should span multiple hours
    hours = pad_rdg['hour'].unique()
    assert len(hours) >= 3, f"PAD→RDG percentiles span only {len(hours)} hours, expected at least 3"


def test_percentiles_realistic_values():
    """Ensure percentile values are realistic (not negative, not extremely large)."""
    p = load_percentiles_df()
    
    # Check for negative delays (should never happen)
    negative_p80 = p[p['p80'] < 0]
    assert len(negative_p80) == 0, f"Found {len(negative_p80)} negative p80 values"
    
    negative_p90 = p[p['p90'] < 0]
    assert len(negative_p90) == 0, f"Found {len(negative_p90)} negative p90 values"
    
    negative_p95 = p[p['p95'] < 0]
    assert len(negative_p95) == 0, f"Found {len(negative_p95)} negative p95 values"
    
    # Check for extremely large delays (likely data quality issues)
    MAX_REASONABLE_DELAY = 180  # 3 hours seems like a reasonable upper bound
    
    extreme_p95 = p[p['p95'] > MAX_REASONABLE_DELAY]
    if len(extreme_p95) > 0:
        print(f"Warning: {len(extreme_p95)} routes have p95 > {MAX_REASONABLE_DELAY} minutes")
        # Don't fail the test, but log for investigation







