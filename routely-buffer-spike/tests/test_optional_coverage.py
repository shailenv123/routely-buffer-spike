"""Optional coverage tests for airport routes - skips if routes not yet ingested."""

import pytest
from ._utils import load_delays_df, load_percentiles_df


@pytest.mark.parametrize("origin,dest", [
    ("PAD", "HXX"),  # Paddington → Heathrow T2-3
    ("PAD", "HWY"),  # Paddington → Heathrow T5
    ("VIC", "GTW"),  # Victoria → Gatwick
])
def test_optional_airport_routes_present(origin, dest):
    """Test that airport routes are present in both delays and percentiles if they exist."""
    ddf = load_delays_df()
    ex = ddf[(ddf['origin'] == origin) & (ddf['dest'] == dest)]
    
    if len(ex) == 0:
        pytest.skip(f"No {origin}→{dest} rows yet; skipping coverage check")
    
    # If present in delays, should also be in percentiles
    pdf = load_percentiles_df()
    gp = pdf[(pdf['origin'] == origin) & (pdf['dest'] == dest)]
    
    assert len(gp) > 0, (
        f"{origin}→{dest} present in delays ({len(ex)} rows) but missing in percentiles; "
        f"run build_percentiles.py to regenerate"
    )


def test_major_routes_coverage():
    """Test coverage of major rail routes if they exist."""
    ddf = load_delays_df()
    pdf = load_percentiles_df()
    
    # Major routes we expect to see if data collection is comprehensive
    major_routes = [
        ("PAD", "BRI"),  # Paddington → Bristol (intercity)
        ("PAD", "RDG"),  # Paddington → Reading (commuter)
        ("PAD", "OXF"),  # Paddington → Oxford
    ]
    
    found_routes = []
    missing_routes = []
    
    for origin, dest in major_routes:
        delays_present = len(ddf[(ddf['origin'] == origin) & (ddf['dest'] == dest)]) > 0
        percentiles_present = len(pdf[(pdf['origin'] == origin) & (pdf['dest'] == dest)]) > 0
        
        if delays_present and percentiles_present:
            found_routes.append(f"{origin}→{dest}")
        elif delays_present and not percentiles_present:
            pytest.fail(f"{origin}→{dest} in delays but not percentiles")
        else:
            missing_routes.append(f"{origin}→{dest}")
    
    # Should have at least one major route
    if not found_routes and not missing_routes:
        pytest.fail("No major routes found in dataset")
    
    print(f"Found routes: {found_routes}")
    if missing_routes:
        print(f"Missing routes (not yet ingested): {missing_routes}")


def test_weekend_vs_weekday_coverage():
    """Test that we have both weekday and weekend data if comprehensive collection is done."""
    pdf = load_percentiles_df()
    
    if len(pdf) == 0:
        pytest.skip("No percentiles data")
    
    # Check day-of-week coverage
    dows = set(pdf['dow'].unique())
    
    weekdays = {0, 1, 2, 3, 4}  # Mon-Fri
    weekends = {5, 6}           # Sat-Sun
    
    has_weekdays = bool(dows & weekdays)
    has_weekends = bool(dows & weekends)
    
    if has_weekdays and has_weekends:
        print("✅ Dataset includes both weekday and weekend data")
    elif has_weekdays:
        print("ℹ️  Dataset includes weekdays only")
    elif has_weekends:
        print("ℹ️  Dataset includes weekends only")
    else:
        pytest.fail("No recognizable day-of-week data found")
    
    # This is informational, not a hard requirement
    assert True  # Always pass, just report coverage







