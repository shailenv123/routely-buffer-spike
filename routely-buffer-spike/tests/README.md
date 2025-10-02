# Routely Buffer – Regression Tests

## What these do

Guard against regressions in two critical places:

1. **Presence of PAD→RDG delay rows** (prevents reintroducing the "terminus vs intermediate stop" bug)
2. **Percentile monotonicity p80 ≤ p90 ≤ p95** (prevents broken delay parsing/quantiles)

## How to run

1. **Ensure you've run a recent ingest + built percentiles:**
   ```bash
   python pipeline.py --days 7 --route PAD,RDG --from_time 0600 --to_time 1159 --rid-cap 80
   # Percentiles are built automatically by the pipeline
   ```

2. **Install dev dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run tests:**
   ```bash
   pytest -q
   ```

## Test Structure

### Core Tests (Must Pass)
- `test_rdg_presence.py` - Ensures PAD→RDG data exists and has reasonable coverage
- `test_monotonic.py` - Ensures percentiles maintain proper ordering and realistic values

### Optional Tests (Informational)
- `test_optional_coverage.py` - Checks airport routes and coverage patterns, skips if not present

## What Each Test Guards Against

### PAD→RDG Presence Tests
- **Bug prevented**: Destination extraction only looking at final destination instead of intermediate stops
- **Symptoms**: PAD→RDG services exist in metrics but disappear in delay records
- **Root cause**: RID capture using service terminus instead of query parameters

### Monotonicity Tests  
- **Bug prevented**: Incorrect delay calculations or quantile computation
- **Symptoms**: p80 > p90 or p90 > p95 in percentile data
- **Root cause**: Negative delays, incorrect time parsing, or broken percentile math

### Coverage Tests
- **Purpose**: Ensure data collection is working across different route types
- **Behavior**: Skip routes that haven't been ingested yet (non-blocking)

## Notes

- **Optional airport coverage tests** skip if those routes aren't present yet
- **Warnings from pandas groupby.apply** are silenced via `pytest.ini`
- Tests load data from either combined files (`data/delays_raw.csv.gz`) or individual daily files (`data/raw_delays/delays_*.csv.gz`)

## Expected Output

```bash
$ pytest -q
.......                                                                    [100%]
7 passed in 2.34s
```

If tests fail, they provide detailed error messages explaining what to check and how to fix the underlying issue.







