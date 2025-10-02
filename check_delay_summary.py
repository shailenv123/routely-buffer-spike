import csv
import math

CSV_PATH = "delays_output.csv"

def percentile(values, p):
    """Return the pth percentile of a list of numeric values. p in [0, 100]."""
    if not values:
        return None
    values = sorted(values)
    k = (p / 100) * (len(values) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return values[int(k)]
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return d0 + d1

def main():
    total = 0
    with_actual = 0
    delays = []

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            actual_ta = row.get("actual_ta") or ""
            delay_str = row.get("delay_min")
            if actual_ta.strip():
                with_actual += 1
            if delay_str not in (None, "", "None"):
                try:
                    delays.append(float(delay_str))
                except ValueError:
                    pass

    mean_delay = sum(delays) / len(delays) if delays else None
    p90_delay = percentile(delays, 90) if delays else None

    print(f"total RIDs processed: {total}")
    print(f"with actual arrivals: {with_actual}")
    print(f"mean delay (min): {mean_delay if mean_delay is not None else 'N/A'}")
    print(f"90th percentile delay (min): {p90_delay if p90_delay is not None else 'N/A'}")

if __name__ == "__main__":
    main()
