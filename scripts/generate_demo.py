#!/usr/bin/env python3
"""Generate deterministic synthetic raw CSVs in the course input layout."""
import argparse
import csv
from pathlib import Path


def generate(destination):
    base = destination / "synthetic-scene" / "metric"
    for kind in ("service", "node", "container", "jvm", "istio"):
        folder = base / kind
        folder.mkdir(parents=True, exist_ok=True)
        with (folder / "metrics.csv").open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["timestamp", "service", "rr", "sr", "count", "mrt"] if kind == "service"
                            else ["timestamp", "cmdb_id", "kpi_name", "value"])
            for i in range(240):
                timestamp = 1700000000 + i * 60
                wave = i % 11 - 5
                spike = i in (150, 200)
                if kind == "service":
                    writer.writerow([timestamp, "demo-service", 99 + wave * .05,
                                     99 + wave * .05, 9000 if spike else 100 + wave,
                                     7000 if spike else 20 + wave * .2])
                else:
                    for kpi, mean in (("cpu", 40), ("memory", 60)):
                        writer.writerow([timestamp, "demo-" + kind, kpi,
                                         10000 if spike else mean + wave])
            # Exactly one invalid timestamp row per type, excluded by cleaning.
            writer.writerow(["invalid", "demo-service", 1, 1, 1, 1] if kind == "service"
                            else ["invalid", "demo-" + kind, "cpu", 1])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destination", type=Path)
    generate(parser.parse_args().destination)
