#!/usr/bin/env python3
"""
Find rows in a samples CSV that have the default coordinates 46.5, 11.35.

Env/Args:
  SAMPLES_CSV (or --samples)
  LAT_COL (default GPS_lat)
  LON_COL (default GPS_long)
  OUT_CSV (optional; if not set, prints to stdout)
"""

import os, sys, argparse
import pandas as pd

DEFAULT_LAT = 46.5
DEFAULT_LON = 11.35

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", default=os.getenv("SAMPLES_CSV"), help="Path to samples CSV")
    ap.add_argument("--lat_col", default=os.getenv("LAT_COL", "GPS_lat"))
    ap.add_argument("--lon_col", default=os.getenv("LON_COL", "GPS_long"))
    ap.add_argument("--out_csv", default=os.getenv("OUT_CSV"))
    args = ap.parse_args()

    if not args.samples or not os.path.exists(args.samples):
        print(f"ERROR: samples CSV not found: {args.samples}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(args.samples, dtype=str, keep_default_na=False)

    # coerce numeric
    lat = pd.to_numeric(df.get(args.lat_col, ""), errors="coerce")
    lon = pd.to_numeric(df.get(args.lon_col, ""), errors="coerce")

    mask = (lat == DEFAULT_LAT) & (lon == DEFAULT_LON)
    bad = df[mask].copy()

    if args.out_csv:
        bad.to_csv(args.out_csv, index=False)
        print(f"[find_default_coords] wrote {len(bad)} rows -> {args.out_csv}")
    else:
        print(bad.to_csv(index=False), end="")

if __name__ == "__main__":
    main()
