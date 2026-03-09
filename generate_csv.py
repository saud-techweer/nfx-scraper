#!/usr/bin/env python3
"""
Generate a single CSV from all 3,192 investor profile JSONs.
Flattens nested structures into consistent columns.
"""

import csv
import json
import os
import sys

PROFILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "profiles")
OUTPUT_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "all_investors.csv")

# CSV columns in order
COLUMNS = [
    "slug",
    "profile_url",
    "name",
    "location",
    "investor_types",
    "signal_score",
    "current_position",
    "current_firm",
    "current_firm_url",
    "position_and_firm",
    "investment_range",
    "sweet_spot",
    "fund_size",
    "investments_on_record",
    "website",
    "profile_picture",
    "linkedin",
    "twitter",
    "crunchbase",
    "angellist",
    "social_website",
    "sector_rankings",
    "experience",
    "investments",
    "scraped_at",
]


def flatten_profile(data):
    """Convert a nested profile JSON into a flat dict for CSV."""
    basic = data.get("basicInfo", {})
    investing = data.get("investingProfile", {})
    socials = data.get("socials", {})

    # Handle currentPosition (can be object or string)
    cp = investing.get("currentPosition")
    if isinstance(cp, dict):
        current_position = cp.get("position", "")
        current_firm = cp.get("firm", "")
        current_firm_url = cp.get("firmUrl", "")
    elif isinstance(cp, str):
        current_position = cp
        current_firm = ""
        current_firm_url = ""
    else:
        current_position = ""
        current_firm = ""
        current_firm_url = ""

    # Flatten investor types array
    types = basic.get("investorTypes", [])
    investor_types = " | ".join(types) if types else ""

    # Flatten sector rankings
    rankings = data.get("sectorRankings", [])
    sector_str = " | ".join(r.get("name", "") for r in rankings) if rankings else ""

    # Flatten experience array
    exp_list = data.get("experience", [])
    exp_parts = []
    for e in exp_list:
        pos = e.get("position", e.get("title", ""))
        company = e.get("company", "")
        dates = e.get("dates", "")
        if company:
            exp_parts.append(f"{pos} @ {company} ({dates})" if dates else f"{pos} @ {company}")
        elif pos:
            exp_parts.append(f"{pos} ({dates})" if dates else pos)
    experience_str = " | ".join(exp_parts) if exp_parts else ""

    # Flatten investments array
    inv_list = data.get("investments", [])
    inv_parts = []
    for inv in inv_list:
        company = inv.get("company", "")
        stage = inv.get("stage", "")
        date = inv.get("date", "")
        round_size = inv.get("roundSize", "")
        parts = [company]
        if stage:
            parts.append(stage)
        if date:
            parts.append(date)
        if round_size:
            parts.append(round_size)
        inv_parts.append(" - ".join(parts))
    investments_str = " | ".join(inv_parts) if inv_parts else ""

    return {
        "slug": data.get("slug", ""),
        "profile_url": data.get("profileUrl", ""),
        "name": basic.get("name", ""),
        "location": basic.get("location", ""),
        "investor_types": investor_types,
        "signal_score": basic.get("signalScore", ""),
        "current_position": current_position,
        "current_firm": current_firm,
        "current_firm_url": current_firm_url,
        "position_and_firm": basic.get("positionAndFirm", ""),
        "investment_range": investing.get("investmentRange", ""),
        "sweet_spot": investing.get("sweetSpot", ""),
        "fund_size": investing.get("fundSize", ""),
        "investments_on_record": investing.get("investmentsOnRecord", ""),
        "website": basic.get("website", ""),
        "profile_picture": data.get("profilePicture", ""),
        "linkedin": socials.get("linkedin", ""),
        "twitter": socials.get("twitter", ""),
        "crunchbase": socials.get("crunchbase", ""),
        "angellist": socials.get("angellist", ""),
        "social_website": socials.get("website", ""),
        "sector_rankings": sector_str,
        "experience": experience_str,
        "investments": investments_str,
        "scraped_at": data.get("scraped_at", ""),
    }


def main():
    files = sorted(f for f in os.listdir(PROFILES_DIR) if f.endswith(".json"))
    print(f"Processing {len(files)} profiles...")

    rows = []
    errors = []

    for fname in files:
        filepath = os.path.join(PROFILES_DIR, fname)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            row = flatten_profile(data)
            rows.append(row)
        except Exception as e:
            errors.append((fname, str(e)))

    # Write CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV written: {OUTPUT_CSV}")
    print(f"Total rows: {len(rows)}")
    print(f"Columns: {len(COLUMNS)}")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for fname, err in errors:
            print(f"  {fname}: {err}")
    else:
        print("No errors.")

    # Quick sanity check
    print(f"\nSample row (first):")
    if rows:
        r = rows[0]
        for col in COLUMNS:
            val = str(r.get(col, ""))
            print(f"  {col}: {val[:80]}{'...' if len(val) > 80 else ''}")


if __name__ == "__main__":
    main()
