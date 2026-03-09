#!/usr/bin/env python3
"""
Convert all JSON profiles in data-saas/profiles/ to a single CSV file.
Each profile becomes one row. Nested/list fields are flattened into columns.
URLs from all_investor_urls.json are matched by slug.
"""

import json
import csv
import glob
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROFILES_DIR = os.path.join(BASE_DIR, "data-saas", "profiles")
URLS_FILE = os.path.join(BASE_DIR, "data-saas", "all_investor_urls.json")
OUTPUT_CSV = os.path.join(BASE_DIR, "data-saas", "saas_investors.csv")

# Load URL mapping (slug -> url)
with open(URLS_FILE) as f:
    url_list = json.load(f)
url_map = {item["slug"]: item["url"] for item in url_list}

# Collect all rows first to discover max counts for list fields
rows = []
max_investments = 0
max_experience = 0
max_sector_rankings = 0
max_investor_types = 0

profile_files = sorted(glob.glob(os.path.join(PROFILES_DIR, "*.json")))
print(f"Processing {len(profile_files)} profiles...")

for filepath in profile_files:
    with open(filepath) as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print(f"  Skipping invalid JSON: {filepath}")
            continue

    slug = data.get("slug", os.path.splitext(os.path.basename(filepath))[0])

    # Basic info
    basic = data.get("basicInfo", {})
    investor_types = basic.get("investorTypes", [])
    max_investor_types = max(max_investor_types, len(investor_types))

    # Investing profile
    investing = data.get("investingProfile", {}) or {}
    current_pos = investing.get("currentPosition", {})
    if not isinstance(current_pos, dict):
        current_pos = {}

    # Experience
    experience = data.get("experience", [])
    max_experience = max(max_experience, len(experience))

    # Investments
    investments = data.get("investments", [])
    max_investments = max(max_investments, len(investments))

    # Sector rankings
    sectors = data.get("sectorRankings", [])
    max_sector_rankings = max(max_sector_rankings, len(sectors))

    # Socials
    socials = data.get("socials", {}) or {}

    row = {
        "slug": slug,
        "matched_url": url_map.get(slug, ""),
        "profile_url": data.get("profileUrl", ""),
        "profile_picture": data.get("profilePicture", ""),
        "scraped_at": data.get("scraped_at", ""),
        # Basic info
        "name": basic.get("name", ""),
        "location": basic.get("location", ""),
        "signal_score": basic.get("signalScore", ""),
        "position_and_firm": basic.get("positionAndFirm", ""),
        "website": basic.get("website", ""),
        "investor_types": "; ".join(investor_types),
        # Investing profile
        "current_firm": current_pos.get("firm", ""),
        "current_firm_url": current_pos.get("firmUrl", ""),
        "current_position": current_pos.get("position", ""),
        "investment_range": investing.get("investmentRange", ""),
        "sweet_spot": investing.get("sweetSpot", ""),
        "fund_size": investing.get("fundSize", ""),
        "investments_on_record": investing.get("investmentsOnRecord", ""),
        # Socials
        "linkedin": socials.get("linkedin", ""),
        "twitter": socials.get("twitter", ""),
        "angellist": socials.get("angellist", ""),
        "crunchbase": socials.get("crunchbase", ""),
        "social_website": socials.get("website", ""),
        # Lists stored for later expansion
        "_experience": experience,
        "_investments": investments,
        "_sectors": sectors,
    }
    rows.append(row)

print(f"Max experience entries: {max_experience}")
print(f"Max investment entries: {max_investments}")
print(f"Max sector rankings: {max_sector_rankings}")

# Build CSV headers
headers = [
    "slug", "matched_url", "profile_url", "profile_picture", "scraped_at",
    "name", "location", "signal_score", "position_and_firm", "website", "investor_types",
    "current_firm", "current_firm_url", "current_position",
    "investment_range", "sweet_spot", "fund_size", "investments_on_record",
    "linkedin", "twitter", "angellist", "crunchbase", "social_website",
]

# Experience columns
for i in range(1, max_experience + 1):
    headers.extend([f"experience_{i}_company", f"experience_{i}_position", f"experience_{i}_dates"])

# Investment columns
for i in range(1, max_investments + 1):
    headers.extend([
        f"investment_{i}_company", f"investment_{i}_stage", f"investment_{i}_date",
        f"investment_{i}_round_size", f"investment_{i}_total_raised", f"investment_{i}_co_investors"
    ])

# Sector ranking columns
for i in range(1, max_sector_rankings + 1):
    headers.extend([f"sector_ranking_{i}_name", f"sector_ranking_{i}_url"])

# Write CSV
with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=headers, extrasaction="ignore")
    writer.writeheader()

    for row in rows:
        # Expand experience
        for i, exp in enumerate(row.pop("_experience"), 1):
            row[f"experience_{i}_company"] = exp.get("company", "")
            row[f"experience_{i}_position"] = exp.get("position", "")
            row[f"experience_{i}_dates"] = exp.get("dates", "")

        # Expand investments
        for i, inv in enumerate(row.pop("_investments"), 1):
            row[f"investment_{i}_company"] = inv.get("company", "")
            row[f"investment_{i}_stage"] = inv.get("stage", "")
            row[f"investment_{i}_date"] = inv.get("date", "")
            row[f"investment_{i}_round_size"] = inv.get("roundSize", "")
            row[f"investment_{i}_total_raised"] = inv.get("totalRaised", "")
            co = inv.get("coInvestors", [])
            row[f"investment_{i}_co_investors"] = "; ".join(co) if isinstance(co, list) else str(co)

        # Expand sector rankings
        for i, sec in enumerate(row.pop("_sectors"), 1):
            row[f"sector_ranking_{i}_name"] = sec.get("name", "")
            row[f"sector_ranking_{i}_url"] = sec.get("url", "")

        writer.writerow(row)

print(f"\nCSV written to: {OUTPUT_CSV}")
print(f"Total rows: {len(rows)}")
print(f"Total columns: {len(headers)}")
