#!/usr/bin/env python3
"""
Comprehensive Quality Analysis of Scraped Investor Profiles
Analyzes all JSON profiles in data/profiles/ directory.
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

PROFILES_DIR = Path("/Users/apple/Desktop/Techweer/SAUD BHAI/Scraping VC/data/profiles")

# Garbage indicators in names
GARBAGE_INDICATORS = [
    "signal.nfx.com",
    "gateway",
    "cloudflare",
    "error",
    "404",
    "403",
    "502",
    "503",
    "500",
    "not found",
    "access denied",
    "timeout",
    "<!doctype",
    "<html",
    "just a moment",
    "captcha",
    "blocked",
    "rate limit",
    "too many requests",
    "page not found",
    "server error",
    "bad gateway",
    "service unavailable",
    "forbidden",
    "unauthorized",
]


def is_populated(value):
    """Check if a value is populated (non-null, non-empty)."""
    if value is None:
        return False
    if isinstance(value, str):
        return len(value.strip()) > 0
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return len(value) > 0
    return True


def is_garbage_name(name):
    """Check if a name is garbage/invalid."""
    if not name or not isinstance(name, str):
        return True
    name_lower = name.strip().lower()
    if len(name_lower) == 0:
        return True
    for indicator in GARBAGE_INDICATORS:
        if indicator in name_lower:
            return True
    return False


def analyze_profile(data):
    """Analyze a single profile and return field presence info."""
    fields = {}

    # basicInfo fields
    basic = data.get("basicInfo", {}) or {}
    if not isinstance(basic, dict):
        basic = {}
    fields["name"] = is_populated(basic.get("name"))
    fields["name_value"] = basic.get("name", "")
    fields["location"] = is_populated(basic.get("location"))
    
    investor_types = basic.get("investorTypes", []) or []
    if not isinstance(investor_types, list):
        investor_types = []
    fields["investorTypes"] = len(investor_types) > 0
    fields["signalScore"] = is_populated(basic.get("signalScore"))
    
    # investingProfile fields
    investing = data.get("investingProfile", {}) or {}
    if not isinstance(investing, dict):
        investing = {}
    
    current_pos = investing.get("currentPosition", None)
    if isinstance(current_pos, dict):
        fields["currentPosition"] = is_populated(current_pos.get("firm")) or is_populated(current_pos.get("position"))
    elif isinstance(current_pos, str):
        fields["currentPosition"] = len(current_pos.strip()) > 0
    else:
        fields["currentPosition"] = False
    
    fields["investmentRange"] = is_populated(investing.get("investmentRange"))
    fields["sweetSpot"] = is_populated(investing.get("sweetSpot"))

    # Array fields
    sectors = data.get("sectorRankings", []) or []
    if not isinstance(sectors, list):
        sectors = []
    fields["sectorRankings"] = len(sectors) > 0

    experience = data.get("experience", []) or []
    if not isinstance(experience, list):
        experience = []
    fields["experience"] = len(experience) > 0

    investments = data.get("investments", []) or []
    if not isinstance(investments, list):
        investments = []
    fields["investments"] = len(investments) > 0

    # Other fields
    fields["profilePicture"] = is_populated(data.get("profilePicture"))

    socials = data.get("socials", {}) or {}
    if not isinstance(socials, dict):
        socials = {}
    fields["linkedin"] = is_populated(socials.get("linkedin"))
    fields["twitter"] = is_populated(socials.get("twitter"))
    fields["crunchbase"] = is_populated(socials.get("crunchbase"))

    return fields


def classify_profile(fields):
    """Classify profile into quality tier."""
    name_val = fields.get("name_value", "")
    
    # Check garbage first
    if is_garbage_name(name_val):
        return "garbage"
    
    # Name is valid from here
    has_location = fields["location"]
    has_investor_types = fields["investorTypes"]
    has_current_position = fields["currentPosition"]
    has_investment_range = fields["investmentRange"]
    has_experience = fields["experience"]
    has_investments = fields["investments"]
    has_sectors = fields["sectorRankings"]

    # Complete: name + location + at least 1 investorType + currentPosition + at least 1 of (experience, investments, sectorRankings)
    if (has_location and has_investor_types and has_current_position and
            (has_experience or has_investments or has_sectors)):
        return "complete"

    # Good: name + at least 2 other key fields
    key_fields_count = sum([has_location, has_investor_types, has_current_position, has_investment_range])
    if key_fields_count >= 2:
        return "good"

    # Minimal: name present but sparse
    return "minimal"


def main():
    json_files = sorted(PROFILES_DIR.glob("*.json"))
    total = len(json_files)

    print("=" * 80)
    print("  COMPREHENSIVE INVESTOR PROFILE QUALITY ANALYSIS")
    print("=" * 80)
    print()

    # 1. Total profiles
    print(f"1. TOTAL PROFILES ON DISK: {total}")
    print()

    # Parse all profiles
    malformed = []
    profiles = {}  # slug -> (fields, data)
    
    for fpath in json_files:
        slug = fpath.stem
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            fields = analyze_profile(data)
            profiles[slug] = (fields, data)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            malformed.append((slug, str(e)))

    # 2. Malformed JSON
    print(f"2. MALFORMED JSON FILES: {len(malformed)}")
    if malformed:
        for slug, err in malformed:
            print(f"   - {slug}: {err}")
    else:
        print("   None - all files are valid JSON.")
    print()

    # 3. Quality tiers
    tiers = defaultdict(list)
    for slug, (fields, data) in profiles.items():
        tier = classify_profile(fields)
        tiers[tier].append(slug)

    parsed_total = len(profiles)
    print("3. QUALITY TIERS:")
    print(f"   {'Tier':<12} {'Count':>6}  {'Percentage':>10}")
    print(f"   {'-'*12} {'-'*6}  {'-'*10}")
    for tier_name in ["complete", "good", "minimal", "garbage"]:
        count = len(tiers[tier_name])
        pct = (count / parsed_total * 100) if parsed_total > 0 else 0
        print(f"   {tier_name.upper():<12} {count:>6}  {pct:>9.1f}%")
    print(f"   {'-'*12} {'-'*6}  {'-'*10}")
    print(f"   {'TOTAL':<12} {parsed_total:>6}  {'100.0%':>10}")
    print()

    # 4. Field coverage
    print("4. FIELD COVERAGE (non-null, non-empty):")
    print(f"   {'Field':<40} {'Present':>7} {'of':>3} {'Total':>5}  {'Coverage':>8}")
    print(f"   {'-'*40} {'-'*7} {'-'*3} {'-'*5}  {'-'*8}")

    field_labels = [
        ("name", "basicInfo.name"),
        ("location", "basicInfo.location"),
        ("investorTypes", "basicInfo.investorTypes (non-empty)"),
        ("signalScore", "basicInfo.signalScore"),
        ("currentPosition", "investingProfile.currentPosition"),
        ("investmentRange", "investingProfile.investmentRange"),
        ("sweetSpot", "investingProfile.sweetSpot"),
        ("sectorRankings", "sectorRankings (non-empty)"),
        ("experience", "experience (non-empty)"),
        ("investments", "investments (non-empty)"),
        ("profilePicture", "profilePicture"),
        ("linkedin", "socials.linkedin"),
        ("twitter", "socials.twitter"),
        ("crunchbase", "socials.crunchbase"),
    ]

    for field_key, label in field_labels:
        count = sum(1 for slug, (fields, _) in profiles.items() if fields.get(field_key, False))
        pct = (count / parsed_total * 100) if parsed_total > 0 else 0
        print(f"   {label:<40} {count:>7} {'of':>3} {parsed_total:>5}  {pct:>7.1f}%")
    print()

    # 5. List ALL garbage/bad profiles
    print("5. ALL GARBAGE/BAD PROFILES:")
    garbage_list = tiers["garbage"]
    if garbage_list:
        print(f"   Total garbage profiles: {len(garbage_list)}")
        print()
        print(f"   {'#':<5} {'Slug':<50} {'Name (truncated)'}")
        print(f"   {'-'*5} {'-'*50} {'-'*40}")
        for i, slug in enumerate(sorted(garbage_list), 1):
            fields, data = profiles[slug]
            name_val = fields.get("name_value", "<NONE>")
            if name_val is None:
                name_val = "<null>"
            elif isinstance(name_val, str) and len(name_val) == 0:
                name_val = "<empty string>"
            elif len(str(name_val)) > 60:
                name_val = str(name_val)[:57] + "..."
            print(f"   {i:<5} {slug:<50} {name_val}")
    else:
        print("   None found - all profiles have valid names!")
    print()

    # 6. Suspiciously short names
    print("6. PROFILES WITH SUSPICIOUSLY SHORT NAMES (< 3 chars, excluding empty):")
    short_names = []
    for slug, (fields, data) in profiles.items():
        name_val = fields.get("name_value", "")
        if name_val and isinstance(name_val, str):
            stripped = name_val.strip()
            if 0 < len(stripped) < 3:
                short_names.append((slug, stripped))

    if short_names:
        print(f"   Total: {len(short_names)}")
        print()
        print(f"   {'Slug':<50} {'Name'}")
        print(f"   {'-'*50} {'-'*20}")
        for slug, name in sorted(short_names):
            print(f"   {slug:<50} '{name}'")
    else:
        print("   None found.")
    print()

    # Summary
    print("=" * 80)
    print("  SUMMARY")
    print("=" * 80)
    
    valid_profiles = len(tiers["complete"]) + len(tiers["good"]) + len(tiers["minimal"])
    valid_pct = (valid_profiles / parsed_total * 100) if parsed_total > 0 else 0
    complete_good = len(tiers["complete"]) + len(tiers["good"])
    cg_pct = (complete_good / parsed_total * 100) if parsed_total > 0 else 0
    
    print(f"  Total files on disk:          {total}")
    print(f"  Successfully parsed:          {parsed_total}")
    print(f"  Malformed/unreadable:         {len(malformed)}")
    print(f"  Valid profiles (non-garbage): {valid_profiles} ({valid_pct:.1f}%)")
    print(f"  Complete + Good:              {complete_good} ({cg_pct:.1f}%)")
    print(f"  Garbage/Bad:                  {len(tiers['garbage'])} ({len(tiers['garbage'])/parsed_total*100:.1f}%)")
    print("=" * 80)


if __name__ == "__main__":
    main()
