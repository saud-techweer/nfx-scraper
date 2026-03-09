#!/usr/bin/env python3
"""
Comprehensive Quality Analysis of All Scraped Investor Profiles
Analyzes every JSON file in data/profiles/ and reports detailed quality metrics.
"""

import json
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

PROFILES_DIR = Path("/Users/apple/Desktop/Techweer/SAUD BHAI/Scraping VC/data/profiles")

def is_populated(value):
    """Check if a value is meaningfully populated (non-null, non-empty)."""
    if value is None:
        return False
    if isinstance(value, str):
        return len(value.strip()) > 0
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return any(is_populated(v) for v in value.values())
    if isinstance(value, (int, float)):
        return True
    return bool(value)

def analyze_profile(data):
    """Analyze a single profile and return field-level details."""
    fields = {}

    # --- basicInfo ---
    bi = data.get("basicInfo") or {}
    fields["basicInfo.name"] = is_populated(bi.get("name"))
    fields["basicInfo.location"] = is_populated(bi.get("location"))
    fields["basicInfo.investorTypes"] = is_populated(bi.get("investorTypes"))
    fields["basicInfo.signalScore"] = bi.get("signalScore") is not None
    fields["basicInfo.website"] = is_populated(bi.get("website"))

    # --- experience ---
    exp = data.get("experience") or []
    if not isinstance(exp, list):
        exp = []
    fields["experience (>=1 entry)"] = len(exp) > 0
    fields["experience_count"] = len(exp)

    # --- investingProfile ---
    ip = data.get("investingProfile") or {}
    if not isinstance(ip, dict):
        ip = {}
    cp = ip.get("currentPosition")

    # Handle currentPosition being a string (just position title) or a dict
    if isinstance(cp, dict):
        fields["investingProfile.currentPosition.firm"] = is_populated(cp.get("firm"))
        fields["investingProfile.currentPosition.position"] = is_populated(cp.get("position"))
        fields["investingProfile.currentPosition.firmUrl"] = is_populated(cp.get("firmUrl"))
    elif isinstance(cp, str) and cp.strip():
        # It's just the position string, no firm info
        fields["investingProfile.currentPosition.firm"] = False
        fields["investingProfile.currentPosition.position"] = True
        fields["investingProfile.currentPosition.firmUrl"] = False
    else:
        fields["investingProfile.currentPosition.firm"] = False
        fields["investingProfile.currentPosition.position"] = False
        fields["investingProfile.currentPosition.firmUrl"] = False

    fields["investingProfile.investmentRange"] = is_populated(ip.get("investmentRange"))
    fields["investingProfile.sweetSpot"] = is_populated(ip.get("sweetSpot"))

    # Track the currentPosition format for reporting
    fields["_cp_is_string"] = isinstance(cp, str)

    # --- investments ---
    inv = data.get("investments") or []
    if not isinstance(inv, list):
        inv = []
    fields["investments (>=1 entry)"] = len(inv) > 0
    fields["investments_count"] = len(inv)

    # --- profilePicture ---
    fields["profilePicture"] = is_populated(data.get("profilePicture"))

    # --- profileUrl ---
    fields["profileUrl"] = is_populated(data.get("profileUrl"))

    # --- sectorRankings ---
    sr = data.get("sectorRankings") or []
    if not isinstance(sr, list):
        sr = []
    fields["sectorRankings (>=1 entry)"] = len(sr) > 0
    fields["sectorRankings_count"] = len(sr)

    # --- slug ---
    fields["slug"] = is_populated(data.get("slug"))

    # --- socials ---
    soc = data.get("socials") or {}
    if not isinstance(soc, dict):
        soc = {}
    fields["socials.linkedin"] = is_populated(soc.get("linkedin"))
    fields["socials.twitter"] = is_populated(soc.get("twitter"))
    fields["socials.angellist"] = is_populated(soc.get("angellist"))
    fields["socials.crunchbase"] = is_populated(soc.get("crunchbase"))
    fields["socials.website"] = is_populated(soc.get("website"))

    # --- scraped_at ---
    fields["scraped_at"] = is_populated(data.get("scraped_at"))

    return fields

def classify_profile(fields):
    """
    Classify profile as 'complete', 'partial', or 'empty/garbage'.
    """
    has_name = fields["basicInfo.name"]
    has_location = fields["basicInfo.location"]
    has_investor_types = fields["basicInfo.investorTypes"]
    has_signal_score = fields["basicInfo.signalScore"]
    has_firm = fields["investingProfile.currentPosition.firm"]
    has_experience = fields["experience (>=1 entry)"]
    has_investments = fields["investments (>=1 entry)"]
    has_sector_rankings = fields["sectorRankings (>=1 entry)"]
    has_depth = has_experience or has_investments or has_sector_rankings

    if has_name and has_location and has_investor_types and has_signal_score and has_firm and has_depth:
        return "complete"

    meaningful_keys = [
        "basicInfo.name", "basicInfo.location", "basicInfo.investorTypes",
        "basicInfo.signalScore", "investingProfile.currentPosition.firm",
        "experience (>=1 entry)", "investments (>=1 entry)", "sectorRankings (>=1 entry)",
        "profileUrl", "slug"
    ]
    populated_count = sum(1 for k in meaningful_keys if fields.get(k))

    if populated_count <= 2:
        return "empty/garbage"
    return "partial"


def main():
    json_files = sorted(PROFILES_DIR.glob("*.json"))
    total = len(json_files)
    print("=" * 80)
    print("  COMPREHENSIVE INVESTOR PROFILE QUALITY ANALYSIS")
    print("=" * 80)
    print(f"\nDirectory : {PROFILES_DIR}")
    print(f"Total JSON files found: {total}\n")

    if total == 0:
        print("No files found. Exiting.")
        sys.exit(1)

    # Accumulators
    field_populated_counts = Counter()
    classification_counts = Counter()
    malformed_files = []
    experience_counts = []
    investment_counts = []
    sector_counts = []
    signal_scores = []
    investor_type_counter = Counter()
    location_counter = Counter()
    cp_string_count = 0

    complete_profiles = []
    partial_profiles = []
    empty_profiles = []

    # Exclude internal/meta keys from display
    SKIP_KEYS = {"experience_count", "investments_count", "sectorRankings_count", "_cp_is_string"}
    boolean_field_keys = None

    for filepath in json_files:
        filename = filepath.name
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                raw = f.read()
            data = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            malformed_files.append((filename, str(e)))
            classification_counts["malformed"] += 1
            continue
        except Exception as e:
            malformed_files.append((filename, f"Unexpected: {e}"))
            classification_counts["malformed"] += 1
            continue

        fields = analyze_profile(data)

        if boolean_field_keys is None:
            boolean_field_keys = [k for k in fields.keys() if k not in SKIP_KEYS]

        for k in boolean_field_keys:
            if fields[k]:
                field_populated_counts[k] += 1

        if fields.get("_cp_is_string"):
            cp_string_count += 1

        experience_counts.append(fields["experience_count"])
        investment_counts.append(fields["investments_count"])
        sector_counts.append(fields["sectorRankings_count"])

        bi = data.get("basicInfo") or {}
        ss = bi.get("signalScore")
        if ss is not None:
            signal_scores.append(ss)

        for it in (bi.get("investorTypes") or []):
            investor_type_counter[it] += 1

        loc = bi.get("location")
        if loc and isinstance(loc, str) and loc.strip():
            location_counter[loc.strip()] += 1

        cat = classify_profile(fields)
        classification_counts[cat] += 1
        if cat == "complete":
            complete_profiles.append(filename)
        elif cat == "partial":
            partial_profiles.append(filename)
        else:
            empty_profiles.append(filename)

    # ── REPORT ──
    valid = total - len(malformed_files)

    print("-" * 80)
    print("  1. OVERALL CLASSIFICATION")
    print("-" * 80)
    print(f"  Total files scanned        : {total}")
    print(f"  Malformed / parse errors   : {classification_counts.get('malformed', 0)}")
    print(f"  Valid JSON profiles        : {valid}")
    print()
    c = classification_counts.get('complete', 0)
    p = classification_counts.get('partial', 0)
    e = classification_counts.get('empty/garbage', 0)
    print(f"  Complete profiles (100%)   : {c:>5}  ({c/valid*100:.1f}%)")
    print(f"  Partial profiles           : {p:>5}  ({p/valid*100:.1f}%)")
    print(f"  Empty / garbage profiles   : {e:>5}  ({e/valid*100:.1f}%)")

    if malformed_files:
        print()
        print("-" * 80)
        print(f"  2. MALFORMED / PARSE ERROR FILES ({len(malformed_files)} total)")
        print("-" * 80)
        for fname, err in malformed_files:
            print(f"    {fname}: {err}")
    else:
        print()
        print("-" * 80)
        print("  2. MALFORMED FILES: None -- all files parsed successfully")
        print("-" * 80)

    print()
    print("-" * 80)
    print(f"  3. FIELD-BY-FIELD COVERAGE  (out of {valid} valid profiles)")
    print("-" * 80)
    print(f"  {'Field':<50} {'Count':>6}  {'Coverage':>8}")
    print(f"  {'─'*50} {'─'*6}  {'─'*8}")
    for k in boolean_field_keys:
        cnt = field_populated_counts.get(k, 0)
        pct = cnt / valid * 100 if valid else 0
        bar = "#" * int(pct // 2)
        print(f"  {k:<50} {cnt:>6}  {pct:>7.1f}%  {bar}")

    print()
    print(f"  NOTE: {cp_string_count} profiles have currentPosition as a plain string")
    print(f"        (position title only, no firm/firmUrl sub-fields)")

    print()
    print("-" * 80)
    print("  4. NUMERIC FIELD DISTRIBUTIONS")
    print("-" * 80)

    def print_distribution(name, values):
        if not values:
            print(f"  {name}: no data")
            return
        vals = sorted(values)
        n = len(vals)
        mean_val = sum(vals) / n
        median_val = vals[n // 2]
        mn, mx = vals[0], vals[-1]
        zero_count = sum(1 for v in vals if v == 0)
        print(f"  {name}:")
        print(f"    Count : {n}")
        print(f"    Min   : {mn}")
        print(f"    Max   : {mx}")
        print(f"    Mean  : {mean_val:.1f}")
        print(f"    Median: {median_val}")
        print(f"    Zeros : {zero_count} ({zero_count/n*100:.1f}%)")
        if name == "Signal Score":
            buckets = [(0, 50), (51, 100), (101, 200), (201, 500), (501, 1000), (1001, 5000), (5001, 999999)]
            bucket_labels = ["0-50", "51-100", "101-200", "201-500", "501-1000", "1001-5000", "5001+"]
        else:
            buckets = [(0, 0), (1, 1), (2, 5), (6, 10), (11, 20), (21, 50), (51, 999999)]
            bucket_labels = ["0", "1", "2-5", "6-10", "11-20", "21-50", "51+"]
        print(f"    Distribution:")
        for (lo, hi), label in zip(buckets, bucket_labels):
            cnt = sum(1 for v in vals if lo <= v <= hi)
            pct = cnt / n * 100
            bar = "#" * int(pct // 2)
            print(f"      {label:>10}: {cnt:>5} ({pct:>5.1f}%)  {bar}")
        print()

    print_distribution("Experience entries", experience_counts)
    print_distribution("Investments entries", investment_counts)
    print_distribution("Sector Rankings entries", sector_counts)
    print_distribution("Signal Score", signal_scores)

    print("-" * 80)
    print("  5. INVESTOR TYPE BREAKDOWN")
    print("-" * 80)
    for itype, cnt in investor_type_counter.most_common():
        print(f"    {itype:<30} {cnt:>5}  ({cnt/valid*100:.1f}%)")

    print()
    print("-" * 80)
    print("  6. TOP 25 LOCATIONS")
    print("-" * 80)
    for loc, cnt in location_counter.most_common(25):
        print(f"    {loc:<45} {cnt:>5}  ({cnt/valid*100:.1f}%)")

    if empty_profiles:
        print()
        print("-" * 80)
        print(f"  7. EMPTY/GARBAGE PROFILES (showing up to 20 of {len(empty_profiles)})")
        print("-" * 80)
        for fname in empty_profiles[:20]:
            fpath = PROFILES_DIR / fname
            try:
                with open(fpath) as f:
                    d = json.load(f)
                bi = d.get("basicInfo") or {}
                name = bi.get("name", "N/A")
                loc = bi.get("location", "N/A")
                ss = bi.get("signalScore", "N/A")
                ip_raw = d.get("investingProfile") or {}
                if isinstance(ip_raw, dict):
                    cp_raw = ip_raw.get("currentPosition")
                    if isinstance(cp_raw, dict):
                        firm = cp_raw.get("firm", "N/A")
                    else:
                        firm = f"(str: {cp_raw})" if cp_raw else "N/A"
                else:
                    firm = "N/A"
                exp_n = len(d.get("experience") or [])
                inv_n = len(d.get("investments") or [])
                sr_n = len(d.get("sectorRankings") or [])
                print(f"    {fname}")
                print(f"      name={name!r}, location={loc!r}, score={ss}, firm={firm!r}")
                print(f"      experience={exp_n}, investments={inv_n}, sectorRankings={sr_n}")
            except Exception as ex:
                print(f"    {fname} -- error reading: {ex}")

    if partial_profiles:
        print()
        print("-" * 80)
        print(f"  8. PARTIAL PROFILES: COMMON MISSING FIELDS (among {len(partial_profiles)} partial profiles)")
        print("-" * 80)
        missing_field_counter = Counter()
        key_fields_for_complete = [
            "basicInfo.name", "basicInfo.location", "basicInfo.investorTypes",
            "basicInfo.signalScore", "investingProfile.currentPosition.firm",
        ]
        depth_fields = ["experience (>=1 entry)", "investments (>=1 entry)", "sectorRankings (>=1 entry)"]

        for fname in partial_profiles:
            fpath = PROFILES_DIR / fname
            try:
                with open(fpath) as f:
                    d = json.load(f)
                fields = analyze_profile(d)
                for k in key_fields_for_complete:
                    if not fields[k]:
                        missing_field_counter[k] += 1
                if not any(fields[dk] for dk in depth_fields):
                    missing_field_counter["ALL depth (exp+inv+sectors)"] += 1
            except:
                pass

        print(f"  Among {len(partial_profiles)} partial profiles, fields most often MISSING:")
        for field, cnt in missing_field_counter.most_common():
            print(f"    {field:<50} missing in {cnt:>5} ({cnt/len(partial_profiles)*100:.1f}%)")

    print()
    print("-" * 80)
    print("  9. SOCIAL MEDIA LINK COVERAGE")
    print("-" * 80)
    social_fields = ["socials.linkedin", "socials.twitter", "socials.angellist", "socials.crunchbase", "socials.website"]
    for sf in social_fields:
        cnt = field_populated_counts.get(sf, 0)
        pct = cnt / valid * 100 if valid else 0
        print(f"    {sf:<30} {cnt:>5}  ({pct:>5.1f}%)")

    print()
    print("=" * 80)
    print("  FINAL SUMMARY")
    print("=" * 80)
    complete_n = classification_counts.get('complete', 0)
    partial_n = classification_counts.get('partial', 0)
    empty_n = classification_counts.get('empty/garbage', 0)
    malformed_n = classification_counts.get('malformed', 0)
    usable = complete_n + partial_n
    print(f"  Total files           : {total}")
    print(f"  Malformed JSON        : {malformed_n}")
    print(f"  Complete (production) : {complete_n} ({complete_n/total*100:.1f}%)")
    print(f"  Partial (usable)      : {partial_n} ({partial_n/total*100:.1f}%)")
    print(f"  Empty/garbage         : {empty_n} ({empty_n/total*100:.1f}%)")
    print(f"  ---")
    print(f"  Usable (complete+partial): {usable} ({usable/total*100:.1f}%)")
    print(f"  Data loss / waste        : {empty_n + malformed_n} ({(empty_n+malformed_n)/total*100:.1f}%)")
    print("=" * 80)


if __name__ == "__main__":
    main()
