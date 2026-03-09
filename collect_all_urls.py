#!/usr/bin/env python3
"""
NFX Signal - URL Collector via GraphQL API
============================================
Directly queries the GraphQL API at signal-api.nfx.com/graphql.
No browser needed. Uses cursor-based pagination.
Merges with existing data, saves incrementally.
"""

import json
import os
import sys
import logging
import time
import urllib.request
import urllib.error

# =============================================================================
# CONFIG
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

GRAPHQL_URL = "https://signal-api.nfx.com/graphql"
LIST_SLUG = "saas-seed"
PAGE_SIZE = 50  # fetch 50 at a time instead of 8 for speed
MIN_URLS = 6000
SAVE_EVERY = 200  # save every N new URLs

DATA_DIR = os.path.join(SCRIPT_DIR, "data-saas")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "collector_headless.log")

GRAPHQL_QUERY = """
query vclInvestors($slug: String!, $after: String) {
  list(slug: $slug) {
    id
    slug
    investor_count
    scored_investors(first: %d, after: $after) {
      pageInfo {
        hasNextPage
        hasPreviousPage
        endCursor
      }
      record_count
      edges {
        node {
          id
          person {
            id
            first_name
            last_name
            name
            slug
          }
          position
          firm {
            id
            name
            slug
          }
        }
      }
    }
  }
}
""" % PAGE_SIZE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# URL FILE I/O
# =============================================================================
def load_existing_urls() -> dict:
    if os.path.exists(ALL_URLS_FILE):
        try:
            with open(ALL_URLS_FILE, "r") as f:
                data = json.load(f)
                return {item["slug"]: item["url"] for item in data}
        except Exception as e:
            log.warning(f"Could not load existing URLs: {e}")
    return {}


def save_urls(url_dict: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    investor_list = sorted(
        [{"slug": s, "url": u} for s, u in url_dict.items()],
        key=lambda x: x["slug"],
    )
    tmp = ALL_URLS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(investor_list, f, indent=2)
    os.replace(tmp, ALL_URLS_FILE)


# =============================================================================
# GRAPHQL CLIENT
# =============================================================================
def graphql_request(after_cursor=None, retries=5):
    """Make a GraphQL request and return parsed JSON."""
    variables = {
        "slug": LIST_SLUG,
        "order": [{}],
    }
    if after_cursor:
        variables["after"] = after_cursor

    payload = json.dumps({
        "operationName": "vclInvestors",
        "variables": variables,
        "query": GRAPHQL_QUERY,
    }).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Origin": "https://signal.nfx.com",
        "Referer": "https://signal.nfx.com/",
    }

    for attempt in range(retries):
        try:
            req = urllib.request.Request(GRAPHQL_URL, data=payload, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
                return json.loads(body)
        except urllib.error.HTTPError as e:
            log.warning(f"HTTP {e.code} on attempt {attempt+1}: {e.reason}")
            if e.code == 429:
                wait = min(60, 5 * (attempt + 1))
                log.info(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
            elif e.code >= 500:
                time.sleep(3 * (attempt + 1))
            else:
                raise
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            log.warning(f"Network error on attempt {attempt+1}: {e}")
            time.sleep(5 * (attempt + 1))

    raise RuntimeError(f"Failed after {retries} retries")


# =============================================================================
# MAIN COLLECTOR
# =============================================================================
def collect():
    all_investors = load_existing_urls()
    start_count = len(all_investors)
    log.info(f"Loaded {start_count} existing URLs from disk")

    if start_count >= MIN_URLS:
        log.info(f"Already have {start_count} URLs (>= {MIN_URLS}). Done!")
        return

    cursor = None
    page_num = 0
    total_record_count = None
    new_since_save = 0

    while True:
        page_num += 1

        try:
            data = graphql_request(after_cursor=cursor)
        except Exception as e:
            log.error(f"Request failed: {e}")
            save_urls(all_investors)
            break

        # Parse response
        list_data = data.get("data", {}).get("list")
        if not list_data:
            log.error(f"No list data in response: {json.dumps(data)[:500]}")
            break

        scored = list_data.get("scored_investors", {})
        page_info = scored.get("pageInfo", {})
        edges = scored.get("edges", [])
        record_count = scored.get("record_count")

        if total_record_count is None and record_count:
            total_record_count = record_count
            log.info(f"Server reports {total_record_count} total investors")

        # Extract investor URLs
        new_this_page = 0
        for edge in edges:
            node = edge.get("node", {})
            person = node.get("person", {})
            slug = person.get("slug")
            if slug and slug not in all_investors:
                all_investors[slug] = f"https://signal.nfx.com/investors/{slug}"
                new_this_page += 1
                new_since_save += 1

        total = len(all_investors)
        has_next = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")

        log.info(
            f"  Page {page_num} | +{new_this_page} new ({len(edges)} fetched) | "
            f"total: {total} | hasNext: {has_next}"
        )

        # Save periodically
        if new_since_save >= SAVE_EVERY:
            save_urls(all_investors)
            new_since_save = 0
            log.info(f"  SAVED ({total} URLs)")

        # Check completion
        if total >= MIN_URLS:
            log.info(f"TARGET REACHED: {total} URLs")
            save_urls(all_investors)
            if not has_next:
                break
            # Keep going to get everything

        if not has_next:
            log.info("No more pages (hasNextPage=false)")
            save_urls(all_investors)
            break

        if not cursor:
            log.warning("No cursor returned — stopping")
            save_urls(all_investors)
            break

        # Small delay to be polite
        time.sleep(0.3)

    # Final save and report
    save_urls(all_investors)
    new_this_run = len(all_investors) - start_count

    log.info("")
    log.info("=" * 60)
    log.info("  URL COLLECTION COMPLETE")
    log.info(f"  Total URLs:     {len(all_investors)}")
    log.info(f"  New this run:   {new_this_run}")
    log.info(f"  Existing kept:  {start_count}")
    log.info(f"  Pages fetched:  {page_num}")
    if total_record_count:
        log.info(f"  Server total:   {total_record_count}")
    log.info(f"  Saved to:       {ALL_URLS_FILE}")
    log.info("=" * 60)


def main():
    log.info("=" * 60)
    log.info("  NFX SIGNAL - GRAPHQL URL COLLECTOR")
    log.info(f"  Target: {MIN_URLS}+ URLs")
    log.info(f"  Page size: {PAGE_SIZE}")
    log.info("=" * 60)

    try:
        collect()
    except KeyboardInterrupt:
        log.info("\nStopped by user.")
        existing = load_existing_urls()
        if existing:
            save_urls(existing)
            log.info(f"Emergency save: {len(existing)} URLs preserved.")
    except Exception as e:
        log.error(f"CRASH: {e}")
        import traceback
        log.error(traceback.format_exc())
        try:
            existing = load_existing_urls()
            if existing:
                save_urls(existing)
                log.info(f"Emergency save: {len(existing)} URLs preserved.")
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
