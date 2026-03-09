"""
NFX Signal Investor Scraper - Fast Edition
==========================================
Scrapes all ~3200 investors from signal.nfx.com investor lists.

Strategy:
1. Phase 1: Use Playwright to intercept the actual API endpoint
2. Phase 2: Hit the API directly with requests (100x faster than browser clicking)
3. Phase 3: Fallback to browser automation if API interception fails

Usage:
    pip install playwright beautifulsoup4 requests pandas
    playwright install chromium
    python nfx_scraper.py
"""

import asyncio
import json
import csv
import time
import re
import os
from datetime import datetime
from urllib.parse import urljoin

# ============================================================
# CONFIGURATION
# ============================================================
TARGET_URL = "https://signal.nfx.com/investor-lists/top-marketplaces-seed-investors"
OUTPUT_CSV = "nfx_investors_marketplaces_seed.csv"
OUTPUT_JSON = "nfx_investors_marketplaces_seed.json"
TOTAL_EXPECTED = 3194  # Number shown on the page
BATCH_PAUSE = 0.5      # Seconds between "Load More" clicks (increase if getting blocked)
MAX_RETRIES = 3        # Retries per load-more click

# ============================================================
# PHASE 1: INTERCEPT API CALLS
# ============================================================

async def discover_api(url: str) -> dict:
    """
    Open the page, click 'Load More' once, and intercept the 
    network request to discover the actual API endpoint & params.
    """
    from playwright.async_api import async_playwright

    api_info = {"endpoint": None, "headers": {}, "method": "GET", "body": None}
    captured_requests = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # Capture all XHR/fetch requests
        async def on_request(request):
            if request.resource_type in ("xhr", "fetch"):
                captured_requests.append({
                    "url": request.url,
                    "method": request.method,
                    "headers": dict(request.headers),
                    "post_data": request.post_data,
                })

        page.on("request", on_request)

        print("[*] Loading page...")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(2000)

        # Find and click "Load More"
        print("[*] Clicking 'Load More' to discover API...")
        load_more = page.locator("text=LOAD MORE INVESTORS").first
        if await load_more.is_visible():
            await load_more.click()
            await page.wait_for_timeout(3000)

            # Click again to get a second request for pattern matching
            if await load_more.is_visible():
                await load_more.click()
                await page.wait_for_timeout(3000)

        await browser.close()

    # Analyze captured requests for the pagination API
    print(f"\n[*] Captured {len(captured_requests)} XHR/fetch requests:")
    for req in captured_requests:
        print(f"    {req['method']} {req['url'][:120]}")
        if "investor" in req["url"].lower() or "page" in req["url"].lower() or "list" in req["url"].lower():
            api_info["endpoint"] = req["url"]
            api_info["headers"] = req["headers"]
            api_info["method"] = req["method"]
            api_info["body"] = req["post_data"]

    return api_info, captured_requests


# ============================================================
# PHASE 2: DIRECT API SCRAPING (if endpoint discovered)
# ============================================================

async def scrape_via_api(api_info: dict, all_requests: list) -> list:
    """
    If we discovered the API endpoint, hit it directly for all pages.
    This is 100x faster than browser-based clicking.
    """
    import requests as req_lib

    investors = []
    endpoint = api_info["endpoint"]
    if not endpoint:
        return investors

    print(f"\n[*] API endpoint found: {endpoint}")
    print("[*] Attempting direct API scraping...")

    headers = {k: v for k, v in api_info["headers"].items() 
               if k.lower() not in ("host", "content-length")}
    headers["Accept"] = "application/json, text/html, */*"

    # Try to figure out pagination pattern
    page_num = 1
    while len(investors) < TOTAL_EXPECTED:
        page_num += 1
        # Try common Rails pagination patterns
        for url_pattern in [
            f"{TARGET_URL}?page={page_num}",
            f"{TARGET_URL}/investors?page={page_num}",
            endpoint.replace("page=2", f"page={page_num}") if "page=" in endpoint else f"{endpoint}?page={page_num}",
        ]:
            try:
                resp = req_lib.get(url_pattern, headers=headers, timeout=15)
                if resp.status_code == 200 and len(resp.text) > 500:
                    # Parse investors from response
                    new_investors = parse_investor_html(resp.text)
                    if new_investors:
                        investors.extend(new_investors)
                        print(f"    Page {page_num}: +{len(new_investors)} investors (total: {len(investors)})")
                        break
            except Exception as e:
                continue

        time.sleep(BATCH_PAUSE)

        if page_num > 500:  # Safety valve
            break

    return investors


# ============================================================
# PHASE 3: BROWSER AUTOMATION (robust fallback)
# ============================================================

async def scrape_via_browser(url: str) -> list:
    """
    Full browser automation: load page, keep clicking 'Load More',
    extract all investor data from the DOM.
    
    Optimizations vs typical scrapers:
    - Blocks images/fonts/CSS to speed up loading
    - Uses efficient DOM extraction via JS
    - Implements smart waiting (not fixed delays)
    - Saves progress incrementally
    """
    from playwright.async_api import async_playwright

    investors = []
    seen_names = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
        )

        # Block unnecessary resources for speed
        await context.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,eot}", lambda route: route.abort())
        await context.route("**/facebook.com/**", lambda route: route.abort())
        await context.route("**/google-analytics.com/**", lambda route: route.abort())
        await context.route("**/googletagmanager.com/**", lambda route: route.abort())

        page = await context.new_page()

        print(f"\n[*] Loading {url} ...")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await page.wait_for_timeout(2000)

        # Extract initial investors
        new_batch = await extract_investors_from_page(page)
        for inv in new_batch:
            key = f"{inv['name']}_{inv['firm']}"
            if key not in seen_names:
                seen_names.add(key)
                investors.append(inv)

        print(f"[*] Initial load: {len(investors)} investors")

        # Keep clicking "Load More"
        click_count = 0
        stale_count = 0
        last_count = len(investors)

        while len(investors) < TOTAL_EXPECTED:
            click_count += 1

            # Find and click "Load More"
            try:
                load_more = page.locator("text=LOAD MORE INVESTORS").first
                if not await load_more.is_visible(timeout=5000):
                    print("[!] 'Load More' button not found. May have loaded all investors.")
                    break

                await load_more.scroll_into_view_if_needed()
                await load_more.click()

                # Smart wait: wait for new content to appear
                await page.wait_for_timeout(800)

                # Wait for network to settle
                try:
                    await page.wait_for_load_state("networkidle", timeout=10000)
                except:
                    await page.wait_for_timeout(2000)

            except Exception as e:
                print(f"[!] Click error: {e}")
                stale_count += 1
                if stale_count >= MAX_RETRIES:
                    print("[!] Too many retries, trying to scroll and retry...")
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(2000)
                    stale_count = 0
                continue

            # Extract all investors currently in DOM
            new_batch = await extract_investors_from_page(page)
            for inv in new_batch:
                key = f"{inv['name']}_{inv['firm']}"
                if key not in seen_names:
                    seen_names.add(key)
                    investors.append(inv)

            # Progress reporting
            new_count = len(investors) - last_count
            if new_count > 0:
                stale_count = 0
                last_count = len(investors)
                progress = (len(investors) / TOTAL_EXPECTED) * 100
                print(f"    Click #{click_count}: +{new_count} new | Total: {len(investors)}/{TOTAL_EXPECTED} ({progress:.1f}%)")

                # Save progress every 100 investors
                if len(investors) % 100 < 10:
                    save_progress(investors, "nfx_progress.json")
            else:
                stale_count += 1
                print(f"    Click #{click_count}: No new investors (stale: {stale_count}/{MAX_RETRIES})")

                if stale_count >= MAX_RETRIES:
                    # Try scrolling to bottom and waiting longer
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(3000)

                    new_batch = await extract_investors_from_page(page)
                    for inv in new_batch:
                        key = f"{inv['name']}_{inv['firm']}"
                        if key not in seen_names:
                            seen_names.add(key)
                            investors.append(inv)

                    if len(investors) == last_count:
                        print(f"\n[!] Stuck at {len(investors)} investors after multiple retries.")
                        print("[*] Saving what we have and stopping.")
                        break
                    else:
                        stale_count = 0
                        last_count = len(investors)

            # Small pause to avoid rate limiting
            await page.wait_for_timeout(int(BATCH_PAUSE * 1000))

        await browser.close()

    return investors


async def extract_investors_from_page(page) -> list:
    """
    Extract all investor data from the current DOM state using fast JS execution.
    This extracts everything in one JS call instead of multiple DOM queries.
    """
    investors = await page.evaluate("""
    () => {
        const investors = [];
        
        // Each investor card is in the list section
        // Look for investor name links that point to /investors/
        const cards = document.querySelectorAll('a[href^="/investors/"]');
        const processed = new Set();
        
        cards.forEach(link => {
            const name = link.textContent.trim();
            const profileUrl = link.getAttribute('href');
            
            if (!name || processed.has(profileUrl)) return;
            processed.add(profileUrl);
            
            // Navigate up to the card container
            let card = link.closest('div') || link.parentElement;
            // Go up a few levels to get the full card
            for (let i = 0; i < 8; i++) {
                if (card && card.parentElement) {
                    const text = card.textContent;
                    if (text.includes('Sweet spot') || text.includes('Range:')) {
                        break;
                    }
                    card = card.parentElement;
                }
            }
            
            if (!card) return;
            
            const cardText = card.textContent;
            
            // Extract firm info
            let firm = '';
            let firmUrl = '';
            let title = '';
            const firmLink = card.querySelector('a[href^="/firms/"]');
            if (firmLink) {
                firm = firmLink.textContent.trim();
                firmUrl = firmLink.getAttribute('href');
            }
            
            // Extract title (text after firm name, before Sweet spot)
            const titleMatch = cardText.match(/(?:·|•)\\s*([A-Za-z\\s]+?)\\s*(?:Sweet spot|Range)/);
            if (titleMatch) {
                title = titleMatch[1].trim();
            }
            
            // Extract sweet spot
            let sweetSpot = '';
            const sweetMatch = cardText.match(/Sweet spot:\\s*\\$([\\d.,]+[KMB]?)/i);
            if (sweetMatch) {
                sweetSpot = '$' + sweetMatch[1];
            }
            
            // Extract range
            let range = '';
            const rangeMatch = cardText.match(/Range:\\s*\\$([\\.\\d,]+[KMB]?)\\s*-\\s*\\$([\\.\\d,]+[KMB]?)/i);
            if (rangeMatch) {
                range = '$' + rangeMatch[1] + ' - $' + rangeMatch[2];
            }
            
            // Extract locations (text between "Investors in" for locations)
            let locations = [];
            const locationLinks = card.querySelectorAll('a[href*="investor-lists/top-"]');
            // First set of links before "Investors in [Sector]" are locations
            
            // Extract photo URL
            let photoUrl = '';
            const img = card.querySelector('img[src*="signal-api"]');
            if (img) {
                photoUrl = img.getAttribute('src');
            }
            
            investors.push({
                name: name,
                profile_url: 'https://signal.nfx.com' + profileUrl,
                firm: firm,
                firm_url: firmUrl ? 'https://signal.nfx.com' + firmUrl : '',
                title: title,
                sweet_spot: sweetSpot,
                range: range,
                photo_url: photoUrl,
                raw_text: cardText.substring(0, 500),
            });
        });
        
        return investors;
    }
    """)

    return investors


# ============================================================
# ENHANCED PARSER: Extract structured data from raw text
# ============================================================

def parse_investor_card(raw_text: str, investor: dict) -> dict:
    """Post-process raw card text to extract structured fields."""
    
    # Better title extraction
    if not investor.get("title"):
        patterns = [
            r"·\s*((?:General |Managing |Venture |Operating )?Partner)\b",
            r"·\s*((?:Co-)?Founder(?:\s*&\s*\w+)?)\b",
            r"·\s*(Principal|Director|Associate|Analyst|VP|CEO|CTO)\b",
        ]
        for pat in patterns:
            m = re.search(pat, raw_text)
            if m:
                investor["title"] = m.group(1).strip()
                break

    # Extract location cities
    loc_pattern = r"Investors in \[([^\]]+)\s*\(([^)]+)\)\]"
    locations = re.findall(loc_pattern, raw_text)
    if locations:
        investor["locations"] = "; ".join([f"{city} ({region})" for city, region in locations[:5]])

    # Extract investment sectors/categories
    sector_pattern = r"Investors in \[([^\]]+)\s*\((Seed|Pre-seed|Series [A-C])\)\]"
    sectors = re.findall(sector_pattern, raw_text)
    if sectors:
        unique_sectors = list(set([s[0] for s in sectors]))[:10]
        investor["sectors"] = "; ".join(unique_sectors)

    return investor


def parse_investor_html(html: str) -> list:
    """Parse investor data from HTML response (for direct API scraping)."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    investors = []

    for link in soup.find_all("a", href=re.compile(r"^/investors/")):
        name = link.get_text(strip=True)
        if not name:
            continue

        card = link.parent
        for _ in range(8):
            if card and card.parent:
                text = card.get_text()
                if "Sweet spot" in text or "Range:" in text:
                    break
                card = card.parent

        card_text = card.get_text(" ", strip=True) if card else ""

        inv = {
            "name": name,
            "profile_url": f"https://signal.nfx.com{link['href']}",
        }

        # Firm
        firm_link = card.find("a", href=re.compile(r"^/firms/")) if card else None
        if firm_link:
            inv["firm"] = firm_link.get_text(strip=True)
            inv["firm_url"] = f"https://signal.nfx.com{firm_link['href']}"

        # Sweet spot & range
        sweet = re.search(r"Sweet spot:\s*\$([\d.,]+[KMB]?)", card_text)
        if sweet:
            inv["sweet_spot"] = f"${sweet.group(1)}"

        range_m = re.search(r"Range:\s*\$([\d.,]+[KMB]?)\s*-\s*\$([\d.,]+[KMB]?)", card_text)
        if range_m:
            inv["range"] = f"${range_m.group(1)} - ${range_m.group(2)}"

        investors.append(inv)

    return investors


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def save_progress(investors: list, filename: str):
    """Save intermediate progress to JSON."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(investors, f, indent=2, ensure_ascii=False)


def save_csv(investors: list, filename: str):
    """Save investors to CSV."""
    if not investors:
        return

    # Determine all fields
    all_fields = set()
    for inv in investors:
        all_fields.update(inv.keys())

    # Remove raw_text from CSV output
    all_fields.discard("raw_text")
    fields = sorted(all_fields)

    # Ensure key fields come first
    priority = ["name", "firm", "title", "sweet_spot", "range", "locations", "sectors", "profile_url", "firm_url"]
    ordered_fields = [f for f in priority if f in fields] + [f for f in fields if f not in priority]

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ordered_fields, extrasaction="ignore")
        writer.writeheader()
        for inv in investors:
            # Clean up raw_text before writing
            clean_inv = {k: v for k, v in inv.items() if k != "raw_text"}
            writer.writerow(clean_inv)


def save_json(investors: list, filename: str):
    """Save investors to JSON (without raw_text)."""
    clean = [{k: v for k, v in inv.items() if k != "raw_text"} for inv in investors]
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(clean, f, indent=2, ensure_ascii=False)


# ============================================================
# MAIN
# ============================================================

async def main():
    print("=" * 60)
    print("  NFX Signal Investor Scraper - Fast Edition")
    print(f"  Target: {TOTAL_EXPECTED} investors")
    print(f"  URL: {TARGET_URL}")
    print("=" * 60)

    start_time = time.time()

    # Phase 1: Try to discover API
    print("\n--- PHASE 1: Discovering API endpoint ---")
    try:
        api_info, all_requests = await discover_api(TARGET_URL)
    except Exception as e:
        print(f"[!] API discovery failed: {e}")
        api_info = {"endpoint": None}
        all_requests = []

    investors = []

    # Phase 2: Try direct API if discovered
    if api_info.get("endpoint"):
        print("\n--- PHASE 2: Direct API scraping ---")
        investors = await scrape_via_api(api_info, all_requests)

    # Phase 3: Fall back to browser automation
    if len(investors) < 100:
        print("\n--- PHASE 3: Browser automation (robust fallback) ---")
        investors = await scrape_via_browser(TARGET_URL)

    # Post-process: enhance extracted data
    print(f"\n[*] Post-processing {len(investors)} investors...")
    for inv in investors:
        raw = inv.get("raw_text", "")
        if raw:
            inv = parse_investor_card(raw, inv)

    # Save outputs
    print(f"\n[*] Saving to {OUTPUT_CSV} and {OUTPUT_JSON}...")
    save_csv(investors, OUTPUT_CSV)
    save_json(investors, OUTPUT_JSON)

    elapsed = time.time() - start_time
    print(f"\n{'=' * 60}")
    print(f"  DONE!")
    print(f"  Total investors scraped: {len(investors)}")
    print(f"  Time taken: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    print(f"  Output: {OUTPUT_CSV} | {OUTPUT_JSON}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    asyncio.run(main())