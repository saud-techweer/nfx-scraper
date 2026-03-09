#!/usr/bin/env python3
"""
NFX Signal Scraper - SERVER EDITION
=====================================
Designed for headless Linux servers (Hostinger VPS, etc.)

Key differences from desktop version:
  - Headless only (no GUI needed)
  - Cookie-based auth (export from local browser, no manual login)
  - Single persistent browser - NEVER restarts (as requested)
  - Aggressive memory management via page.goto("about:blank") between profiles
  - Runs unattended in tmux/screen
  - Auto-saves progress for crash recovery

Setup:
    pip3 install playwright
    playwright install chromium --with-deps

Auth (one-time):
    1. On your LOCAL PC, log into signal.nfx.com in Chrome
    2. Run: python3 nfx_server_scraper.py --export-cookies
       (this opens a browser, you log in, it saves cookies.json)
    3. Upload cookies.json to your server alongside this script
    4. On server: python3 nfx_server_scraper.py --skip-phase1

Run on server:
    python3 nfx_server_scraper.py                    # full run
    python3 nfx_server_scraper.py --skip-phase1      # skip URL collection
    python3 nfx_server_scraper.py --retry-failed     # retry failures
    python3 nfx_server_scraper.py --skip-phase2      # just compile CSV
"""

import asyncio
import argparse
import json
import os
import sys
import logging
import csv
import signal
from datetime import datetime
from pathlib import Path

try:
    from playwright.async_api import async_playwright, TimeoutError as PWTimeout
except ImportError:
    print("\n❌ Install dependencies:\n")
    print("   pip3 install playwright")
    print("   playwright install chromium --with-deps\n")
    sys.exit(1)


# ─── CONFIG ──────────────────────────────────────────────────────────────────

LIST_URL = "https://signal.nfx.com/investor-lists/top-marketplaces-seed-investors"

DATA_DIR = "data"
PROFILES_DIR = os.path.join(DATA_DIR, "profiles")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
FAILED_FILE = os.path.join(DATA_DIR, "failed_profiles.json")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")
FINAL_CSV = os.path.join(DATA_DIR, "investors_final.csv")
FINAL_JSON = os.path.join(DATA_DIR, "investors_final.json")
COOKIES_FILE = "cookies.json"

# URL collection
MIN_URLS = 3100                  # DO NOT stop Phase 1 until we have this many
SAVE_URLS_EVERY = 5              # save to disk every N Load More clicks

# Timing
DELAY_BETWEEN_PROFILES = 1.5     # seconds between each profile scrape
DELAY_AFTER_LOAD_MORE = 3.5      # seconds after clicking Load More
PAGE_TIMEOUT = 25000              # ms, per page load timeout
MAX_RETRIES = 3                   # retries per profile before marking failed

# Memory management - instead of browser restart, we clear page state
MEMORY_CLEANUP_EVERY = 50        # clear cache every N profiles

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("nfx")


# ─── GRACEFUL SHUTDOWN ───────────────────────────────────────────────────────

_shutdown_requested = False

def handle_signal(signum, frame):
    global _shutdown_requested
    _shutdown_requested = True
    log.info("\n⏹  Shutdown signal received. Finishing current profile and saving...")

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


# ─── FILE I/O ────────────────────────────────────────────────────────────────

def ensure_dirs():
    os.makedirs(PROFILES_DIR, exist_ok=True)

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
                return set(json.load(f).get("scraped", []))
        except Exception:
            pass
    return set()

def save_progress(scraped: set):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"scraped": sorted(scraped), "count": len(scraped),
                    "updated": datetime.now().isoformat()}, f)

def load_failed():
    if os.path.exists(FAILED_FILE):
        try:
            with open(FAILED_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return []

def save_failed(failed: list):
    with open(FAILED_FILE, "w") as f:
        json.dump(failed, f, indent=2)

def save_profile(slug: str, data: dict):
    path = os.path.join(PROFILES_DIR, f"{slug}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_urls():
    if os.path.exists(ALL_URLS_FILE):
        with open(ALL_URLS_FILE) as f:
            return json.load(f)
    return []

def save_urls(urls: list):
    with open(ALL_URLS_FILE, "w") as f:
        json.dump(urls, f, indent=2)


# ─── COOKIE MANAGEMENT ──────────────────────────────────────────────────────

async def export_cookies():
    """Run on LOCAL machine with GUI to capture login cookies."""
    log.info("=" * 60)
    log.info("  COOKIE EXPORT MODE")
    log.info("  1. A browser will open")
    log.info("  2. Log into signal.nfx.com")
    log.info("  3. After login, come back here and press ENTER")
    log.info("=" * 60)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()
        await page.goto("https://signal.nfx.com/login", wait_until="domcontentloaded", timeout=60000)

        input("\n  >> Press ENTER after logging in... ")

        # Save cookies
        cookies = await context.cookies()
        with open(COOKIES_FILE, "w") as f:
            json.dump(cookies, f, indent=2)

        log.info(f"\n  ✓ {len(cookies)} cookies saved to {COOKIES_FILE}")
        log.info("  Upload this file to your server alongside the scraper script.")

        await browser.close()


async def load_cookies(context):
    """Load cookies into browser context for auth."""
    if not os.path.exists(COOKIES_FILE):
        log.error(f"No {COOKIES_FILE} found!")
        log.error("Run on your local machine first: python3 nfx_server_scraper.py --export-cookies")
        return False

    try:
        with open(COOKIES_FILE) as f:
            cookies = json.load(f)
        await context.add_cookies(cookies)
        log.info(f"  ✓ Loaded {len(cookies)} cookies from {COOKIES_FILE}")
        return True
    except Exception as e:
        log.error(f"Failed to load cookies: {e}")
        return False


# ─── BROWSER MANAGEMENT ─────────────────────────────────────────────────────

async def create_browser(playwright):
    """Launch a single headless browser - this instance runs for the ENTIRE scrape."""
    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-sync",
            "--no-first-run",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            # Server memory optimization
            "--js-flags=--max-old-space-size=512",
            "--single-process",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
        ],
    )
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1920, "height": 1080},
    )

    # Load auth cookies
    if not await load_cookies(context):
        await browser.close()
        return None, None, None

    page = await context.new_page()
    return browser, context, page


# ─── PROFILE SCRAPE JS ──────────────────────────────────────────────────────

SCRAPE_JS = r"""
() => {
    const data = {
        basicInfo: {},
        investingProfile: {},
        sectorRankings: [],
        investments: [],
        experience: [],
        socials: {},
        profilePicture: null,
        profileUrl: window.location.href,
        slug: (window.location.pathname.split('/investors/')[1] || '').split('?')[0],
    };

    // Name + Signal score
    const h1 = document.querySelector('h1.f3.f1-ns, h1');
    if (h1) {
        const t = h1.textContent.trim();
        const m = t.match(/^(.+?)\s*\((\d+)\)/);
        if (m) {
            data.basicInfo.name = m[1].trim();
            data.basicInfo.signalScore = parseInt(m[2]);
        } else {
            data.basicInfo.name = t;
        }
    }

    // Investor types
    const typeDiv = document.querySelector('.subheader.white-subheader.b');
    if (typeDiv) {
        const types = [];
        typeDiv.querySelectorAll('span').forEach(s => {
            const t = s.textContent.trim();
            if (t && t.length > 0 && t !== '·') types.push(t);
        });
        data.basicInfo.investorTypes = types;
    }

    // Position and firm
    const ib = document.querySelector('.identity-block');
    if (ib) {
        ib.querySelectorAll('.subheader.lower-subheader').forEach(div => {
            if (div.querySelector('a') || div.querySelector('.glyphicon')) return;
            const t = div.textContent.trim();
            if (t && !t.includes('http')) data.basicInfo.positionAndFirm = t;
        });
    }

    // Website
    const wl = document.querySelector('a.subheader.lower-subheader[href]');
    if (wl) data.basicInfo.website = wl.href;

    // Location
    const mapMarker = document.querySelector('.glyphicon-map-marker');
    if (mapMarker && mapMarker.nextElementSibling) {
        data.basicInfo.location = mapMarker.nextElementSibling.textContent.trim();
    } else {
        document.querySelectorAll('.subheader.lower-subheader span').forEach(s => {
            const t = s.textContent.trim();
            if (/^[A-Z][a-z]+.*,\s*[A-Z]/.test(t) && !t.includes('http')) {
                data.basicInfo.location = t;
            }
        });
    }

    // Profile picture
    const col1 = document.querySelector('.col-sm-3.col-xs-12, .col-sm-6.col-xs-12:first-child');
    if (col1) {
        const img = col1.querySelector('img');
        if (img && img.src) data.profilePicture = img.src;
    }
    if (!data.profilePicture) {
        const imgs = document.querySelectorAll('img[src*="active_storage"], img[src*="cloudinary"], img[src*="profile"], img[src*="avatar"]');
        if (imgs.length > 0) data.profilePicture = imgs[0].src;
    }
    if (!data.profilePicture) {
        document.querySelectorAll('[style*="background-image"]').forEach(c => {
            const m = c.getAttribute('style').match(/url\(['"]?([^'")\s]+)['"]?\)/);
            if (m) data.profilePicture = m[1];
        });
    }

    // Investing profile fields
    document.querySelectorAll('.line-separated-row.row').forEach(row => {
        const label = row.querySelector('.section-label, .col-xs-5 span');
        const valueEl = row.querySelector('.col-xs-7 span, .col-xs-7');
        if (!label || !valueEl) return;

        const lt = label.textContent.trim().toLowerCase();
        const link = valueEl.querySelector('a');
        let val;

        if (link) {
            val = {
                text: link.textContent.trim(),
                url: link.href,
                extra: valueEl.textContent.replace(link.textContent, '').trim().replace(/[·•]/g, '').trim()
            };
        } else {
            val = valueEl.textContent.trim();
        }

        if (lt.includes('current investing position')) data.investingProfile.currentPosition = val;
        else if (lt.includes('investment range')) data.investingProfile.investmentRange = val;
        else if (lt.includes('sweet spot')) data.investingProfile.sweetSpot = val;
        else if (lt.includes('investments on record')) data.investingProfile.investmentsOnRecord = val;
        else if (lt.includes('fund size')) data.investingProfile.fundSize = val;
    });

    // Sector rankings
    document.querySelectorAll('a.vc-list-chip').forEach(chip => {
        data.sectorRankings.push({ name: chip.textContent.trim(), url: chip.href });
    });

    // Past investments
    let curInv = null;
    document.querySelectorAll('.past-investments-table-body tr').forEach(row => {
        const isCoInvestor = row.querySelector('.coinvestors-row, td[colspan]');
        if (isCoInvestor) {
            if (curInv) {
                const m = row.textContent.trim().match(/Co-investors:\s*(.+)/i);
                if (m) curInv.coInvestors = m[1].split(',').map(s => s.trim()).filter(s => s);
            }
        } else {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 2) {
                let stage = null, date = null, roundSize = null;
                const sc = cells[1];
                if (sc) {
                    const inner = sc.querySelector('.round-padding') || sc;
                    const clone = inner.cloneNode(true);
                    clone.querySelectorAll('i').forEach(s => s.replaceWith(' ||| '));
                    const parts = clone.textContent.split('|||').map(p => p.trim()).filter(Boolean);
                    if (parts.length >= 1) stage = parts[0];
                    if (parts.length >= 2) date = parts[1];
                    if (parts.length >= 3) roundSize = parts[2];
                }
                curInv = {
                    company: cells[0]?.textContent?.trim() || null,
                    stage, date, roundSize,
                    totalRaised: cells[2]?.textContent?.trim() || null,
                    coInvestors: []
                };
                data.investments.push(curInv);
            }
        }
    });

    // Experience
    const expLabel = Array.from(document.querySelectorAll('.section-label'))
        .find(e => e.textContent.includes('Experience'));
    if (expLabel) {
        const sec = expLabel.closest('.sn-margin-top-30');
        if (sec) {
            sec.querySelectorAll('.line-separated-row.flex').forEach(row => {
                const mainSpan = row.querySelector('span:first-child');
                const dateSpan = row.querySelector('span[style*="text-align"]');
                if (mainSpan) {
                    const ft = mainSpan.textContent.trim();
                    const dt = dateSpan?.textContent?.trim() || null;
                    const parts = ft.split(/\s*[·•\u00B7\u2022]\s*|\s{2,}/);
                    if (parts.length >= 2) {
                        data.experience.push({ position: parts[0]?.trim(), company: parts[1]?.trim(), dates: dt });
                    } else {
                        data.experience.push({ title: ft, dates: dt });
                    }
                }
            });
        }
    }

    // Social links
    const linkSet = document.querySelector('.sn-linkset');
    if (linkSet) {
        linkSet.querySelectorAll('a.iconlink, a[href]').forEach(link => {
            const h = link.href;
            if (h.includes('linkedin.com')) data.socials.linkedin = h;
            else if (h.includes('twitter.com') || h.includes('x.com')) data.socials.twitter = h;
            else if (h.includes('angel.co') || h.includes('angellist')) data.socials.angellist = h;
            else if (h.includes('crunchbase.com')) data.socials.crunchbase = h;
        });
    }
    if (!data.socials.linkedin) {
        document.querySelectorAll('a[href*="linkedin.com/in/"]').forEach(a => { data.socials.linkedin = a.href; });
    }
    if (!data.socials.twitter) {
        document.querySelectorAll('a[href*="twitter.com/"], a[href*="x.com/"]').forEach(a => { data.socials.twitter = a.href; });
    }

    return data;
}
"""


# ─── PHASE 1: COLLECT ALL INVESTOR URLs ─────────────────────────────────────

async def phase1_collect_urls(page):
    """
    Click Load More until we have MIN_URLS (3100+) investor URLs.
    NEVER reloads the page. NEVER stops early. Waits as long as needed.
    """
    log.info("=" * 60)
    log.info("  PHASE 1: Collecting ALL investor URLs")
    log.info(f"  Target: {MIN_URLS}+ URLs")
    log.info("=" * 60)

    # Load any URLs already saved from previous runs
    existing = load_urls()
    all_urls = {item["slug"]: item["url"] for item in existing}
    log.info(f"  Loaded {len(all_urls)} existing URLs from disk")

    if len(all_urls) >= MIN_URLS:
        log.info(f"  Already have {len(all_urls)} URLs (>= {MIN_URLS}). Skipping Phase 1.")
        return existing

    await page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
    await page.wait_for_timeout(5000)

    # Verify we're logged in
    try:
        await page.wait_for_selector('a[href*="/investors/"]', timeout=15000)
    except PWTimeout:
        if "login" in page.url.lower():
            log.error("NOT LOGGED IN! Cookies may have expired.")
            log.error("Re-export cookies: python3 nfx_server_scraper.py --export-cookies")
            return []
        log.error("No investor links found on page!")
        return []

    clicks = 0
    no_button_streak = 0

    while True:
        if _shutdown_requested:
            break

        # Collect visible links
        try:
            new_links = await page.evaluate("""
                () => {
                    const results = {};
                    document.querySelectorAll('a[href*="/investors/"]').forEach(a => {
                        const href = a.href;
                        const slug = href.split('/investors/').pop().split('?')[0].split('#')[0];
                        if (slug && slug !== '' && slug !== 'edit' && !slug.startsWith('http')) {
                            results[slug] = href;
                        }
                    });
                    return results;
                }
            """)
            if new_links:
                all_urls.update(new_links)
        except Exception as e:
            log.warning(f"  Error collecting links: {e}")

        total = len(all_urls)

        # === HIT TARGET ===
        if total >= MIN_URLS:
            investor_list = [{"slug": s, "url": u} for s, u in all_urls.items()]
            save_urls(investor_list)
            log.info(f"  HIT TARGET! {total} URLs (>= {MIN_URLS}). Final sweep...")

            # Few more clicks to squeeze out remaining
            for _ in range(15):
                clicked = await _click_load_more(page)
                if not clicked:
                    await page.wait_for_timeout(5000)
                    continue
                await page.wait_for_timeout(int(DELAY_AFTER_LOAD_MORE * 1000))
                try:
                    links = await page.evaluate("""
                        () => {
                            const r = {};
                            document.querySelectorAll('a[href*="/investors/"]').forEach(a => {
                                const slug = a.href.split('/investors/').pop().split('?')[0].split('#')[0];
                                if (slug && slug !== '' && slug !== 'edit' && !slug.startsWith('http')) r[slug] = a.href;
                            });
                            return r;
                        }
                    """)
                    if links:
                        all_urls.update(links)
                except Exception:
                    pass

            investor_list = [{"slug": s, "url": u} for s, u in all_urls.items()]
            save_urls(investor_list)
            log.info(f"  Phase 1 DONE: {len(investor_list)} URLs saved")
            log.info(f"  Clicks: {clicks}")
            return investor_list

        # === CLICK LOAD MORE ===
        clicked = await _click_load_more(page)

        if clicked:
            no_button_streak = 0
            clicks += 1
            await page.wait_for_timeout(int(DELAY_AFTER_LOAD_MORE * 1000))

            if clicks % SAVE_URLS_EVERY == 0:
                investor_list = [{"slug": s, "url": u} for s, u in all_urls.items()]
                save_urls(investor_list)
                log.info(f"  Click #{clicks} | {total} URLs | saved")
            elif clicks % 10 == 0:
                log.info(f"  Click #{clicks} | {total} URLs")
        else:
            # Button not found. NEVER reload. Just wait and retry.
            no_button_streak += 1

            if no_button_streak <= 3:
                await page.wait_for_timeout(5000)
            elif no_button_streak <= 10:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(8000)
            else:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(15000)

            if no_button_streak % 10 == 0:
                investor_list = [{"slug": s, "url": u} for s, u in all_urls.items()]
                save_urls(investor_list)
                log.warning(
                    f"  No button for {no_button_streak} rounds. "
                    f"Have {total}/{MIN_URLS}. Waiting..."
                )

    # Shutdown requested — save what we have
    investor_list = [{"slug": s, "url": u} for s, u in all_urls.items()]
    save_urls(investor_list)
    log.info(f"  Saved {len(investor_list)} URLs before shutdown")
    log.info(f"  Clicks: {clicks}")
    return investor_list


async def _click_load_more(page):
    """Try to find and click a Load More button. Returns True if clicked."""
    try:
        buttons = await page.query_selector_all("button")
        for btn in buttons:
            try:
                text = await btn.inner_text()
                if "LOAD MORE" in text.upper():
                    await btn.scroll_into_view_if_needed()
                    await btn.click()
                    return True
            except Exception:
                continue
    except Exception:
        pass
    return False


# ─── PHASE 2: SCRAPE EACH PROFILE ───────────────────────────────────────────

async def scrape_one_profile(page, slug: str, url: str) -> dict | None:
    """Scrape a single investor profile using the SAME page (navigate in place)."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
    except PWTimeout:
        log.warning(f"    Timeout loading {slug}, trying anyway...")
    except Exception as e:
        log.warning(f"    Navigation error for {slug}: {str(e)[:80]}")
        return None

    try:
        await page.wait_for_selector("h1", timeout=10000)
    except PWTimeout:
        log.warning(f"    No h1 found for {slug}")

    # Wait for investing profile section
    try:
        await page.wait_for_selector(".line-separated-row", timeout=8000)
    except PWTimeout:
        pass  # some profiles may not have this

    await page.wait_for_timeout(1000)

    try:
        data = await page.evaluate(SCRAPE_JS)
        data["scraped_at"] = datetime.now().isoformat()
        data["slug"] = slug
        return data
    except Exception as e:
        log.warning(f"    JS extraction failed for {slug}: {str(e)[:80]}")
        return None


async def phase2_scrape(browser, context, page, investor_list, scraped_set, failed_list):
    to_scrape = [inv for inv in investor_list if inv["slug"] not in scraped_set]

    log.info("=" * 60)
    log.info(f"  PHASE 2: Scraping {len(to_scrape)} profiles")
    log.info(f"  Already done: {len(scraped_set)}")
    log.info(f"  Memory cleanup every: {MEMORY_CLEANUP_EVERY} profiles")
    log.info(f"  Browser restarts: NEVER (single instance)")
    log.info("=" * 60)

    if not to_scrape:
        log.info("  Nothing to scrape — all done!")
        return

    session_ok = 0
    session_fail = 0
    profiles_since_cleanup = 0

    for i, inv in enumerate(to_scrape):
        if _shutdown_requested:
            log.info("  Shutdown requested. Saving and exiting...")
            break

        slug = inv["slug"]
        url = inv["url"]
        remaining = len(to_scrape) - i - 1
        total_done = len(scraped_set)

        # ── Memory cleanup (without restarting browser) ──
        if profiles_since_cleanup >= MEMORY_CLEANUP_EVERY:
            try:
                # Navigate to blank page to release DOM memory
                await page.goto("about:blank", timeout=5000)
                await page.wait_for_timeout(500)
                # Run garbage collection hint
                await page.evaluate("() => { if (window.gc) window.gc(); }")
                profiles_since_cleanup = 0
                log.info(f"  🧹 Memory cleanup done (after {MEMORY_CLEANUP_EVERY} profiles)")
            except Exception:
                pass

        # ── Scrape with retry ──
        data = None
        for attempt in range(1, MAX_RETRIES + 1):
            data = await scrape_one_profile(page, slug, url)
            if data and data.get("basicInfo", {}).get("name"):
                break
            if attempt < MAX_RETRIES:
                wait = attempt * 2
                log.info(f"    Retry {attempt}/{MAX_RETRIES} for {slug} in {wait}s...")
                await asyncio.sleep(wait)
                # If page is dead, get a new one from same context
                try:
                    await page.goto("about:blank", timeout=5000)
                except Exception:
                    try:
                        await page.close()
                    except Exception:
                        pass
                    page = await context.new_page()

        if data and data.get("basicInfo", {}).get("name"):
            save_profile(slug, data)
            scraped_set.add(slug)
            session_ok += 1
            name = data["basicInfo"]["name"]
            log.info(f"  ✓ [{total_done+1}] {name} ({slug}) | {remaining} left")
        else:
            session_fail += 1
            failed_list.append({
                "slug": slug, "url": url,
                "error": "No data after retries",
                "timestamp": datetime.now().isoformat(),
            })
            log.warning(f"  ✗ [{total_done+1}] FAILED {slug} | {remaining} left")

        profiles_since_cleanup += 1

        # ── Save progress every 10 profiles ──
        if (session_ok + session_fail) % 10 == 0:
            save_progress(scraped_set)
            save_failed(failed_list)

        # ── Rate limiting ──
        await asyncio.sleep(DELAY_BETWEEN_PROFILES)

    # Final save
    save_progress(scraped_set)
    save_failed(failed_list)

    log.info("")
    log.info("=" * 60)
    log.info(f"  PHASE 2 COMPLETE")
    log.info(f"  Total scraped: {len(scraped_set)}")
    log.info(f"  This session:  ✓ {session_ok}  ✗ {session_fail}")
    log.info(f"  Profiles dir:  {PROFILES_DIR}/")
    log.info("=" * 60)

    return page  # return page in case it was replaced


# ─── PHASE 3: COMPILE INTO CSV + JSON ───────────────────────────────────────

def phase3_compile():
    log.info("")
    log.info("=" * 60)
    log.info("  PHASE 3: Compiling final output")
    log.info("=" * 60)

    profiles = []
    for f in sorted(Path(PROFILES_DIR).glob("*.json")):
        try:
            with open(f) as fh:
                data = json.load(fh)

            bi = data.get("basicInfo", {})
            ip = data.get("investingProfile", {})
            soc = data.get("socials", {})

            cp = ip.get("currentPosition", "")
            if isinstance(cp, dict):
                position_text = cp.get("text", "")
                position_extra = cp.get("extra", "")
                cp = f"{position_extra} @ {position_text}".strip(" @")

            profiles.append({
                "name": bi.get("name", ""),
                "slug": data.get("slug", ""),
                "signal_score": bi.get("signalScore", ""),
                "investor_types": ", ".join(bi.get("investorTypes", [])),
                "position_and_firm": bi.get("positionAndFirm", "") or cp,
                "location": bi.get("location", ""),
                "website": bi.get("website", ""),
                "sweet_spot": ip.get("sweetSpot", ""),
                "investment_range": ip.get("investmentRange", ""),
                "fund_size": ip.get("fundSize", ""),
                "investments_on_record": ip.get("investmentsOnRecord", ""),
                "sectors": "; ".join([s["name"] for s in data.get("sectorRankings", [])[:15]]),
                "num_investments": len(data.get("investments", [])),
                "linkedin": soc.get("linkedin", ""),
                "twitter": soc.get("twitter", ""),
                "crunchbase": soc.get("crunchbase", ""),
                "angellist": soc.get("angellist", ""),
                "profile_url": data.get("profileUrl", ""),
                "profile_picture": data.get("profilePicture", ""),
                "scraped_at": data.get("scraped_at", ""),
            })
        except Exception as e:
            log.warning(f"  Error reading {f.name}: {e}")

    if not profiles:
        log.warning("  No profiles found to compile!")
        return

    with open(FINAL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=profiles[0].keys())
        writer.writeheader()
        writer.writerows(profiles)

    with open(FINAL_JSON, "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2, ensure_ascii=False)

    log.info(f"  ✓ {len(profiles)} profiles compiled")
    log.info(f"  CSV:  {FINAL_CSV}")
    log.info(f"  JSON: {FINAL_JSON}")
    log.info("=" * 60)


# ─── MAIN ────────────────────────────────────────────────────────────────────

async def main():
    global DELAY_BETWEEN_PROFILES, LIST_URL

    parser = argparse.ArgumentParser(description="NFX Signal Scraper - Server Edition")
    parser.add_argument("--url", default=LIST_URL, help="Investor list URL")
    parser.add_argument("--skip-phase1", action="store_true", help="Skip URL collection")
    parser.add_argument("--skip-phase2", action="store_true", help="Skip scraping (just compile)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed profiles only")
    parser.add_argument("--export-cookies", action="store_true", help="Export cookies (run on LOCAL machine)")
    parser.add_argument("--delay", type=float, default=DELAY_BETWEEN_PROFILES,
                        help=f"Delay between profiles (default: {DELAY_BETWEEN_PROFILES})")
    args = parser.parse_args()

    # Cookie export mode (run on local machine with GUI)
    if args.export_cookies:
        await export_cookies()
        return

    DELAY_BETWEEN_PROFILES = args.delay
    LIST_URL = args.url

    ensure_dirs()

    log.info("=" * 60)
    log.info("  NFX SIGNAL SCRAPER - SERVER EDITION")
    log.info("=" * 60)
    log.info(f"  URL:     {LIST_URL}")
    log.info(f"  Mode:    Headless (server)")
    log.info(f"  Delay:   {DELAY_BETWEEN_PROFILES}s between profiles")
    log.info(f"  Browser: Single instance, never restarts")
    log.info("=" * 60)

    scraped_set = load_progress()
    failed_list = load_failed()
    log.info(f"  Resume: {len(scraped_set)} already scraped, {len(failed_list)} previously failed")

    if not args.skip_phase2:
        async with async_playwright() as pw:
            # Launch ONE browser for the entire session
            browser, context, page = await create_browser(pw)
            if browser is None:
                return

            # ── PHASE 1 ──
            if args.skip_phase1:
                investor_list = load_urls()
                if not investor_list:
                    log.error("No URLs file found! Run without --skip-phase1 first.")
                    await browser.close()
                    return
                log.info(f"  Loaded {len(investor_list)} URLs from file (Phase 1 skipped)")
            else:
                investor_list = await phase1_collect_urls(page)
                if not investor_list:
                    log.error("No investors found! Cookies may have expired.")
                    await browser.close()
                    return

            # ── RETRY FAILED ONLY ──
            if args.retry_failed:
                failed_slugs = {f["slug"] for f in failed_list}
                investor_list = [inv for inv in investor_list if inv["slug"] in failed_slugs]
                scraped_set -= failed_slugs
                failed_list = []
                log.info(f"  Retrying {len(investor_list)} failed profiles")

            # ── PHASE 2 ──
            page = await phase2_scrape(browser, context, page, investor_list, scraped_set, failed_list)

            # Close browser
            try:
                await browser.close()
            except Exception:
                pass

    # ── PHASE 3: COMPILE ──
    phase3_compile()

    log.info("")
    log.info("🎉 ALL DONE!")
    log.info(f"   Profiles: {PROFILES_DIR}/")
    log.info(f"   CSV:      {FINAL_CSV}")
    log.info(f"   JSON:     {FINAL_JSON}")
    log.info(f"   Failed:   {FAILED_FILE}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\n⏹ Stopped by user. Progress saved. Run again to resume.")
    except Exception as e:
        log.error(f"\n💥 Fatal error: {e}")
        import traceback
        traceback.print_exc()
        log.info("Progress saved. Run again to resume.")
