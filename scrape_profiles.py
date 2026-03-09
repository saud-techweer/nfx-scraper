#!/usr/bin/env python3
"""
NFX Signal - Profile Scraper (Headless)
========================================
Scrapes full profile data for every investor URL in data/all_investor_urls.json.
Uses the battle-tested SCRAPE_JS from the original nfx_scraper.py.
Runs headless, no login needed, batches of 4 concurrent pages.
Saves progress after every batch, retries failures at the end.
Auto-restarts browser when blocked by Cloudflare/rate-limiting.
"""

import asyncio
import json
import os
import sys
import logging
import traceback
import random
from datetime import datetime

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# =============================================================================
# CONFIG
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(SCRIPT_DIR, "data")
PROFILES_DIR = os.path.join(DATA_DIR, "profiles")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
FAILED_FILE = os.path.join(DATA_DIR, "failed_profiles.json")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "scraper_profiles.log")

BATCH_SIZE = 4
PAGE_LOAD_TIMEOUT = 30000   # 30s for page.goto
H1_TIMEOUT = 15000           # 15s for h1 to appear
CONTENT_TIMEOUT = 10000      # 10s for .line-separated-row
EXTRA_WAIT = 2000            # 2s buffer for lazy content
BATCH_PAUSE = 5.0            # seconds between batches (avoid rate-limit)

# Retry pass settings
RETRY_PAGE_TIMEOUT = 45000
RETRY_H1_TIMEOUT = 20000
RETRY_CONTENT_TIMEOUT = 15000
RETRY_EXTRA_WAIT = 4000

# Browser restart threshold
MAX_CONSECUTIVE_FAILURES = 3
BROWSER_RESTART_COOLDOWN = 120  # seconds
PREVENTIVE_RESTART_BATCHES = 80

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
# SCRAPE_JS — exact copy from playwright claude/nfx_scraper.py (IIFE)
# =============================================================================
SCRAPE_JS = r"""
() => {
    const data = {
        basicInfo: {},
        investingProfile: {},
        sectorRankings: [],
        investments: [],
        experience: [],
        socials: {},
        profilePicture: null
    };

    // === BASIC INFO ===
    const h1 = document.querySelector('h1.f3.f1-ns, h1');
    if (h1) {
        const t = h1.textContent.trim();
        const m = t.match(/^(.+?)\s*\(\d+\)/);
        data.basicInfo.name = m ? m[1].trim() : t;
        const n = t.match(/\((\d+)\)/);
        if (n) data.basicInfo.signalScore = parseInt(n[1]);
    }

    const td = document.querySelector('.subheader.white-subheader.b');
    if (td) {
        const types = [];
        td.querySelectorAll('span').forEach(s => {
            const t = s.textContent.trim();
            if (t && !t.includes('middot') && t.length > 0) types.push(t);
        });
        data.basicInfo.investorTypes = types.filter(t => t.length > 0);
    }

    const ib = document.querySelector('.identity-block');
    if (ib) {
        ib.querySelectorAll('.subheader.lower-subheader').forEach(div => {
            if (div.querySelector('a') || div.querySelector('.glyphicon')) return;
            const t = div.textContent.trim();
            if (t && t.includes(',') && !t.includes('http')) data.basicInfo.positionAndFirm = t;
        });
    }

    const wl = document.querySelector('a.subheader.lower-subheader[href]');
    if (wl) data.basicInfo.website = wl.href;

    const ls = document.querySelector('.glyphicon-map-marker');
    if (ls && ls.nextElementSibling) {
        data.basicInfo.location = ls.nextElementSibling.textContent.trim();
    } else {
        document.querySelectorAll('.subheader.lower-subheader span').forEach(s => {
            const t = s.textContent.trim();
            if (/^[A-Z][a-z]+,\s*[A-Z][a-z]+/.test(t)) data.basicInfo.location = t;
        });
    }

    // === PROFILE PICTURE ===
    const c1 = document.querySelector('.col-sm-6.col-xs-12:first-child, main > div > div > div:first-child');
    if (c1) { const img = c1.querySelector('img'); if (img && img.src) data.profilePicture = img.src; }
    if (!data.profilePicture) {
        document.querySelectorAll('[style*="background-image"]').forEach(c => {
            const m = c.getAttribute('style').match(/url\(['"]?([^'")\s]+)['"]?\)/);
            if (m) data.profilePicture = m[1];
        });
    }
    if (!data.profilePicture) {
        const imgs = document.querySelectorAll('img[src*="cloudinary"], img[src*="profile"], img[src*="avatar"]');
        if (imgs.length > 0) data.profilePicture = imgs[0].src;
    }

    // === INVESTING PROFILE ===
    document.querySelectorAll('.line-separated-row.row').forEach(row => {
        const label = row.querySelector('.section-label, .col-xs-5 span');
        const value = row.querySelector('.col-xs-7 span, .col-xs-7');
        if (label && value) {
            const lt = label.textContent.trim().toLowerCase();
            let vt = value.textContent.trim();
            if (value.querySelector('a')) {
                const link = value.querySelector('a');
                vt = { firm: link.textContent.trim(), firmUrl: link.href, position: value.textContent.replace(link.textContent, '').trim().replace(/[·•]/g, '').trim() };
            }
            if (lt.includes('current investing position')) data.investingProfile.currentPosition = vt;
            else if (lt.includes('investment range')) data.investingProfile.investmentRange = vt;
            else if (lt.includes('sweet spot')) data.investingProfile.sweetSpot = vt;
            else if (lt.includes('investments on record')) data.investingProfile.investmentsOnRecord = parseInt(vt) || vt;
            else if (lt.includes('fund size')) data.investingProfile.fundSize = vt;
        }
    });

    // === SECTOR RANKINGS ===
    document.querySelectorAll('a.vc-list-chip').forEach(chip => {
        data.sectorRankings.push({ name: chip.textContent.trim(), url: chip.href });
    });

    // === INVESTMENTS ===
    let curInv = null;
    document.querySelectorAll('.past-investments-table-body tr').forEach(row => {
        const cc = row.querySelector('.coinvestors-row, td[colspan]');
        if (cc) {
            if (curInv) {
                const m = row.textContent.trim().match(/Co-investors:\s*(.+)/i);
                if (m) curInv.coInvestors = m[1].split(',').map(s => s.trim());
            }
        } else {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 2) {
                let stage=null, date=null, roundSize=null;
                const sc = cells[1];
                if (sc) {
                    const inner = sc.querySelector('.round-padding') || sc;
                    const clone = inner.cloneNode(true);
                    clone.querySelectorAll('i').forEach(s => s.replaceWith(' ||| '));
                    const parts = clone.textContent.split('|||').map(p => p.trim()).filter(p => p);
                    if (parts.length >= 1) stage = parts[0];
                    if (parts.length >= 2) date = parts[1];
                    if (parts.length >= 3) roundSize = parts[2];
                }
                curInv = { company: cells[0]?.textContent?.trim()||null, stage, date, roundSize, totalRaised: cells[2]?.textContent?.trim()||null, coInvestors: [] };
                data.investments.push(curInv);
            }
        }
    });

    // === EXPERIENCE ===
    const el = Array.from(document.querySelectorAll('.section-label')).find(e => e.textContent.includes('Experience'));
    if (el) {
        const sec = el.closest('.sn-margin-top-30');
        if (sec) {
            sec.querySelectorAll('.line-separated-row.flex').forEach(row => {
                const ms = row.querySelector('span:first-child');
                const ds = row.querySelector('span[style*="text-align"]');
                if (ms) {
                    const ft = ms.textContent.trim(), dt = ds?.textContent?.trim()||null;
                    const parts = ft.split(/\s*[·•\u00B7\u2022]\s*|\s{2,}/);
                    if (parts.length >= 2) data.experience.push({ position: parts[0]?.trim(), company: parts[1]?.trim(), dates: dt });
                    else data.experience.push({ title: ft, dates: dt });
                }
            });
        }
    }

    // === SOCIAL LINKS ===
    const slc = document.querySelector('.sn-linkset');
    if (slc) {
        slc.querySelectorAll('a.iconlink').forEach(link => {
            const href = link.href, ic = link.querySelector('i')?.className || '';
            if (href.includes('linkedin.com')) data.socials.linkedin = href;
            else if (href.includes('twitter.com') || href.includes('x.com')) data.socials.twitter = href;
            else if (href.includes('angel.co') || href.includes('angellist')) data.socials.angellist = href;
            else if (href.includes('crunchbase.com')) data.socials.crunchbase = href;
            else if (ic.includes('globe') || (!href.includes('linkedin') && !href.includes('twitter') && !href.includes('angel') && !href.includes('crunchbase'))) data.socials.website = href;
        });
    }
    if (Object.keys(data.socials).length === 0) {
        document.querySelectorAll('a[href]').forEach(link => {
            const h = link.href;
            if (h.includes('linkedin.com/in/')) data.socials.linkedin = h;
            if (h.includes('twitter.com/') || h.includes('x.com/')) data.socials.twitter = h;
            if (h.includes('angel.co/')) data.socials.angellist = h;
            if (h.includes('crunchbase.com/person/')) data.socials.crunchbase = h;
        });
    }

    data.profileUrl = window.location.href;
    data.slug = window.location.pathname.split('/investors/')[1] || null;
    return data;
}
"""


# =============================================================================
# FILE I/O
# =============================================================================
def load_progress() -> set:
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("scraped", []))
        except Exception:
            pass
    return set()


def save_progress(scraped_set: set):
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"scraped": sorted(scraped_set), "total_scraped": len(scraped_set)}, f)
    os.replace(tmp, PROGRESS_FILE)


def load_failed() -> dict:
    if os.path.exists(FAILED_FILE):
        try:
            with open(FAILED_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"failed": []}


def save_failed(failed: dict):
    tmp = FAILED_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(failed, f, indent=2)
    os.replace(tmp, FAILED_FILE)


def save_profile(slug: str, data: dict) -> bool:
    filepath = os.path.join(PROFILES_DIR, f"{slug}.json")
    try:
        tmp = filepath + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, filepath)
        return True
    except Exception as e:
        log.error(f"Error saving {slug}: {e}")
        return False


def profile_exists(slug: str) -> bool:
    return os.path.exists(os.path.join(PROFILES_DIR, f"{slug}.json"))


def is_garbage_name(name):
    """Check if extracted name is an error page, not a real person."""
    name_lower = name.strip().lower() if name else ""
    return (
        not name_lower
        or len(name_lower) < 2
        or "signal.nfx.com" in name_lower
        or "nfx signal" in name_lower
        or "bad gateway" in name_lower
        or "bad request" in name_lower
        or "error code" in name_lower
        or "not found" in name_lower
        or "forbidden" in name_lower
        or "server error" in name_lower
        or "service unavailable" in name_lower
        or "access denied" in name_lower
        or name_lower.startswith("400")
        or name_lower.startswith("401")
        or name_lower.startswith("403")
        or name_lower.startswith("404")
        or name_lower.startswith("500")
        or name_lower.startswith("502")
        or name_lower.startswith("503")
    )


# =============================================================================
# BROWSER MANAGEMENT
# =============================================================================
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

BLOCKED_DOMAINS = [
    "facebook.com", "facebook.net", "google-analytics.com",
    "googletagmanager.com", "nr-data.net", "mixpanel.com",
    "intercom.io", "ads-twitter.com",
]


async def create_browser_context(p):
    """Create a fresh browser + context with resource blocking."""
    browser = await p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={"width": 1920, "height": 1080},
    )
    await context.add_init_script(
        "Object.defineProperty(navigator, 'webdriver', { get: () => false });"
    )
    await context.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,eot}", lambda route: route.abort())
    for domain in BLOCKED_DOMAINS:
        await context.route(f"**/{domain}/**", lambda route: route.abort())
    return browser, context


async def restart_browser(p, browser):
    """Close old browser and create a fresh one."""
    try:
        await browser.close()
    except Exception:
        pass
    return await create_browser_context(p)


# =============================================================================
# SCRAPE A SINGLE PAGE
# =============================================================================
async def scrape_single_page(context, slug, url, page_timeout, h1_timeout, content_timeout, extra_wait):
    """Open a new page, scrape a single investor profile, close the page."""
    page = None
    try:
        page = await context.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=page_timeout)

        # Wait for h1 (name)
        try:
            await page.wait_for_selector("h1", timeout=h1_timeout)
        except PlaywrightTimeout:
            return None, "h1 never appeared"

        # Wait for investing profile section
        try:
            await page.wait_for_selector(".line-separated-row", timeout=content_timeout)
        except PlaywrightTimeout:
            pass  # some profiles may not have this

        # Buffer for lazy content
        await page.wait_for_timeout(extra_wait)

        data = await page.evaluate(SCRAPE_JS)

        # Validate
        name = data.get("basicInfo", {}).get("name", "")
        if not name or len(name.strip()) < 2:
            return None, f"No valid name (got: '{name}')"

        data["scraped_at"] = datetime.now().isoformat()
        data["slug"] = slug
        return data, None

    except Exception as e:
        return None, str(e)[:200]
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass


# =============================================================================
# MAIN SCRAPER
# =============================================================================
async def run():
    os.makedirs(PROFILES_DIR, exist_ok=True)

    # Load data
    with open(ALL_URLS_FILE, "r") as f:
        all_urls = json.load(f)

    scraped_set = load_progress()
    failed_tracker = load_failed()

    # Sync scraped_set with actual profile files on disk
    for slug in list(scraped_set):
        if not profile_exists(slug):
            scraped_set.discard(slug)
    for inv in all_urls:
        if profile_exists(inv["slug"]):
            scraped_set.add(inv["slug"])
    save_progress(scraped_set)

    # Determine what to scrape
    to_scrape = [inv for inv in all_urls if inv["slug"] not in scraped_set]

    log.info("=" * 60)
    log.info("  NFX SIGNAL - PROFILE SCRAPER (HEADLESS)")
    log.info(f"  Total URLs:       {len(all_urls)}")
    log.info(f"  Already scraped:  {len(scraped_set)}")
    log.info(f"  To scrape:        {len(to_scrape)}")
    log.info(f"  Batch size:       {BATCH_SIZE}")
    log.info("=" * 60)

    if not to_scrape:
        log.info("Nothing to scrape! All done.")
        return

    async with async_playwright() as p:
        browser, context = await create_browser_context(p)
        batches_since_restart = 0
        consecutive_failures = 0
        session_scraped = 0
        session_failed = 0
        idx = 0

        # ── MAIN SCRAPE PASS ──
        while idx < len(to_scrape):
            batch = to_scrape[idx:idx + BATCH_SIZE]
            idx += BATCH_SIZE

            # Skip already-scraped
            batch = [inv for inv in batch if inv["slug"] not in scraped_set]
            if not batch:
                continue

            batches_since_restart += 1
            log.info(f"BATCH | {len(batch)} pages | ~{len(to_scrape) - idx} queued | {len(scraped_set)} total on disk")

            tasks = [
                scrape_single_page(
                    context, inv["slug"], inv["url"],
                    PAGE_LOAD_TIMEOUT, H1_TIMEOUT, CONTENT_TIMEOUT, EXTRA_WAIT,
                )
                for inv in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_ok = 0
            batch_fail = 0
            batch_garbage = 0

            for i, result in enumerate(results):
                slug = batch[i]["slug"]

                if isinstance(result, Exception):
                    error_msg = str(result)[:200]
                    log.warning(f"  FAIL {slug}: {error_msg[:60]}")
                    failed_tracker["failed"].append({
                        "slug": slug, "url": batch[i]["url"],
                        "error": error_msg, "timestamp": datetime.now().isoformat(),
                    })
                    session_failed += 1
                    batch_fail += 1
                    continue

                data, error = result

                if error:
                    log.warning(f"  FAIL {slug}: {error[:60]}")
                    failed_tracker["failed"].append({
                        "slug": slug, "url": batch[i]["url"],
                        "error": error, "timestamp": datetime.now().isoformat(),
                    })
                    session_failed += 1
                    batch_fail += 1
                    continue

                name = data.get("basicInfo", {}).get("name", "")
                if is_garbage_name(name):
                    log.warning(f"  FAIL {slug}: garbage name '{name}'")
                    failed_tracker["failed"].append({
                        "slug": slug, "url": batch[i]["url"],
                        "error": f"garbage name: {name}", "timestamp": datetime.now().isoformat(),
                    })
                    session_failed += 1
                    batch_fail += 1
                    batch_garbage += 1
                    continue

                if save_profile(slug, data):
                    scraped_set.add(slug)
                    session_scraped += 1
                    batch_ok += 1
                    log.info(f"  OK   {slug} ({name})")
                else:
                    session_failed += 1
                    batch_fail += 1

            save_progress(scraped_set)
            save_failed(failed_tracker)

            # Entire batch failed → server is blocking us
            if batch_fail >= len(batch) and batch_ok == 0:
                consecutive_failures += 1
                # Re-queue failed items for later
                for inv in batch:
                    if inv["slug"] not in scraped_set:
                        to_scrape.append(inv)

                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    log.warning(f"  {consecutive_failures} consecutive failed batches — RESTARTING BROWSER + waiting {BROWSER_RESTART_COOLDOWN}s...")
                    await asyncio.sleep(BROWSER_RESTART_COOLDOWN)
                    browser, context = await restart_browser(p, browser)
                    batches_since_restart = 0
                    consecutive_failures = 0
                else:
                    wait_time = 15 * consecutive_failures
                    log.warning(f"  Blocked batch ({consecutive_failures}x). Waiting {wait_time}s, re-queuing...")
                    await asyncio.sleep(wait_time)
            else:
                consecutive_failures = 0
                # Randomized pause to look more natural
                pause = BATCH_PAUSE + random.uniform(0, 2)
                await asyncio.sleep(pause)

            # Preventive browser restart every N batches
            if batches_since_restart >= PREVENTIVE_RESTART_BATCHES:
                log.info(f"  Preventive browser restart ({batches_since_restart} batches)...")
                await asyncio.sleep(30)
                browser, context = await restart_browser(p, browser)
                batches_since_restart = 0

        log.info("")
        log.info(f"Main pass done: {session_scraped} scraped, {session_failed} failed")

        # ── RETRY PASS (one-at-a-time, fresh browser) ──
        failed_slugs = {f["slug"]: f for f in failed_tracker.get("failed", []) if f["slug"] not in scraped_set}
        failed_list = list(failed_slugs.values())

        if failed_list:
            log.info("")
            log.info("=" * 60)
            log.info(f"  RETRY PASS: {len(failed_list)} failed profiles (one-at-a-time, fresh browser)")
            log.info("=" * 60)

            # Fresh browser for retry
            await asyncio.sleep(30)
            browser, context = await restart_browser(p, browser)

            retry_ok = 0
            retry_fail = 0
            new_failures = []
            retry_consecutive_fails = 0

            for i, f in enumerate(failed_list, 1):
                slug = f["slug"]
                url = f["url"]

                if slug in scraped_set or profile_exists(slug):
                    continue

                log.info(f"  Retry {i}/{len(failed_list)}: {slug}")

                data, error = await scrape_single_page(
                    context, slug, url,
                    RETRY_PAGE_TIMEOUT, RETRY_H1_TIMEOUT, RETRY_CONTENT_TIMEOUT, RETRY_EXTRA_WAIT,
                )

                if error:
                    log.warning(f"    FAIL: {error[:60]}")
                    new_failures.append({"slug": slug, "url": url, "error": error, "timestamp": datetime.now().isoformat()})
                    retry_fail += 1
                    retry_consecutive_fails += 1
                elif data:
                    name = data.get("basicInfo", {}).get("name", "")
                    if is_garbage_name(name):
                        log.warning(f"    FAIL: garbage name '{name}'")
                        new_failures.append({"slug": slug, "url": url, "error": f"garbage name: {name}", "timestamp": datetime.now().isoformat()})
                        retry_fail += 1
                        retry_consecutive_fails += 1
                    elif save_profile(slug, data):
                        scraped_set.add(slug)
                        retry_ok += 1
                        retry_consecutive_fails = 0
                        log.info(f"    OK  {slug} ({name})")
                    else:
                        retry_fail += 1
                        retry_consecutive_fails += 1
                else:
                    retry_fail += 1
                    retry_consecutive_fails += 1

                save_progress(scraped_set)
                await asyncio.sleep(3 + random.uniform(0, 2))

                # Restart browser if seeing too many consecutive failures in retry
                if retry_consecutive_fails >= 10:
                    log.info("  Retry: restarting browser (10 consecutive fails)...")
                    await asyncio.sleep(60)
                    browser, context = await restart_browser(p, browser)
                    retry_consecutive_fails = 0

                # Also restart every 50 retries as prevention
                if i % 50 == 0:
                    log.info("  Retry: preventive browser restart...")
                    await asyncio.sleep(20)
                    browser, context = await restart_browser(p, browser)
                    retry_consecutive_fails = 0

            failed_tracker["failed"] = new_failures
            save_failed(failed_tracker)
            log.info(f"  Retry pass: {retry_ok} recovered, {retry_fail} still failed")

        try:
            await browser.close()
        except Exception:
            pass

    # ── FINAL REPORT ──
    all_slugs = {inv["slug"] for inv in all_urls}
    scraped_on_disk = {
        f.replace(".json", "")
        for f in os.listdir(PROFILES_DIR)
        if f.endswith(".json")
    }
    missing = all_slugs - scraped_on_disk

    log.info("")
    log.info("=" * 60)
    log.info("  FINAL REPORT")
    log.info(f"  Total in all_investor_urls.json:  {len(all_slugs)}")
    log.info(f"  Profiles on disk:                 {len(scraped_on_disk)}")
    log.info(f"  Missing profiles:                 {len(missing)}")
    log.info(f"  Failed (after retry):             {len(failed_tracker.get('failed', []))}")
    log.info("=" * 60)

    if missing and len(missing) <= 50:
        log.info("Missing slugs:")
        for s in sorted(missing):
            log.info(f"  - {s}")
    elif missing:
        log.info(f"First 50 missing slugs:")
        for s in sorted(missing)[:50]:
            log.info(f"  - {s}")


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("\nStopped by user. Progress saved.")
    except Exception as e:
        log.error(f"CRASH: {e}")
        log.error(traceback.format_exc())
        # Emergency save
        try:
            existing = load_progress()
            if existing:
                save_progress(existing)
                log.info(f"Emergency save: {len(existing)} in progress.json")
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
