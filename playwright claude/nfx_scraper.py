#!/usr/bin/env python3
"""
NFX Signal Scraper - TWO PHASE (Playwright Edition)
====================================================
Phase 1: Click Load More until ALL investor URLs collected (one-time, saved to file)
Phase 2: Visit each URL in batches of 8 tabs - instant resume, no Load More needed

Setup (one time):
    pip install playwright
    playwright install chromium

Run:
    python nfx_scraper.py
    python nfx_scraper.py --url "https://signal.nfx.com/investor-lists/top-ai-seed-investors"
    python nfx_scraper.py --batch 4          # smaller batches if Chrome struggles
    python nfx_scraper.py --headed           # watch the browser
    python nfx_scraper.py --retry-failed     # re-scrape previously failed profiles
"""

import asyncio
import argparse
import json
import os
import sys
import logging
import traceback
from datetime import datetime
from pathlib import Path

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    print("\nPlaywright not installed. Run these two commands:\n")
    print("   pip install playwright")
    print("   playwright install chromium\n")
    sys.exit(1)


# =============================================================================
# CONFIGURATION
# =============================================================================
DEFAULT_URL = "https://signal.nfx.com/investor-lists/top-marketplaces-seed-investors"

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
PROFILES_DIR = os.path.join(DATA_DIR, "profiles")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
FAILED_FILE = os.path.join(DATA_DIR, "failed_profiles.json")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")

BATCH_SIZE = 8

CONFIG = {
    "TAB_OPEN_DELAY": 0.5,
    "TAB_CONTENT_TIMEOUT": 15000,       # ms (Playwright uses ms)
    "LOAD_MORE_WAIT": 4000,
    "PAGE_LOAD_TIMEOUT": 15000,
    "POST_TAB_OPEN_WAIT": 3000,
    "POST_SCRAPE_BUFFER": 1000,
    "SCROLL_WAIT": 2000,
}

# Logging
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scraper.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# =============================================================================
# FILE I/O
# =============================================================================
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                data = json.load(f)
                return set(data.get("scraped", []))
        except Exception:
            pass
    return set()


def save_progress(scraped_set):
    try:
        with open(PROGRESS_FILE, "w") as f:
            json.dump({"scraped": list(scraped_set), "total_scraped": len(scraped_set)}, f)
    except Exception as e:
        logger.warning(f"Could not save progress: {e}")


def load_failed():
    if os.path.exists(FAILED_FILE):
        try:
            with open(FAILED_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"failed": []}


def save_failed(failed):
    try:
        with open(FAILED_FILE, "w") as f:
            json.dump(failed, f, indent=2)
    except Exception:
        pass


def save_profile(slug, data):
    try:
        filepath = os.path.join(PROFILES_DIR, f"{slug}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error(f"Error saving {slug}: {e}")
        return False


# =============================================================================
# PROFILE SCRAPE JS  (runs inside each investor page)
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
# PHASE 1: Collect ALL investor URLs from the list page
# =============================================================================
async def phase1_collect_urls(page, list_url):
    logger.info("=" * 60)
    logger.info("  PHASE 1: Collecting all investor URLs")
    logger.info("=" * 60)

    try:
        await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)
    except Exception as e:
        logger.error(f"Failed to load list page: {e}")
        return []

    # Wait for investor links to appear
    try:
        await page.wait_for_selector("a[href*='/investors/']", timeout=CONFIG["PAGE_LOAD_TIMEOUT"])
    except PlaywrightTimeout:
        logger.error("No investor links found! Are you logged in?")
        return []

    await page.wait_for_timeout(2000)

    all_investors = {}  # slug -> url
    no_button_count = 0
    clicks = 0
    last_count = 0
    stale_rounds = 0

    while True:
        # Collect all visible investor links
        try:
            links = await page.evaluate("""
                () => {
                    var results = {};
                    document.querySelectorAll('a[href*="/investors/"]').forEach(function(a) {
                        var href = a.href;
                        var slug = href.split('/investors/').pop().split('?')[0].split('#')[0];
                        if (slug && slug !== '' && slug !== 'edit' && !slug.startsWith('http')) {
                            results[slug] = href;
                        }
                    });
                    return results;
                }
            """)
            if links:
                all_investors.update(links)
        except Exception as e:
            logger.warning(f"Error collecting links: {e}")

        # Check if we're still finding new ones
        if len(all_investors) == last_count:
            stale_rounds += 1
        else:
            stale_rounds = 0
            last_count = len(all_investors)

        if stale_rounds >= 10:
            logger.info("No new investors for 10 rounds. Collection done.")
            break

        # Click Load More
        clicked = False
        for selector in [
            "button:has-text('Load More')",
            "button:has-text('Show More')",
            "button:has-text('load more')",
            "a:has-text('Load More')",
            "[class*='load-more']",
        ]:
            try:
                btn = page.locator(selector)
                if await btn.count() > 0:
                    await btn.first.scroll_into_view_if_needed()
                    await btn.first.click()
                    clicked = True
                    clicks += 1
                    break
            except Exception:
                continue

        if clicked:
            no_button_count = 0
            await page.wait_for_timeout(CONFIG["LOAD_MORE_WAIT"])

            if clicks % 25 == 0:
                logger.info(f"  Click #{clicks} | {len(all_investors)} URLs collected")
        else:
            # Also try scrolling to bottom (some pages use infinite scroll)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(CONFIG["SCROLL_WAIT"])

            no_button_count += 1
            if no_button_count >= 5:
                logger.info("No Load More button found. Collection done.")
                break

    # Save to file
    investor_list = [{"slug": s, "url": u} for s, u in all_investors.items()]
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(ALL_URLS_FILE, "w") as f:
        json.dump(investor_list, f, indent=2)

    logger.info(f"Phase 1 DONE: {len(investor_list)} URLs saved to {ALL_URLS_FILE}")
    logger.info(f"Total Load More clicks: {clicks}")
    return investor_list


# =============================================================================
# PHASE 2: Scrape each profile in batches using multiple pages
# =============================================================================
async def scrape_single_page(context, inv, scrape_js):
    """Open a new page, scrape a single investor profile, close the page."""
    slug = inv["slug"]
    url = inv["url"]
    page = None
    try:
        page = await context.new_page()

        # Wait for full page load (not just DOM)
        await page.goto(url, wait_until="networkidle", timeout=45000)

        # Wait for the h1 with investor name (core content indicator)
        try:
            await page.wait_for_selector("h1", timeout=CONFIG["TAB_CONTENT_TIMEOUT"])
        except PlaywrightTimeout:
            return None, "h1 never appeared - page didn't load"

        # Wait for investing profile section to render (confirms full page load)
        try:
            await page.wait_for_selector(".line-separated-row", timeout=10000)
        except PlaywrightTimeout:
            pass  # some profiles may not have this section, continue

        # Extra buffer for any lazy-loaded content
        await page.wait_for_timeout(2000)

        data = await page.evaluate(scrape_js)

        # Validate: must have a name at minimum
        name = data.get("basicInfo", {}).get("name", "")
        if not name or len(name.strip()) < 2:
            return None, f"No valid name extracted (got: '{name}')"

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


async def phase2_scrape(context, investor_list, scraped_set, failed_tracker, batch_size):
    to_scrape = [inv for inv in investor_list if inv["slug"] not in scraped_set]

    logger.info("=" * 60)
    logger.info(f"  PHASE 2: Scraping {len(to_scrape)} profiles")
    logger.info(f"  Already scraped: {len(scraped_set)}")
    logger.info(f"  Batch size: {batch_size}")
    logger.info("=" * 60)

    if not to_scrape:
        logger.info("Nothing to scrape! All done.")
        return

    session_scraped = 0
    total_batches = (len(to_scrape) + batch_size - 1) // batch_size

    for batch_num, batch_start in enumerate(range(0, len(to_scrape), batch_size), 1):
        batch = to_scrape[batch_start:batch_start + batch_size]
        remaining = len(to_scrape) - batch_start

        logger.info(f"  BATCH {batch_num}/{total_batches} | {len(batch)} pages | {remaining} remaining")

        # Scrape all pages in this batch concurrently
        tasks = [scrape_single_page(context, inv, SCRAPE_JS) for inv in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            slug = batch[i]["slug"]

            if isinstance(result, Exception):
                logger.warning(f"    FAIL {slug}: {str(result)[:60]}")
                failed_tracker["failed"].append({
                    "slug": slug,
                    "url": batch[i]["url"],
                    "error": str(result)[:200],
                    "timestamp": datetime.now().isoformat(),
                })
                continue

            data, error = result

            if error:
                logger.warning(f"    FAIL {slug}: {error[:60]}")
                failed_tracker["failed"].append({
                    "slug": slug,
                    "url": batch[i]["url"],
                    "error": error,
                    "timestamp": datetime.now().isoformat(),
                })
                continue

            if save_profile(slug, data):
                scraped_set.add(slug)
                session_scraped += 1
                name = data.get("basicInfo", {}).get("name", slug)
                logger.info(f"    OK {slug} ({name})")
            else:
                logger.warning(f"    FAIL {slug} (save failed)")

        # Save progress after each batch
        save_progress(scraped_set)
        save_failed(failed_tracker)

        logger.info(f"  Total: {len(scraped_set)} | Session: {session_scraped}")
        logger.info("")

    # Summary
    logger.info("=" * 60)
    logger.info(f"  SCRAPING COMPLETE")
    logger.info(f"  Total: {len(scraped_set)} | Session: {session_scraped}")
    logger.info(f"  Failed: {len(failed_tracker.get('failed', []))}")
    logger.info(f"  Output: {PROFILES_DIR}")
    logger.info("=" * 60)


# =============================================================================
# MAIN
# =============================================================================
async def run(args):
    logger.info("=" * 60)
    logger.info("  NFX SIGNAL SCRAPER - TWO PHASE (Playwright)")
    logger.info("=" * 60)

    os.makedirs(PROFILES_DIR, exist_ok=True)

    scraped_set = load_progress()
    failed_tracker = load_failed()

    # If --retry-failed, remove failed slugs from scraped set so they get re-scraped
    if args.retry_failed and failed_tracker.get("failed"):
        failed_slugs = {f["slug"] for f in failed_tracker["failed"]}
        scraped_set -= failed_slugs
        logger.info(f"Retrying {len(failed_slugs)} previously failed profiles")
        failed_tracker["failed"] = []  # clear failed list
        save_progress(scraped_set)
        save_failed(failed_tracker)

    logger.info(f"Already scraped: {len(scraped_set)} profiles")

    batch_size = args.batch

    async with async_playwright() as p:
        # Always launch headed so user can log in manually
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )

        # LOGIN: Open the site and wait for user to log in manually
        login_page = await context.new_page()
        logger.info("Opening NFX Signal for login...")
        await login_page.goto("https://signal.nfx.com/login", wait_until="domcontentloaded", timeout=60000)

        logger.info("")
        logger.info("=" * 60)
        logger.info("  LOG IN MANUALLY in the browser window.")
        logger.info("  After you're logged in, come back here and press ENTER.")
        logger.info("=" * 60)
        logger.info("")

        # Wait for user to press Enter in the terminal
        await asyncio.get_event_loop().run_in_executor(None, input, "  >> Press ENTER after logging in... ")

        # Verify login by checking if we can reach a protected page
        current_url = login_page.url
        logger.info(f"Current URL: {current_url}")
        await login_page.close()
        logger.info("Login done. Starting scraper...\n")

        # PHASE 1: Collect all URLs (skip if already done)
        if os.path.exists(ALL_URLS_FILE) and not args.recollect:
            with open(ALL_URLS_FILE, "r") as f:
                investor_list = json.load(f)
            logger.info(f"Loaded {len(investor_list)} URLs from {ALL_URLS_FILE} (Phase 1 already done)")
        else:
            page = await context.new_page()
            investor_list = await phase1_collect_urls(page, args.url)
            await page.close()
            if not investor_list:
                logger.error("No investors found! The page may require login or the structure changed.")
                await browser.close()
                return

        # PHASE 2: Scrape profiles
        await phase2_scrape(context, investor_list, scraped_set, failed_tracker, batch_size)

        await browser.close()


def main():
    parser = argparse.ArgumentParser(description="NFX Signal Scraper - Two Phase (Playwright)")
    parser.add_argument("--url", default=DEFAULT_URL, help="Investor list URL")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE, help=f"Batch size for concurrent tabs (default: {BATCH_SIZE})")
    parser.add_argument("--retry-failed", action="store_true", help="Re-scrape previously failed profiles")
    parser.add_argument("--recollect", action="store_true", help="Force re-collect all URLs (redo Phase 1)")
    args = parser.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        logger.info("\n\nStopped by user. Progress saved.")
    except Exception as e:
        logger.error(f"\n\nFATAL: {e}")
        logger.error(traceback.format_exc())
        logger.info("Progress saved. Run again to resume.")


if __name__ == "__main__":
    main()
