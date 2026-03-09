#!/usr/bin/env python3
"""
NFX Signal - Retry Remaining Profiles
======================================
Dedicated scraper for the ~388 profiles that previously failed.
Uses slower, more careful approach: one at a time, longer waits,
fresh browser every 25 profiles, non-headless option for Cloudflare.
"""

import asyncio
import json
import os
import sys
import logging
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
LOG_FILE = os.path.join(SCRIPT_DIR, "retry_remaining.log")

# Generous timeouts for stubborn pages
PAGE_TIMEOUT = 60000      # 60s
H1_TIMEOUT = 30000        # 30s
CONTENT_TIMEOUT = 20000   # 20s
EXTRA_WAIT = 4000         # 4s buffer
BETWEEN_PROFILES = 6      # seconds between each profile
RESTART_EVERY = 25        # restart browser every N profiles
RESTART_COOLDOWN = 60     # seconds after restart

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
# SCRAPE JS (same as scrape_profiles.py)
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

    document.querySelectorAll('a.vc-list-chip').forEach(chip => {
        data.sectorRankings.push({ name: chip.textContent.trim(), url: chip.href });
    });

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
# HELPERS
# =============================================================================
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

BLOCKED_DOMAINS = [
    "facebook.com", "facebook.net", "google-analytics.com",
    "googletagmanager.com", "nr-data.net", "mixpanel.com",
    "intercom.io", "ads-twitter.com",
]


def is_garbage_name(name):
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
        or name_lower.startswith(("400", "401", "403", "404", "500", "502", "503"))
    )


def save_profile(slug, data):
    filepath = os.path.join(PROFILES_DIR, f"{slug}.json")
    tmp = filepath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, filepath)


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r") as f:
                return set(json.load(f).get("scraped", []))
        except Exception:
            pass
    return set()


def save_progress(scraped_set):
    tmp = PROGRESS_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"scraped": sorted(scraped_set), "total_scraped": len(scraped_set)}, f)
    os.replace(tmp, PROGRESS_FILE)


def profile_exists(slug):
    return os.path.exists(os.path.join(PROFILES_DIR, f"{slug}.json"))


def get_remaining():
    """Get list of URLs that don't have a profile file yet."""
    with open(ALL_URLS_FILE, "r") as f:
        all_urls = json.load(f)

    existing = {f.replace(".json", "") for f in os.listdir(PROFILES_DIR) if f.endswith(".json")}
    remaining = [inv for inv in all_urls if inv["slug"] not in existing]
    return remaining, existing


# =============================================================================
# BROWSER
# =============================================================================
async def create_browser(p):
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
        locale="en-US",
        timezone_id="America/New_York",
    )
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
    """)
    await context.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,eot}", lambda route: route.abort())
    for domain in BLOCKED_DOMAINS:
        await context.route(f"**/{domain}/**", lambda route: route.abort())
    return browser, context


async def scrape_one(context, slug, url, attempt=1):
    """Scrape a single profile with up to 2 attempts per browser session."""
    page = None
    try:
        page = await context.new_page()

        # Go to page
        await page.goto(url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)

        # Wait for h1
        try:
            await page.wait_for_selector("h1", timeout=H1_TIMEOUT)
        except PlaywrightTimeout:
            return None, "h1 never appeared"

        # Wait for content rows
        try:
            await page.wait_for_selector(".line-separated-row", timeout=CONTENT_TIMEOUT)
        except PlaywrightTimeout:
            pass

        # Extra buffer
        await page.wait_for_timeout(EXTRA_WAIT)

        # Check page title for error pages
        title = await page.title()
        if any(err in title.lower() for err in ["404", "not found", "error", "forbidden"]):
            return None, f"error page: {title}"

        data = await page.evaluate(SCRAPE_JS)

        name = data.get("basicInfo", {}).get("name", "")
        if is_garbage_name(name):
            if attempt == 1:
                # Try once more with a full page reload + networkidle
                await page.close()
                page = await context.new_page()
                await page.goto(url, wait_until="networkidle", timeout=PAGE_TIMEOUT)
                await page.wait_for_timeout(EXTRA_WAIT + 2000)
                data = await page.evaluate(SCRAPE_JS)
                name = data.get("basicInfo", {}).get("name", "")
                if is_garbage_name(name):
                    return None, f"garbage name: {name}"
            else:
                return None, f"garbage name: {name}"

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
# MAIN
# =============================================================================
async def run():
    os.makedirs(PROFILES_DIR, exist_ok=True)

    remaining, existing = get_remaining()
    scraped_set = load_progress()
    # Sync with disk
    scraped_set = scraped_set | existing

    log.info("=" * 60)
    log.info("  NFX SIGNAL - RETRY REMAINING PROFILES")
    log.info(f"  Already on disk:  {len(existing)}")
    log.info(f"  Remaining:        {len(remaining)}")
    log.info("=" * 60)

    if not remaining:
        log.info("Nothing to scrape! All profiles exist.")
        return

    # Shuffle to avoid hitting same part of the site
    random.shuffle(remaining)

    succeeded = 0
    failed = 0
    still_failed = []
    consecutive_fails = 0

    async with async_playwright() as p:
        browser, context = await create_browser(p)
        profiles_since_restart = 0

        for i, inv in enumerate(remaining, 1):
            slug = inv["slug"]
            url = inv["url"]

            if profile_exists(slug):
                log.info(f"  [{i}/{len(remaining)}] SKIP {slug} (already exists)")
                scraped_set.add(slug)
                continue

            log.info(f"  [{i}/{len(remaining)}] Scraping {slug}...")

            data, error = await scrape_one(context, slug, url)

            if data and not error:
                save_profile(slug, data)
                scraped_set.add(slug)
                succeeded += 1
                consecutive_fails = 0
                name = data.get("basicInfo", {}).get("name", "")
                log.info(f"    OK - {name}")
            else:
                failed += 1
                consecutive_fails += 1
                still_failed.append({"slug": slug, "url": url, "error": error, "timestamp": datetime.now().isoformat()})
                log.warning(f"    FAIL - {error}")

            # Save progress every 10
            if i % 10 == 0:
                save_progress(scraped_set)

            profiles_since_restart += 1

            # Restart browser periodically
            if profiles_since_restart >= RESTART_EVERY:
                log.info(f"  Restarting browser (every {RESTART_EVERY} profiles)...")
                try:
                    await browser.close()
                except Exception:
                    pass
                await asyncio.sleep(RESTART_COOLDOWN)
                browser, context = await create_browser(p)
                profiles_since_restart = 0
                consecutive_fails = 0

            # If too many consecutive failures, restart with longer cooldown
            elif consecutive_fails >= 8:
                log.warning(f"  {consecutive_fails} consecutive fails - restarting browser + cooling down 120s...")
                try:
                    await browser.close()
                except Exception:
                    pass
                await asyncio.sleep(120)
                browser, context = await create_browser(p)
                profiles_since_restart = 0
                consecutive_fails = 0

            else:
                # Normal pause between profiles
                wait = BETWEEN_PROFILES + random.uniform(0, 3)
                await asyncio.sleep(wait)

        # Close browser
        try:
            await browser.close()
        except Exception:
            pass

    # Final save
    save_progress(scraped_set)

    # Update failed_profiles.json with only the still-failed ones
    with open(FAILED_FILE, "w") as f:
        json.dump({"failed": still_failed}, f, indent=2)

    # Final report
    remaining_after, existing_after = get_remaining()
    log.info("")
    log.info("=" * 60)
    log.info("  RETRY COMPLETE")
    log.info(f"  Succeeded:          {succeeded}")
    log.info(f"  Still failed:       {failed}")
    log.info(f"  Total on disk now:  {len(existing_after)}")
    log.info(f"  Still remaining:    {len(remaining_after)}")
    log.info("=" * 60)


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("\nStopped by user. Progress saved.")
    except Exception as e:
        log.error(f"CRASH: {e}")
        import traceback
        log.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
