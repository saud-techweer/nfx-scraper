"""
NFX Signal Scraper - TWO PHASE
Phase 1: Click Load More until ALL investor URLs collected (one-time, saved to file)
Phase 2: Visit each URL directly in batches of 8 tabs - instant resume, no Load More needed
"""

import os
import json
import time
import logging
import traceback
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

LIST_URL = "https://signal.nfx.com/investor-lists/top-marketplaces-seed-investors"

DATA_DIR = "data"
PROFILES_DIR = os.path.join(DATA_DIR, "profiles")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
FAILED_FILE = os.path.join(DATA_DIR, "failed_profiles.json")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")

BATCH_SIZE = 8

CONFIG = {
    "TAB_OPEN_DELAY": 0.5,
    "TAB_CONTENT_TIMEOUT": 15,
    "LOAD_MORE_WAIT": 4,
    "PAGE_LOAD_TIMEOUT": 15,
    "POST_TAB_OPEN_WAIT": 3,
    "POST_SCRAPE_BUFFER": 1,
}

# Logging to console + file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
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
# CHROME
# =============================================================================
def connect_to_chrome():
    for attempt in range(5):
        try:
            logger.info(f"Connecting to Chrome (attempt {attempt + 1}/5)...")
            opts = Options()
            opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
            driver = webdriver.Chrome(options=opts)
            _ = driver.current_url
            logger.info("Connected to Chrome!")
            return driver
        except Exception as e:
            logger.warning(f"Failed: {e}")
            if attempt < 4:
                time.sleep(3)
    raise Exception("Could not connect to Chrome after 5 attempts")


def is_chrome_alive(driver):
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


def close_extra_tabs(driver, keep_handle):
    try:
        handles = driver.window_handles
        for handle in handles:
            if handle != keep_handle:
                try:
                    driver.switch_to.window(handle)
                    driver.close()
                except Exception:
                    pass
        driver.switch_to.window(keep_handle)
    except Exception:
        pass


# =============================================================================
# SCRAPE JS
# =============================================================================
SCRAPE_JS = r"""
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
"""


# =============================================================================
# PHASE 1: Collect ALL investor URLs from the list page
# =============================================================================
def phase1_collect_urls(driver):
    logger.info("=" * 60)
    logger.info("  PHASE 1: Collecting all investor URLs")
    logger.info("=" * 60)

    driver.get(LIST_URL)
    time.sleep(5)

    try:
        WebDriverWait(driver, CONFIG["PAGE_LOAD_TIMEOUT"]).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/investors/']"))
        )
    except TimeoutException:
        logger.error("No investor links found! Are you logged in?")
        return []

    time.sleep(2)

    all_investors = {}  # slug -> url
    no_button_count = 0
    clicks = 0
    last_count = 0
    stale_rounds = 0

    while True:
        if not is_chrome_alive(driver):
            logger.error("Chrome died during URL collection!")
            break

        # Collect all visible investor links
        try:
            links = driver.execute_script("""
                var results = {};
                document.querySelectorAll('a[href*="/investors/"]').forEach(function(a) {
                    var href = a.href;
                    var slug = href.split('/investors/').pop().split('?')[0].split('#')[0];
                    if (slug && slug !== '' && slug !== 'edit' && !slug.startsWith('http')) {
                        results[slug] = href;
                    }
                });
                return results;
            """) or {}
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
            logger.info(f"No new investors for 10 rounds. Collection done.")
            break

        # Click Load More
        clicked = False
        try:
            buttons = driver.find_elements(By.TAG_NAME, "button")
            for btn in buttons:
                try:
                    if "LOAD MORE" in btn.text.upper():
                        btn.click()
                        clicked = True
                        clicks += 1
                        break
                except Exception:
                    continue
        except Exception:
            pass

        if clicked:
            no_button_count = 0
            time.sleep(CONFIG["LOAD_MORE_WAIT"])

            if clicks % 25 == 0:
                logger.info(f"  Click #{clicks} | {len(all_investors)} URLs collected")
        else:
            no_button_count += 1
            if no_button_count >= 5:
                logger.info("No Load More button found. Collection done.")
                break
            time.sleep(2)

    # Save to file
    investor_list = [{"slug": s, "url": u} for s, u in all_investors.items()]
    with open(ALL_URLS_FILE, "w") as f:
        json.dump(investor_list, f, indent=2)

    logger.info(f"Phase 1 DONE: {len(investor_list)} URLs saved to {ALL_URLS_FILE}")
    logger.info(f"Total Load More clicks: {clicks}")
    return investor_list


# =============================================================================
# PHASE 2: Scrape each profile directly (batch 8 tabs)
# =============================================================================
def phase2_scrape(driver, investor_list, scraped_set, failed_tracker):
    to_scrape = [inv for inv in investor_list if inv["slug"] not in scraped_set]

    logger.info("=" * 60)
    logger.info(f"  PHASE 2: Scraping {len(to_scrape)} profiles")
    logger.info(f"  Already scraped: {len(scraped_set)}")
    logger.info("=" * 60)

    if not to_scrape:
        logger.info("Nothing to scrape! All done.")
        return

    # Clean main tab
    driver.get("about:blank")
    time.sleep(1)
    main_handle = driver.current_window_handle

    session_scraped = 0
    total_batches = (len(to_scrape) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, batch_start in enumerate(range(0, len(to_scrape), BATCH_SIZE), 1):
        batch = to_scrape[batch_start:batch_start + BATCH_SIZE]

        if not is_chrome_alive(driver):
            logger.error("Chrome died! Progress saved. Run again to resume.")
            break

        remaining = len(to_scrape) - batch_start
        logger.info(f"  BATCH {batch_num}/{total_batches} | {len(batch)} tabs | {remaining} remaining")

        # Open all tabs
        for inv in batch:
            try:
                driver.execute_script("window.open(arguments[0], '_blank');", inv["url"])
                time.sleep(CONFIG["TAB_OPEN_DELAY"])
            except Exception as e:
                logger.warning(f"  Failed to open tab for {inv['slug']}: {str(e)[:50]}")

        # Wait for all tabs to start loading
        time.sleep(CONFIG["POST_TAB_OPEN_WAIT"])

        # Scrape each tab
        handles = driver.window_handles
        tab_handles = [h for h in handles if h != main_handle]

        for i, handle in enumerate(tab_handles):
            slug = batch[i]["slug"] if i < len(batch) else f"unknown-{i}"
            try:
                driver.switch_to.window(handle)

                # Wait for content to load
                try:
                    WebDriverWait(driver, CONFIG["TAB_CONTENT_TIMEOUT"]).until(
                        EC.presence_of_element_located((By.TAG_NAME, "h1"))
                    )
                except TimeoutException:
                    logger.warning(f"    Timeout for {slug}, scraping anyway...")

                time.sleep(CONFIG["POST_SCRAPE_BUFFER"])

                data = driver.execute_script(SCRAPE_JS)
                data["scraped_at"] = datetime.now().isoformat()
                data["slug"] = slug

                if save_profile(slug, data):
                    scraped_set.add(slug)
                    session_scraped += 1
                    logger.info(f"    OK {slug}")
                else:
                    logger.warning(f"    FAIL {slug} (save failed)")

            except Exception as e:
                logger.warning(f"    FAIL {slug}: {str(e)[:60]}")
                failed_tracker["failed"].append({
                    "slug": slug,
                    "url": batch[i]["url"] if i < len(batch) else "",
                    "error": str(e)[:200],
                    "timestamp": datetime.now().isoformat()
                })

            # Close tab
            try:
                driver.close()
            except Exception:
                pass

        # Back to main tab
        try:
            driver.switch_to.window(main_handle)
        except Exception:
            logger.warning("Lost main tab!")
            try:
                handles = driver.window_handles
                if handles:
                    driver.switch_to.window(handles[0])
                    main_handle = handles[0]
            except Exception:
                break

        # Save after each batch
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
def main():
    logger.info("=" * 60)
    logger.info("  NFX SIGNAL SCRAPER - TWO PHASE")
    logger.info("=" * 60)

    os.makedirs(PROFILES_DIR, exist_ok=True)

    scraped_set = load_progress()
    failed_tracker = load_failed()

    logger.info(f"Already scraped: {len(scraped_set)} profiles")

    driver = connect_to_chrome()

    # PHASE 1: Collect all URLs (skip if already done)
    if os.path.exists(ALL_URLS_FILE):
        with open(ALL_URLS_FILE, "r") as f:
            investor_list = json.load(f)
        logger.info(f"Loaded {len(investor_list)} URLs from {ALL_URLS_FILE} (Phase 1 already done)")
    else:
        investor_list = phase1_collect_urls(driver)
        if not investor_list:
            logger.error("No investors found! Make sure you're logged in.")
            return

    # PHASE 2: Scrape profiles
    phase2_scrape(driver, investor_list, scraped_set, failed_tracker)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\nStopped by user. Progress saved.")
    except Exception as e:
        logger.error(f"\n\nFATAL: {e}")
        logger.error(traceback.format_exc())
        logger.info("Progress saved. Run again to resume.")
