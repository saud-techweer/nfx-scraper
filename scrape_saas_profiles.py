#!/usr/bin/env python3
"""
NFX Signal - SaaS Investors Profile Scraper (Headless, Sequential)
===================================================================
Single-tab sequential scraping with adaptive rate-limit backoff.
Headless Chrome, auto-login, validates each profile, retries failures.
Resumable via progress.json.
"""

import os
import json
import time
import logging
import traceback
import random
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# =============================================================================
# CONFIG
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, ".env"))

NFX_EMAIL = os.getenv("email", "").strip()
NFX_PASSWORD = os.getenv("password", "").strip()

DATA_DIR = os.path.join(SCRIPT_DIR, "data-saas")
PROFILES_DIR = os.path.join(DATA_DIR, "profiles")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
FAILED_FILE = os.path.join(DATA_DIR, "failed_profiles.json")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")
LOG_FILE = os.path.join(SCRIPT_DIR, "scraper_saas.log")

# Adaptive delays
BASE_DELAY = 1.5          # seconds between profiles
MAX_DELAY = 30            # max backoff delay
COOL_DOWN_EVERY = 50      # take a break every N profiles
COOL_DOWN_TIME = 10       # seconds for the break
PAGE_LOAD_WAIT = 15       # seconds to wait for page content
MAX_RETRY_ROUNDS = 3

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
# SCRAPE JS  (identical to data/profiles structure)
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
# FILE I/O
# =============================================================================
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


def save_failed(failed_slugs, url_lookup):
    tmp = FAILED_FILE + ".tmp"
    data = {"failed": [
        {"slug": s, "url": url_lookup.get(s, f"https://signal.nfx.com/investors/{s}"),
         "timestamp": datetime.now().isoformat()}
        for s in failed_slugs
    ]}
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, FAILED_FILE)


def save_profile(slug, data):
    filepath = os.path.join(PROFILES_DIR, f"{slug}.json")
    try:
        tmp = filepath + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, filepath)
        return True
    except Exception as e:
        log.error(f"Save error {slug}: {e}")
        return False


def is_garbage_name(name):
    if not name:
        return True
    name_lower = name.strip().lower()
    # Must be real text, not whitespace/zero-width chars
    clean = ''.join(c for c in name_lower if c.isprintable() and not c.isspace())
    if len(clean) < 2:
        return True
    garbage = [
        "signal.nfx.com", "nfx signal", "bad gateway", "bad request",
        "error code", "not found", "forbidden", "server error",
        "service unavailable", "access denied", "this page",
        "isn't working", "not available", "timed out",
        "too many requests", "cloudflare", "press space", "to play",
        "loading", "just a moment", "please wait", "captcha",
        "verify you", "checking your browser", "error",
    ]
    for g in garbage:
        if g in name_lower:
            return True
    if name_lower.startswith(("400", "401", "403", "404", "429", "500", "502", "503")):
        return True
    return False


def is_profile_valid(data):
    if not data or not isinstance(data, dict):
        return False
    name = data.get("basicInfo", {}).get("name", "")
    return not is_garbage_name(name)


# =============================================================================
# CHROME HEADLESS + AUTO LOGIN
# =============================================================================
def launch_chrome():
    log.info("Launching headless Chrome...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', { get: () => false });"
    })
    driver.set_page_load_timeout(30)
    return driver


def login(driver):
    log.info("Logging in to NFX Signal...")
    driver.get("https://signal.nfx.com/login")
    time.sleep(5)

    # Click LOGIN link
    for link in driver.find_elements(By.TAG_NAME, "a"):
        if link.text.strip().upper() == "LOGIN":
            link.click()
            break
    time.sleep(5)

    # Fill auth form
    driver.find_element(By.CSS_SELECTOR, "#email").send_keys(NFX_EMAIL)
    time.sleep(0.3)
    driver.find_element(By.CSS_SELECTOR, "#password").send_keys(NFX_PASSWORD)
    time.sleep(0.3)

    for btn in driver.find_elements(By.TAG_NAME, "button"):
        if "log in" in btn.text.strip().lower() and "google" not in btn.text.strip().lower():
            btn.click()
            break

    # Wait for redirect
    for _ in range(20):
        time.sleep(1)
        url = driver.current_url
        if "signal.nfx.com" in url and "/login" not in url and "auth.nfx.com" not in url:
            log.info(f"Login OK: {url}")
            return True

    log.error(f"Login failed. URL: {driver.current_url}")
    return False


def is_alive(driver):
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


# =============================================================================
# SINGLE PROFILE SCRAPER
# =============================================================================
def scrape_one(driver, url, slug):
    """Navigate to URL, wait for content to fully render, scrape. Returns (data, error)."""
    try:
        driver.get(url)
    except TimeoutException:
        return None, "page_load_timeout"
    except Exception as e:
        return None, f"nav_error: {str(e)[:60]}"

    # Wait for h1 with REAL text (not just element present, but text rendered)
    try:
        WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            lambda d: d.execute_script("""
                var h1 = document.querySelector('h1.f3.f1-ns, h1');
                if (!h1) return false;
                var text = h1.textContent.trim();
                // Must have printable chars > 2 characters
                var clean = text.replace(/[\\u200B-\\u200D\\uFEFF\\u00AD]/g, '').trim();
                return clean.length > 2;
            """)
        )
    except TimeoutException:
        pass  # scrape anyway, validation will catch bad data

    time.sleep(0.5)

    try:
        data = driver.execute_script(SCRAPE_JS)
        data["scraped_at"] = datetime.now().isoformat()
        data["slug"] = slug
        return data, None
    except Exception as e:
        return None, f"scrape_error: {str(e)[:60]}"


# =============================================================================
# MAIN
# =============================================================================
def run_pass(driver, to_scrape, scraped_set, url_lookup, pass_name="MAIN"):
    """Scrape a list of {slug, url} dicts one by one. Returns list of failed slugs."""
    total = len(to_scrape)
    failed_slugs = []
    consecutive_fails = 0
    current_delay = BASE_DELAY
    ok_count = 0

    log.info(f"  {pass_name}: {total} profiles to scrape")

    for i, inv in enumerate(to_scrape):
        slug = inv["slug"]
        url = inv["url"]

        if slug in scraped_set:
            continue

        # Periodic cool-down
        if ok_count > 0 and ok_count % COOL_DOWN_EVERY == 0:
            log.info(f"  Cool-down pause ({COOL_DOWN_TIME}s)...")
            time.sleep(COOL_DOWN_TIME)

        # Check Chrome health
        if not is_alive(driver):
            log.warning("Chrome died! Restarting...")
            try:
                driver.quit()
            except Exception:
                pass
            driver = launch_chrome()
            if not login(driver):
                log.error("Re-login failed. Aborting pass.")
                failed_slugs.extend(inv2["slug"] for inv2 in to_scrape[i:] if inv2["slug"] not in scraped_set)
                break

        data, error = scrape_one(driver, url, slug)

        if data and is_profile_valid(data):
            if save_profile(slug, data):
                scraped_set.add(slug)
                ok_count += 1
                name = data.get("basicInfo", {}).get("name", "?")
                consecutive_fails = 0
                current_delay = BASE_DELAY  # reset delay on success

                if ok_count % 25 == 0:
                    log.info(f"  [{ok_count}/{total}] OK {slug} ({name}) | total: {len(scraped_set)}")
                else:
                    log.info(f"  OK {slug} ({name})")

                # Save progress every 10 profiles
                if ok_count % 10 == 0:
                    save_progress(scraped_set)
            else:
                failed_slugs.append(slug)
        else:
            err_msg = error or f"invalid: {data.get('basicInfo',{}).get('name','') if data else 'no data'}"
            log.warning(f"  FAIL {slug}: {err_msg}")
            failed_slugs.append(slug)
            consecutive_fails += 1

            # Adaptive backoff: if many consecutive fails, slow down
            if consecutive_fails >= 5:
                current_delay = min(current_delay * 1.5, MAX_DELAY)
                log.warning(f"  {consecutive_fails} consecutive fails, delay={current_delay:.1f}s")

            if consecutive_fails >= 15:
                log.warning(f"  15 consecutive fails — long pause (60s)...")
                time.sleep(60)
                consecutive_fails = 0
                current_delay = BASE_DELAY

        # Delay between profiles (with jitter)
        jitter = random.uniform(0.5, 1.5)
        time.sleep(current_delay * jitter)

    save_progress(scraped_set)
    log.info(f"  {pass_name} done: {ok_count} OK, {len(failed_slugs)} failed")
    return driver, failed_slugs


def main():
    if not NFX_EMAIL or not NFX_PASSWORD:
        log.error("Missing credentials in .env!")
        return

    os.makedirs(PROFILES_DIR, exist_ok=True)

    with open(ALL_URLS_FILE, "r") as f:
        all_urls = json.load(f)

    url_lookup = {inv["slug"]: inv["url"] for inv in all_urls}

    # Load + sync progress
    scraped_set = load_progress()
    for inv in all_urls:
        fp = os.path.join(PROFILES_DIR, f"{inv['slug']}.json")
        if os.path.exists(fp):
            scraped_set.add(inv["slug"])
    save_progress(scraped_set)

    to_scrape = [inv for inv in all_urls if inv["slug"] not in scraped_set]

    log.info("=" * 60)
    log.info("  NFX SIGNAL - SAAS SCRAPER (HEADLESS SEQUENTIAL)")
    log.info(f"  Total:     {len(all_urls)}")
    log.info(f"  Done:      {len(scraped_set)}")
    log.info(f"  Remaining: {len(to_scrape)}")
    log.info(f"  Retries:   {MAX_RETRY_ROUNDS}")
    log.info("=" * 60)

    if not to_scrape:
        log.info("All done!")
        return

    driver = launch_chrome()
    if not login(driver):
        log.error("Initial login failed!")
        driver.quit()
        return

    # ── MAIN PASS ──
    driver, failed = run_pass(driver, to_scrape, scraped_set, url_lookup, "MAIN PASS")

    # ── RETRY ROUNDS ──
    for rnd in range(1, MAX_RETRY_ROUNDS + 1):
        retry_slugs = [s for s in set(failed) if s not in scraped_set]
        if not retry_slugs:
            log.info("No retries needed!")
            break

        log.info("")
        log.info(f"RETRY {rnd}/{MAX_RETRY_ROUNDS}: {len(retry_slugs)} profiles")

        # Longer cool-down before retry round
        time.sleep(30)

        retry_list = [{"slug": s, "url": url_lookup.get(s, f"https://signal.nfx.com/investors/{s}")}
                      for s in retry_slugs]
        driver, failed = run_pass(driver, retry_list, scraped_set, url_lookup, f"RETRY {rnd}")

    # ── FINAL REPORT ──
    final_failed = [s for s in set(failed) if s not in scraped_set]
    save_failed(final_failed, url_lookup)

    all_slugs = {inv["slug"] for inv in all_urls}
    on_disk = {f.replace(".json", "") for f in os.listdir(PROFILES_DIR) if f.endswith(".json")}

    log.info("")
    log.info("=" * 60)
    log.info("  COMPLETE")
    log.info(f"  Total URLs:       {len(all_slugs)}")
    log.info(f"  On disk:          {len(on_disk)}")
    log.info(f"  In progress:      {len(scraped_set)}")
    log.info(f"  Final failed:     {len(final_failed)}")
    log.info(f"  Output:           {PROFILES_DIR}")
    log.info("=" * 60)

    try:
        driver.quit()
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("\nStopped. Progress saved.")
    except Exception as e:
        log.error(f"CRASH: {e}")
        log.error(traceback.format_exc())
        try:
            existing = load_progress()
            if existing:
                save_progress(existing)
                log.info(f"Emergency save: {len(existing)}")
        except Exception:
            pass
