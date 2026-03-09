#!/usr/bin/env python3
"""
NFX Signal - Fintech Seed Profile Scraper (Connects to Real Chrome via CDP)
============================================================================
Connects to an already-running Chrome with --remote-debugging-port=9222.
This bypasses Cloudflare because it uses your real, logged-in browser.

HOW TO START CHROME (run once before this script):
  pkill -a "Google Chrome"
  /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome \\
    --remote-debugging-port=9222 \\
    --user-data-dir="$HOME/Library/Application Support/Google/Chrome"
  Then navigate to signal.nfx.com and confirm you're logged in.

- INSTANT skip on failure — no per-failure 15s stalls
- One bulk pause (30s) every 5 consecutive fails, then keeps moving
- Saves progress every 10 successes
- Up to 8 retry rounds for failures (shuffled to vary request pattern)
"""

import os
import json
import time
import logging
import traceback
import random
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, InvalidSessionIdException, WebDriverException

# =============================================================================
# CONFIG
# =============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR      = os.path.join(SCRIPT_DIR, "data-fintech-seed")
PROFILES_DIR  = os.path.join(DATA_DIR, "profiles")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")
FAILED_FILE   = os.path.join(DATA_DIR, "failed_profiles.json")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")
LOG_FILE      = os.path.join(SCRIPT_DIR, "scraper_fintech.log")

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
NFX_EMAIL    = os.getenv("email", "").strip()
NFX_PASSWORD = os.getenv("password", "").strip()

PAGE_LOAD_WAIT        = 12
SUCCESS_DELAY         = 0.5
CONSEC_FAIL_PAUSE     = 30
CONSEC_FAIL_THRESHOLD = 5
SAVE_EVERY            = 10
MAX_RETRY_ROUNDS      = 8

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

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

GARBAGE_NAMES = [
    "signal.nfx.com", "nfx signal", "bad gateway", "bad request",
    "error code", "not found", "forbidden", "server error",
    "service unavailable", "access denied", "this page",
    "isn't working", "not available", "timed out",
    "too many requests", "cloudflare", "press space", "to play",
    "loading", "just a moment", "please wait", "captcha",
    "verify you", "checking your browser",
]


def is_valid_profile(data):
    if not data or not isinstance(data, dict):
        return False
    name = (data.get("basicInfo") or {}).get("name", "") or ""
    name_lower = name.strip().lower()
    if len(name_lower) < 2:
        return False
    for g in GARBAGE_NAMES:
        if g in name_lower:
            return False
    if name_lower[:3] in ("400", "401", "403", "404", "429", "500", "502", "503"):
        return False
    return True


def is_session_expired(driver):
    """Returns True if current page is login/auth/CF challenge."""
    try:
        url = driver.current_url
        bad = ["auth.nfx.com", "/login", "challenge", "captcha"]
        if any(b in url for b in bad):
            return True
    except Exception:
        pass
    return False


# =============================================================================
# FILE I/O
# =============================================================================
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE) as f:
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


# =============================================================================
# CHROME — headless with auto-login
# =============================================================================
def launch_chrome(ua=None):
    ua = ua or random.choice(USER_AGENTS)
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--disable-backgrounding-occluded-windows")
    opts.add_argument("--js-flags=--max-old-space-size=512")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => false});"
    })
    driver.set_page_load_timeout(30)
    return driver


def login(driver):
    log.info("Logging in to NFX Signal...")
    driver.get("https://signal.nfx.com/login")
    time.sleep(4)

    # Click the LOGIN link — redirects to auth.nfx.com
    for a in driver.find_elements(By.TAG_NAME, "a"):
        if "LOGIN" in a.text.strip().upper():
            a.click()
            break

    # Wait for auth0 email input
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "email"))
        )
    except TimeoutException:
        log.error(f"Login form not found. URL: {driver.current_url} Title: {driver.title}")
        return False

    try:
        driver.find_element(By.ID, "email").send_keys(NFX_EMAIL)
        time.sleep(0.3)
        driver.find_element(By.ID, "password").send_keys(NFX_PASSWORD)
        time.sleep(0.3)
    except Exception as e:
        log.error(f"Could not fill login form: {e}")
        return False

    # Submit
    for btn in driver.find_elements(By.TAG_NAME, "button"):
        t = btn.text.strip().lower()
        if ("log in" in t or "sign in" in t or "continue" in t) and "google" not in t:
            btn.click()
            break

    # Wait for redirect back to signal.nfx.com
    for _ in range(25):
        time.sleep(1)
        url = driver.current_url
        if "signal.nfx.com" in url and "/login" not in url and "auth.nfx.com" not in url:
            log.info(f"Login OK: {url}")
            return True

    log.error(f"Login failed. URL: {driver.current_url}")
    return False


def connect_to_chrome():
    """Launch headless Chrome and login."""
    driver = launch_chrome()
    if not login(driver):
        try: driver.quit()
        except: pass
        raise Exception("Login failed — check credentials in .env")
    return driver


def is_alive(driver):
    try:
        _ = driver.current_url
        return True
    except Exception:
        return False


# =============================================================================
# SINGLE PROFILE
# =============================================================================
def scrape_one(driver, slug, url):
    try:
        driver.get(url)
    except TimeoutException:
        return None, "nav_timeout"
    except Exception as e:
        return None, f"nav_error:{str(e)[:50]}"

    # Check for session expiry immediately after nav
    if is_session_expired(driver):
        return None, "session_expired"

    try:
        WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            lambda d: d.execute_script("""
                var h1 = document.querySelector('h1.f3.f1-ns, h1');
                if (!h1) return false;
                var text = h1.textContent.replace(/[\\u200B-\\u200D\\uFEFF]/g, '').trim();
                return text.length > 2;
            """)
        )
    except (InvalidSessionIdException, WebDriverException) as e:
        msg = str(e)[:60]
        if "invalid session" in msg.lower() or "session" in msg.lower():
            return None, "session_expired"
        return None, f"wd_error:{msg}"
    except Exception:
        pass

    time.sleep(0.4)

    try:
        data = driver.execute_script(SCRAPE_JS)
        data["scraped_at"] = datetime.now().isoformat()
        data["slug"] = slug
        return data, None
    except Exception as e:
        return None, f"js_error:{str(e)[:50]}"


# =============================================================================
# PASS RUNNER
# =============================================================================
def run_pass(driver, to_scrape, scraped_set, url_lookup, pass_name="MAIN"):
    total = len(to_scrape)
    failed_slugs = []
    ok_count = 0
    save_counter = 0
    consec_fails = 0

    log.info(f"  {pass_name}: {total} profiles")

    for i, inv in enumerate(to_scrape):
        slug = inv["slug"]
        url  = inv["url"]

        if slug in scraped_set:
            continue

        if not is_alive(driver):
            log.warning("Chrome died! Restarting with new UA...")
            try: driver.quit()
            except: pass
            try:
                driver = connect_to_chrome()
            except Exception as e:
                log.error(f"Reconnect failed: {e}")
                failed_slugs.extend(x["slug"] for x in to_scrape[i:] if x["slug"] not in scraped_set)
                break

        try:
            data, error = scrape_one(driver, slug, url)
        except (InvalidSessionIdException, WebDriverException) as e:
            data, error = None, "session_expired"
        except Exception as e:
            data, error = None, f"unexpected:{str(e)[:60]}"

        # Session expired → re-login with fresh UA
        if error == "session_expired":
            log.warning(f"  Session expired at {slug} — re-logging in with new UA...")
            try: driver.quit()
            except: pass
            time.sleep(5)
            try:
                driver = launch_chrome(random.choice(USER_AGENTS))
                if login(driver):
                    data, error = scrape_one(driver, slug, url)
                else:
                    log.error("Re-login failed!")
                    failed_slugs.extend(x["slug"] for x in to_scrape[i:] if x["slug"] not in scraped_set)
                    break
            except Exception as e:
                log.error(f"Re-login crashed: {e}")
                failed_slugs.extend(x["slug"] for x in to_scrape[i:] if x["slug"] not in scraped_set)
                break

        if data and is_valid_profile(data):
            if save_profile(slug, data):
                scraped_set.add(slug)
                ok_count += 1
                save_counter += 1
                consec_fails = 0
                name = (data.get("basicInfo") or {}).get("name", "?")

                if ok_count % 25 == 0:
                    log.info(f"  [{ok_count}/{total}] OK {slug} ({name}) | total={len(scraped_set)}")
                else:
                    log.info(f"  OK {slug} ({name})")

                if save_counter >= SAVE_EVERY:
                    save_progress(scraped_set)
                    save_counter = 0

                time.sleep(SUCCESS_DELAY + random.uniform(0, 0.3))
            else:
                failed_slugs.append(slug)
        else:
            err = error or f"invalid:{(data or {}).get('basicInfo', {}).get('name','') if data else 'nodata'}"
            log.warning(f"  FAIL {slug}: {err}")
            failed_slugs.append(slug)
            consec_fails += 1

            # One bulk pause every CONSEC_FAIL_THRESHOLD consecutive fails — then keep moving
            if consec_fails > 0 and consec_fails % CONSEC_FAIL_THRESHOLD == 0:
                log.warning(f"  {consec_fails} consecutive fails — pausing {CONSEC_FAIL_PAUSE}s then continuing...")
                time.sleep(CONSEC_FAIL_PAUSE)
            # NO per-failure delay — move to next immediately

    save_progress(scraped_set)
    log.info(f"  {pass_name} done: {ok_count} OK, {len(failed_slugs)} failed")
    try: driver.quit()
    except: pass
    return failed_slugs


# =============================================================================
# MAIN
# =============================================================================
def main():
    os.makedirs(PROFILES_DIR, exist_ok=True)

    with open(ALL_URLS_FILE) as f:
        all_urls = json.load(f)

    url_lookup = {inv["slug"]: inv["url"] for inv in all_urls}

    # Sync progress with disk
    scraped_set = load_progress()
    for inv in all_urls:
        if os.path.exists(os.path.join(PROFILES_DIR, f"{inv['slug']}.json")):
            scraped_set.add(inv["slug"])
    save_progress(scraped_set)

    to_scrape = [inv for inv in all_urls if inv["slug"] not in scraped_set]

    log.info("=" * 60)
    log.info("  NFX SIGNAL - FINTECH SEED SCRAPER (REAL CHROME CDP)")
    log.info(f"  Total URLs:   {len(all_urls)}")
    log.info(f"  Already done: {len(scraped_set)}")
    log.info(f"  Remaining:    {len(to_scrape)}")
    log.info(f"  Retry rounds: {MAX_RETRY_ROUNDS}")
    log.info("=" * 60)

    if not to_scrape:
        log.info("All done!")
        return

    # MAIN PASS
    failed = run_pass(connect_to_chrome(), to_scrape, scraped_set, url_lookup, "MAIN PASS")

    # RETRY ROUNDS
    for rnd in range(1, MAX_RETRY_ROUNDS + 1):
        retry_slugs = list({s for s in failed if s not in scraped_set})
        if not retry_slugs:
            log.info("No retries needed!")
            break

        log.info("")
        log.info(f"RETRY {rnd}/{MAX_RETRY_ROUNDS}: {len(retry_slugs)} profiles")
        time.sleep(20)

        retry_list = [{"slug": s, "url": url_lookup.get(s, f"https://signal.nfx.com/investors/{s}")}
                      for s in retry_slugs]
        random.shuffle(retry_list)
        failed = run_pass(connect_to_chrome(), retry_list, scraped_set, url_lookup, f"RETRY {rnd}")

    # FINAL REPORT
    final_failed = [s for s in set(failed) if s not in scraped_set]
    save_failed(final_failed, url_lookup)

    on_disk = sum(1 for f in os.listdir(PROFILES_DIR) if f.endswith(".json"))

    log.info("")
    log.info("=" * 60)
    log.info("  COMPLETE")
    log.info(f"  Total URLs:    {len(all_urls)}")
    log.info(f"  On disk:       {on_disk}")
    log.info(f"  In progress:   {len(scraped_set)}")
    log.info(f"  Final failed:  {len(final_failed)}")
    log.info(f"  Output:        {PROFILES_DIR}")
    log.info("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("\nStopped. Progress saved.")
    except Exception as e:
        log.error(f"CRASH: {e}")
        log.error(traceback.format_exc())
