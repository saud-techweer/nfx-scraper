#!/usr/bin/env python3
"""
NFX Signal - URL Collector
===========================
Clicks "Load More" until 3100+ URLs collected. NEVER reloads. NEVER quits early.
Saves incrementally. If it crashes, bat file restarts and it picks up.

Run locally:   collect_urls.bat
Run on server:  python collect_urls.py --server
"""

import asyncio
import json
import os
import sys
import logging
import traceback

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    print("\nPlaywright not installed. Run:\n")
    print("   pip install playwright")
    print("   playwright install chromium\n")
    sys.exit(1)


# =============================================================================
# CONFIG
# =============================================================================
LIST_URL = "https://signal.nfx.com/investor-lists/top-marketplaces-seed-investors"
MIN_URLS = 3100

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
ALL_URLS_FILE = os.path.join(DATA_DIR, "all_investor_urls.json")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collector.log")

SAVE_EVERY = 5

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
# URL FILE I/O
# =============================================================================
def load_existing_urls():
    if os.path.exists(ALL_URLS_FILE):
        try:
            with open(ALL_URLS_FILE, "r") as f:
                data = json.load(f)
                return {item["slug"]: item["url"] for item in data}
        except Exception:
            pass
    return {}


def save_urls(url_dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    investor_list = [{"slug": s, "url": u} for s, u in url_dict.items()]
    with open(ALL_URLS_FILE, "w") as f:
        json.dump(investor_list, f, indent=2)


COLLECT_JS = """
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
"""


# =============================================================================
# COLLECTOR
# =============================================================================
async def collect(list_url, server_mode=False):
    all_investors = load_existing_urls()
    start_count = len(all_investors)
    logger.info(f"Loaded {start_count} existing URLs from disk")

    if start_count >= MIN_URLS:
        logger.info(f"Already have {start_count} URLs (>= {MIN_URLS}). Done!")
        return True

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=server_mode,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
        )
        page = await context.new_page()

        # --- LOGIN ---
        if not server_mode:
            logger.info("Opening NFX Signal for login...")
            await page.goto("https://signal.nfx.com/login", wait_until="domcontentloaded", timeout=60000)
            logger.info("")
            logger.info("=" * 60)
            logger.info("  LOG IN MANUALLY in the browser window.")
            logger.info("  After you're logged in, press ENTER here.")
            logger.info("=" * 60)
            logger.info("")
            await asyncio.get_event_loop().run_in_executor(None, input, "  >> Press ENTER after logging in... ")
            logger.info("Login done.\n")

        # --- NAVIGATE TO LIST ---
        logger.info(f"Loading: {list_url}")
        await page.goto(list_url, wait_until="domcontentloaded", timeout=60000)

        try:
            await page.wait_for_selector("a[href*='/investors/']", timeout=30000)
        except PlaywrightTimeout:
            logger.error("No investor links on page! Check login.")
            await browser.close()
            return False

        await page.wait_for_timeout(3000)

        # --- CLICK LOAD MORE FOREVER ---
        clicks = 0
        no_button_streak = 0

        logger.info(f"Target: {MIN_URLS} URLs. Have: {len(all_investors)}. Clicking Load More...\n")

        while True:
            # Collect visible links
            try:
                links = await page.evaluate(COLLECT_JS)
                if links:
                    all_investors.update(links)
            except Exception as e:
                logger.warning(f"Error collecting links: {e}")

            total = len(all_investors)

            # === DONE CHECK: only exit if we have enough ===
            if total >= MIN_URLS:
                save_urls(all_investors)
                logger.info(f"  HIT TARGET! {total} URLs (>= {MIN_URLS}). Final sweep...")

                # Few more clicks to squeeze out any remaining
                for _ in range(15):
                    clicked = await _click_load_more(page)
                    if not clicked:
                        await page.wait_for_timeout(5000)
                        continue
                    await page.wait_for_timeout(3500)
                    try:
                        links = await page.evaluate(COLLECT_JS)
                        if links:
                            all_investors.update(links)
                    except Exception:
                        pass

                save_urls(all_investors)
                break

            # === CLICK LOAD MORE ===
            clicked = await _click_load_more(page)

            if clicked:
                no_button_streak = 0
                clicks += 1
                await page.wait_for_timeout(3500)

                if clicks % SAVE_EVERY == 0:
                    save_urls(all_investors)
                    logger.info(f"  Click #{clicks} | {total} URLs | saved")
                elif clicks % 10 == 0:
                    logger.info(f"  Click #{clicks} | {total} URLs")
            else:
                # Button not found. DO NOT RELOAD. Just wait and retry.
                no_button_streak += 1

                if no_button_streak <= 3:
                    # Short wait — button might just be loading
                    await page.wait_for_timeout(5000)
                elif no_button_streak <= 10:
                    # Medium wait — scroll down, give it time
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(8000)
                else:
                    # Long wait — button is really gone, wait 15s and scroll
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(15000)

                if no_button_streak % 10 == 0:
                    save_urls(all_investors)
                    logger.warning(
                        f"  No button for {no_button_streak} rounds. "
                        f"Have {total}/{MIN_URLS}. Waiting and retrying..."
                    )

        # --- FINAL SAVE ---
        save_urls(all_investors)
        new_this_run = len(all_investors) - start_count

        logger.info("")
        logger.info("=" * 60)
        logger.info(f"  URL COLLECTION COMPLETE")
        logger.info(f"  Total URLs: {len(all_investors)}")
        logger.info(f"  New this run: {new_this_run}")
        logger.info(f"  Clicks: {clicks}")
        logger.info(f"  Saved to: {ALL_URLS_FILE}")
        logger.info("=" * 60)

        await browser.close()
        return True


async def _click_load_more(page):
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
                return True
        except Exception:
            continue
    return False


# =============================================================================
# ENTRY POINT
# =============================================================================
def main():
    server_mode = "--server" in sys.argv

    logger.info("=" * 60)
    logger.info(f"  NFX SIGNAL - URL COLLECTOR {'(SERVER)' if server_mode else '(LOCAL)'}")
    logger.info(f"  Target: {MIN_URLS}+ URLs")
    logger.info("=" * 60)

    try:
        success = asyncio.run(collect(LIST_URL, server_mode=server_mode))
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nStopped by user. Progress saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"CRASH: {e}")
        logger.error(traceback.format_exc())
        try:
            existing = load_existing_urls()
            if existing:
                save_urls(existing)
                logger.info(f"Emergency save: {len(existing)} URLs preserved.")
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
