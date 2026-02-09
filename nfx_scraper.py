"""
NFX Signal Scraper - Simple & Resumable
Run with: python nfx_scraper.py
"""

import os
import json
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =============================================================================
# CONFIGURATION - Add your URLs here
# =============================================================================
URLS_TO_SCRAPE = [
    "https://signal.nfx.com/investor-lists/top-marketplaces-seed-investors",
    # "https://signal.nfx.com/investor-lists/top-saas-seed-investors",
    # "https://signal.nfx.com/investor-lists/top-fintech-seed-investors",
    # "https://signal.nfx.com/investor-lists/top-enterprise-seed-investors",
]

# Folders
DATA_DIR = "data"
PROFILES_DIR = os.path.join(DATA_DIR, "profiles")
LISTS_DIR = os.path.join(DATA_DIR, "lists")
PROGRESS_FILE = os.path.join(DATA_DIR, "progress.json")

# Limits (for testing, set to 10. Remove limit by setting to None)
MAX_PROFILES = None

# Waits
WAIT_AFTER_LOAD_MORE = 5  # seconds
WAIT_AFTER_100_PROFILES = 10  # 10 seconds


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_progress():
    """Load already scraped profile slugs"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"scraped": [], "current_url": None, "current_page_index": 0}


def save_progress(progress):
    """Save progress to file"""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def save_profile(slug, data):
    """Save profile JSON to file"""
    filepath = os.path.join(PROFILES_DIR, f"{slug}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def connect_to_chrome():
    """Connect to Chrome with remote debugging"""
    log("Connecting to Chrome on port 9222...")
    opts = Options()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(options=opts)
    log("Connected!")
    return driver


def get_investor_links(driver):
    """Get all investor profile links from the current page"""
    links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/investors/']")
    investors = []
    seen = set()

    for link in links:
        href = link.get_attribute("href")
        if href and "/investors/" in href and href not in seen:
            slug = href.split("/investors/")[-1].split("?")[0]
            if slug and not slug.startswith("http"):
                investors.append({"slug": slug, "url": href})
                seen.add(href)

    return investors


def click_load_more(driver):
    """Click the LOAD MORE button if present"""
    try:
        buttons = driver.find_elements(By.TAG_NAME, "button")
        for btn in buttons:
            if "LOAD MORE" in btn.text.upper():
                log("Clicking LOAD MORE...")
                btn.click()
                log(f"Waiting {WAIT_AFTER_LOAD_MORE} seconds...")
                time.sleep(WAIT_AFTER_LOAD_MORE)
                return True
    except Exception as e:
        log(f"Load more error: {e}")
    return False


def scrape_profile(driver, url, original_window):
    """Open profile in new tab, scrape, then close tab and return to list"""
    try:
        # Open a new tab using Selenium's built-in method (more reliable than JS)
        driver.switch_to.new_window('tab')
        time.sleep(1)

        # Navigate to the profile URL in the new tab
        driver.get(url)
        time.sleep(4)

        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "h1"))
        )
        time.sleep(2)

        # Extract data using JavaScript - comprehensive extraction
        script = r"""
        const data = {
            basicInfo: {},
            investingProfile: {},
            sectorRankings: [],
            investments: [],
            experience: [],
            socials: {},
            profilePicture: null
        };

        // =========================================================================
        // BASIC INFO - Name, Types, Position, Location
        // =========================================================================

        // Name (from h1)
        const h1 = document.querySelector('h1.f3.f1-ns, h1');
        if (h1) {
            const nameText = h1.textContent.trim();
            const match = nameText.match(/^(.+?)\s*\(\d+\)/);
            data.basicInfo.name = match ? match[1].trim() : nameText;

            const numMatch = nameText.match(/\((\d+)\)/);
            if (numMatch) data.basicInfo.signalScore = parseInt(numMatch[1]);
        }

        // Investor Types (VC, Angel, Investor, etc.)
        const typesDiv = document.querySelector('.subheader.white-subheader.b');
        if (typesDiv) {
            const types = [];
            typesDiv.querySelectorAll('span').forEach(span => {
                const text = span.textContent.trim();
                if (text && !text.includes('middot') && text.length > 0) {
                    types.push(text);
                }
            });
            data.basicInfo.investorTypes = types.filter(t => t.length > 0);
        }

        // Position and Firm
        const identityBlock = document.querySelector('.identity-block');
        if (identityBlock) {
            const positionDivs = identityBlock.querySelectorAll('.subheader.lower-subheader');
            positionDivs.forEach(div => {
                if (div.querySelector('a') || div.querySelector('.glyphicon')) return;
                const text = div.textContent.trim();
                if (text && text.includes(',') && !text.includes('http')) {
                    data.basicInfo.positionAndFirm = text;
                }
            });
        }

        // Website link
        const websiteLink = document.querySelector('a.subheader.lower-subheader[href]');
        if (websiteLink) {
            data.basicInfo.website = websiteLink.href;
        }

        // Location
        const locationSpan = document.querySelector('.glyphicon-map-marker');
        if (locationSpan && locationSpan.nextElementSibling) {
            data.basicInfo.location = locationSpan.nextElementSibling.textContent.trim();
        } else {
            const allSpans = document.querySelectorAll('.subheader.lower-subheader span');
            allSpans.forEach(span => {
                const text = span.textContent.trim();
                if (/^[A-Z][a-z]+,\s*[A-Z][a-z]+/.test(text)) {
                    data.basicInfo.location = text;
                }
            });
        }

        // =========================================================================
        // PROFILE PICTURE
        // =========================================================================
        const col1 = document.querySelector('.col-sm-6.col-xs-12:first-child, main > div > div > div:first-child');
        if (col1) {
            const img = col1.querySelector('img');
            if (img && img.src) {
                data.profilePicture = img.src;
            }
        }

        if (!data.profilePicture) {
            const imgContainers = document.querySelectorAll('[style*="background-image"]');
            imgContainers.forEach(container => {
                const style = container.getAttribute('style');
                const match = style.match(/url\(['"]?([^'")\s]+)['"]?\)/);
                if (match) {
                    data.profilePicture = match[1];
                }
            });
        }

        if (!data.profilePicture) {
            const allImages = document.querySelectorAll('img[src*="cloudinary"], img[src*="profile"], img[src*="avatar"]');
            if (allImages.length > 0) {
                data.profilePicture = allImages[0].src;
            }
        }

        // =========================================================================
        // INVESTING PROFILE - Investment Range, Sweet Spot, Fund Size, etc.
        // =========================================================================
        const rows = document.querySelectorAll('.line-separated-row.row');
        rows.forEach(row => {
            const label = row.querySelector('.section-label, .col-xs-5 span');
            const value = row.querySelector('.col-xs-7 span, .col-xs-7');

            if (label && value) {
                const labelText = label.textContent.trim().toLowerCase();
                let valueText = value.textContent.trim();

                if (value.querySelector('a')) {
                    const link = value.querySelector('a');
                    const position = value.textContent.replace(link.textContent, '').trim();
                    valueText = {
                        firm: link.textContent.trim(),
                        firmUrl: link.href,
                        position: position.replace(/[·•]/g, '').trim()
                    };
                }

                if (labelText.includes('current investing position')) {
                    data.investingProfile.currentPosition = valueText;
                } else if (labelText.includes('investment range')) {
                    data.investingProfile.investmentRange = valueText;
                } else if (labelText.includes('sweet spot')) {
                    data.investingProfile.sweetSpot = valueText;
                } else if (labelText.includes('investments on record')) {
                    data.investingProfile.investmentsOnRecord = parseInt(valueText) || valueText;
                } else if (labelText.includes('fund size')) {
                    data.investingProfile.fundSize = valueText;
                }
            }
        });

        // =========================================================================
        // SECTOR & STAGE RANKINGS
        // =========================================================================
        const rankingChips = document.querySelectorAll('a.vc-list-chip');
        rankingChips.forEach(chip => {
            data.sectorRankings.push({
                name: chip.textContent.trim(),
                url: chip.href
            });
        });

        // =========================================================================
        // PAST INVESTMENTS
        // =========================================================================
        const investmentRows = document.querySelectorAll('.past-investments-table-body tr');
        let currentInvestment = null;

        investmentRows.forEach(row => {
            const coinvestorCell = row.querySelector('.coinvestors-row, td[colspan]');
            if (coinvestorCell) {
                if (currentInvestment) {
                    const coInvestorsText = row.textContent.trim();
                    const match = coInvestorsText.match(/Co-investors:\s*(.+)/i);
                    if (match) {
                        currentInvestment.coInvestors = match[1].split(',').map(s => s.trim());
                    }
                }
            } else {
                const cells = row.querySelectorAll('td');
                if (cells.length >= 2) {
                    const stageCell = cells[1];
                    let stage = null, date = null, roundSize = null;

                    if (stageCell) {
                        const innerDiv = stageCell.querySelector('.round-padding') || stageCell;
                        const clone = innerDiv.cloneNode(true);
                        const separators = clone.querySelectorAll('i');
                        separators.forEach(sep => {
                            sep.replaceWith(' ||| ');
                        });
                        const parts = clone.textContent.split('|||').map(p => p.trim()).filter(p => p);

                        if (parts.length >= 1) stage = parts[0];
                        if (parts.length >= 2) date = parts[1];
                        if (parts.length >= 3) roundSize = parts[2];
                    }

                    currentInvestment = {
                        company: cells[0]?.textContent?.trim() || null,
                        stage: stage,
                        date: date,
                        roundSize: roundSize,
                        totalRaised: cells[2]?.textContent?.trim() || null,
                        coInvestors: []
                    };

                    data.investments.push(currentInvestment);
                }
            }
        });

        // =========================================================================
        // EXPERIENCE
        // =========================================================================
        const experienceLabel = Array.from(document.querySelectorAll('.section-label')).find(
            el => el.textContent.includes('Experience')
        );

        if (experienceLabel) {
            const experienceSection = experienceLabel.closest('.sn-margin-top-30');
            if (experienceSection) {
                const experienceRows = experienceSection.querySelectorAll('.line-separated-row.flex');
                experienceRows.forEach(row => {
                    const mainSpan = row.querySelector('span:first-child');
                    const dateSpan = row.querySelector('span[style*="text-align"]');

                    if (mainSpan) {
                        const fullText = mainSpan.textContent.trim();
                        const dateText = dateSpan?.textContent?.trim() || null;

                        const parts = fullText.split(/\s*[·•\u00B7\u2022]\s*|\s{2,}/);

                        if (parts.length >= 2) {
                            data.experience.push({
                                position: parts[0]?.trim(),
                                company: parts[1]?.trim(),
                                dates: dateText
                            });
                        } else {
                            data.experience.push({
                                title: fullText,
                                dates: dateText
                            });
                        }
                    }
                });
            }
        }

        // =========================================================================
        // SOCIAL LINKS - Critical section
        // =========================================================================
        const socialContainer = document.querySelector('.sn-linkset');
        if (socialContainer) {
            const socialLinks = socialContainer.querySelectorAll('a.iconlink');
            socialLinks.forEach(link => {
                const href = link.href;
                const icon = link.querySelector('i');
                const iconClass = icon?.className || link.textContent.trim();

                if (href.includes('linkedin.com')) {
                    data.socials.linkedin = href;
                } else if (href.includes('twitter.com') || href.includes('x.com')) {
                    data.socials.twitter = href;
                } else if (href.includes('angel.co') || href.includes('angellist')) {
                    data.socials.angellist = href;
                } else if (href.includes('crunchbase.com')) {
                    data.socials.crunchbase = href;
                } else if (iconClass.includes('globe') || (!href.includes('linkedin') && !href.includes('twitter') && !href.includes('angel') && !href.includes('crunchbase'))) {
                    data.socials.website = href;
                }
            });
        }

        // Fallback: search all links for socials
        if (Object.keys(data.socials).length === 0) {
            const allLinks = document.querySelectorAll('a[href]');
            allLinks.forEach(link => {
                const href = link.href;
                if (href.includes('linkedin.com/in/')) data.socials.linkedin = href;
                if (href.includes('twitter.com/') || href.includes('x.com/')) data.socials.twitter = href;
                if (href.includes('angel.co/')) data.socials.angellist = href;
                if (href.includes('crunchbase.com/person/')) data.socials.crunchbase = href;
            });
        }

        // =========================================================================
        // ADDITIONAL DATA
        // =========================================================================
        data.profileUrl = window.location.href;
        data.slug = window.location.pathname.split('/investors/')[1] || null;

        return data;
        """

        data = driver.execute_script(script)
        data["scraped_at"] = datetime.now().isoformat()

        # Close the profile tab and switch back to the list tab
        driver.close()
        driver.switch_to.window(original_window)

        return data

    except Exception as e:
        log(f"Error scraping profile: {e}")
        # Make sure we close any extra tabs and return to original window
        try:
            if len(driver.window_handles) > 1:
                driver.close()
            driver.switch_to.window(original_window)
        except:
            pass
        return {"error": str(e), "url": url}


def main():
    log("=" * 60)
    log("NFX SIGNAL SCRAPER")
    log("=" * 60)

    # Ensure folders exist
    os.makedirs(PROFILES_DIR, exist_ok=True)
    os.makedirs(LISTS_DIR, exist_ok=True)

    # Load progress
    progress = load_progress()
    log(f"Already scraped: {len(progress['scraped'])} profiles")

    # Connect to Chrome
    driver = connect_to_chrome()

    total_scraped = 0

    for list_url in URLS_TO_SCRAPE:
        log(f"\n>>> Scraping: {list_url}")

        # Navigate to list page
        driver.get(list_url)
        time.sleep(3)

        # Wait for investor links to appear
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/investors/']"))
            )
            time.sleep(2)
        except Exception as e:
            log(f"Warning: Could not find investor links, continuing anyway: {e}")

        # Store the list tab handle so we can return to it
        list_tab = driver.current_window_handle

        while True:
            # Get investor links on current page
            investors = get_investor_links(driver)
            log(f"Found {len(investors)} investors on page")

            # Filter out already scraped
            to_scrape = [inv for inv in investors if inv["slug"] not in progress["scraped"]]
            log(f"New to scrape: {len(to_scrape)}")

            if not to_scrape:
                # Try loading more
                if not click_load_more(driver):
                    log("No more investors to load")
                    break
                continue

            # Scrape each profile
            for inv in to_scrape:
                if MAX_PROFILES and total_scraped >= MAX_PROFILES:
                    log(f"\nReached limit of {MAX_PROFILES} profiles")
                    break

                slug = inv["slug"]
                log(f"\n[{total_scraped + 1}] Scraping: {slug}")

                # Scrape profile (opens in new tab, scrapes, then closes tab)
                profile_data = scrape_profile(driver, inv["url"], list_tab)
                profile_data["slug"] = slug

                # Save profile JSON
                save_profile(slug, profile_data)
                log(f"  Saved: {slug}.json")

                # Update progress
                progress["scraped"].append(slug)
                save_progress(progress)

                total_scraped += 1

                # Wait after 100 profiles
                if total_scraped % 100 == 0:
                    log(f"\n*** Pausing for {WAIT_AFTER_100_PROFILES // 60} minutes after {total_scraped} profiles ***")
                    time.sleep(WAIT_AFTER_100_PROFILES)

                # Small delay before next profile (list tab stays open)
                time.sleep(2)

            if MAX_PROFILES and total_scraped >= MAX_PROFILES:
                break

            # Try to load more
            if not click_load_more(driver):
                log("No more LOAD MORE button found")
                break

        if MAX_PROFILES and total_scraped >= MAX_PROFILES:
            break

    log("\n" + "=" * 60)
    log("COMPLETE!")
    log(f"Total profiles scraped: {total_scraped}")
    log(f"Saved to: {PROFILES_DIR}")
    log("=" * 60)


if __name__ == "__main__":
    main()
