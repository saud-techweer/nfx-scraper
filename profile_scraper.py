"""
NFX Signal Profile Scraper - Detailed Profile Extraction
Extracts comprehensive investor data from a single profile page.
Run with: python profile_scraper.py
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
# CONFIGURATION
# =============================================================================
TEST_URL = "https://signal.nfx.com/investors/david-frankel_1"
OUTPUT_DIR = "data/profiles"


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def connect_to_chrome():
    """Connect to Chrome with remote debugging"""
    log("Connecting to Chrome on port 9222...")
    opts = Options()
    opts.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    driver = webdriver.Chrome(options=opts)
    log("Connected!")
    return driver


def scrape_profile(driver, url):
    """Scrape all data from an investor profile page"""
    log(f"Navigating to: {url}")
    driver.get(url)
    time.sleep(4)

    # Wait for page to load
    WebDriverWait(driver, 15).until(
        EC.presence_of_element_located((By.TAG_NAME, "h1"))
    )
    time.sleep(2)

    # Extract all data using JavaScript for reliability
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
        // Get just the name, not the number in parentheses
        const nameText = h1.textContent.trim();
        const match = nameText.match(/^(.+?)\s*\(\d+\)/);
        data.basicInfo.name = match ? match[1].trim() : nameText;

        // Get the number in parentheses (investments count indicator)
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

    // Position and Firm (e.g., "Managing Partner, Founder Collective")
    // Look for the specific div structure in identity-block
    const identityBlock = document.querySelector('.identity-block');
    if (identityBlock) {
        const positionDivs = identityBlock.querySelectorAll('.subheader.lower-subheader');
        positionDivs.forEach(div => {
            // Skip divs with links or icons (those are website/location rows)
            if (div.querySelector('a') || div.querySelector('.glyphicon')) return;

            const text = div.textContent.trim();
            // Position format: "Managing Partner, Founder Collective"
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

    // Location (look for map-marker icon)
    const locationSpan = document.querySelector('.glyphicon-map-marker');
    if (locationSpan && locationSpan.nextElementSibling) {
        data.basicInfo.location = locationSpan.nextElementSibling.textContent.trim();
    } else {
        // Alternative: find span with location text
        const allSpans = document.querySelectorAll('.subheader.lower-subheader span');
        allSpans.forEach(span => {
            const text = span.textContent.trim();
            // Location pattern: City, State
            if (/^[A-Z][a-z]+,\s*[A-Z][a-z]+/.test(text)) {
                data.basicInfo.location = text;
            }
        });
    }

    // =========================================================================
    // PROFILE PICTURE
    // =========================================================================
    // Check column 1 (left side) for profile image
    const col1 = document.querySelector('.col-sm-6.col-xs-12:first-child, main > div > div > div:first-child');
    if (col1) {
        const img = col1.querySelector('img');
        if (img && img.src) {
            data.profilePicture = img.src;
        }
    }

    // Check for background-image style (common for profile photos)
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

    // Fallback: any img with cloudinary or profile in URL
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

            // Clean up value - get just the text, not nested spans
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
        // Check if this is a co-investors row
        const coinvestorCell = row.querySelector('.coinvestors-row, td[colspan]');
        if (coinvestorCell) {
            // This is co-investors row
            if (currentInvestment) {
                const coInvestorsText = row.textContent.trim();
                const match = coInvestorsText.match(/Co-investors:\s*(.+)/i);
                if (match) {
                    currentInvestment.coInvestors = match[1].split(',').map(s => s.trim());
                }
            }
        } else {
            // Main investment row
            const cells = row.querySelectorAll('td');
            if (cells.length >= 2) {
                // Get the details cell - separators are <i> elements, not text
                const stageCell = cells[1];
                let stage = null, date = null, roundSize = null;

                if (stageCell) {
                    // Get the inner div which contains the text and separators
                    const innerDiv = stageCell.querySelector('.round-padding') || stageCell;

                    // Clone and remove separator elements to get clean text
                    const clone = innerDiv.cloneNode(true);
                    const separators = clone.querySelectorAll('i');

                    // Replace separators with a delimiter
                    separators.forEach(sep => {
                        sep.replaceWith(' ||| ');
                    });

                    // Now split by delimiter
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
    // Find the Experience section
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

                    // Parse "Position · Company" or "Position  Company" format
                    // Use various separators including Unicode middot
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

    return data


def save_profile(slug, data, output_dir):
    """Save profile data to JSON file"""
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"{slug}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return filepath


def main():
    log("=" * 60)
    log("NFX SIGNAL PROFILE SCRAPER")
    log("=" * 60)

    # Connect to Chrome
    driver = connect_to_chrome()

    try:
        # Scrape the test profile
        data = scrape_profile(driver, TEST_URL)

        # Extract slug for filename
        slug = data.get("slug", "unknown")

        # Save to file
        filepath = save_profile(slug, data, OUTPUT_DIR)
        log(f"Saved to: {filepath}")

        # Print summary
        log("\n" + "=" * 60)
        log("SCRAPED DATA SUMMARY")
        log("=" * 60)

        print(f"\nName: {data.get('basicInfo', {}).get('name', 'N/A')}")
        print(f"Types: {', '.join(data.get('basicInfo', {}).get('investorTypes', []))}")
        print(f"Position: {data.get('basicInfo', {}).get('positionAndFirm', 'N/A')}")
        print(f"Location: {data.get('basicInfo', {}).get('location', 'N/A')}")

        print(f"\nInvestment Range: {data.get('investingProfile', {}).get('investmentRange', 'N/A')}")
        print(f"Sweet Spot: {data.get('investingProfile', {}).get('sweetSpot', 'N/A')}")
        print(f"Fund Size: {data.get('investingProfile', {}).get('fundSize', 'N/A')}")
        print(f"Investments on Record: {data.get('investingProfile', {}).get('investmentsOnRecord', 'N/A')}")

        print(f"\nSector Rankings: {len(data.get('sectorRankings', []))} categories")
        print(f"Past Investments: {len(data.get('investments', []))} on record")
        print(f"Experience: {len(data.get('experience', []))} positions")

        print("\nSOCIAL LINKS:")
        socials = data.get('socials', {})
        for platform, url in socials.items():
            print(f"  {platform}: {url}")

        if data.get('profilePicture'):
            print(f"\nProfile Picture: {data.get('profilePicture')}")

        print("\n" + "=" * 60)
        print("Full data saved to JSON file")
        print("=" * 60)

    except Exception as e:
        log(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
