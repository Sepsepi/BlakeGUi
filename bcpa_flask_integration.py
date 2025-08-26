"""
BCPA Flask Integration - Reverse Address Search
Integrates with existing Flask app for reverse address lookup from BCPA website
"""

import pandas as pd
import re
import time
from datetime import datetime
from pathlib import Path
import logging
from flask import current_app
import os

def read_data_file(filepath, encoding='utf-8', sheet_name=0, **kwargs):
    """
    Universal file reader for CSV, Excel (.xlsx), and Excel (.xls) files

    Args:
        filepath: Path to the file
        encoding: Encoding for CSV files (default: utf-8)
        sheet_name: Sheet name or index for Excel files (default: 0 - first sheet)
        **kwargs: Additional arguments passed to pandas read functions

    Returns:
        pd.DataFrame: Loaded data
    """
    try:
        if filepath.endswith('.csv'):
            return pd.read_csv(filepath, encoding=encoding, **kwargs)
        elif filepath.endswith(('.xlsx', '.xls')):
            return pd.read_excel(filepath, sheet_name=sheet_name, **kwargs)
        else:
            # Fallback to CSV
            logging.warning(f"Unknown file extension for {filepath}, trying CSV format")
            return pd.read_csv(filepath, encoding=encoding, **kwargs)
    except Exception as e:
        logging.error(f"Error reading file {filepath}: {e}")
        raise

# Try to import playwright, but make it optional for now
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

logger = logging.getLogger(__name__)

class BCPAAddressFormatter:
    """Handles address formatting for BCPA search from different CSV formats"""

    @staticmethod
    def detect_csv_format(df):
        """Detect the CSV format based on column names and structure"""
        columns = df.columns.tolist()
        logger.info(f"Analyzing columns: {columns}")

        # Format 1: Structured format with separate address components
        # ["Owner Name 1","Owner Name 2","House Number","Prefix Direction","Street Name","Street Type",...]
        structured_cols = ["House Number", "Street Name", "Street Type", "City Name"]
        if all(col in columns for col in structured_cols):
            logger.info("Detected structured address format")
            return "structured"

        # Format 2: Simple format with address in single column
        # Usually has columns like: ID, Status, Price, Address, City...
        elif len(columns) >= 4:
            # Check if 4th column contains address-like data
            sample_addr = str(df.iloc[0, 3]) if len(df) > 0 else ""
            if any(indicator in sample_addr.upper() for indicator in ["DRIVE", "AVE", "ST ", "ROAD", "CIRCLE", "WAY", "LANE"]):
                logger.info("Detected simple address format")
                return "simple"

        logger.warning("Unknown CSV format detected")
        return "unknown"

    @staticmethod
    def format_structured_address(row):
        """Format address from structured CSV format (Hollywood/Broward files)"""
        try:
            address_parts = []

            # House number
            house_num = str(row.get("House Number", "")).strip()
            if house_num and house_num not in ["nan", "None", ""]:
                address_parts.append(house_num)

            # Prefix direction (SW, NW, etc.)
            prefix_dir = str(row.get("Prefix Direction", "")).strip()
            if prefix_dir and prefix_dir not in ["nan", "None", ""]:
                address_parts.append(prefix_dir)

            # Street name
            street_name = str(row.get("Street Name", "")).strip()
            if street_name and street_name not in ["nan", "None", ""]:
                address_parts.append(street_name)

            # Street type (AVE, ST, TER, etc.)
            street_type = str(row.get("Street Type", "")).strip()
            if street_type and street_type not in ["nan", "None", ""]:
                address_parts.append(street_type)

            # Post direction
            post_dir = str(row.get("Post Direction", "")).strip()
            if post_dir and post_dir not in ["nan", "None", ""]:
                address_parts.append(post_dir)

            # Unit information
            unit_type = str(row.get("Unit Type", "")).strip()
            unit_number = str(row.get("Unit Number", "")).strip()

            if (unit_type and unit_type not in ["nan", "None", ""] and
                unit_number and unit_number not in ["nan", "None", ""]):
                address_parts.append(f"{unit_type} {unit_number}")
            elif unit_number and unit_number not in ["nan", "None", ""]:
                address_parts.append(f"#{unit_number}")

            formatted_address = " ".join(address_parts)
            formatted_address = re.sub(r'\s+', ' ', formatted_address).strip()

            return formatted_address if formatted_address else None

        except Exception as e:
            logger.error(f"Error formatting structured address: {e}")
            return None

    @staticmethod
    def format_simple_address(address_str):
        """Format address from simple CSV format (pending file)"""
        try:
            if pd.isna(address_str) or not isinstance(address_str, str):
                return None

            address = address_str.strip()

            # Clean up the address string
            address = re.sub(r'\s+', ' ', address)  # Multiple spaces to single space
            address = re.sub(r'[,]$', '', address)  # Trailing comma

            # Remove incomplete unit indicators like "#4..."
            address = re.sub(r'#\d+\.\.\.', '', address).strip()

            return address if address else None

        except Exception as e:
            logger.error(f"Error formatting simple address: {e}")
            return None

    @staticmethod
    def extract_addresses_from_csv(df):
        """Extract and format addresses from a DataFrame"""
        csv_format = BCPAAddressFormatter.detect_csv_format(df)
        addresses = []

        if csv_format == "structured":
            for idx, row in df.iterrows():
                formatted_addr = BCPAAddressFormatter.format_structured_address(row)
                addresses.append(formatted_addr)

        elif csv_format == "simple":
            # Address is typically in the 4th column (index 3)
            address_col = df.columns[3]
            for idx, row in df.iterrows():
                formatted_addr = BCPAAddressFormatter.format_simple_address(row[address_col])
                addresses.append(formatted_addr)

        else:
            logger.error(f"Cannot process unknown CSV format")
            return None, None

        return addresses, csv_format


class BCPAWebScraper:
    """Web scraper for BCPA website using Playwright"""

    def __init__(self):
        self.browser = None
        self.page = None
        self.playwright = None
        self.initialized = False

    def initialize(self):
        """Initialize the browser - call this before using scraper"""
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("Playwright not available. Please install: pip install playwright")
            return False

        try:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=False,  # Changed to False for debugging
                args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--disable-web-security'],
                slow_mo=500  # Reduced slow motion for better performance
            )
            self.page = self.browser.new_page()
            # Set much longer timeout for initial navigation
            self.page.set_default_timeout(120000)  # 2 minutes for very slow connections
            self.initialized = True
            logger.info("BCPA scraper initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize BCPA scraper: {e}")
            return False

    def cleanup(self):
        """Clean up browser resources"""
        try:
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            self.initialized = False
            logger.info("BCPA scraper cleaned up successfully")
        except Exception as e:
            logger.error(f"Error during BCPA scraper cleanup: {e}")

    def search_property_owner(self, address):
        """Search for property owner by address"""
        if not self.initialized:
            logger.error("Scraper not initialized")
            return "Error: Not Initialized"

        try:
            logger.info(f"Searching BCPA for address: {address}")

            # Always reload the page for each search to ensure fresh state
            logger.info("🔄 Reloading BCPA search page for fresh state...")
            try:
                self.page.goto("https://web.bcpa.net/BcpaClient/#/Record-Search",
                              wait_until="load", timeout=120000)
                logger.info("✅ Page loaded successfully")
            except Exception as nav_error:
                logger.warning(f"⚠️ Navigation timeout, trying page reload...")
                self.page.reload(wait_until="load", timeout=60000)
                logger.info("🔄 Page reloaded after timeout")

            # Wait for page to be fully interactive - same as first search
            time.sleep(8)  # Increased wait time for the Angular app to fully load

            # Additional wait for any dynamic content to settle
            try:
                # Wait for the search form to be fully ready
                self.page.wait_for_load_state("networkidle", timeout=10000)
                logger.info("✅ Page reached network idle state")
            except:
                logger.info("⚠️ Network idle timeout - continuing with longer wait")
                time.sleep(3)  # Additional fallback wait

            # Find and fill search box - use same robust approach each time
            search_input = None
            max_retries = 3

            for attempt in range(max_retries):
                try:
                    logger.info(f"🔍 Attempt {attempt + 1}: Looking for search input...")

                    # First try the role-based selector
                    search_input = self.page.get_by_role('textbox', name='Name, Address, Folio')
                    search_input.wait_for(state="visible", timeout=15000)
                    logger.info("✅ Found search input using role-based method")
                    break

                except:
                    try:
                        # Try direct selector by ID (most reliable)
                        search_input = self.page.locator('#txtField')
                        search_input.wait_for(state="visible", timeout=15000)
                        logger.info("✅ Found search input using ID selector")
                        break
                    except:
                        if attempt == max_retries - 1:
                            logger.error("❌ Could not find visible search input after all attempts")
                            raise Exception("Search input not found")
                        else:
                            logger.warning(f"⚠️ Attempt {attempt + 1} failed, waiting 2 seconds and retrying...")
                            time.sleep(2)

            # Debug: Check if input is found
            logger.info(f"✅ Search input found and visible")

            # Clear and fill the search box
            search_input.clear()
            search_input.fill(address)

            # Debug: Verify the address was entered
            filled_value = search_input.input_value()
            logger.info(f"🔍 Address filled into search box: '{filled_value}'")

            # Submit search
            logger.info("🔍 Pressing Enter to submit search...")
            search_input.press("Enter")

            # Give the page a moment to process the search - same timing as first
            time.sleep(3)

            # Debug: Log current page URL and take screenshot
            current_url = self.page.url
            logger.info(f"🔍 Current page URL: {current_url}")

            # Take debug screenshot
            try:
                self.page.screenshot(path="debug_search_result.png")
                logger.info("📸 Debug screenshot saved as 'debug_search_result.png'")
            except:
                pass

            # Check what type of page we're on
            logger.info("🔍 Checking search results...")

            # Method 1: Check for Parcel Result tab (direct hit)
            try:
                self.page.wait_for_selector('tab[selected]:has-text("Parcel Result")', timeout=5000)
                logger.info(f"✅ Direct hit - found property on Parcel Result tab")

                # Extract owner information
                owner_name = self._extract_owner_from_parcel_page()
                if owner_name:
                    return owner_name
                else:
                    logger.info(f"❌ Could not extract owner from parcel page")
                    return None

            except Exception:
                logger.info("⚠️ Not on Parcel Result tab - checking for Search Results...")

            # Method 2: Check for Search Results tab (multiple results)
            try:
                self.page.wait_for_selector('tab:has-text("Search Results")', timeout=3000)
                logger.info(f"✅ Multiple results found on Search Results tab")

                # Click first result
                first_result = self.page.locator('table tr a').first
                if first_result.count() > 0:
                    logger.info("🔍 Clicking first search result...")
                    first_result.click()
                    time.sleep(2)

                    # Now check for Parcel Result tab
                    self.page.wait_for_selector('tab[selected]:has-text("Parcel Result")', timeout=10000)
                    logger.info(f"✅ Now on Parcel Result tab after clicking")

                    # Extract owner information
                    owner_name = self._extract_owner_from_parcel_page()
                    if owner_name:
                        return owner_name
                    else:
                        logger.info(f"❌ Could not extract owner after clicking result")
                        return None
                else:
                    logger.info(f"❌ No clickable results found")
                    return None

            except Exception as e:
                logger.info(f"⚠️ Not on Search Results tab either: {str(e)}")

            # Method 3: Check for actual content instead of tabs
            try:
                # Look for the owner information directly on the page, regardless of tabs
                logger.info("🔍 Checking for owner content directly on page...")

                # Wait a bit longer for content to load
                time.sleep(3)

                # Try to find Property Owner text anywhere on the page
                owner_text_selector = 'text=Property Owner(s)'
                if self.page.locator(owner_text_selector).count() > 0:
                    logger.info("✅ Found 'Property Owner(s)' text on page - extracting owner...")
                    owner_name = self._extract_owner_from_parcel_page()
                    if owner_name:
                        return owner_name
                    else:
                        logger.info("❌ Found owner section but could not extract text")
                        return None

                # Check if we're still on search page with no results
                property_search_text = self.page.locator('text=Property Search')
                if property_search_text.count() > 0:
                    logger.info(f"✅ Still on Property Search page - no results found")
                    return None
                else:
                    logger.info(f"⚠️ Unknown page state - check debug screenshot")
                    return None

            except Exception as e:
                logger.info(f"⚠️ Error checking page content: {str(e)}")

            except Exception as search_error:
                # Check if we got search results instead (multiple results)
                try:
                    self.page.wait_for_selector('tab:has-text("Search Results")', timeout=3000)
                    logger.info(f"✅ Multiple results found - clicking first result")

                    # Click on the first result link
                    first_result = self.page.locator('table tr a').first
                    if first_result.count() > 0:
                        first_result.click()

                        # Now wait for Parcel Result page
                        self.page.wait_for_selector('tab[selected]:has-text("Parcel Result")', timeout=10000)

                        # Extract owner using same logic
                        owner_row_selector = 'row:has(cell:has-text("Property Owner(s):"))'
                        if self.page.locator(owner_row_selector).count() > 0:
                            owner_cell = self.page.locator(owner_row_selector).locator('cell').nth(1)
                            owner_generic = owner_cell.locator('generic').first

                            if owner_generic.count() > 0:
                                owner_name = owner_generic.inner_text().strip()
                                if owner_name:
                                    logger.info(f"✅ Found owner after clicking result: {owner_name}")
                                    return owner_name

                    logger.info(f"❌ Could not extract owner from search results")
                    return None

                except Exception:
                    # No search results either - probably no records found
                    logger.info(f"✅ Confirmed: No records found for this address")
                    return None

                logger.warning(f"No owner found for: {address}")
                return "No Owner Found"

            except Exception as inner_e:
                logger.warning(f"Error extracting owner for {address}: {inner_e}")
                return "Extraction Error"

        except Exception as e:
            logger.error(f"Error searching {address}: {e}")
            return "Search Error"

    def _extract_owner_from_parcel_page(self):
        """Extract owner name from the parcel details page"""
        try:
            logger.info("🔍 Attempting to extract owner using multiple strategies...")

            # Strategy 1: Look for the row containing "Property Owner(s):"
            owner_row_selector = 'row:has(cell:has-text("Property Owner(s):"))'
            if self.page.locator(owner_row_selector).count() > 0:
                logger.info("✅ Strategy 1: Found Property Owner(s) row")
                owner_cell = self.page.locator(owner_row_selector).locator('cell').nth(1)
                owner_generic = owner_cell.locator('generic').first

                if owner_generic.count() > 0:
                    owner_name = owner_generic.inner_text().strip()
                    if owner_name:
                        logger.info(f"✅ Strategy 1 success: {owner_name}")
                        return owner_name

            # Strategy 2: Look for text after "Property Owner(s):" directly
            logger.info("⚠️ Strategy 1 failed, trying Strategy 2...")
            owner_label = self.page.locator('text=Property Owner(s):')
            if owner_label.count() > 0:
                logger.info("✅ Strategy 2: Found 'Property Owner(s):' label")

                # Try to get the next sibling or parent container
                parent_element = owner_label.locator('..')
                if parent_element.count() > 0:
                    # Look for any text in the parent that's not the label itself
                    all_text = parent_element.inner_text()
                    logger.info(f"🔍 Parent element text: '{all_text}'")

                    # Extract owner name by removing the label
                    if "Property Owner(s):" in all_text:
                        owner_text = all_text.replace("Property Owner(s):", "").strip()
                        if owner_text:
                            logger.info(f"✅ Strategy 2 success: {owner_text}")
                            return owner_text

            # Strategy 3: Search for any element containing names patterns
            logger.info("⚠️ Strategy 2 failed, trying Strategy 3...")

            # Look for patterns like "LASTNAME, FIRSTNAME" which are common in property records
            import re
            page_content = self.page.content()

            # Pattern for names like "SMITH, JOHN" or "SMITH, JOHN & JANE"
            name_pattern = r'([A-Z]{2,}(?:\s+[A-Z]{2,})*,\s+[A-Z]{2,}(?:\s+[A-Z])*(?:\s+&\s+[A-Z]{2,}(?:\s+[A-Z])*)*)'
            matches = re.findall(name_pattern, page_content)

            if matches:
                # Filter out common false positives
                filtered_matches = []
                exclude_words = ['BROWARD', 'COUNTY', 'APPRAISER', 'PROPERTY', 'FLORIDA', 'STATE']

                for match in matches:
                    if not any(word in match.upper() for word in exclude_words):
                        filtered_matches.append(match)

                if filtered_matches:
                    owner_name = filtered_matches[0]  # Take the first valid match
                    logger.info(f"✅ Strategy 3 success: {owner_name}")
                    return owner_name

            logger.info("❌ All extraction strategies failed")
            return None

        except Exception as e:
            logger.error(f"❌ Error extracting owner: {str(e)}")
            return None

    def batch_search_owners(self, addresses, delay=5, max_addresses=50):
        """Search for multiple property owners using fresh browser context for each search"""
        if not self.initialized:
            return {}

        # Limit batch size to be respectful to the server
        limited_addresses = addresses[:max_addresses] if len(addresses) > max_addresses else addresses

        results = {}
        for i, address in enumerate(limited_addresses):
            if not address or pd.isna(address):
                results[address] = "Invalid Address"
                continue

            logger.info(f"🔍 Processing {i+1}/{len(limited_addresses)}: {address}")

            # For multiple addresses, create fresh context for each after the first
            if i > 0:
                logger.info("🔄 Creating fresh browser context for reliable search...")
                try:
                    # Close current page and create new one
                    if self.page:
                        self.page.close()

                    # Create new context and page
                    context = self.browser.new_context()
                    context.set_default_timeout(120000)
                    self.page = context.new_page()
                    logger.info("✅ Fresh browser context created")
                except Exception as e:
                    logger.warning(f"⚠️ Could not create fresh context: {e}")

            owner = self.search_property_owner(address)
            results[address] = owner

            # Save progress after each successful search
            if owner and "Error" not in str(owner):
                logger.info(f"✅ Successfully found owner for {address}: {owner}")
            else:
                logger.info(f"❌ No owner found for {address}")

            # Rate limiting - wait between searches to be respectful to the server
            # Also gives time for any browser cleanup
            if i < len(limited_addresses) - 1:
                logger.info(f"⏰ Waiting {delay} seconds before next search...")
                time.sleep(delay)

        return results


def process_bcpa_reverse_search(input_file_path, output_dir="uploads", max_addresses=25):
    """
    Process a CSV file for reverse address lookup
    Returns: (success: bool, output_file_path: str, summary: dict)
    """
    try:
        # Read the CSV file
        logger.info(f"Processing BCPA reverse search for: {input_file_path}")
        df = read_data_file(input_file_path)

        if len(df) == 0:
            return False, None, {"error": "Empty CSV file"}

        # Extract addresses
        addresses, csv_format = BCPAAddressFormatter.extract_addresses_from_csv(df)

        if addresses is None:
            return False, None, {"error": "Could not parse CSV format"}

        # Filter valid addresses
        valid_addresses = [addr for addr in addresses if addr]
        logger.info(f"Found {len(valid_addresses)} valid addresses out of {len(addresses)} total")

        if not valid_addresses:
            return False, None, {"error": "No valid addresses found"}

        # Initialize scraper
        scraper = BCPAWebScraper()
        if not scraper.initialize():
            return False, None, {"error": "Could not initialize web scraper"}

        try:
            # Perform searches
            logger.info(f"Starting BCPA search for {min(len(valid_addresses), max_addresses)} addresses")
            search_results = scraper.batch_search_owners(valid_addresses, delay=2, max_addresses=max_addresses)

            # Create output dataframe
            output_df = df.copy()
            output_df['BCPA_Formatted_Address'] = addresses
            output_df['BCPA_Owner_Name'] = [search_results.get(addr, "Not Searched") for addr in addresses]
            output_df['BCPA_Search_Date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Generate output filename
            input_name = Path(input_file_path).stem
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{timestamp}_{input_name}_bcpa_owners.csv"
            output_path = Path(output_dir) / output_filename

            # Save results
            output_df.to_csv(output_path, index=False)

            # Generate summary
            found_owners = sum(1 for result in search_results.values()
                             if result and "Error" not in result and "Not Found" not in result)

            summary = {
                "total_records": len(df),
                "valid_addresses": len(valid_addresses),
                "searched_addresses": len(search_results),
                "owners_found": found_owners,
                "csv_format": csv_format,
                "output_file": str(output_path)
            }

            logger.info(f"BCPA search completed. Found {found_owners}/{len(search_results)} owners")
            return True, str(output_path), summary

        finally:
            scraper.cleanup()

    except Exception as e:
        logger.error(f"Error in BCPA reverse search: {e}")
        return False, None, {"error": str(e)}


# Test function to verify the script works
def test_bcpa_search():
    """Test the BCPA search functionality"""
    # Test address formatting
    test_data = {
        'House Number': ['5920', '5910'],
        'Prefix Direction': ['SW', 'SW'],
        'Street Name': ['33RD', '33RD'],
        'Street Type': ['AVE', 'AVE'],
        'Post Direction': ['', ''],
        'City Name': ['HOLLYWOOD', 'HOLLYWOOD']
    }

    test_df = pd.DataFrame(test_data)
    addresses, format_type = BCPAAddressFormatter.extract_addresses_from_csv(test_df)

    print(f"Test addresses: {addresses}")
    print(f"Format detected: {format_type}")

    # Test single address search (comment out for automated testing)
    # scraper = BCPAWebScraper()
    # if scraper.initialize():
    #     owner = scraper.search_property_owner("5920 SW 33RD AVE")
    #     print(f"Test search result: {owner}")
    #     scraper.cleanup()


if __name__ == "__main__":
    test_bcpa_search()
