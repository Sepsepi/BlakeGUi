#!/usr/bin/env python3
"""
Broward County Property Appraiser Owner Search
Uses Playwright to search for property owners by address when names are missing

RECENT UPDATES (August 6, 2025):
- ✅ Tested with real BCPA website using Playwright MCP
- ✅ Confirmed optimal address format: "STREET, CITY" (without FL state)
- ✅ Added support for multiple owner extraction from property details
- ✅ Enhanced parsing for owners like "BARATZ, PHILIP J & LISA T"
11
✅ WORKS: "5920 SW 33RD AVE, HOLLYWOOD" → Found: "BARATZ, PHILIP J & LISA T"
❌ FAILS: "5920 SW 33RD AVE, HOLLYWOOD, FL" → No records found
❌ FAILS: "1540 Cordova Rd, Fort Lauderdale" → No records found

EXTRACTION METHODS:
1. Property Details Page: Extracts from "Property Owner(s):" field
2. Search Results Table: Extracts from owner name column
3. Multiple Owner Parsing: Handles "&", "AND", ";" separators
4. Name Cleaning: Converts to proper format for downstream processing
"""

import asyncio
import pandas as pd
import logging
import re
import os
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

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
import time

# Import the enhanced address parser
try:
    from enhanced_address_parser import process_file_for_bcpa
    ENHANCED_PARSER_AVAILABLE = True
    print("✅ Enhanced Address Parser: Available")
except ImportError as e:
    print(f"❌ Enhanced Address Parser: Not available - {e}")
    ENHANCED_PARSER_AVAILABLE = False

# Try to import intelligent formatter as well
try:
    from intelligent_address_formatter import IntelligentAddressFormatter
    INTELLIGENT_FORMATTER_AVAILABLE = True
    print("✅ Intelligent AI Formatter: Available")
except ImportError as e:
    print(f"❌ Intelligent AI Formatter: Not available - {e}")
    INTELLIGENT_FORMATTER_AVAILABLE = False

# Setup logging to logs folder
log_folder = Path('logs')
log_folder.mkdir(exist_ok=True)
log_file = log_folder / f'bcpa_search_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def process_bcpa_lookup_headless(csv_file_path, max_records=None):
    """Process BCPA lookup with enforced headless mode - for integration with other scripts"""
    try:
        logger.info("🔒 Starting BCPA lookup in ENFORCED HEADLESS mode")

        # Create BCPA searcher with forced headless mode and optimized speed
        searcher = BCPAOwnerSearch(headless=True, delay_between_searches=0.3)

        # Run the async process
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result = loop.run_until_complete(searcher.process_csv(csv_file_path))
            logger.info(f"✅ BCPA lookup completed: {result}")
            return result
        finally:
            loop.close()

    except Exception as e:
        logger.error(f"❌ Error in BCPA lookup: {e}")
        raise


class BCPAOwnerSearch:
    def __init__(self, headless=True, delay_between_searches=0.5):  # Make headless by default, reduce delay
        """Initialize BCPA Owner Search"""
        self.headless = True  # Force headless mode for all operations
        self.delay_between_searches = delay_between_searches
        self.base_url = "https://web.bcpa.net/BcpaClient/#/Record-Search"
        self.results_found = 0
        self.searches_performed = 0

        # Broward County cities (for geographic validation) - Updated based on testing
        self.broward_cities = {
            'HOLLYWOOD', 'FORT LAUDERDALE', 'PEMBROKE PINES', 'CORAL SPRINGS',
            'MIRAMAR', 'SUNRISE', 'PLANTATION', 'DAVIE', 'WESTON', 'MARGATE',
            'TAMARAC', 'COCONUT CREEK', 'POMPANO BEACH', 'LAUDERHILL',
            'LAUDERDALE LAKES', 'WILTON MANORS', 'OAKLAND PARK', 'HALLANDALE BEACH',
            'COOPER CITY', 'DEERFIELD BEACH', 'LIGHTHOUSE POINT', 'NORTH LAUDERDALE',
            'PARKLAND', 'SEA RANCH LAKES', 'SOUTHWEST RANCHES', 'WEST PARK',
            'HILLSBORO BEACH', 'LAZY LAKE', 'PEMBROKE PARK', 'HIGHLAND BEACH',
            # Cities confirmed working during manual testing
            'HOLLYWOOD BEACH', 'FORT LAUDERDALE BEACH', 'LAUDERDALE BY THE SEA',
            # Edge cases that may work
            'BOULEVARD GARDENS', 'BROADVIEW PARK', 'FRANKLIN PARK', 'ROOSEVELT GARDENS',
            'WASHINGTON PARK', 'TWIN LAKES', 'CARVER RANCHES'
        }

    def is_broward_county(self, city: str) -> bool:
        """Check if a city is in Broward County with improved validation"""
        if not city:
            return False

        city_clean = city.upper().strip()

        # Remove common suffixes that might interfere
        suffixes_to_remove = ['BEACH', 'CITY', 'LAKES', 'PARK', 'GARDENS']
        city_base = city_clean
        for suffix in suffixes_to_remove:
            if city_clean.endswith(f' {suffix}'):
                city_base = city_clean.replace(f' {suffix}', '')
                break

        # Check both full name and base name
        return (city_clean in self.broward_cities or
                city_base in self.broward_cities or
                self.is_likely_broward_area(city_clean))

    def is_likely_broward_area(self, city: str) -> bool:
        """Check if city might be in Broward County based on patterns"""
        # Common Broward area indicators
        broward_indicators = [
            'LAUDERDALE', 'HOLLYWOOD', 'PEMBROKE', 'CORAL', 'COCONUT',
            'PLANTATION', 'WESTON', 'MARGATE', 'SUNRISE', 'DAVIE'
        ]

        return any(indicator in city for indicator in broward_indicators)

    def clean_address_for_search(self, row):
        """Clean and format address for BCPA search using improved formatting logic"""
        try:
            # PRIORITY 1: Use pre-formatted BCPA_Search_Format column if available
            if 'BCPA_Search_Format' in row and pd.notna(row['BCPA_Search_Format']):
                search_format = str(row['BCPA_Search_Format']).strip()
                if search_format and search_format.lower() not in ['nan', 'none', '', 'false']:
                    logger.info(f"✅ Using pre-formatted address: {search_format}")
                    return search_format

            # PRIORITY 2: Use improved address formatting logic from bcpa_flask_integration
            from bcpa_flask_integration import BCPAAddressFormatter

            # Try to detect format and use appropriate formatter
            temp_df = pd.DataFrame([row])
            csv_format = BCPAAddressFormatter.detect_csv_format(temp_df)

            if csv_format == "structured":
                # Use structured formatting
                formatted_addr = BCPAAddressFormatter.format_structured_address(row)
                if formatted_addr:
                    # Add city for complete BCPA format
                    city = str(row.get('City Name', '')).strip()
                    if not city or city.lower() in ['nan', 'none', '']:
                        city = str(row.get('BCPA_City', '')).strip()

                    if city and city.lower() not in ['nan', 'none', '']:
                        return f"{formatted_addr}, {city.upper()}"
                    return formatted_addr

            elif csv_format == "simple":
                # For simple format, try to get address from 4th column
                if len(row) > 3:
                    address_col = row.iloc[3] if hasattr(row, 'iloc') else list(row.values())[3]
                    formatted_addr = BCPAAddressFormatter.format_simple_address(address_col)
                    if formatted_addr:
                        return formatted_addr

            # Fallback to original logic for backwards compatibility
            address_parts = []

            # Get house number
            house_num = str(row.get('House Number', '')).strip()
            if house_num and house_num.lower() not in ['nan', 'none', '']:
                address_parts.append(house_num)

            # Get prefix direction
            prefix_dir = str(row.get('Prefix Direction', '')).strip()
            if prefix_dir and prefix_dir.lower() not in ['nan', 'none', '']:
                address_parts.append(prefix_dir)

            # Get street name
            street_name = str(row.get('Street Name', '')).strip()
            if street_name and street_name.lower() not in ['nan', 'none', '']:
                address_parts.append(street_name)

            # Get street type
            street_type = str(row.get('Street Type', '')).strip()
            if street_type and street_type.lower() not in ['nan', 'none', '']:
                address_parts.append(street_type)

            # Get post direction
            post_dir = str(row.get('Post Direction', '')).strip()
            if post_dir and post_dir.lower() not in ['nan', 'none', '']:
                address_parts.append(post_dir)

            # Build the street address
            street_address = " ".join(address_parts)

            # Add city only (exclude state and zip code for better search results)
            city = str(row.get('City Name', '')).strip()
            if not city or city.lower() in ['nan', 'none', '']:
                city = str(row.get('BCPA_City', '')).strip()

            # Format as: "1540 Cordova Rd, Fort Lauderdale" (no state, no zip code)
            full_address_parts = []
            if street_address:
                full_address_parts.append(street_address)

            if city and city.lower() not in ['nan', 'none', '']:
                full_address_parts.append(city)

            search_address = ", ".join(full_address_parts)
            return search_address.strip()

        except Exception as e:
            logger.error(f"Error cleaning address: {e}")
            return ""

    def has_valid_owner_name(self, row):
        """Check if row already has a valid owner name"""
        name1 = str(row.get('Owner Name 1', '')).strip()

        # Check if names are empty or just business indicators
        business_indicators = ['LLC', 'INC', 'CORP', 'LTD', 'CO', 'COMPANY', 'TRUST', 'TR']

        valid_name1 = name1 and name1.lower() not in ['nan', 'none', ''] and len(name1) > 2

        # Check if it's not just a business
        if valid_name1:
            is_business = any(indicator in name1.upper() for indicator in business_indicators)
            if not is_business:
                return True

        return False

    async def search_address_on_bcpa(self, page, address):
        """Search for an address on BCPA website with working Playwright MCP approach"""
        max_retries = 2
        retry_count = 0

        while retry_count < max_retries:
            try:
                logger.info(f"🔍 Searching BCPA for: {address} (attempt {retry_count + 1}/{max_retries})")

                # Navigate to search page
                await page.goto(self.base_url, wait_until='domcontentloaded', timeout=60000)

                # Quick wait for page basics
                await page.wait_for_timeout(2000)

                # Find the search input box using the working approach from Playwright MCP
                search_input = None
                try:
                    # Use the exact working selector from our successful test
                    search_input = page.get_by_role('textbox', name='Name, Address, Folio')
                    await search_input.wait_for(state='visible', timeout=10000)
                    logger.info("✅ Found search input box")
                except Exception:
                    # Fallback to simple locator
                    try:
                        search_input = page.locator('input[type="text"]').first
                        await search_input.wait_for(state='visible', timeout=5000)
                        logger.info("✅ Found search input with fallback")
                    except Exception as e:
                        logger.error(f"❌ Could not find search input: {e}")
                        return None

                # Clear and enter the address (exactly like successful Playwright MCP run)
                await search_input.click()
                await page.wait_for_timeout(500)  # Wait after clicking
                await search_input.fill(address)
                await page.wait_for_timeout(1000)  # Increased wait after filling

                # Submit the search
                await search_input.press('Enter')

                # Wait for results with proper timing
                await self.wait_for_search_results(page)

                # Extract owner info
                owner_info = await self.extract_owner_from_results(page)

                if owner_info:
                    logger.info(f"✅ Found owner: {owner_info}")
                    return owner_info
                else:
                    logger.info(f"❌ No owner found for: {address}")
                    return None

            except Exception as e:
                retry_count += 1
                logger.error(f"Error searching BCPA for {address} (attempt {retry_count}): {e}")

                if retry_count < max_retries:
                    logger.info(f"Retrying in 2 seconds...")
                    await page.wait_for_timeout(2000)
                else:
                    logger.error(f"Failed after {max_retries} attempts")
                    return None

        return None

    async def wait_for_search_results(self, page):
        """Wait for search results to load with proper timing based on manual testing"""
        try:
            # Wait longer for the page transition to complete (from manual testing we know this takes time)
            await page.wait_for_timeout(6000)  # Increased to 6 seconds for transition
            logger.info("✅ Initial wait completed")

            # Check for page transition indicators with longer timeout
            try:
                # Wait for either success indicators or failure message with more time
                await page.wait_for_selector([
                    'text="Property Owner(s):"',  # Success: Property details page
                    'text="No record found"',     # Failure: No records
                    'tabpanel:has-text("Property Summary")',  # Success: Property page
                    'tab[selected]:has-text("Parcel Result")'   # Success: Property found
                ], timeout=15000)  # Increased to 15 seconds
                logger.info("✅ Found completion indicator")
            except:
                logger.info("⚠️ No specific indicators found, proceeding with extra wait...")
                await page.wait_for_timeout(5000)  # Extra wait for slow loads

        except Exception as e:
            logger.warning(f"Wait timeout: {e}")
            await page.wait_for_timeout(5000)  # Longer fallback wait

    async def extract_owner_from_results(self, page):
        """Extract owner information with improved reliability and proper timing"""
        try:
            # Wait longer for content to be fully ready (based on manual testing)
            await page.wait_for_timeout(3000)  # Increased to 3 seconds

            # First priority: Check if we're on a Property Details page (successful search)
            # This should be checked BEFORE looking for "no record found"
            property_detail_indicators = [
                'td:has-text("Property Owner(s):")',  # Standard HTML table cell
                'th:has-text("Property Owner(s):")',  # Table header variant
                'text="Property Owner(s):"',          # Direct text match
                'tab:has-text("Parcel Result")[aria-selected="true"]',  # Selected tab
                '[role="tab"][aria-selected="true"]:has-text("Parcel Result")',  # ARIA tab
                'text="Property Summary"',             # Property summary text
                'text="Property ID:"'                  # Property ID text
            ]

            logger.info("🔍 Checking for property details page...")
            for i, indicator in enumerate(property_detail_indicators):
                try:
                    count = await page.locator(indicator).count()
                    logger.info(f"Property detail indicator {i+1}: '{indicator}' -> {count} matches")
                    if count > 0:
                        logger.info("✅ Found property details page - extracting owner")
                        return await self.extract_owner_from_property_details(page)
                except Exception as e:
                    logger.debug(f"Property details check failed for {indicator}: {e}")
                    continue

            # Second priority: Check for Search Results table (ONLY if not on property details)
            # Be very specific - only look for the actual "Search Results" tab
            search_results_indicators = [
                'tab[selected]:has-text("Search Results")'
            ]

            for indicator in search_results_indicators:
                try:
                    if await page.locator(indicator).count() > 0:
                        logger.info("✅ Found search results table")
                        return await self.extract_owner_from_search_results(page)
                except Exception as e:
                    logger.debug(f"Search results check failed for {indicator}: {e}")
                    continue

            # ONLY AFTER checking for success should we check for "No record found"
            # And be more specific about this check
            no_record_found = False
            try:
                # Be very specific - only check for the exact error message
                error_element = page.locator('p:has-text("No record found, please check your criteria.")')
                if await error_element.count() > 0:
                    # Double-check this is actually visible and contains the right text
                    text = await error_element.first.text_content()
                    if text and "no record found" in text.lower() and "check your criteria" in text.lower():
                        logger.info("✅ Confirmed: No records found for this address")
                        no_record_found = True
            except:
                pass

            if no_record_found:
                return None

            # Fallback - try to extract from any visible content
            logger.info("No specific page type detected - attempting fallback extraction")

            # Try property details first
            property_result = await self.extract_owner_from_property_details(page)
            if property_result:
                return property_result

            # Then try search results
            search_result = await self.extract_owner_from_search_results(page)
            if search_result:
                return search_result

            # Last resort - scan page text for owner patterns
            try:
                page_content = await page.content()
                owner_from_text = await self.extract_owner_from_page_text(page_content)
                if owner_from_text:
                    logger.info("✅ Found owner via text pattern scanning")
                    return owner_from_text
            except Exception as e:
                logger.debug(f"Page text extraction failed: {e}")

            logger.info("❌ No owner information found using any method")
            return None

        except Exception as e:
            logger.error(f"Error extracting owner from results: {e}")
            return None

    async def extract_owner_from_page_text(self, page_content):
        """Extract owner from raw page text as fallback method"""
        try:
            import re

            # Pattern 1: Property Owner(s): format
            owner_patterns = [
                r'Property Owner\(s\):\s*([A-Z][A-Za-z\s,&/\-]{3,150})',
                r'Owner[:\s]+([A-Z][A-Z\s,&/\-]{3,100})',
                r'Taxpayer[:\s]+([A-Z][A-Z\s,&/\-]{3,100})'
            ]

            for pattern in owner_patterns:
                matches = re.findall(pattern, page_content, re.IGNORECASE)
                if matches:
                    for match in matches:
                        # Clean and validate the match
                        cleaned_text = match.strip()
                        if len(cleaned_text) > 3 and ',' in cleaned_text:
                            owner_names = self.parse_multiple_owners(cleaned_text)
                            if owner_names:
                                logger.info(f"Found owners via text pattern: {owner_names}")
                                return owner_names

            return None

        except Exception as e:
            logger.error(f"Error in page text extraction: {e}")
            return None

    async def extract_owner_from_property_details(self, page):
        """Extract owner from property details page using the WORKING Playwright MCP approach"""
        try:
            # Wait longer for content to load (based on manual testing)
            await page.wait_for_timeout(2000)  # Increased to 2 seconds
            logger.info("🔍 Extracting owner from property details...")

            # Debug: Let's see what's actually on the page
            try:
                page_title = await page.title()
                current_url = page.url
                logger.info(f"🔍 Debug - Page title: {page_title}")
                logger.info(f"🔍 Debug - Current URL: {current_url}")

                # Check for basic elements
                body_text = await page.text_content('body')
                if body_text:
                    has_property_owner = 'Property Owner' in body_text
                    has_property_summary = 'Property Summary' in body_text
                    has_parcel_result = 'Parcel Result' in body_text
                    has_no_record = 'No record found' in body_text
                    logger.info(f"🔍 Debug - Has 'Property Owner': {has_property_owner}")
                    logger.info(f"🔍 Debug - Has 'Property Summary': {has_property_summary}")
                    logger.info(f"🔍 Debug - Has 'Parcel Result': {has_parcel_result}")
                    logger.info(f"🔍 Debug - Has 'No record found': {has_no_record}")

                    # Show first 300 chars of body text
                    preview = body_text[:300].replace('\n', ' ').strip()
                    logger.info(f"🔍 Debug - Page preview: {preview}")
            except Exception as debug_e:
                logger.info(f"🔍 Debug failed: {debug_e}")

            # Method 0: Direct text search (most reliable based on manual test)
            try:
                page_content = await page.content()
                if 'Property Owner(s):' in page_content:
                    logger.info("Method 0: Found 'Property Owner(s):' in page content")
                    # Use regex to extract the owner text
                    import re
                    # Look for the pattern: Property Owner(s): followed by the owner name
                    pattern = r'Property Owner\(s\):\s*</[^>]*>\s*<[^>]*>([^<]+)'
                    matches = re.findall(pattern, page_content, re.IGNORECASE | re.DOTALL)
                    if matches:
                        owner_text = matches[0].strip()
                        logger.info(f"✅ Found owner via regex: {owner_text}")
                        owner_names = self.parse_multiple_owners(owner_text)
                        if owner_names:
                            return owner_names
                    else:
                        logger.info("Method 0: Regex pattern didn't match")
                else:
                    logger.info("Method 0: 'Property Owner(s):' not found in page content")
            except Exception as e:
                logger.info(f"Method 0 failed: {e}")

            # Method 1: Use standard HTML table selectors (most reliable)
            try:
                # Look for table cells containing "Property Owner(s):" and get the adjacent cell
                owner_label_cells = page.locator('td:has-text("Property Owner(s):"), th:has-text("Property Owner(s):")')
                cell_count = await owner_label_cells.count()
                logger.info(f"Method 1: Found {cell_count} Property Owner label cells")

                if cell_count > 0:
                    # Get the parent row and then find the next cell
                    for i in range(cell_count):
                        try:
                            label_cell = owner_label_cells.nth(i)
                            # Get the next sibling cell
                            owner_cell = label_cell.locator('~ td').first
                            owner_text = await owner_cell.text_content()

                            if owner_text and owner_text.strip():
                                owner_text = owner_text.strip()
                                logger.info(f"✅ Found owner via table cell #{i}: {owner_text}")
                                owner_names = self.parse_multiple_owners(owner_text)
                                if owner_names:
                                    return owner_names
                        except Exception as e:
                            logger.debug(f"Table cell #{i} failed: {e}")
                            continue
                else:
                    logger.info("Method 1: No Property Owner label cells found")
            except Exception as e:
                logger.info(f"Method 1 failed: {e}")

            # Method 2: Simple text content extraction
            try:
                # Get all text content and split on "Property Owner(s):"
                all_text = await page.text_content('body')
                if all_text and 'Property Owner(s):' in all_text:
                    logger.info("Method 2: Found Property Owner(s) in body text")
                    # Split and get the part after "Property Owner(s):"
                    parts = all_text.split('Property Owner(s):', 1)
                    if len(parts) > 1:
                        owner_section = parts[1].strip()
                        # Get text until next field (like "Mailing Address:")
                        if 'Mailing Address:' in owner_section:
                            owner_text = owner_section.split('Mailing Address:')[0].strip()
                        else:
                            # Take first reasonable chunk
                            lines = owner_section.split('\n')
                            owner_text = lines[0].strip() if lines else ""

                        if owner_text and len(owner_text) > 3:
                            logger.info(f"✅ Found owner via text split: {owner_text}")
                            owner_names = self.parse_multiple_owners(owner_text)
                            if owner_names:
                                return owner_names
                else:
                    logger.info("Method 2: Property Owner(s) not found in body text")
            except Exception as e:
                logger.info(f"Method 2 failed: {e}")

            # Method 3: Use accessibility role selectors (Playwright MCP approach)
            try:
                # This is based on our successful manual test
                owner_rows = page.locator('[role="row"]:has-text("Property Owner(s):")')
                row_count = await owner_rows.count()
                logger.info(f"Method 2: Found {row_count} owner rows with role")

                if row_count > 0:
                    # Get the cell with the owner name (second cell in the row)
                    owner_cell = owner_rows.first.locator('[role="cell"]').nth(1)
                    owner_text = await owner_cell.text_content()

                    if owner_text and owner_text.strip():
                        owner_text = owner_text.strip()
                        logger.info(f"✅ Found owner via role row: {owner_text}")
                        owner_names = self.parse_multiple_owners(owner_text)
                        if owner_names:
                            return owner_names
                else:
                    logger.info("Method 2: No owner rows with role found")
            except Exception as e:
                logger.info(f"Method 2 failed: {e}")

            # Method 3: Fallback using text content search
            try:
                page_text = await page.text_content('body')
                if page_text and 'Property Owner(s):' in page_text:
                    # Split by the property owner label
                    parts = page_text.split('Property Owner(s):', 1)
                    if len(parts) > 1:
                        # Get the text after "Property Owner(s):"
                        owner_section = parts[1].strip()
                        # Take everything up to the next section (like "Mailing Address")
                        if 'Mailing Address:' in owner_section:
                            owner_text = owner_section.split('Mailing Address:')[0].strip()
                        else:
                            # Take first line/reasonable portion
                            lines = owner_section.split('\n')
                            owner_text = lines[0].strip() if lines else owner_section[:100].strip()

                        if owner_text and len(owner_text) > 3:
                            logger.info(f"✅ Found owner via text parsing: {owner_text}")
                            owner_names = self.parse_multiple_owners(owner_text)
                            if owner_names:
                                return owner_names
            except Exception as e:
                logger.debug(f"Text parsing method failed: {e}")

            # Method 4: Search for specific table structure patterns
            try:
                # Look for any tr containing Property Owner text
                owner_tr_elements = page.locator('tr:has-text("Property Owner(s):")')
                tr_count = await owner_tr_elements.count()
                logger.info(f"Found {tr_count} table rows with Property Owner text")

                if tr_count > 0:
                    # Get all cells in the first matching row
                    cells = owner_tr_elements.first.locator('td, th')
                    cell_count = await cells.count()
                    logger.info(f"Row has {cell_count} cells")

                    # Try different cell positions to find the owner name
                    for cell_idx in range(cell_count):
                        try:
                            cell_text = await cells.nth(cell_idx).text_content()
                            if cell_text and cell_text.strip() and "Property Owner(s):" not in cell_text:
                                # This might be the owner name cell
                                cell_text = cell_text.strip()
                                if len(cell_text) > 3 and any(c.isalpha() for c in cell_text):
                                    logger.info(f"✅ Found potential owner in cell {cell_idx}: {cell_text}")
                                    owner_names = self.parse_multiple_owners(cell_text)
                                    if owner_names:
                                        return owner_names
                        except Exception as e:
                            logger.debug(f"Cell {cell_idx} extraction failed: {e}")
                            continue
            except Exception as e:
                logger.debug(f"Table row search failed: {e}")

            logger.info("❌ Could not extract owner using any method")
            return None

        except Exception as e:
            logger.error(f"❌ Error in extract_owner_from_property_details: {e}")
            return None

    async def extract_owner_from_search_results(self, page):
        """Extract owner from search results table"""
        try:
            # Look for the results table - it has specific structure:
            # Folio Number | Owner Name | Site Address
            results_table = page.locator('table:has(rowgroup)')
            if await results_table.count() == 0:
                logger.info("No search results table found")
                return None

            # Get all data rows (skip header row)
            data_rows = results_table.locator('rowgroup').nth(1).locator('row')
            row_count = await data_rows.count()

            if row_count == 0:
                logger.info("No data rows found in results table")
                return None

            # Process the first result row to extract owner name
            first_row = data_rows.first

            # The owner name is in the second cell (index 1)
            owner_cell = first_row.locator('cell').nth(1)
            owner_text = await owner_cell.text_content()

            if owner_text and owner_text.strip():
                owner_names = self.parse_multiple_owners(owner_text.strip())
                if owner_names:
                    logger.info(f"Found owner in search results: {owner_names}")
                    return owner_names

            # Alternative approach: Look for cells with individual/personal names
            # (avoiding LLC, CORP, etc. business entities)
            for i in range(min(row_count, 5)):  # Check first 5 results
                row = data_rows.nth(i)
                owner_cell = row.locator('cell').nth(1)
                owner_text = await owner_cell.text_content()

                if owner_text and owner_text.strip():
                    # Check if this looks like an individual name (not business)
                    if self.is_individual_name(owner_text.strip()):
                        owner_names = self.parse_multiple_owners(owner_text.strip())
                        if owner_names:
                            logger.info(f"Found individual owner: {owner_names}")
                            return owner_names

            # If no individual names found, take the first result anyway
            if row_count > 0:
                first_row = data_rows.first
                owner_cell = first_row.locator('cell').nth(1)
                owner_text = await owner_cell.text_content()

                if owner_text and owner_text.strip():
                    owner_names = self.parse_multiple_owners(owner_text.strip())
                    if owner_names:
                        logger.info(f"Using first available owner: {owner_names}")
                        return owner_names

            return None

        except Exception as e:
            logger.error(f"Error extracting from search results: {e}")
            return None

    def parse_multiple_owners(self, owner_text):
        """Parse multiple owner names from BCPA results and return as list"""
        if not owner_text:
            return None

        # Clean the initial text and remove trailing content like "Mailing Address"
        owner_text = owner_text.strip()
        # Remove everything after double newline (removes "Mailing Address" section)
        owner_text = owner_text.split('\n\n')[0].strip()
        logger.info(f"Parsing owner text: '{owner_text}'")

        # Handle multiple owner formats discovered through manual testing:
        # 1. H/E format: "CROOKS, LLONI-RAE C H/ETHOMAS, ROSETTA A" (no space before H/E)
        # 2. & format: "BARATZ, PHILIP J & LISA T"
        # 3. Single owner: "GREENAWAY, JAMES E" (no splitting needed)
        # 4. Space-separated potential: Handle carefully to avoid splitting middle names

        owners = []

        # Check for H/E format first (most complex)
        if 'H/E' in owner_text:
            # Handle both spaced and non-spaced H/E formats
            if ' H/E ' in owner_text:
                owners = [part.strip() for part in owner_text.split(' H/E ') if part.strip()]
            else:
                # Non-spaced H/E - need to find where first name ends and second begins
                # Look for pattern: "LASTNAME, FIRSTNAME H/ELASTNAME, FIRSTNAME"
                he_index = owner_text.find('H/E')
                if he_index > 0:
                    first_owner = owner_text[:he_index].strip()
                    second_owner = owner_text[he_index + 3:].strip()  # Skip 'H/E'
                    owners = [first_owner, second_owner]
            logger.info(f"Split H/E format: {owners}")

        # Check for & format
        elif ' & ' in owner_text:
            owners = [part.strip() for part in owner_text.split(' & ') if part.strip()]
            logger.info(f"Split & format: {owners}")

        # Check for other common separators
        elif any(sep in owner_text for sep in [' AND ', ' and ', '; ', ' / ']):
            separators = [' AND ', ' and ', '; ', ' / ']
            owners = [owner_text]
            for separator in separators:
                if separator in owner_text:
                    owners = [part.strip() for part in owner_text.split(separator) if part.strip()]
                    logger.info(f"Split by '{separator}': {owners}")
                    break

        # Single owner format - no splitting needed
        else:
            owners = [owner_text]
            logger.info(f"Single owner format: {owners}")

        # Clean each owner name - also remove any trailing newlines or extra content
        cleaned_owners = []
        for owner in owners:
            # Remove newlines and extra whitespace from each owner
            owner_cleaned = owner.strip().split('\n')[0].strip()
            if owner_cleaned:
                cleaned = self.clean_extracted_name(owner_cleaned)
                if cleaned:
                    cleaned_owners.append(cleaned)
                    logger.info(f"Cleaned owner: '{owner}' -> '{cleaned}'")

        logger.info(f"Final cleaned owners: {cleaned_owners}")
        return cleaned_owners if cleaned_owners else None

    def is_individual_name(self, name_text):
        """Check if the name appears to be an individual person vs business entity"""
        if not name_text:
            return False

        name_upper = name_text.upper()

        # Business indicators to avoid
        business_indicators = [
            'LLC', 'INC', 'CORP', 'LTD', 'CO', 'COMPANY', 'TRUST', 'TR',
            'BANK', 'MORTGAGE', 'PROPERTIES', 'REALTY', 'FUND', 'GROUP',
            'HOLDINGS', 'INVESTMENTS', 'ENTERPRISES', 'BORROWER', 'LP'
        ]

        # If it contains business indicators, it's likely a business
        if any(indicator in name_upper for indicator in business_indicators):
            return False

        # If it has comma (LAST, FIRST format) and no business words, likely individual
        if ',' in name_text and not any(indicator in name_upper for indicator in business_indicators):
            return True

        # If it has multiple names without business words, likely individual
        parts = name_text.split()
        if len(parts) >= 2 and len(parts) <= 4:  # Reasonable name length
            return True

        return False

    def clean_extracted_name(self, name_text):
        """Clean and validate extracted owner name - format for Zaba/Radaris compatibility"""
        if not name_text:
            return None

        # Clean the name
        name = name_text.strip()

        # Remove common prefixes/suffixes
        prefixes_to_remove = ['Owner:', 'Property Owner:', 'Taxpayer:', 'Name:']
        for prefix in prefixes_to_remove:
            if name.startswith(prefix):
                name = name[len(prefix):].strip()

        # Basic validation
        if len(name) < 3 or len(name) > 100:
            return None

        # Check if it's not just numbers or special characters
        if not re.search(r'[A-Za-z]', name):
            return None

        # Skip obvious non-names (folio numbers, addresses, etc.)
        if re.search(r'^\d+$', name):  # Just numbers
            return None
        if re.search(r'\d{10,}', name):  # Long number sequences (folio numbers)
            return None
        if re.search(r'(FL|FLORIDA)\s+\d{5}', name.upper()):  # Address with FL zipcode
            return None
        if re.search(r'^(FOLIO|PARCEL|LOT)', name.upper()):  # Property identifiers
            return None

        # Handle business entities - but still return them, just log them
        business_indicators = [
            'LLC', 'INC', 'CORP', 'LTD', 'CO', 'COMPANY', 'TRUST', 'TR',
            'BANK', 'MORTGAGE', 'PROPERTIES', 'REALTY', 'FUND', 'GROUP',
            'HOLDINGS', 'INVESTMENTS', 'ENTERPRISES', 'BORROWER', 'LP'
        ]
        name_upper = name.upper()
        is_business = any(indicator in name_upper for indicator in business_indicators)

        if is_business:
            logger.info(f"Found business entity: {name}")
            # For now, let's include business entities too, but prefer individuals
            # You can return None here if you want to skip businesses entirely

        # Convert to title case and clean up formatting
        if ',' in name:
            # Handle "LAST, FIRST" format
            parts = name.split(',', 1)
            if len(parts) == 2:
                last_name = parts[0].strip().title()
                first_name_part = parts[1].strip().title()

                # Clean up any remaining business indicators or extra text
                first_name_part = re.sub(r'\s+(ETAL|ET AL|TR|TRUSTEE).*$', '', first_name_part, flags=re.IGNORECASE)

                # Extract only first name (remove middle names, Jr, Sr, etc.)
                first_name = self.extract_first_name_only(first_name_part)
                last_name = self.extract_last_name_only(last_name)

                return f"{first_name} {last_name}"

        # Clean up business suffixes and extra text for non-comma names
        name = re.sub(r'\s+(ETAL|ET AL|TR|TRUSTEE).*$', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s*%.*$', '', name)  # Remove "% COMPANY" style suffixes

        # For non-comma names, try to extract first and last name only
        name_parts = name.title().split()
        if len(name_parts) >= 2:
            # Clean the first name part (remove middle names, initials)
            first_name = self.extract_first_name_only(name_parts[0])
            # Clean the last name part (remove suffixes)
            last_name_part = " ".join(name_parts[1:])  # Join everything after first name
            last_name = self.extract_last_name_only(last_name_part)
            return f"{first_name} {last_name}"

        return name.title()  # Convert to title case

    def extract_first_name_only(self, first_name_part):
        """Extract only the first name, removing middle names and initials"""
        if not first_name_part:
            return ""

        # Remove suffixes like Jr, Sr, III, etc.
        first_name_part = re.sub(r'\s+(JR|SR|III|IV|II)\.?$', '', first_name_part, flags=re.IGNORECASE)

        # Split by spaces and take only the first part (removes middle names/initials)
        parts = first_name_part.strip().split()
        if parts:
            first_name = parts[0]
            # Remove any trailing periods (from initials)
            first_name = first_name.rstrip('.')
            return first_name

        return first_name_part

    def extract_last_name_only(self, last_name_part):
        """Extract only the last name, removing suffixes"""
        if not last_name_part:
            return ""

        # Remove common suffixes
        last_name_part = re.sub(r'\s+(JR|SR|III|IV|II)\.?$', '', last_name_part, flags=re.IGNORECASE)

        # Split into words and take the actual last name
        # For "WILSON JR" -> "WILSON", for "DE LA CRUZ JR" -> "CRUZ", etc.
        parts = last_name_part.strip().split()
        if parts:
            # Filter out common suffixes that might still be present
            suffixes = ['JR', 'SR', 'III', 'IV', 'II', 'JR.', 'SR.']
            parts = [part for part in parts if part.upper() not in suffixes]

            if parts:
                return parts[-1]  # Take the last remaining part as the surname

        return last_name_part.strip()

    async def process_csv(self, input_csv_path, output_csv_path=None):
        """Process CSV file to find missing owners using BCPA search with improved address formatting"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING BCPA OWNER SEARCH WITH IMPROVED ADDRESS FORMATTING")
            logger.info("=" * 60)

            # First, try to use the superior address formatter from bcpa_flask_integration
            try:
                from bcpa_flask_integration import BCPAAddressFormatter
                logger.info("🏠 Using superior BCPAAddressFormatter for address standardization...")

                # Read the CSV file
                df = read_data_file(input_csv_path)
                logger.info(f"📄 Loaded {len(df)} records from CSV")

                # Use the superior address formatter to extract addresses
                addresses, csv_format = BCPAAddressFormatter.extract_addresses_from_csv(df)

                if addresses is not None:
                    logger.info(f"✅ Superior formatter processed {len(addresses)} addresses, format: {csv_format}")
                    # Add the formatted addresses as a new column for processing
                    df['BCPA_Formatted_Address'] = addresses
                else:
                    logger.warning("⚠️ Superior formatter could not process file, falling back to enhanced parser")
                    raise ImportError("Formatter failed")

            except (ImportError, Exception) as e:
                logger.info(f"📄 Falling back to enhanced address parser: {e}")
                # Use enhanced address parser if available
                if ENHANCED_PARSER_AVAILABLE:
                    logger.info("🏠 Using Enhanced Address Parser for address standardization...")
                    df = process_file_for_bcpa(input_csv_path)
                    logger.info(f"📄 Enhanced parser processed {len(df)} records")
                else:
                    # Fallback to basic CSV reading
                    logger.info("📄 Using basic CSV reading (enhanced parser not available)")
                    df = read_data_file(input_csv_path)
                    logger.info(f"📄 Loaded {len(df)} records from CSV")

            # Set output path
            if output_csv_path is None:
                input_file = Path(input_csv_path)
                output_csv_path = str(input_file.parent / f"{input_file.stem}_with_bcpa_owners.csv")

            # Identify rows without valid owner names
            rows_to_search = []
            for index, row in df.iterrows():
                if not self.has_valid_owner_name(row):
                    # PRIORITY 1: Try to get pre-formatted address from AI formatter
                    if 'BCPA_Search_Format' in df.columns and pd.notna(row['BCPA_Search_Format']):
                        address = str(row['BCPA_Search_Format']).strip()
                        if address and address.lower() not in ['nan', 'none', '', 'false']:
                            rows_to_search.append((index, address, row))
                            logger.info(f"🏠 Row {index}: Using AI-formatted address: {address}")
                            continue

                    # PRIORITY 2: Try to get pre-formatted address from superior formatter
                    if 'BCPA_Formatted_Address' in df.columns and pd.notna(row['BCPA_Formatted_Address']):
                        address = str(row['BCPA_Formatted_Address']).strip()
                        if address and address.lower() not in ['nan', 'none', '']:
                            rows_to_search.append((index, address, row))
                            logger.info(f"🏠 Row {index}: Using formatted address: {address}")
                            continue

                    # PRIORITY 3: Fallback to cleaning address
                    address = self.clean_address_for_search(row)
                    if address:
                        rows_to_search.append((index, address, row))
                        logger.info(f"🏠 Row {index}: Using cleaned address: {address}")

            logger.info(f"🎯 Found {len(rows_to_search)} rows needing owner search")

            if not rows_to_search:
                logger.info("✅ All rows already have valid owner names!")

                # Remove unwanted columns before saving
                columns_to_remove = ['BCPA_Multiple_Owners', 'Owner Name 2', 'Primary_Phone', 'Secondary_Phone', 'BCPA_Skip_Reason']
                for col in columns_to_remove:
                    if col in df.columns:
                        df = df.drop(columns=[col])
                        logger.info(f"🗑️ Removed column: {col}")

                df.to_csv(output_csv_path, index=False)
                return output_csv_path

            # Start Playwright browser with improved configuration
            async with async_playwright() as p:
                logger.info("🌐 Starting browser with enhanced settings...")

                # Launch browser with better configuration - ALWAYS HEADLESS
                browser = await p.chromium.launch(
                    headless=True,  # Force headless mode regardless of instance setting
                    args=[
                        '--no-sandbox',
                        '--disable-blink-features=AutomationControlled',
                        '--disable-web-security',
                        '--disable-extensions',
                        '--disable-plugins',
                        '--disable-images',  # Speed up loading
                        '--disable-gpu',     # Disable GPU for headless
                        '--disable-dev-shm-usage',  # Overcome limited resource problems
                        '--no-first-run',    # Skip first run wizards
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-renderer-backgrounding'
                    ]
                )

                # Create context with realistic settings
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    viewport={'width': 1366, 'height': 768},
                    java_script_enabled=True,
                    accept_downloads=False,
                    ignore_https_errors=True
                )

                # Set reduced timeouts for better performance
                context.set_default_timeout(60000)  # Reduced from 120 to 60 seconds
                context.set_default_navigation_timeout(45000)  # Reduced from 90 to 45 seconds

                page = await context.new_page()

                # Process each address
                for index, address, row in rows_to_search:
                    try:
                        self.searches_performed += 1
                        logger.info(f"📍 Searching {self.searches_performed}/{len(rows_to_search)}: {address}")

                        # Create a fresh page for each search to avoid state issues
                        fresh_page = await context.new_page()

                        # Extract city from address for geographic validation
                        city = None
                        if 'BCPA_City' in row and row['BCPA_City']:
                            city = str(row['BCPA_City']).strip()
                        else:
                            # Try to extract city from address string
                            if ',' in address:
                                city = address.split(',')[-1].strip()

                        # Check if address is in Broward County
                        if city and not self.is_broward_county(city):
                            logger.warning(f"⚠️ Skipping {address} - {city} is outside Broward County jurisdiction")
                            # Leave Owner Name 1 empty for non-Broward addresses
                            df.at[index, 'BCPA_Owner_Found'] = 'Skipped'
                            await fresh_page.close()
                            continue

                        # Search for owner
                        owner_info = await self.search_address_on_bcpa(fresh_page, address)

                        if owner_info:
                            if isinstance(owner_info, list) and len(owner_info) > 1:
                                # Multiple owners found - create additional rows
                                logger.info(f"✅ Found {len(owner_info)} owners for row {index}")

                                # Update the original row with the first owner
                                df.at[index, 'Owner Name 1'] = owner_info[0]
                                df.at[index, 'BCPA_Owner_Found'] = 'Yes'
                                logger.info(f"✅ Updated row {index} with first owner: {owner_info[0]}")

                                # Create additional rows for remaining owners
                                rows_to_add = []
                                for i, additional_owner in enumerate(owner_info[1:], 1):
                                    # Create a copy of the original row
                                    new_row = row.copy()
                                    new_row['Owner Name 1'] = additional_owner
                                    new_row['BCPA_Owner_Found'] = 'Yes'
                                    rows_to_add.append(new_row)
                                    logger.info(f"✅ Created additional row for owner {i+1}: {additional_owner}")

                                # Add the new rows to the dataframe
                                if rows_to_add:
                                    new_rows_df = pd.DataFrame(rows_to_add)
                                    df = pd.concat([df, new_rows_df], ignore_index=True)
                                    logger.info(f"📝 Added {len(rows_to_add)} additional rows for multiple owners")

                                self.results_found += len(owner_info)

                            else:
                                # Single owner found (or owner_info is already a string)
                                single_owner = owner_info[0] if isinstance(owner_info, list) else owner_info
                                df.at[index, 'Owner Name 1'] = single_owner
                                df.at[index, 'BCPA_Owner_Found'] = 'Yes'
                                self.results_found += 1
                                logger.info(f"✅ Updated row {index} with owner: {single_owner}")
                        else:
                            df.at[index, 'BCPA_Owner_Found'] = 'No'
                            logger.info(f"❌ No owner found for row {index}")

                        # Clean up the page after each search
                        await fresh_page.close()

                        # Delay between searches to be respectful - reduced timing
                        if self.searches_performed < len(rows_to_search):
                            await asyncio.sleep(self.delay_between_searches)  # Now 0.5 second instead of 1

                    except Exception as e:
                        logger.error(f"Error processing row {index}: {e}")
                        df.at[index, 'BCPA_Owner_Found'] = 'Error'
                        # Make sure to close the page even on error
                        try:
                            await fresh_page.close()
                        except:
                            pass
                        continue

                await browser.close()

            # Remove unwanted columns before saving
            columns_to_remove = ['BCPA_Multiple_Owners', 'Owner Name 2', 'Primary_Phone', 'Secondary_Phone', 'BCPA_Skip_Reason']
            for col in columns_to_remove:
                if col in df.columns:
                    df = df.drop(columns=[col])
                    logger.info(f"🗑️ Removed column: {col}")

            # Save updated CSV
            df.to_csv(output_csv_path, index=False)

            logger.info("=" * 60)
            logger.info("BCPA OWNER SEARCH COMPLETED")
            logger.info(f"📊 Searches performed: {self.searches_performed}")
            logger.info(f"✅ Owners found: {self.results_found}")
            logger.info(f"📄 Updated CSV saved: {output_csv_path}")
            logger.info("=" * 60)

            return output_csv_path

        except Exception as e:
            logger.error(f"Error processing CSV: {e}")
            raise

# Flask integration functions
def process_bcpa_lookup(csv_path: str, max_records: int = 50):
    """
    Process BCPA lookup for Flask integration

    Args:
        csv_path: Path to input CSV file
        max_records: Maximum records to process

    Returns:
        str: Path to output file or None if failed
    """
    try:
        # Create output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"bcpa_lookup_{timestamp}.csv"
        output_path = os.path.join('results', output_filename)

        # Create a temporary file with limited records
        df = read_data_file(csv_path)
        if max_records > 0 and len(df) > max_records:
            df = df.head(max_records)
            logger.info(f"Limited processing to {max_records} records")

        temp_input = f"temp/temp_bcpa_{timestamp}.csv"
        df.to_csv(temp_input, index=False)

        # Run BCPA processing - ENFORCE HEADLESS MODE
        searcher = BCPAOwnerSearch(headless=True, delay_between_searches=0.5)  # Faster for batch processing
        logger.info("🔒 BCPA processing running in FORCED HEADLESS mode")

        # Use asyncio to run the async function
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            result_path = loop.run_until_complete(searcher.process_csv(temp_input, output_path))
            if result_path and os.path.exists(result_path):
                # Clean up temp file
                if os.path.exists(temp_input):
                    os.remove(temp_input)
                return result_path
            else:
                return None
        finally:
            loop.close()

    except Exception as e:
        logger.error(f"BCPA lookup failed: {e}")
        return None

async def main():
    """Main function for CLI usage - ALWAYS HEADLESS"""
    import argparse

    parser = argparse.ArgumentParser(description="BCPA Owner Search Script - HEADLESS MODE")
    parser.add_argument('input_csv', help='Input CSV file path')
    parser.add_argument('--output', help='Output CSV file path')
    parser.add_argument('--delay', type=int, default=0.3, help='Delay between searches in seconds')

    args = parser.parse_args()

    # Force headless mode regardless of arguments
    searcher = BCPAOwnerSearch(headless=True, delay_between_searches=args.delay)
    logger.info("🔒 BCPA Search running in FORCED HEADLESS mode")

    try:
        output_path = await searcher.process_csv(args.input_csv, args.output)
        logger.info(f"Process completed! Output saved to: {output_path}")

    except Exception as e:
        logger.error(f"Process failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
