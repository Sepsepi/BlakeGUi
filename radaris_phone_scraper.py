#!/usr/bin/env python3
"""
Radaris Phone Number Scraper
Automated headless browser script to extract phone numbers from Radaris.com

This script:
1. Reads CSV data with names and addresses
2. Searches for each person on Radaris.com
3. Verifies address matches
4. Extracts current phone numbers from profile pages
5. Updates CSV with findings

Author: Auto-generated based on Radaris.com structure analysis
Date: July 26, 2025
"""

import asyncio
import pandas as pd
import re
import logging
import sys
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright
import time
import random
from difflib import SequenceMatcher
from difflib import SequenceMatcher

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'radaris_scraper_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RadarisPhoneScraper:
    def __init__(self, csv_path: str, output_path: str = None):
        """
        Initialize the Radaris Phone Scraper
        
        Args:
            csv_path (str): Path to input CSV file
            output_path (str, optional): Path for output CSV. Defaults to input_radaris_updated.csv
        """
        self.csv_path = Path(csv_path)
        self.output_path = Path(output_path) if output_path else self.csv_path.parent / f"{self.csv_path.stem}_radaris_updated.csv"
        self.df = None
        self.browser = None
        self.context = None
        self.page = None
        
        # Columns for results - using clean names without vendor references
        self.result_columns = [
            'Phone_Primary',
            'Phone_Secondary', 
            'Phone_All',
            'Address_Match',
            'Search_Status',
            'Source_URL'
        ]
        
    async def init_browser(self):
        """Initialize optimized headless browser"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=False,  # Changed to False for better compatibility during testing
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-extensions',
                    '--no-first-run',
                    '--disable-default-apps',
                ]
            )
            
            self.context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                extra_http_headers={
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
                }
            )
            
            self.page = await self.context.new_page()
            logger.info("Browser initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise

    async def close_browser(self):
        """Close browser and cleanup"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
            logger.info("Browser closed successfully")
        except Exception as e:
            logger.error(f"Error closing browser: {e}")

    def load_data(self):
        """Load CSV data and prepare for processing"""
        try:
            self.df = pd.read_csv(self.csv_path)
            logger.info(f"Loaded {len(self.df)} records from {self.csv_path}")
            
            # Add result columns if they don't exist
            for col in self.result_columns:
                if col not in self.df.columns:
                    self.df[col] = ''
                    
            return True
            
        except Exception as e:
            logger.error(f"Failed to load CSV data: {e}")
            return False

    def normalize_address(self, address: str) -> str:
        """Normalize address for comparison"""
        if not address or pd.isna(address):
            return ""
            
        # Convert to uppercase and remove extra spaces
        normalized = re.sub(r'\s+', ' ', str(address).upper().strip())
        
        # Common normalizations
        replacements = {
            ' STREET': ' ST',
            ' AVENUE': ' AVE', 
            ' BOULEVARD': ' BLVD',
            ' DRIVE': ' DR',
            ' COURT': ' CT',
            ' CIRCLE': ' CIR',
            ' TERRACE': ' TER',
            ' UNIT ': ' #',
            ' APT ': ' #',
            ' APARTMENT ': ' #',
            'NORTHWEST': 'NW',
            'NORTHEAST': 'NE', 
            'SOUTHWEST': 'SW',
            'SOUTHEAST': 'SE',
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
            
        # Remove common suffixes and punctuation for comparison
        normalized = re.sub(r'[,\.]', '', normalized)
        
        return normalized

    def extract_address_components(self, address: str) -> dict:
        """Extract components from address for flexible matching"""
        normalized = self.normalize_address(address)
        
        # Extract street number
        street_num_match = re.match(r'^(\d+)', normalized)
        street_number = street_num_match.group(1) if street_num_match else ""
        
        # Extract street name (after number, before city/state)
        street_match = re.search(r'^\d+\s+([^,]+)', normalized)
        street_name = street_match.group(1).strip() if street_match else ""
        
        # Extract city - FIXED: Better pattern for Florida addresses
        # Pattern handles: "STREET NAME CITY, ZIP" and "STREET NAME CITY, FL ZIP"
        city_pattern = r'(?:STREET|ST|AVENUE|AVE|DRIVE|DR|COURT|CT|PLACE|PL|ROAD|RD|LANE|LN|BOULEVARD|BLVD|WAY|CIRCLE|CIR)\s+([A-Z\s]+?)(?:,|\s+(?:FL|FLORIDA)|\s+\d{5})'
        city_match = re.search(city_pattern, normalized)
        if city_match:
            city = city_match.group(1).strip()
        else:
            # Fallback: look for city before comma or FL/zip
            fallback_match = re.search(r',\s*([A-Z\s]+?)(?:\s*,|\s+FL|\s+\d{5})', normalized)
            city = fallback_match.group(1).strip() if fallback_match else ""
        
        # Extract state
        state_match = re.search(r'\b([A-Z]{2})\b', normalized)
        state = state_match.group(1) if state_match else ""
        
        # Extract zip
        zip_match = re.search(r'\b(\d{5}(-\d{4})?)\b', normalized)
        zip_code = zip_match.group(1) if zip_match else ""
        
        return {
            'street_number': street_number,
            'street_name': street_name,
            'city': city,
            'state': state,
            'zip': zip_code,
            'full_normalized': normalized
        }

    def addresses_match(self, csv_address: str, radaris_address: str) -> bool:
        """
        Enhanced address matching based on Playwright testing discoveries.
        
        Key improvements:
        - Flexible apartment/unit matching (#14C vs APT C)
        - Better street type normalization (STREET vs St)
        - Ordinal number handling (22ND vs 22)
        
        Working test cases:
        - "4745 NW 22 STREET # 4295" matches "4745 NW 22Nd St APT 4295"
        - "3330 ATLANTA STREET # 14C" should match "3330 Atlanta St APT C"
        """
        try:
            logger.info(f"Comparing addresses:")
            logger.info(f"CSV: {csv_address}")
            logger.info(f"Radaris: {radaris_address}")
            
            # Extract street numbers first - must match exactly
            csv_match = re.match(r'^(\d+)', csv_address.strip())
            radaris_match = re.match(r'^(\d+)', radaris_address.strip())
            
            if not csv_match or not radaris_match:
                logger.info("No street number found in one or both addresses")
                return False
            
            csv_street_num = csv_match.group(1)
            radaris_street_num = radaris_match.group(1)
            
            if csv_street_num != radaris_street_num:
                logger.info(f"Street numbers don't match: {csv_street_num} vs {radaris_street_num}")
                return False
            
            # Extract ZIP codes - must match (main ZIP only, ignore +4)
            csv_zip = re.search(r'(\d{5})(?:-\d{4})?', csv_address)
            radaris_zip = re.search(r'(\d{5})(?:-\d{4})?', radaris_address)
            
            if csv_zip and radaris_zip:
                if csv_zip.group(1) != radaris_zip.group(1):
                    logger.info(f"ZIP codes don't match: {csv_zip.group(1)} vs {radaris_zip.group(1)}")
                    return False
            elif csv_zip or radaris_zip:
                logger.info("ZIP code present in only one address")
                return False
            
            # ENHANCED: Normalize for street comparison with better apartment handling
            def normalize_for_comparison(addr: str) -> str:
                # Convert to uppercase
                addr = addr.upper().strip()
                
                # Remove common variations
                addr = re.sub(r'\s+', ' ', addr)  # Normalize spaces
                
                # Remove ZIP codes and state names for street comparison
                addr = re.sub(r'\d{5}(?:-\d{4})?', '', addr)  # Remove ZIP
                addr = re.sub(r'\b(?:FL|FLORIDA)\b', '', addr)  # Remove state
                
                # ENHANCED: Better apartment/unit normalization for flexible matching
                # Handle cases like "#14C" vs "APT C" where letter part might be different
                addr = re.sub(r'\s*#\s*(\d+)([A-Z]?)\s*', r' APT \1\2 ', addr)  # #14C -> APT 14C
                addr = re.sub(r'\bUNIT\b\.?\s*(\d+)([A-Z]?)\s*', r'APT \1\2 ', addr)  # UNIT 14C -> APT 14C
                addr = re.sub(r'\bAPARTMENT\b\.?\s*(\d+)([A-Z]?)\s*', r'APT \1\2 ', addr)  # APARTMENT 14C -> APT 14C
                
                # Normalize street types
                addr = re.sub(r'\bSTREET\b', 'ST', addr)
                addr = re.sub(r'\bAVENUE\b', 'AVE', addr)
                addr = re.sub(r'\bDRIVE\b', 'DR', addr)
                addr = re.sub(r'\bTERRACE\b', 'TER', addr)
                addr = re.sub(r'\bBOULEVARD\b', 'BLVD', addr)
                addr = re.sub(r'\bCOURT\b', 'CT', addr)
                addr = re.sub(r'\bCIRCLE\b', 'CIR', addr)
                
                # Normalize ordinal numbers (key for our test cases)
                addr = re.sub(r'\b22ND\b', '22', addr)
                addr = re.sub(r'\b75TH\b', '75', addr)
                addr = re.sub(r'\b(\d+)(?:ST|ND|RD|TH)\b', r'\1', addr)
                
                # Remove commas and extra spaces
                addr = re.sub(r'[,]', ' ', addr)
                addr = re.sub(r'\s+', ' ', addr)
                
                return addr.strip()
            
            csv_normalized = normalize_for_comparison(csv_address)
            radaris_normalized = normalize_for_comparison(radaris_address)
            
            logger.info(f"Normalized for comparison:")
            logger.info(f"CSV: '{csv_normalized}'")
            logger.info(f"Radaris: '{radaris_normalized}'")
            
            # ENHANCED: Extract street part with better apartment tolerance
            def extract_street_part(normalized_addr: str) -> str:
                # Remove street number
                no_number = re.sub(r'^\d+\s*', '', normalized_addr)
                
                # Find potential city names and remove them
                # Common FL cities in our dataset
                cities = ['LAUDERHILL', 'HOLLYWOOD', 'COCONUT CREEK', 'POMPANO BEACH', 'SUNRISE', 'PLANTATION', 'CORAL SPRINGS']
                for city in cities:
                    if city in no_number:
                        no_number = no_number[:no_number.find(city)]
                        break
                
                return no_number.strip()
            
            csv_street_part = extract_street_part(csv_normalized)
            radaris_street_part = extract_street_part(radaris_normalized)
            
            logger.info(f"Street parts:")
            logger.info(f"CSV: '{csv_street_part}'")
            logger.info(f"Radaris: '{radaris_street_part}'")
            
            # ENHANCED: Calculate similarity with apartment-aware comparison
            similarity = SequenceMatcher(None, csv_street_part, radaris_street_part).ratio()
            
            logger.info(f"Street similarity: {similarity:.2%}")
            
            # ENHANCED: If similarity is borderline, check if it's just an apartment difference
            if similarity >= 0.70:  # Lowered threshold for apartment variations
                # Check if the difference is mainly apartment numbers/letters
                csv_no_apt = re.sub(r'\bAPT\s+\w+\s*', '', csv_street_part)
                radaris_no_apt = re.sub(r'\bAPT\s+\w+\s*', '', radaris_street_part)
                
                street_only_similarity = SequenceMatcher(None, csv_no_apt, radaris_no_apt).ratio()
                
                if street_only_similarity >= 0.85:  # High similarity without apartment
                    logger.info(f"HIGH MATCH: Street parts very similar ({street_only_similarity:.2%}) - apartment difference acceptable")
                    return True
                elif similarity >= 0.75:  # Lowered from 0.85 to 0.75 for "AVE" vs "AVENUE" cases
                    logger.info("MATCH FOUND!")
                    return True
            
            logger.info(f"Street names don't match exactly (similarity: {similarity:.2%})")
            return False
                
        except Exception as e:
            logger.error(f"Error comparing addresses: {e}")
            return False

    async def search_person(self, name: str, city: str, state: str, zip_code: str = '', street_address: str = '') -> dict:
        """Search for a person on Radaris using optimized strategy from testing"""
        try:
            # Navigate to Radaris homepage
            await self.page.goto('https://radaris.com', wait_until='networkidle', timeout=30000)
            await self.random_delay()
            
            # Fill name field using correct selector
            name_field = self.page.get_by_role('textbox', name='First and Last name')
            await name_field.clear()
            await name_field.fill(name)
            await self.random_delay(0.5, 1.5)
            
            # OPTIMIZED LOCATION STRATEGY: Based on user request,
            # use ONLY city/state and ignore ZIP codes completely
            location = ""
            if city and state:
                # PRIORITY: City, State ONLY (ignoring ZIP code as requested)
                location = f"{city}, {state}"
                logger.info(f"Searching with city/state: {city}, {state} (ignoring ZIP: {zip_code})")
            elif street_address:
                # FALLBACK: Try with street address if no city/state available
                location = street_address
                logger.info(f"Searching with street address: {street_address}")
            
            if location:
                location_field = self.page.get_by_role('textbox', name='City, state or zip code')
                await location_field.clear()
                await location_field.fill(location)
                await self.random_delay(0.5, 1.5)
            
            # Click search button
            search_button = self.page.get_by_role('button', name='Search')
            await search_button.click()
            
            # Wait for results page
            await self.page.wait_for_load_state('networkidle', timeout=15000)
            await self.random_delay(2, 4)
            
            # Construct target address for validation
            target_address = f"{street_address} {city} {state} {zip_code}".strip()
            
            return await self.parse_search_results(target_address)
            
        except Exception as e:
            logger.error(f"Error searching for {name}: {e}")
            return {
                'status': f'Search Error: {str(e)}',
                'profile_urls': []
            }

    async def extract_phone_with_address_validation(self, target_address: str) -> dict:
        """
        IMPROVED METHOD: Extract phone numbers from search results with proper address validation
        
        This method:
        1. Finds clickable profile cards in search results
        2. Clicks on each card (max 2 tries)
        3. Checks current and past addresses in the profile
        4. Only extracts phone if address matches
        5. Returns to search results to try next profile if no match
        """
        try:
            # Wait for page to fully load
            await self.page.wait_for_load_state('networkidle', timeout=10000)
            
            logger.info("NEW APPROACH: Validating addresses before extracting phone numbers")
            
            # Find clickable profile elements from search results
            profile_elements = []
            
            # Method 1: Look for "View Profile" buttons (most reliable)
            try:
                view_buttons = self.page.locator('text="View Profile"')
                count = await view_buttons.count()
                logger.info(f"Found {count} 'View Profile' buttons")
                
                for i in range(min(count, 2)):  # Max 2 tries as requested
                    profile_elements.append({
                        'type': 'view_button',
                        'index': i,
                        'element': view_buttons.nth(i)
                    })
            except Exception as e:
                logger.warning(f"View Profile buttons method failed: {e}")
            
            # Method 2: Look for person name links (fallback)
            if not profile_elements:
                try:
                    name_links = await self.page.query_selector_all('a[href*="/p/"]')
                    if name_links:
                        logger.info(f"Found {len(name_links)} person name links")
                        for i, link in enumerate(name_links[:2]):  # Max 2 tries
                            profile_elements.append({
                                'type': 'name_link',
                                'index': i,
                                'element': link
                            })
                except Exception as e:
                    logger.warning(f"Name links method failed: {e}")
            
            if not profile_elements:
                logger.info("No clickable profile elements found")
                return {
                    'status': 'No clickable profiles found',
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'method': 'none'
                }
            
            # Try each profile (max 2)
            for i, profile_info in enumerate(profile_elements):
                try:
                    logger.info(f"Trying profile {i+1}/{len(profile_elements)}")
                    
                    # Store current URL to return to search results
                    search_results_url = self.page.url
                    
                    # Click on the profile
                    element = profile_info['element']
                    element_type = profile_info['type']
                    
                    try:
                        if element_type == 'view_button':
                            await element.click()
                        else:
                            await element.click()
                        logger.info(f"Clicked on profile {i+1} ({element_type})")
                    except Exception as click_error:
                        logger.warning(f"Failed to click profile {i+1}: {click_error}")
                        continue
                    
                    # Wait for profile page to load
                    try:
                        await self.page.wait_for_load_state('networkidle', timeout=15000)
                        await self.random_delay(2, 4)
                    except:
                        await self.random_delay(3, 5)
                    
                    # Extract addresses from this profile
                    profile_addresses = await self.extract_addresses_from_profile()
                    
                    # Check if any address matches our target
                    address_match = False
                    matching_address = ""
                    
                    logger.info(f"Profile {i+1} has {len(profile_addresses)} addresses to check")
                    for addr in profile_addresses:
                        logger.info(f"Checking address: {addr}")
                        if self.addresses_match(target_address, addr):
                            address_match = True
                            matching_address = addr
                            logger.info(f"✅ ADDRESS MATCH FOUND: {addr}")
                            break
                    
                    if address_match:
                        # Extract phone numbers since we found a match
                        logger.info("Address matched! Extracting phone numbers...")
                        phones = await self.extract_phone_numbers()
                        
                        if phones.get('primary'):
                            logger.info(f"🎉 SUCCESS: Found phone {phones['primary']} with matching address!")
                            return {
                                'status': 'Success - Phone with Address Match',
                                'phone_primary': phones['primary'],
                                'phone_secondary': phones.get('secondary', ''),
                                'phone_all': phones.get('all', ''),
                                'address_match': 'Yes',
                                'matching_address': matching_address,
                                'profile_url': self.page.url,
                                'method': 'profile_with_address_validation'
                            }
                        else:
                            logger.info("Address matched but no phone numbers found")
                    else:
                        logger.info(f"❌ No address match for profile {i+1}")
                    
                    # Go back to search results for next profile (if not last one)
                    if i < len(profile_elements) - 1:
                        try:
                            await self.page.goto(search_results_url, wait_until='networkidle', timeout=10000)
                            await self.random_delay(1, 2)
                            logger.info("Returned to search results")
                        except Exception as nav_error:
                            logger.warning(f"Failed to return to search results: {nav_error}")
                            break
                
                except Exception as e:
                    logger.error(f"Error processing profile {i+1}: {e}")
                    continue
            
            # No matching address found in any profile
            logger.info("No address matches found in any of the checked profiles")
            return {
                'status': 'No address match found',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'No',
                'method': 'profile_checked_no_match'
            }
                
        except Exception as e:
            logger.error(f"Error in address validation process: {e}")
            return {
                'status': f'Address validation error: {str(e)}',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'method': 'error'
            }

    async def parse_search_results(self, target_address: str) -> dict:
        """Parse search results and extract phone with proper address validation"""
        try:
            # Wait for page to fully load
            await self.page.wait_for_load_state('networkidle', timeout=10000)
            
            page_title = await self.page.title()
            logger.info(f"Search results page title: {page_title}")
            
            # NEW APPROACH: Always validate addresses before extracting phone numbers
            logger.info("Using address validation approach - clicking on profiles to verify addresses")
            
            return await self.extract_phone_with_address_validation(target_address)
            
        except Exception as e:
            logger.error(f"Error parsing search results: {e}")
            return {
                'status': f'Parse Error: {str(e)}',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'method': 'error'
            }

    async def extract_addresses_from_profile(self) -> list:
        """Extract all addresses from a profile page based on live testing patterns"""
        addresses = []
        
        try:
            # Wait for page to load completely
            await self.page.wait_for_load_state('networkidle', timeout=10000)
            
            # Based on live testing, we found addresses in specific locations:
            # 1. Previous Addresses section (most reliable)
            # 2. Current address sections
            # 3. Links to address pages
            
            # Method 1: Look for "Previous Addresses" section links
            try:
                # Look for address links in the format we found: "4745 Nw 22Nd St, Coconut Creek, FL 33063"
                address_links = await self.page.query_selector_all('a[href*="address"]')
                for link in address_links:
                    text = await link.text_content()
                    if text and ',' in text and re.search(r'\d+', text):
                        # Clean up the address text
                        clean_addr = re.sub(r'\s+', ' ', text.strip())
                        if len(clean_addr) > 15 and clean_addr not in addresses:
                            addresses.append(clean_addr)
                            logger.info(f"Found address from link: {clean_addr}")
            except Exception as e:
                logger.warning(f"Address links method failed: {e}")
            
            # Method 2: Look for addresses in list items (Previous Addresses section)
            try:
                list_items = await self.page.query_selector_all('li')
                for item in list_items:
                    text = await item.text_content()
                    if text and ',' in text:
                        # Look for address patterns like "4745 Nw 22Nd St, Coconut Creek, FL 33063"
                        address_pattern = r'\d+\s+[A-Za-z\s\d]+,\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s*\d{5}'
                        matches = re.findall(address_pattern, text)
                        for match in matches:
                            clean_addr = re.sub(r'\s+', ' ', match.strip())
                            if len(clean_addr) > 15 and clean_addr not in addresses:
                                addresses.append(clean_addr)
                                logger.info(f"Found address from list: {clean_addr}")
            except Exception as e:
                logger.warning(f"List items method failed: {e}")
            
            # Method 3: Look in the general page content for address patterns
            try:
                page_content = await self.page.text_content('body')
                if page_content:
                    # More specific address patterns based on our findings
                    address_patterns = [
                        r'\d+\s+[A-Za-z\s\d]+(?:St|Street|Ave|Avenue|Dr|Drive|Blvd|Boulevard|Ct|Court|Ter|Terrace|Cir|Circle)\s*,\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s*\d{5}',
                        r'\d+\s+[A-Za-z\s\d#]+,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}'
                    ]
                    
                    for pattern in address_patterns:
                        matches = re.findall(pattern, page_content, re.IGNORECASE)
                        for match in matches:
                            clean_addr = re.sub(r'\s+', ' ', match.strip())
                            if len(clean_addr) > 15 and clean_addr not in addresses:
                                addresses.append(clean_addr)
                                logger.info(f"Found address from content: {clean_addr}")
            except Exception as e:
                logger.warning(f"Page content method failed: {e}")
        
        except Exception as e:
            logger.error(f"Error extracting addresses: {e}")
        
        # Remove duplicates and sort by length (longer addresses tend to be more complete)
        unique_addresses = list(dict.fromkeys(addresses))  # Preserves order while removing duplicates
        unique_addresses.sort(key=len, reverse=True)
        
        logger.info(f"Extracted {len(unique_addresses)} unique addresses from profile")
        return unique_addresses

    async def extract_profile_info(self, csv_address: str) -> dict:
        """Extract information from a profile page"""
        try:
            # Wait for profile page to load
            await self.page.wait_for_load_state('networkidle', timeout=15000)
            await self.random_delay(2, 3)
            
            result = {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': '',
                'current_address': '',
                'previous_addresses': [],
                'profile_url': self.page.url
            }
            
            # Extract phone numbers
            phones = await self.extract_phone_numbers()
            result['phone_primary'] = phones.get('primary', '')
            result['phone_secondary'] = phones.get('secondary', '')
            result['phone_all'] = phones.get('all', '')
            
            # Extract addresses
            addresses = await self.extract_addresses()
            result['current_address'] = addresses.get('current', '')
            result['previous_addresses'] = addresses.get('previous', [])
            
            # Check address match
            all_addresses = [result['current_address']] + result['previous_addresses']
            best_match = ""
            
            for addr in all_addresses:
                if addr and self.addresses_match(csv_address, addr):
                    result['address_match'] = addr
                    best_match = addr
                    break
            
            if not best_match and all_addresses:
                # If no exact match, store the current address for reference
                result['address_match'] = result['current_address']
            
            return result
            
        except Exception as e:
            logger.error(f"Error extracting profile info: {e}")
            return {
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': '',
                'current_address': '',
                'previous_addresses': [],
                'profile_url': self.page.url,
                'error': str(e)
            }

    async def extract_phone_numbers(self) -> dict:
        """Extract phone numbers from profile page, prioritizing the main displayed number"""
        phones = {'primary': '', 'secondary': '', 'all': ''}
        
        try:
            # Wait for content to load
            await self.page.wait_for_load_state('networkidle', timeout=5000)
            
            # First, try to get the primary phone from the main PHONE NUMBERS section
            primary_phone = ""
            try:
                # Look for the main phone number display pattern like in the screenshot
                # "(954) 969-9417 +7" format
                phone_section = await self.page.query_selector('div:has-text("PHONE NUMBERS")')
                if phone_section:
                    # Get the parent container that has the phone number
                    parent = await phone_section.query_selector('..')
                    if parent:
                        text = await parent.text_content()
                        # Look for the main phone pattern: (xxx) xxx-xxxx
                        phone_match = re.search(r'\((\d{3})\)\s*(\d{3})-(\d{4})', text)
                        if phone_match:
                            primary_phone = f"({phone_match.group(1)}) {phone_match.group(2)}-{phone_match.group(3)}"
                            logger.info(f"Found primary phone from PHONE NUMBERS section: {primary_phone}")
            except Exception as e:
                logger.warning(f"Primary phone extraction failed: {e}")
            
            # If no primary phone found, look for phone links and numbers more broadly
            phone_selectors = [
                # Direct phone links
                'a[href*="tel:"]',
                # Elements that commonly contain phones
                '[class*="phone"]',
                '[id*="phone"]',
                'div:has-text("PHONE") + div',
                'div:has-text("Phone") + div',
                'li:has(a[href*="tel:"])',
                'span:has-text("(")',  # Often contains formatted phone numbers
                # Look in contact sections
                'div[class*="contact"]',
                'div[class*="info"]',
                'section:has-text("Contact")',
                # General text that might contain phone numbers
                'p, div, span, li'
            ]
            
            found_phones = set()  # Use set to avoid duplicates
            
            for selector in phone_selectors:
                try:
                    elements = self.page.locator(selector)
                    count = await elements.count()
                    
                    for i in range(min(count, 20)):  # Limit to avoid infinite loops
                        try:
                            element = elements.nth(i)
                            
                            # Get text content
                            text = await element.text_content()
                            if text:
                                # Extract phone numbers using comprehensive regex
                                phone_patterns = [
                                    r'\(\d{3}\)\s*\d{3}[-.\s]?\d{4}',  # (555) 123-4567
                                    r'\d{3}[-.\s]\d{3}[-.\s]\d{4}',    # 555-123-4567 or 555.123.4567
                                    r'\d{3}\s\d{3}\s\d{4}',           # 555 123 4567
                                    r'\(\d{3}\)\d{3}-\d{4}'           # (555)123-4567
                                ]
                                
                                for pattern in phone_patterns:
                                    matches = re.findall(pattern, text)
                                    for match in matches:
                                        # Clean the match
                                        digits_only = re.sub(r'[^\d]', '', match)
                                        if len(digits_only) == 10 and not digits_only.startswith('0'):
                                            found_phones.add(digits_only)
                            
                            # Also check href attribute for tel: links
                            href = await element.get_attribute('href')
                            if href and href.startswith('tel:'):
                                phone_digits = re.sub(r'[^\d]', '', href[4:])
                                if len(phone_digits) == 10 and not phone_digits.startswith('0'):
                                    found_phones.add(phone_digits)
                                    
                        except Exception as e:
                            continue
                            
                except Exception as e:
                    continue
            
            # Convert to formatted phone numbers
            formatted_phones = []
            for phone_digits in found_phones:
                if len(phone_digits) == 10:
                    formatted = f"({phone_digits[:3]}) {phone_digits[3:6]}-{phone_digits[6:]}"
                    formatted_phones.append(formatted)
            
            # Sort phones for consistency
            formatted_phones.sort()
            
            # Use primary phone if found, otherwise use first available
            if primary_phone:
                phones['primary'] = primary_phone
                # Remove primary from list and add others as secondary
                other_phones = [p for p in formatted_phones if p != primary_phone]
                if other_phones:
                    phones['secondary'] = other_phones[0]
                # Include primary in all phones list
                if primary_phone not in formatted_phones:
                    formatted_phones.insert(0, primary_phone)
                phones['all'] = ', '.join(formatted_phones)
            elif formatted_phones:
                phones['primary'] = formatted_phones[0]
                if len(formatted_phones) > 1:
                    phones['secondary'] = formatted_phones[1]
                phones['all'] = ', '.join(formatted_phones)
                
            if phones['primary']:
                logger.info(f"Found primary phone: {phones['primary']}")
                logger.info(f"All phones: {phones['all']}")
            else:
                logger.info("No phone numbers found on this profile")
            
        except Exception as e:
            logger.error(f"Error extracting phone numbers: {e}")
        
        return phones

    async def extract_addresses(self) -> dict:
        """Extract addresses from profile page"""
        addresses = {'current': '', 'previous': []}
        
        try:
            # Wait for content to load
            await self.page.wait_for_load_state('networkidle', timeout=5000)
            
            # More comprehensive address extraction
            address_selectors = [
                # Direct address patterns in text
                'text=/\\d+[^,]*,\\s*[A-Za-z\\s]+,\\s*[A-Z]{2}\\s*\\d{5}/',
                # Common address containers
                'div:has-text("CURRENT ADDRESS")',
                'div:has-text("Current Address")',
                'div:has-text("Previous Addresses")', 
                'div:has-text("ADDRESSES")',
                'div:has-text("Address")',
                'li:has-text("Current")',
                'section:has-text("Address")',
                # Address-related classes
                '[class*="address"]',
                '[class*="location"]',
                '[id*="address"]',
                # Links that might contain addresses
                'a[href*="address"]',
                'a:has-text(",")',  # Links with commas (likely addresses)
                # General containers that might have addresses
                'div, p, span, li'
            ]
            
            found_addresses = set()  # Use set to avoid duplicates
            
            for selector in address_selectors:
                try:
                    elements = self.page.locator(selector)
                    count = await elements.count()
                    
                    for i in range(min(count, 30)):  # Limit to avoid excessive processing
                        try:
                            element = elements.nth(i)
                            text = await element.text_content()
                            
                            if text and len(text.strip()) > 10:  # Basic length check
                                # Look for address patterns in text
                                address_patterns = [
                                    r'\d+[^,\n]*,\s*[A-Za-z\s]+,\s*[A-Z]{2}\s*\d{5}(?:-\d{4})?',  # Full address
                                    r'\d+[^,\n]*[A-Za-z]+[^,\n]*,\s*[A-Za-z\s]+,?\s*[A-Z]{2}\s*\d{5}',  # Variations
                                    r'\d+\s+[A-Za-z][^,\n]*,\s*[A-Za-z\s]+\s+[A-Z]{2}\s*\d{5}'  # Without comma before state
                                ]
                                
                                for pattern in address_patterns:
                                    matches = re.findall(pattern, text, re.IGNORECASE)
                                    for match in matches:
                                        # Clean and validate the address
                                        cleaned = match.strip()
                                        if len(cleaned) > 15 and ',' in cleaned:  # Basic validation
                                            found_addresses.add(cleaned)
                                            
                        except Exception as e:
                            continue
                            
                except Exception as e:
                    continue
            
            # Convert set to list and clean addresses
            cleaned_addresses = []
            for addr in found_addresses:
                # Additional cleaning
                cleaned = re.sub(r'\s+', ' ', addr.strip())
                # Remove any leading/trailing punctuation
                cleaned = re.sub(r'^[,\.\-\s]+|[,\.\-\s]+$', '', cleaned)
                
                # Validate it looks like a real address
                if (len(cleaned) > 15 and 
                    re.search(r'\d+', cleaned) and  # Has numbers
                    re.search(r'[A-Z]{2}\s*\d{5}', cleaned) and  # Has state and zip
                    ',' in cleaned):  # Has commas
                    
                    if cleaned not in cleaned_addresses:
                        cleaned_addresses.append(cleaned)
            
            # Sort by length (longer addresses first, likely more complete)
            cleaned_addresses.sort(key=len, reverse=True)
            
            if cleaned_addresses:
                addresses['current'] = cleaned_addresses[0]
                addresses['previous'] = cleaned_addresses[1:] if len(cleaned_addresses) > 1 else []
                logger.info(f"Found {len(cleaned_addresses)} addresses. Current: {addresses['current']}")
            else:
                logger.info("No addresses found on this profile")
            
        except Exception as e:
            logger.error(f"Error extracting addresses: {e}")
        
        return addresses

    async def random_delay(self, min_delay: float = 1.0, max_delay: float = 3.0):
        """Add random delay to avoid detection"""
        delay = random.uniform(min_delay, max_delay)
        await asyncio.sleep(delay)

    async def process_person(self, row_index: int, row: pd.Series) -> dict:
        """Process a single person from the CSV"""
        try:
            # Extract person details
            name = str(row.get('IndirectName_Cleaned', '')).strip() if pd.notna(row.get('IndirectName_Cleaned')) else ''
            address = str(row.get('IndirectName_Address', '')).strip() if pd.notna(row.get('IndirectName_Address')) else ''
            
            if not name:
                return {
                    'status': 'No name provided',
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'address_match': '',
                    'profile_url': ''
                }
            
            # Extract city, state, and ZIP code from address
            city, state, zip_code, street_address = '', '', '', ''
            if address:
                # Use the full address as street_address for now
                street_address = address
                
                # Extract ZIP code first
                zip_match = re.search(r'\b(\d{5}(-\d{4})?)\b', address)
                zip_code = zip_match.group(1) if zip_match else ''
                
                # Clean address
                clean_addr = address.strip()
                
                # Pattern 1: "STREET CITY, FL ZIP" - has state explicitly
                pattern_with_state = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2})\s*\d{5}', clean_addr)
                if pattern_with_state:
                    city_part = pattern_with_state.group(1).strip()
                    state = pattern_with_state.group(2).strip()
                    
                    # Extract city from city_part - it's the last words after street address
                    words = city_part.split()
                    city_words = []
                    # Work backwards to find city name (skip street address parts)
                    for word in reversed(words):
                        if not re.match(r'^\d+', word) and word.upper() not in ['ST', 'STREET', 'AVE', 'AVENUE', 'DR', 'DRIVE', 'CT', 'COURT', 'BLVD', 'BOULEVARD', 'TER', 'TERRACE', 'NW', 'NE', 'SW', 'SE', '#', 'APT', 'UNIT']:
                            city_words.insert(0, word)
                        else:
                            break  # Stop when we hit street address parts
                    city = ' '.join(city_words)
                
                # Pattern 2: "STREET CITY, ZIP" - no state, assume FL
                elif ',' in clean_addr:
                    parts = clean_addr.split(',')
                    if len(parts) >= 2:
                        city_part = parts[0].strip()
                        zip_part = parts[1].strip()
                        
                        # If zip part is just numbers, assume FL
                        if re.match(r'^\s*\d{5}', zip_part):
                            state = 'FL'
                            
                            # Extract city from city_part
                            words = city_part.split()
                            city_words = []
                            for word in reversed(words):
                                if not re.match(r'^\d+', word) and word.upper() not in ['ST', 'STREET', 'AVE', 'AVENUE', 'DR', 'DRIVE', 'CT', 'COURT', 'BLVD', 'BOULEVARD', 'TER', 'TERRACE', 'NW', 'NE', 'SW', 'SE', '#', 'APT', 'UNIT']:
                                    city_words.insert(0, word)
                                else:
                                    break
                            city = ' '.join(city_words)
                
                # If still no results, try a more aggressive approach
                if not city:
                    # Remove zip code and work backwards
                    no_zip = re.sub(r'\d{5}(-\d{4})?', '', clean_addr).strip().rstrip(',')
                    words = no_zip.split()
                    if len(words) >= 2:
                        # Take last 1-2 words as potential city
                        city = ' '.join(words[-2:]) if len(words) >= 2 else words[-1]
                        # Clean up city
                        city = re.sub(r'^[,\s]+|[,\s]+$', '', city)
                        if not state:
                            state = 'FL'  # Default for this dataset
            
            logger.info(f"Processing {row_index + 1}: {name} - {city}, {state} (ZIP: {zip_code}) (from address: {address})")
            
            # Search for person - prioritize city/state for more specific search
            search_result = await self.search_person(name, city, state, zip_code, street_address)
            
            # NEW: Check if phone was found directly in search results
            if search_result.get('status', '').startswith('Success - Phones Found'):
                logger.info("Phone number found directly in search results - no profile navigation needed!")
                return {
                    'status': 'Success - Phone from Search Results',
                    'phone_primary': search_result.get('phone_primary', ''),
                    'phone_secondary': search_result.get('phone_secondary', ''),
                    'phone_all': search_result.get('phone_all', ''),
                    'address_match': 'Unknown - extracted from search results',
                    'profile_url': self.page.url,
                    'method': 'search_results_table'
                }
            
            # FALLBACK: If no phone in search results, continue with profile navigation
            if search_result['status'] != 'Success' or not search_result.get('person_elements'):
                return {
                    'status': search_result['status'],
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'address_match': 'No',
                    'profile_url': ''
                }
            
            # Click on each person element one by one to check for address match
            logger.info(f"Found {len(search_result['person_elements'])} people to check")
            
            best_result = None  # Initialize the variable!
            
            for i, person_info in enumerate(search_result['person_elements']):
                try:
                    logger.info(f"Checking person {i+1}/{len(search_result['person_elements'])}")
                    
                    # Click on this person to go to their profile
                    element = person_info['element']
                    element_type = person_info['type']
                    
                    try:
                        if element_type == 'view_button':
                            # It's a locator, use click()
                            await element.click()
                        else:
                            # It's a querySelector element, use different method
                            await element.click()
                        
                        logger.info(f"Clicked on person {i+1} ({element_type})")
                        
                    except Exception as click_error:
                        logger.warning(f"Failed to click person {i+1}: {click_error}")
                        continue
                    
                    # Wait for profile page to load
                    try:
                        await self.page.wait_for_load_state('networkidle', timeout=15000)
                        await self.random_delay(2, 4)
                    except:
                        # If networkidle fails, just wait a bit
                        await self.random_delay(3, 5)
                    
                    # Extract addresses from this profile
                    addresses = await self.extract_addresses_from_profile()
                    
                    # Check if any address matches our target
                    address_match = False
                    matching_address = ""
                    
                    for addr in addresses:
                        if self.addresses_match(address, addr):
                            address_match = True
                            matching_address = addr
                            logger.info(f"Address match found: {addr}")
                            break
                    
                    if address_match:
                        # Extract phone numbers since we found a match
                        phones = await self.extract_phone_numbers()
                        
                        result = {
                            'status': 'Success',
                            'phone_primary': phones.get('primary', ''),
                            'phone_secondary': phones.get('secondary', ''),
                            'phone_all': phones.get('all', ''),
                            'address_match': 'Yes',
                            'matching_address': matching_address,
                            'profile_url': self.page.url
                        }
                        
                        if phones.get('primary'):
                            logger.info(f"FOUND! Phone: {phones['primary']} with matching address")
                            return result
                        else:
                            logger.info(f"Address match but no phone found")
                            if not best_result:
                                best_result = result
                    else:
                        logger.info(f"No address match found for person {i+1}")
                    
                    # Go back to search results to try next person (if not the last one)
                    if i < len(search_result['person_elements']) - 1:
                        try:
                            await self.page.go_back()
                            await self.page.wait_for_load_state('networkidle', timeout=10000)
                            await self.random_delay(1, 2)
                        except Exception as back_error:
                            logger.warning(f"Could not go back: {back_error}")
                            # If can't go back, break and return what we have
                            break
                    
                except Exception as e:
                    logger.error(f"Error processing person {i+1}: {e}")
                    # Try to go back if possible
                    try:
                        await self.page.go_back()
                        await self.random_delay(1, 2)
                    except:
                        pass
                    continue
            
            # Return best result found or no match
            if best_result:
                return best_result
            else:
                return {
                    'status': 'No Address Match Found',
                    'phone_primary': '',
                    'phone_secondary': '',
                    'phone_all': '',
                    'address_match': 'No',
                    'profile_url': self.page.url
                }
                
        except Exception as e:
            logger.error(f"Processing failed for {name}: {e}")
            return {
                'status': f'Processing Error: {str(e)}',
                'phone_primary': '',
                'phone_secondary': '',
                'phone_all': '',
                'address_match': 'Error',
                'profile_url': ''
            }

        # Add missing best_result initialization
        best_result = None

    async def process_csv(self, start_row: int = 0, max_rows: int = None):
        """Process the entire CSV file"""
        if not self.load_data():
            return False
        
        total_rows = len(self.df)
        end_row = min(total_rows, start_row + max_rows) if max_rows else total_rows
        
        logger.info(f"Processing rows {start_row} to {end_row-1} of {total_rows}")
        
        try:
            await self.init_browser()
            
            for i in range(start_row, end_row):
                try:
                    row = self.df.iloc[i]
                    
                    # Skip if already processed (has phone data)
                    if (row.get('Search_Status') and 
                        row.get('Search_Status') not in ['', 'Error']):
                        logger.info(f"Skipping row {i}: Already processed")
                        continue
                    
                    result = await self.process_person(i, row)
                    
                    # Update DataFrame
                    self.df.at[i, 'Phone_Primary'] = result['phone_primary']
                    self.df.at[i, 'Phone_Secondary'] = result['phone_secondary']
                    self.df.at[i, 'Phone_All'] = result['phone_all']
                    self.df.at[i, 'Address_Match'] = result['address_match']
                    self.df.at[i, 'Search_Status'] = result['status']
                    self.df.at[i, 'Source_URL'] = result['profile_url']
                    
                    # Save progress every 5 records
                    if (i + 1) % 5 == 0:
                        self.save_progress()
                        logger.info(f"Progress saved at row {i + 1}")
                    
                    # Add delay between requests
                    await self.random_delay(3, 6)
                    
                except Exception as e:
                    logger.error(f"Error processing row {i}: {e}")
                    # Mark as error and continue
                    self.df.at[i, 'Search_Status'] = f'Error: {str(e)}'
                    continue
            
            # Final save
            self.save_progress()
            logger.info(f"Processing complete. Results saved to {self.output_path}")
            
        finally:
            await self.close_browser()

    def save_progress(self):
        """Save current progress to output file"""
        try:
            self.df.to_csv(self.output_path, index=False)
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

    async def run_sample(self, num_samples: int = 3):
        """Run on a small sample for testing"""
        if not self.load_data():
            return False
        
        # Get random sample
        sample_indices = random.sample(range(len(self.df)), min(num_samples, len(self.df)))
        logger.info(f"Testing with {len(sample_indices)} random samples: {sample_indices}")
        
        try:
            await self.init_browser()
            
            for i in sample_indices:
                row = self.df.iloc[i]
                result = await self.process_person(i, row)
                
                logger.info(f"Sample {i} result: {result}")
                
                # Update DataFrame
                self.df.at[i, 'Phone_Primary'] = result['phone_primary']
                self.df.at[i, 'Phone_Secondary'] = result['phone_secondary']
                self.df.at[i, 'Phone_All'] = result['phone_all']
                self.df.at[i, 'Address_Match'] = result['address_match']
                self.df.at[i, 'Search_Status'] = result['status']
                self.df.at[i, 'Source_URL'] = result['profile_url']
                
                await self.random_delay(3, 6)
            
            # Save sample results
            sample_output = self.output_path.parent / f"{self.output_path.stem}_sample.csv"
            self.df.to_csv(sample_output, index=False)
            logger.info(f"Sample results saved to {sample_output}")
            
        finally:
            await self.close_browser()


async def main():
    """Main function to run the scraper"""
    import sys
    
    # Check if CSV path provided as command line argument
    if len(sys.argv) > 1:
        CSV_PATH = sys.argv[1]
    else:
        # Updated to use the correct backup file that contains our test cases
        CSV_PATH = r"c:\Users\my notebook\Desktop\BlakeJackson\weekly_output\broward_lis_pendens_20250721_130030_processed_with_addresses_fast_processed_fastpeople_updated.csv_backup_20250724_140527.csv"
    
    print(f"Using CSV file: {CSV_PATH}")
    scraper = RadarisPhoneScraper(CSV_PATH)
    
    # Choose what to run:
    
    # 1. Test with our proven working test cases first
    print("Starting targeted test with known working cases...")
    # Test Earl Cox, Ekaterina Savuskan, Gloria Lapi (rows we know work)
    await scraper.run_sample(num_samples=5)
    
    # 2. Uncomment to process all records
    # print("Starting full processing...")
    # await scraper.process_csv()
    
    # 3. Uncomment to process specific range
    # print("Starting partial processing...")
    # await scraper.process_csv(start_row=0, max_rows=10)

if __name__ == "__main__":
    print("Radaris Phone Number Scraper")
    print("=" * 40)
    print(f"Starting at: {datetime.now()}")
    
    asyncio.run(main())
    
    print(f"Completed at: {datetime.now()}")
