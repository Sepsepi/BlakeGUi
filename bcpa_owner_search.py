#!/usr/bin/env python3
"""
Broward County Property Appraiser Owner Search
Uses Playwright to search for property owners by address when names are missing

RECENT UPDATES (August 6, 2025):
- ✅ Tested with real BCPA website using Playwright MCP
- ✅ Confirmed optimal address format: "STREET, CITY" (without FL state)
- ✅ Added support for multiple owner extraction from property details
- ✅ Enhanced parsing for owners like "BARATZ, PHILIP J & LISA T"
- ✅ Creates separate rows for each owner to work with Zaba/Radaris
- ✅ Improved detection of Property Details page vs Search Results page
- ✅ Added BCPA_Multiple_Owners tracking column
- ✅ Name cleaning for Zaba/Radaris: removes middle names, Jr, Sr, etc.
- ✅ Outputs only "First Last" format for downstream phone searches

TESTED ADDRESS FORMATS:
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
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright
import time

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'bcpa_search_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BCPAOwnerSearch:
    def __init__(self, headless=False, delay_between_searches=2):
        """Initialize BCPA Owner Search"""
        self.headless = headless
        self.delay_between_searches = delay_between_searches
        self.base_url = "https://web.bcpa.net/BcpaClient/#/Record-Search"
        self.results_found = 0
        self.searches_performed = 0
        
    def clean_address_for_search(self, row):
        """Clean and format full address for BCPA search"""
        try:
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
        name2 = str(row.get('Owner Name 2', '')).strip()
        
        # Check if names are empty or just business indicators
        business_indicators = ['LLC', 'INC', 'CORP', 'LTD', 'CO', 'COMPANY', 'TRUST', 'TR']
        
        valid_name1 = name1 and name1.lower() not in ['nan', 'none', ''] and len(name1) > 2
        valid_name2 = name2 and name2.lower() not in ['nan', 'none', ''] and len(name2) > 2
        
        # Check if it's not just a business
        if valid_name1:
            is_business = any(indicator in name1.upper() for indicator in business_indicators)
            if not is_business:
                return True
                
        if valid_name2:
            is_business = any(indicator in name2.upper() for indicator in business_indicators)
            if not is_business:
                return True
        
        return False
    
    async def search_address_on_bcpa(self, page, address):
        """Search for an address on BCPA website"""
        try:
            logger.info(f"🔍 Searching BCPA for: {address}")
            
            # Navigate to search page
            await page.goto(self.base_url, wait_until='domcontentloaded')
            await page.wait_for_timeout(3000)  # Wait for page to fully load
            
            # Find the search input box using multiple strategies
            search_input = None
            
            # Strategy 1: By placeholder text
            try:
                search_input = page.get_by_role('textbox', name='Name, Address, Folio')
                await search_input.wait_for(state='visible', timeout=5000)
            except:
                pass
            
            # Strategy 2: By CSS selector
            if not search_input:
                try:
                    search_input = page.locator('input[placeholder*="Name, Address, Folio"]')
                    await search_input.wait_for(state='visible', timeout=5000)
                except:
                    pass
            
            # Strategy 3: Look for any textbox in the search area
            if not search_input:
                try:
                    search_input = page.locator('textbox').first
                    await search_input.wait_for(state='visible', timeout=5000)
                except:
                    pass
            
            if not search_input:
                logger.error("Could not find search input box")
                return None
            
            # Clear and enter the address
            await search_input.clear()
            await page.wait_for_timeout(500)
            await search_input.fill(address)
            await page.wait_for_timeout(1000)
            
            # Submit the search (try Enter key first)
            await search_input.press('Enter')
            
            # Wait for results to load
            await page.wait_for_timeout(5000)
            
            # Look for search results
            owner_info = await self.extract_owner_from_results(page)
            
            if owner_info:
                logger.info(f"✅ Found owner: {owner_info}")
                return owner_info
            else:
                logger.info(f"❌ No owner found for: {address}")
                return None
                
        except Exception as e:
            logger.error(f"Error searching BCPA for {address}: {e}")
            return None
    
    async def extract_owner_from_results(self, page):
        """Extract owner information from search results or property details page"""
        try:
            # Wait for results to load
            await page.wait_for_timeout(3000)
            
            # First check if we're on the property details page (Parcel Result tab)
            parcel_result_tab = page.locator('tab[selected]:has-text("Parcel Result")')
            if await parcel_result_tab.count() > 0:
                logger.info("Found property details page - extracting from property summary")
                return await self.extract_owner_from_property_details(page)
            
            # Check if we have a "No record found" message on search page
            no_record_msg = page.locator('text="No record found, please check your criteria."')
            if await no_record_msg.count() > 0:
                logger.info("No records found for this address")
                return None
            
            # Check if we're now on the Search Results tab
            search_results_tab = page.locator('tab[selected]:has-text("Search Results")')
            if await search_results_tab.count() > 0:
                logger.info("Found search results table - extracting from results")
                return await self.extract_owner_from_search_results(page)
            
            # If no specific tab found, try both methods
            logger.info("No specific tab detected - trying property details first")
            owner_info = await self.extract_owner_from_property_details(page)
            if owner_info:
                return owner_info
                
            logger.info("Trying search results extraction")
            return await self.extract_owner_from_search_results(page)
            
        except Exception as e:
            logger.error(f"Error extracting owner from results: {e}")
            return None
    
    async def extract_owner_from_property_details(self, page):
        """Extract owner from property details page (when search goes directly to property)"""
        try:
            # Wait for property details to load
            await page.wait_for_timeout(2000)
            
            # Look for the Property Owner(s) cell in the property summary table
            # Based on our testing: "Property Owner(s): BARATZ, PHILIP J & LISA T"
            owner_cell = page.locator('cell:has-text("Property Owner(s):")').locator('xpath=following-sibling::cell[1]')
            
            if await owner_cell.count() > 0:
                owner_text = await owner_cell.text_content()
                if owner_text and owner_text.strip():
                    owner_names = self.parse_multiple_owners(owner_text.strip())
                    if owner_names:
                        logger.info(f"Found property owners: {owner_names}")
                        return owner_names
            
            # Alternative: Look for text patterns in the page
            page_text = await page.text_content('body')
            if page_text:
                # Look for "Property Owner(s):" pattern
                import re
                owner_match = re.search(r'Property Owner\(s\):\s*([A-Z][A-Za-z\s,&]{3,100})', page_text)
                if owner_match:
                    owner_names = self.parse_multiple_owners(owner_match.group(1))
                    if owner_names:
                        logger.info(f"Found property owners via text pattern: {owner_names}")
                        return owner_names
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting from property details: {e}")
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
            
        # Clean the initial text
        owner_text = owner_text.strip()
        
        # Handle common separators for multiple owners
        # Examples: "BARATZ, PHILIP J & LISA T", "SMITH, JOHN AND MARY", "DOE, JANE; SMITH, ROBERT"
        separators = [' & ', ' AND ', ' and ', '; ', ' / ']
        
        owners = [owner_text]  # Start with the full text
        
        # Split by each separator
        for separator in separators:
            new_owners = []
            for owner in owners:
                if separator in owner:
                    split_parts = owner.split(separator)
                    new_owners.extend([part.strip() for part in split_parts if part.strip()])
                else:
                    new_owners.append(owner)
            owners = new_owners
        
        # Clean each owner name
        cleaned_owners = []
        for owner in owners:
            cleaned = self.clean_extracted_name(owner)
            if cleaned:
                cleaned_owners.append(cleaned)
        
        # Handle special case: "LAST, FIRST & SECOND" format
        # Example: "BARATZ, PHILIP J & LISA T" should become ["PHILIP J BARATZ", "LISA T BARATZ"]
        if len(cleaned_owners) == 2 and ',' in owner_text and ' & ' in owner_text:
            first_owner = cleaned_owners[0]
            second_owner = cleaned_owners[1]
            
            # Extract the last name from the original format (before the comma)
            original_parts = owner_text.split(',')
            if len(original_parts) >= 2:
                original_last_name = original_parts[0].strip()
                
                # Check if second owner needs the last name
                second_parts = second_owner.strip().split()
                first_parts = first_owner.split()
                
                # If the second owner doesn't end with the same last name as first owner
                if len(first_parts) >= 2 and len(second_parts) >= 1:
                    first_last_name = first_parts[-1]
                    second_last_name = second_parts[-1] if len(second_parts) > 1 else None
                    
                if second_last_name != first_last_name:
                    # Add the last name to second owner and clean it
                    full_second_name = f"{second_parts[0]} {first_last_name}"
                    # Re-clean the full name to ensure proper formatting
                    second_owner = self.clean_extracted_name(full_second_name)
                    if second_owner:
                        cleaned_owners[1] = second_owner
                        logger.info(f"Inherited and cleaned last name: {second_owner}")
        
        if cleaned_owners:
            logger.info(f"Parsed {len(cleaned_owners)} owner(s): {cleaned_owners}")
            return cleaned_owners
        
        return None
    
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
        """Process CSV file to find missing owners using BCPA search"""
        try:
            logger.info("=" * 60)
            logger.info("STARTING BCPA OWNER SEARCH")
            logger.info("=" * 60)
            
            # Read CSV
            df = pd.read_csv(input_csv_path)
            logger.info(f"📄 Loaded {len(df)} records from CSV")
            
            # Set output path
            if output_csv_path is None:
                input_file = Path(input_csv_path)
                output_csv_path = str(input_file.parent / f"{input_file.stem}_with_bcpa_owners.csv")
            
            # Identify rows without valid owner names
            rows_to_search = []
            for index, row in df.iterrows():
                if not self.has_valid_owner_name(row):
                    address = self.clean_address_for_search(row)
                    if address:
                        rows_to_search.append((index, address, row))
                        logger.info(f"🏠 Row {index}: Will search for address: {address}")
            
            logger.info(f"🎯 Found {len(rows_to_search)} rows needing owner search")
            
            if not rows_to_search:
                logger.info("✅ All rows already have valid owner names!")
                df.to_csv(output_csv_path, index=False)
                return output_csv_path
            
            # Start Playwright browser
            async with async_playwright() as p:
                logger.info("🌐 Starting browser...")
                browser = await p.chromium.launch(headless=self.headless)
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = await context.new_page()
                
                # Process each address
                for index, address, row in rows_to_search:
                    try:
                        self.searches_performed += 1
                        logger.info(f"📍 Searching {self.searches_performed}/{len(rows_to_search)}: {address}")
                        
                        # Search for owner
                        owner_info = await self.search_address_on_bcpa(page, address)
                        
                        if owner_info:
                            if isinstance(owner_info, list) and len(owner_info) > 1:
                                # Multiple owners found - create additional rows
                                logger.info(f"✅ Found {len(owner_info)} owners for row {index}")
                                
                                # Update the original row with the first owner
                                df.at[index, 'Owner Name 1'] = owner_info[0]
                                df.at[index, 'BCPA_Owner_Found'] = 'Yes'
                                df.at[index, 'BCPA_Multiple_Owners'] = f"Yes ({len(owner_info)} owners)"
                                logger.info(f"✅ Updated row {index} with first owner: {owner_info[0]}")
                                
                                # Create additional rows for remaining owners
                                rows_to_add = []
                                for i, additional_owner in enumerate(owner_info[1:], 1):
                                    # Create a copy of the original row
                                    new_row = row.copy()
                                    new_row['Owner Name 1'] = additional_owner
                                    new_row['Owner Name 2'] = ''  # Clear second name to avoid conflicts
                                    new_row['BCPA_Owner_Found'] = 'Yes'
                                    new_row['BCPA_Multiple_Owners'] = f"Yes (owner {i+1} of {len(owner_info)})"
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
                                df.at[index, 'BCPA_Multiple_Owners'] = 'No'
                                self.results_found += 1
                                logger.info(f"✅ Updated row {index} with owner: {single_owner}")
                        else:
                            df.at[index, 'BCPA_Owner_Found'] = 'No'
                            df.at[index, 'BCPA_Multiple_Owners'] = 'No'
                            logger.info(f"❌ No owner found for row {index}")
                        
                        # Delay between searches to be respectful
                        if self.searches_performed < len(rows_to_search):
                            await asyncio.sleep(self.delay_between_searches)
                            
                    except Exception as e:
                        logger.error(f"Error processing row {index}: {e}")
                        df.at[index, 'BCPA_Owner_Found'] = 'Error'
                        continue
                
                await browser.close()
            
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

async def main():
    """Main function for CLI usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description="BCPA Owner Search Script")
    parser.add_argument('input_csv', help='Input CSV file path')
    parser.add_argument('--output', help='Output CSV file path')
    parser.add_argument('--headless', action='store_true', default=False, help='Run browser in headless mode')
    parser.add_argument('--delay', type=int, default=2, help='Delay between searches in seconds')
    
    args = parser.parse_args()
    
    searcher = BCPAOwnerSearch(headless=args.headless, delay_between_searches=args.delay)
    
    try:
        output_path = await searcher.process_csv(args.input_csv, args.output)
        logger.info(f"Process completed! Output saved to: {output_path}")
        
    except Exception as e:
        logger.error(f"Process failed: {e}")
        import sys
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
