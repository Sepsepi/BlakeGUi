#!/usr/bin/env python3
"""
Universal CSV Format Handler for Phone Number Search Pipeline
Handles multiple CSV formats and standardizes them for ZabaSearch and Radaris processing

Supported formats:
1. Raw Broward County property data (HOLLYWOOD format)
2. Pre-processed search format (Broward Single family format)
3. Any CSV with name and address columns

Author: Auto-generated for Blake GUI system
Date: August 6, 2025
"""

import pandas as pd
import re
import os
from pathlib import Path
import logging
from datetime import datetime

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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CSVFormatHandler:
    """Universal CSV format handler and standardizer"""

    def __init__(self):
        self.supported_formats = {
            'broward_raw': {
                'name_columns': ['Owner Name 1'],
                'address_components': ['House Number', 'Prefix Direction', 'Street Name', 'Street Type', 'Post Direction', 'Unit Type', 'Unit Number'],
                'city_column': 'City Name',
                'state_column': 'State Abbreviation',
                'zip_column': 'Zip Code',
                'phone_column': 'Telephone Number'
            },
            'search_ready': {
                'name_columns': ['DirectName_Cleaned', 'IndirectName_Cleaned'],
                'address_column': 'DirectName_Address',
                'type_column': 'DirectName_Type'
            },
            'generic': {
                # Will be auto-detected
                'name_columns': [],
                'address_columns': []
            }
        }

    def detect_csv_format(self, csv_path: str) -> str:
        """Detect the format of the input CSV file"""
        try:
            # Read first few rows to examine structure
            df = read_data_file(csv_path, nrows=5)
            columns = list(df.columns)

            logger.info(f"Detected columns: {columns}")

            # Check for Broward raw format
            if all(col in columns for col in ['Owner Name 1', 'House Number', 'Street Name', 'City Name']):
                logger.info("🏠 Detected: Broward County raw property data format")
                return 'broward_raw'

            # Check for search-ready format
            if 'DirectName_Cleaned' in columns and 'DirectName_Address' in columns:
                logger.info("🔍 Detected: Pre-processed search-ready format")
                return 'search_ready'

            # Check for generic name/address format
            name_indicators = ['name', 'owner', 'person', 'first', 'last']
            address_indicators = ['address', 'street', 'location', 'addr']

            # Safe column checking - convert to string first to handle integer column names
            name_cols = [col for col in columns if isinstance(col, str) and any(indicator in col.lower() for indicator in name_indicators)]
            addr_cols = [col for col in columns if isinstance(col, str) and any(indicator in col.lower() for indicator in address_indicators)]

            if name_cols and addr_cols:
                logger.info(f"📝 Detected: Generic format with names: {name_cols}, addresses: {addr_cols}")
                return 'generic'

            logger.warning("❓ Unknown CSV format - will attempt generic processing")
            return 'unknown'

        except Exception as e:
            logger.error(f"Error detecting CSV format: {e}")
            return 'unknown'

    def clean_name(self, name: str) -> str:
        """Clean and standardize a name string to FIRST LAST format"""
        if pd.isna(name) or not name or name.strip() == "":
            return ""

        # Remove common business suffixes and titles
        business_suffixes = ['LLC', 'INC', 'CORP', 'LTD', 'CO', 'COMPANY', 'TRUST', 'TR', 'REV TR', 'LIV TR', 'FAM TR']
        titles = ['MR', 'MRS', 'MS', 'DR', 'PROF', 'REV']

        name = str(name).strip().upper()
        original_name = name

        # Smart business detection - only skip if it's clearly a business entity
        # Look for patterns that indicate a business rather than a person with business indicators
        business_only_patterns = [
            r'^[A-Z\s]+\s+(LLC|INC|CORP|LTD|COMPANY)$',  # "ACME CORPORATION LLC"
            r'^[A-Z\s]+\s+(TRUST)$',  # "FAMILY TRUST" (but not "JANE DOE TRUST")
            r'^[A-Z\s]+\s+(CO|COMPANY)$',  # "SMITH CO"
        ]

        # Check if this is clearly a business entity (no person names)
        for pattern in business_only_patterns:
            if re.match(pattern, name):
                # Check if it contains typical person name patterns
                person_indicators = r'\b(JOHN|JANE|ROBERT|MARY|JAMES|PATRICIA|MICHAEL|LINDA|WILLIAM|ELIZABETH|DAVID|BARBARA|RICHARD|SUSAN|JOSEPH|JESSICA|THOMAS|SARAH|CHARLES|KAREN|CHRISTOPHER|NANCY|DANIEL|LISA|MATTHEW|BETTY|ANTHONY|HELEN|MARK|SANDRA|DONALD|DONNA|STEVEN|CAROL|PAUL|RUTH|ANDREW|SHARON|JOSHUA|MICHELLE|KENNETH|LAURA|KEVIN|SARAH|BRIAN|KIMBERLY|GEORGE|DEBORAH|TIMOTHY|DOROTHY|RONALD|LISA|JASON|NANCY|EDWARD|KAREN|JEFFREY|BETTY|RYAN|HELEN|JACOB|SANDRA|GARY|DONNA|NICHOLAS|CAROL|ERIC|RUTH|JONATHAN|SHARON|STEPHEN|MICHELLE|LARRY|LAURA|JUSTIN|SARAH|SCOTT|KIMBERLY|BRANDON|DEBORAH|BENJAMIN|DOROTHY|SAMUEL|LISA|GREGORY|NANCY|ALEXANDER|KAREN|FRANK|BETTY|RAYMOND|HELEN|JACK|SANDRA|DENNIS|DONNA|JERRY|CAROL|TYLER|RUTH|AARON|SHARON|JOSE|MICHELLE|HENRY|LAURA|ADAM|SARAH|DOUGLAS|KIMBERLY|NATHAN|DEBORAH|PETER|DOROTHY|ZACHARY|LISA|KYLE|NANCY|WALTER|KAREN|HAROLD|BETTY|CARL|HELEN)\b'
                if not re.search(person_indicators, name):
                    return ""  # Skip pure business entities

        # Remove business suffixes from the end (but keep the person name)
        for suffix in business_suffixes:
            # Remove suffix from end of name
            if name.endswith(f' {suffix}'):
                name = name[:-len(f' {suffix}')].strip()
            elif name.endswith(suffix) and len(suffix) < len(name):
                name = name[:-len(suffix)].strip()

        # Remove titles
        for title in titles:
            name = re.sub(rf'\b{title}\.?\s*', '', name)

        # Clean up punctuation
        name = re.sub(r'[&]+', ' & ', name)
        name = re.sub(r'\s+', ' ', name).strip()

        # Handle comma-separated format: "LAST, FIRST" -> "FIRST LAST"
        if ',' in name:
            parts = name.split(',', 1)
            if len(parts) == 2:
                last_part = parts[0].strip()
                first_part = parts[1].strip()

                # Handle multiple names separated by &
                if ' & ' in first_part:
                    first_part = first_part.split(' & ')[0].strip()

                first_words = first_part.split()
                if first_words:
                    first_name = first_words[0]
                    last_words = last_part.split()
                    if last_words:
                        last_name = last_words[0]
                        return f"{first_name} {last_name}"
                    else:
                        return first_name
        else:
            # Handle "FIRST LAST" format
            parts = name.split()
            filtered_parts = []
            business_words = ['LIV', 'TR', 'REV', 'TRUST', 'LLC', 'INC', 'CORP', 'FAM', 'ETAL', 'III', 'JR', 'SR']

            for part in parts:
                if part.upper() not in business_words:
                    filtered_parts.append(part)

            # Handle "&" - take only first person
            if ' & ' in ' '.join(filtered_parts):
                full_name = ' '.join(filtered_parts)
                filtered_parts = full_name.split(' & ')[0].split()

            if len(filtered_parts) >= 2:
                first_name = filtered_parts[0]
                last_name = filtered_parts[-1]
                return f"{first_name} {last_name}"
            elif len(filtered_parts) == 1:
                return filtered_parts[0]

        return ""

    def normalize_address_format(self, address: str) -> str:
        """
        Normalize address format to work with ZabaSearch.
        Convert 'STREET, CITY, STATE' to 'STREET, CITY STATE' (remove comma before state)
        """
        if not address:
            return address

        # Pattern to match: "STREET, CITY, FL" -> "STREET, CITY FL"
        # Look for comma followed by space and 2-letter state code at the end
        pattern = r'^(.+),\s+([A-Z\s]+),\s+([A-Z]{2})$'
        match = re.match(pattern, address.strip())

        if match:
            street_part = match.group(1)
            city_part = match.group(2)
            state_part = match.group(3)
            normalized = f"{street_part}, {city_part} {state_part}"
            return normalized

        return address

    def combine_address_components(self, row) -> str:
        """Combine address components into full address"""
        try:
            address_parts = []

            # House number
            house_num = str(row.get('House Number', '')).strip()
            if house_num and house_num.lower() not in ['nan', 'none', '']:
                address_parts.append(house_num)

            # Prefix direction
            prefix_dir = str(row.get('Prefix Direction', '')).strip()
            if prefix_dir and prefix_dir.lower() not in ['nan', 'none', '']:
                address_parts.append(prefix_dir)

            # Street name
            street_name = str(row.get('Street Name', '')).strip()
            if street_name and street_name.lower() not in ['nan', 'none', '']:
                address_parts.append(street_name)

            # Street type
            street_type = str(row.get('Street Type', '')).strip()
            if street_type and street_type.lower() not in ['nan', 'none', '']:
                address_parts.append(street_type)

            # Post direction
            post_dir = str(row.get('Post Direction', '')).strip()
            if post_dir and post_dir.lower() not in ['nan', 'none', '']:
                address_parts.append(post_dir)

            # Unit information
            unit_type = str(row.get('Unit Type', '')).strip()
            unit_number = str(row.get('Unit Number', '')).strip()

            if unit_type and unit_type.lower() not in ['nan', 'none', '']:
                if unit_number and unit_number.lower() not in ['nan', 'none', '']:
                    address_parts.append(f"{unit_type} {unit_number}")
                else:
                    address_parts.append(unit_type)

            # Create street address
            street_address = " ".join(address_parts)

            # Add city, state, zip
            full_parts = []
            if street_address:
                full_parts.append(street_address)

            # City
            city = str(row.get('City Name', '')).strip().replace('"', '')
            if city and city.lower() not in ['nan', 'none', '']:
                full_parts.append(city)

            # State
            state = str(row.get('State Abbreviation', '')).strip().replace('"', '').upper()
            if state and state.lower() not in ['nan', 'none', '']:
                if len(state) == 2 and state.isalpha():
                    full_parts.append(state)

            # ZIP
            zip_code = str(row.get('Zip Code', '')).strip().replace('"', '')
            if zip_code and zip_code.lower() not in ['nan', 'none', '']:
                if '-' in zip_code:
                    zip_code = zip_code.split('-')[0]
                if zip_code.isdigit() and len(zip_code) == 5:
                    full_parts.append(zip_code)

            return ", ".join(full_parts)

        except Exception as e:
            logger.error(f"Error combining address: {e}")
            return ""

    def _read_file_universal(self, csv_path: str) -> pd.DataFrame:
        """
        Universal file reader that handles any CSV/Excel format with or without headers
        """
        logger = logging.getLogger(__name__)

        try:
            # Try reading with standard method first
            if csv_path.endswith('.xlsx') or csv_path.endswith('.xls'):
                df = pd.read_excel(csv_path)
            else:
                # Try multiple encodings
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        df = pd.read_csv(csv_path, encoding=encoding)
                        logger.info(f"Successfully read file with {encoding} encoding")
                        break
                    except:
                        continue
                else:
                    raise ValueError("Could not read file with any standard encoding")

            # Check if file has headers or is just raw data
            first_row = df.iloc[0].astype(str).tolist() if len(df) > 0 else []

            # If first row looks like data (contains names/addresses), treat as headerless
            if any(re.search(r'[A-Za-z]{3,}', str(val)) for val in first_row[:3]):
                # Check if current column names are generic (Unnamed: 0, etc.)
                if any(isinstance(col, str) and col.startswith('Unnamed:') for col in df.columns):
                    logger.info("📄 File appears to have no headers - reading as raw data")
                    # Re-read without headers
                    if csv_path.endswith('.xlsx') or csv_path.endswith('.xls'):
                        df = pd.read_excel(csv_path, header=None)
                    else:
                        df = pd.read_csv(csv_path, header=None, encoding=encoding)

                    # Create generic column names
                    df.columns = [f'Column_{i}' for i in range(len(df.columns))]
                    logger.info(f"✅ Created generic column names for {len(df.columns)} columns")

            logger.info(f"📊 Loaded {len(df)} records from {csv_path}")
            return df

        except Exception as e:
            logger.error(f"❌ Failed to read file {csv_path}: {e}")
            raise

    def standardize_to_search_format(self, csv_path: str, output_path: str | None = None) -> str | None:
        """
        TRUE UNIVERSAL CSV standardizer - scans every cell for names, addresses, and phones
        Completely ignores column structure and headers

        Args:
            csv_path: Path to input CSV/Excel file
            output_path: Optional output path

        Returns:
            str: Path to standardized file or None if failed
        """
        try:
            if output_path is None:
                base_name = Path(csv_path).stem
                output_path = f"{csv_path.replace('.csv', '')}_standardized.csv"

            logger = logging.getLogger(__name__)
            logger.info(f"🌍 TRUE UNIVERSAL STANDARDIZER: Processing {csv_path}")

            # Read any file format
            df = read_data_file(csv_path)
            if df is None or len(df) == 0:
                logger.error("❌ No data found in file")
                return None

            logger.info(f"📊 Loaded {len(df)} records from file")

            # TRUE ROW-BY-ROW CONTENT SCANNER - IGNORES COLUMN STRUCTURE COMPLETELY
            logger.info("🔍 Using true universal row-by-row content scanner (structure-independent)")

            # Phone number patterns
            phone_patterns = [
                r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # 561-776-8577 or 5617768577
                r'\b\(\d{3}\)\s?\d{3}[-.]?\d{4}\b',  # (561) 776-8577
                r'\b\d{10}\b'  # 5617768577
            ]

            # Address patterns - comprehensive coverage for all single-column formats
            address_patterns = [
                # Full addresses with city and state (various formats)
                r'\d+\s+[A-Z\s\w]+(?:ST|AVE|RD|BLVD|DR|CT|LN|WAY|PL|CIR|PKWY|TER|STREET|AVENUE|ROAD|BOULEVARD|DRIVE|COURT|LANE|PLACE)\s*,\s*[A-Z\s]+,?\s*[A-Z]{2}\s*\d*',
                # Full addresses without commas: "123 MAIN ST MIAMI FL"
                r'\d+\s+[A-Z\s\w]+(?:ST|AVE|RD|BLVD|DR|CT|LN|WAY|PL|CIR|PKWY|TER|STREET|AVENUE|ROAD|BOULEVARD|DRIVE|COURT|LANE|PLACE)\s+[A-Z\s]+\s+[A-Z]{2}',
                # Partial addresses with directionals: "123 NE 45TH ST"
                r'\d+\s+(?:N|S|E|W|NE|NW|SE|SW)\s+[A-Z\d\w\s]+(?:ST|AVE|RD|BLVD|DR|CT|LN|WAY|PL|CIR|PKWY|TER|STREET|AVENUE|ROAD|BOULEVARD|DRIVE|COURT|LANE|PLACE)\b',
                # Basic addresses: "123 MAIN STREET"
                r'\d+\s+[A-Z\w\s]+(?:ST|AVE|RD|BLVD|DR|CT|LN|WAY|PL|CIR|PKWY|TER|STREET|AVENUE|ROAD|BOULEVARD|DRIVE|COURT|LANE|PLACE)\b',
                # Numbered streets: "123 42ND ST" or "456 1ST AVE"
                r'\d+\s+\d+(ST|ND|RD|TH)\s+(?:ST|AVE|RD|BLVD|DR|CT|LN|WAY|PL|CIR|PKWY|TER|STREET|AVENUE|ROAD|BOULEVARD|DRIVE|COURT|LANE|PLACE)\b',
                # Highways and special road types: "999 N FEDERAL HWY" or "123 US1"
                r'\d+\s+(?:N|S|E|W|NE|NW|SE|SW)?\s*[A-Z\w\s]*(?:HWY|HIGHWAY|FWY|FREEWAY|PIKE|TURNPIKE|EXPWY|EXPRESSWAY|US\d+|STATE\d+|SR\d+|A1A)\b',
                # Any pattern that starts with house number and has street indicators (fallback)
                r'\d+\s+[A-Z\w\s]*(?:ST|AVE|RD|BLVD|DR|CT|LN|WAY|PL)\b',
            ]

            processed_records = []

            for index, row in df.iterrows():
                logger.info(f"🔍 Scanning row {index+1} for content...")

                # Collect ALL content from this row first
                all_cells = []
                phones_found = []
                potential_addresses = []
                potential_names = []

                # First pass: Look for complete addresses first, then extract components
                house_number = None
                street_parts = []
                city = None
                state = None
                complete_address_found = False

                # Check for complete addresses first
                for col_name in df.columns:
                    cell_value = str(row[col_name]).strip()
                    if pd.isna(row[col_name]) or cell_value in ['', 'nan', 'NaN']:
                        continue

                    all_cells.append(cell_value)

                    # Check if this cell contains a complete address
                    for addr_pattern in address_patterns:
                        matches = re.findall(addr_pattern, cell_value, re.IGNORECASE)
                        if matches:
                            # Found a complete address - normalize and use it
                            normalized_match = self.normalize_address_format(matches[0])
                            potential_addresses.append(normalized_match)
                            logger.info(f"    🏠 FOUND COMPLETE ADDRESS: {normalized_match}")
                            complete_address_found = True
                            break

                    if complete_address_found:
                        break

                # Now extract phones and names from all cells
                for col_name in df.columns:
                    cell_value = str(row[col_name]).strip()
                    if pd.isna(row[col_name]) or cell_value in ['', 'nan', 'NaN']:
                        continue

                    if cell_value not in all_cells:
                        all_cells.append(cell_value)

                    logger.info(f"  📱 Checking cell '{col_name}': '{cell_value}'")

                    # Check for phone numbers first
                    found_phone = False
                    for phone_pattern in phone_patterns:
                        if re.search(phone_pattern, cell_value):
                            phone_match = re.search(phone_pattern, cell_value)
                            if phone_match:
                                clean_phone = re.sub(r'[^\d]', '', phone_match.group())
                                if len(clean_phone) == 10:
                                    formatted_phone = f"{clean_phone[:3]}-{clean_phone[3:6]}-{clean_phone[6:]}"
                                    phones_found.append(formatted_phone)
                                    logger.info(f"    📞 FOUND PHONE: {formatted_phone}")
                                    found_phone = True
                                    break

                    if found_phone:
                        continue

                    # Skip the cell that contained the complete address
                    if complete_address_found and any(addr in cell_value for addr in potential_addresses):
                        continue

                    # Only parse components if no complete address was found
                    if not complete_address_found:
                        # Check if it's a house number (pure digits or float that converts to integer)
                        if re.match(r'^\d{1,6}\.?0*$', cell_value) or (cell_value.replace('.0', '').isdigit() and len(cell_value.replace('.0', '')) <= 6):
                            house_number = cell_value.replace('.0', '') if '.0' in cell_value else cell_value
                            logger.info(f"    🏠 FOUND HOUSE NUMBER: {house_number}")
                            continue

                        # Check if it's a state (2 letter code)
                        if re.match(r'^[A-Z]{2}$', cell_value) and cell_value in ['FL', 'CA', 'NY', 'TX', 'GA']:
                            state = cell_value
                            logger.info(f"    🏛️ FOUND STATE: {cell_value}")
                            continue

                        # Check if it's a direction (NW, SW, etc.)
                        if re.match(r'^(N|S|E|W|NE|NW|SE|SW)$', cell_value):
                            street_parts.append(cell_value)
                            logger.info(f"    🧭 FOUND DIRECTION: {cell_value}")
                            continue

                        # Check if it's a street type
                        if cell_value.upper() in ['ST', 'AVE', 'RD', 'BLVD', 'DR', 'CT', 'LN', 'WAY', 'PL', 'CIR', 'PKWY', 'TER', 'STREET', 'AVENUE', 'ROAD', 'BOULEVARD', 'DRIVE', 'COURT', 'LANE', 'PLACE']:
                            street_parts.append(cell_value)
                            logger.info(f"    🛣️ FOUND STREET TYPE: {cell_value}")
                            continue

                        # Check if it's a city (all caps, multiple words possible)
                        if re.match(r'^[A-Z\s]+$', cell_value) and len(cell_value) > 2:
                            # Common city patterns
                            if any(city_word in cell_value for city_word in ['BEACH', 'CITY', 'PARK', 'LAND', 'VILLE', 'TOWN', 'DALE', 'FIELD', 'WOOD']):
                                city = cell_value
                                logger.info(f"    🏙️ FOUND CITY: {cell_value}")
                                continue

                        # Check if it looks like a street name (numbers + letters, like "4TH", "66TH")
                        if re.match(r'^\d+(ST|ND|RD|TH)$', cell_value) or re.match(r'^[A-Z0-9\s]{2,}$', cell_value):
                            street_parts.append(cell_value)
                            logger.info(f"    🛤️ FOUND STREET NAME: {cell_value}")
                            continue

                    # Check for names (regardless of whether we found complete address or not)
                    if len(cell_value) > 2 and re.search(r'[A-Za-z]', cell_value):
                        # Skip if this cell contained the complete address
                        if complete_address_found and any(addr in cell_value for addr in potential_addresses):
                            continue

                        # Clean up the name
                        cleaned_name = self.clean_name(cell_value)
                        if cleaned_name and len(cleaned_name) > 2:
                            potential_names.append(cleaned_name)
                            logger.info(f"    👤 FOUND NAME: {cleaned_name}")

                # Second pass: construct full address from components (only if no complete address found)
                full_address = ""
                if not complete_address_found and house_number and street_parts:
                    address_parts = [house_number] + street_parts
                    if city and state:
                        # Use format that works with ZabaSearch: "STREET, CITY STATE" (no comma before state)
                        full_address = f"{' '.join(address_parts)}, {city} {state}"
                    elif city:
                        full_address = f"{' '.join(address_parts)}, {city}"
                    else:
                        full_address = ' '.join(address_parts)

                    logger.info(f"    🏗️ CONSTRUCTED ADDRESS: {full_address}")

                # Skip the regex search if we already found complete addresses
                if not complete_address_found:
                    # Third pass: check for any pre-formed addresses using regex
                    full_row_text = ' '.join(all_cells)
                    for addr_pattern in address_patterns:
                        matches = re.findall(addr_pattern, full_row_text, re.IGNORECASE)
                        for match in matches:
                            # Normalize address format: remove comma before state
                            normalized_match = self.normalize_address_format(match)
                            potential_addresses.append(normalized_match)
                            logger.info(f"    🏠 FOUND REGEX ADDRESS: {normalized_match}")

                # Prioritize: complete addresses first, then constructed, then regex matches
                final_address = ""
                if potential_addresses:
                    final_address = potential_addresses[0]  # Complete addresses were added first
                elif full_address:
                    final_address = full_address

                # Create records from discovered content
                logger.info(f"  📊 Row {index+1} summary: {len(potential_names)} names, 1 address: '{final_address}', {len(phones_found)} phones")

                if potential_names and final_address:
                    # Create record for each name with the address
                    for i, name in enumerate(potential_names):
                        primary_phone = phones_found[0] if len(phones_found) > 0 else ''
                        secondary_phone = phones_found[1] if len(phones_found) > 1 else ''

                        record = {
                            'DirectName_Cleaned': name,
                            'DirectName_Address': final_address,
                            'DirectName_Type': 'Person',
                            'DirectName_Phone_Primary': primary_phone,
                            'DirectName_Phone_Secondary': secondary_phone,
                            'original_index': index,
                            'source': f'Universal_Content_Scanner_Name_{i}'
                        }
                        processed_records.append(record)
                        logger.info(f"    ✅ Created record: {name} - {final_address} - {primary_phone}")

                elif potential_names:
                    # Names without addresses - still create records for phone search
                    for i, name in enumerate(potential_names):
                        primary_phone = phones_found[0] if len(phones_found) > 0 else ''
                        secondary_phone = phones_found[1] if len(phones_found) > 1 else ''

                        record = {
                            'DirectName_Cleaned': name,
                            'DirectName_Address': '',
                            'DirectName_Type': 'Person',
                            'DirectName_Phone_Primary': primary_phone,
                            'DirectName_Phone_Secondary': secondary_phone,
                            'original_index': index,
                            'source': f'Universal_Content_Scanner_Name_Only_{i}'
                        }
                        processed_records.append(record)
                        logger.info(f"    ✅ Created name-only record: {name} - {primary_phone}")

                elif final_address and phones_found:
                    # Address with phone but no name - create for potential reverse lookup
                    record = {
                        'DirectName_Cleaned': 'PHONE_ADDRESS_ONLY',
                        'DirectName_Address': final_address,
                        'DirectName_Type': 'Address',
                        'DirectName_Phone_Primary': phones_found[0],
                        'DirectName_Phone_Secondary': phones_found[1] if len(phones_found) > 1 else '',
                        'original_index': index,
                        'source': 'Universal_Content_Scanner_Address_Phone'
                    }
                    processed_records.append(record)
                    logger.info(f"    ✅ Created address+phone record: {final_address} - {phones_found[0]}")

            # Save standardized CSV
            if processed_records:
                result_df = pd.DataFrame(processed_records)
                result_df.to_csv(output_path, index=False)
                logger.info(f"✅ TRUE UNIVERSAL PROCESSING: Standardized {len(processed_records)} records")
                logger.info(f"💾 Saved to: {output_path}")
                return str(output_path)
            else:
                logger.warning("⚠️ No valid records found")
                return None

        except Exception as e:
            logger.error(f"❌ True universal standardization failed: {e}")
            return None

    def _looks_like_name(self, text: str) -> bool:
        """Check if text looks like a person or company name"""
        if not text or len(text.strip()) < 2:
            return False

        text = text.strip().upper()

        # Skip obviously non-name content
        if text in ['', 'NAN', 'NULL', 'NONE', '0', '0.0']:
            return False

        # Skip pure numbers or addresses
        if text.replace('.', '').replace(',', '').isdigit():
            return False

        # Look for name indicators
        name_indicators = [
            # Person name patterns
            r'[A-Z][a-z]+\s+[A-Z][a-z]+',  # First Last
            r'[A-Z]+,\s*[A-Z]+',  # LAST, FIRST
            # Company indicators
            'LLC', 'INC', 'CORP', 'CO', 'COMPANY', 'TRUST', 'TR', 'ETAL', 'ESTATE',
            'FAMILY', 'FAM', '&', 'AND', 'REVOCABLE', 'LIVING'
        ]

        return any(re.search(pattern, text) for pattern in name_indicators)

    def _looks_like_address(self, text: str) -> bool:
        """Check if text looks like an address"""
        if not text or len(text.strip()) < 5:
            return False

        text = text.strip().upper()

        # Look for address indicators
        address_indicators = [
            r'\d+.*\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|CT|COURT|PL|PLACE|WAY|BLVD|BOULEVARD|LN|LANE)\b',
            r'\d+.*\b(N|S|E|W|NE|NW|SE|SW|NORTH|SOUTH|EAST|WEST|NORTHEAST|NORTHWEST|SOUTHEAST|SOUTHWEST)\b',
            r'\b(BEACH|CITY|TOWN|PARK|VILLAGE|HILLS|HEIGHTS|GARDENS|LAKES)\b',
            r'\b(FL|CA|NY|TX|FLORIDA|CALIFORNIA)\b',
            r'\d{5}(-\d{4})?'  # ZIP codes
        ]

        return any(re.search(pattern, text) for pattern in address_indicators)

    def _intelligent_address_merger(self, row_data: dict) -> str:
        """
        Intelligently merge address components from ANY row format by reading the entire row
        and figuring out what's what - names, address parts, city, state, zip, etc.
        """
        # Collect ALL non-empty values from the row
        all_values = []
        for col_name, value in row_data.items():
            if pd.isna(value) or str(value).strip() == '':
                continue

            value_str = str(value).strip()
            # Handle float numbers (like 3111.0 -> 3111)
            if isinstance(value, (int, float)) and not pd.isna(value):
                value_str = str(int(value)) if value == int(value) else str(value)

            all_values.append(value_str.upper())

        if not all_values:
            return ''

        # Check if this looks like a header row
        header_indicators = [
            'UNNAMED:',
            'COLUMN',
            'FIELD',
            'NAME',
            'ADDRESS',
            'CITY',
            'STATE',
            'ZIP',
            'PHONE'
        ]

        # If more than half the values look like headers, skip this row
        header_count = sum(1 for value in all_values if any(indicator in value for indicator in header_indicators))
        if header_count > len(all_values) * 0.5:
            print(f"🚫 Skipping header row: {all_values[:3]}...")
            return ''

        print(f"🔍 Analyzing row with {len(all_values)} values: {all_values[:5]}{'...' if len(all_values) > 5 else ''}")

        # Step 1: Look for complete addresses first, but also check for enhancement opportunities
        complete_address_found = False
        for value in all_values:
            if self._looks_like_complete_address(value):
                confidence = self._address_confidence_score(value)
                if confidence > 0.7:
                    # Check if we can enhance this address with additional city/state info
                    enhanced = self._try_enhance_address_with_row_data(value, all_values)
                    print(f"✅ Found complete address: '{value}' → Enhanced: '{enhanced}' (confidence: {confidence})")
                    return self._clean_address_for_zabasearch(enhanced)
                elif confidence > 0.5:
                    # Partial complete address - try to enhance it
                    enhanced = self._try_enhance_address_with_row_data(value, all_values)
                    if enhanced != value:
                        print(f"✅ Enhanced partial address: '{value}' → '{enhanced}' (confidence: {confidence})")
                        return self._clean_address_for_zabasearch(enhanced)


        # Step 2: Intelligent row analysis - extract what we can find
        extracted_info = {
            'names': [],
            'house_numbers': [],
            'directions': [],
            'street_names': [],
            'street_types': [],
            'cities': [],
            'states': [],
            'zip_codes': [],
            'other': []
        }

        # Analyze each value and categorize intelligently
        for value in all_values:
            self._categorize_value(value, extracted_info)

        print(f"📊 Extracted info: {dict((k, v) for k, v in extracted_info.items() if v)}")

        # Step 3: Build address from extracted components
        return self._construct_smart_address(extracted_info)

    def _looks_like_complete_address(self, text: str) -> bool:
        """Check if text looks like a complete address string"""
        if not text or len(text.strip()) < 10:
            return False

        text_upper = text.upper()

        # Must have a house number
        if not re.search(r'\b\d+\b', text):
            return False

        # Must have a street type or direction
        street_indicators = [
            'ST', 'STREET', 'AVE', 'AVENUE', 'RD', 'ROAD', 'DR', 'DRIVE',
            'CT', 'COURT', 'PL', 'PLACE', 'WAY', 'BLVD', 'BOULEVARD', 'LN', 'LANE',
            'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW'
        ]

        return any(f' {indicator} ' in f' {text_upper} ' or f' {indicator}' in f' {text_upper}'
                  for indicator in street_indicators)

    def _enhance_complete_address(self, address: str) -> str:
        """
        Enhance a complete address to ensure proper ZabaSearch format.
        Converts formats like '505 NE 6TH AVE DEERFIELD BEACH FL 33076'
        to '505 NE 6TH AVE, DEERFIELD BEACH FL'
        """
        if not address:
            return ''

        address = address.strip().upper()

        # If already has comma, it's likely in good format
        if ',' in address:
            return address

        # Parse complete address without comma
        words = address.split()
        if len(words) < 4:
            return address

        # Find city start by looking for common patterns
        city_start_idx = -1

        # Method 1: Look for known cities
        known_cities = ['DEERFIELD BEACH', 'FORT LAUDERDALE', 'POMPANO BEACH', 'BOCA RATON',
                       'DELRAY BEACH', 'PARKLAND', 'CORAL SPRINGS', 'MARGATE', 'COCONUT CREEK',
                       'TAMARAC', 'SUNRISE', 'PLANTATION', 'DAVIE', 'COOPER CITY', 'HOLLYWOOD',
                       'HALLANDALE', 'AVENTURA', 'MIAMI', 'HIALEAH', 'HOMESTEAD']

        for city in known_cities:
            city_words = city.split()
            if len(city_words) == 1:
                # Single word city
                try:
                    idx = words.index(city_words[0])
                    city_start_idx = idx
                    break
                except ValueError:
                    continue
            else:
                # Multi-word city - check for sequence
                for i in range(len(words) - len(city_words) + 1):
                    if words[i:i+len(city_words)] == city_words:
                        city_start_idx = i
                        break
                if city_start_idx >= 0:
                    break

        # Method 2: If no known city found, look for state and work backwards
        if city_start_idx == -1:
            state_words = ['FL', 'FLORIDA', 'CA', 'CALIFORNIA', 'NY', 'NEW YORK', 'TX', 'TEXAS']
            for i, word in enumerate(words):
                if word in state_words:
                    # State found, city is probably the word(s) before it
                    if i > 0:
                        city_start_idx = i - 1
                        # Check if previous word is part of a 2-word city
                        if i > 1 and len(words[i-1]) < 8:  # Short word, might be part of "FORT LAUDERDALE"
                            city_start_idx = i - 2
                    break

        # Method 3: If still not found, assume last few non-numeric words are city
        if city_start_idx == -1:
            for i in range(len(words) - 1, -1, -1):
                if not words[i].isdigit() and len(words[i]) > 2:  # Not a zip code
                    city_start_idx = i
                    break

        # Split into street and city parts
        if city_start_idx > 0:
            street_part = ' '.join(words[:city_start_idx])
            city_part = ' '.join(words[city_start_idx:])
            return f"{street_part}, {city_part}"

        return address

    def _looks_like_person_name(self, text: str) -> bool:
        """Enhanced person name detection to distinguish from cities"""

        if not text or len(text.strip()) < 2:
            return False

        text = text.strip().upper()

        # Skip obvious non-names
        if any(indicator in text for indicator in ['ST', 'AVE', 'RD', 'DR', 'CT', 'PL', 'WAY', 'BLVD']):
            return False

        # Skip if it's a known city pattern
        city_indicators = ['BEACH', 'SPRINGS', 'LAKES', 'HEIGHTS', 'GARDENS', 'FORT', 'BOCA', 'CORAL']
        if any(indicator in text for indicator in city_indicators):
            return False

        # TRUST/BUSINESS PATTERNS - These should be treated as names/businesses, not cities
        trust_patterns = [
            r'\bTR\b',      # "REV TR", "FAMILY TR"
            r'\bTRUST\b',   # "FAMILY TRUST"
            r'\bREV\b',     # "REV TR", "REV TRUST"
            r'\bLLC\b',     # "SMITH LLC"
            r'\bINC\b',     # "COMPANY INC"
            r'\bCORP\b',    # "BUSINESS CORP"
            r'\bLTD\b',     # "COMPANY LTD"
            r'\bCO\b$',     # "SMITH & CO"
            r'\bREVOCABLE\b', # "REVOCABLE TRUST"
            r'\bFAMILY\b',  # "FAMILY TRUST"
            r'\bLIVING\b'   # "LIVING TRUST"
        ]

        # If it contains trust/business indicators, treat as name/business entity
        for pattern in trust_patterns:
            if re.search(pattern, text):
                return True  # Treat as name entity, not city

        # Skip pure numbers or directions
        if re.match(r'^\d+$', text) or text in ['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW']:
            return False

        # Common first names (to help distinguish from cities)
        common_first_names = [
            'JOHN', 'MICHAEL', 'WILLIAM', 'JAMES', 'ROBERT', 'DAVID', 'RICHARD', 'CHARLES',
            'JOSEPH', 'THOMAS', 'CHRISTOPHER', 'DANIEL', 'PAUL', 'MARK', 'DONALD', 'STEVEN',
            'MATTHEW', 'ANTHONY', 'JOSHUA', 'KENNETH', 'ANDREW', 'BRIAN', 'EDWARD', 'RONALD',
            'MARY', 'PATRICIA', 'JENNIFER', 'LINDA', 'ELIZABETH', 'BARBARA', 'SUSAN', 'JESSICA',
            'SARAH', 'KAREN', 'NANCY', 'LISA', 'BETTY', 'HELEN', 'SANDRA', 'DONNA', 'CAROL',
            'RUTH', 'SHARON', 'MICHELLE', 'LAURA', 'SARAH', 'KIMBERLY', 'DEBORAH', 'DOROTHY',
            'AMY', 'ANGELA', 'ASHLEY', 'BRENDA', 'EMMA', 'OLIVIA', 'CYNTHIA', 'MARIE'
        ]

        # Common last names
        common_last_names = [
            'SMITH', 'JOHNSON', 'WILLIAMS', 'BROWN', 'JONES', 'GARCIA', 'MILLER', 'DAVIS',
            'RODRIGUEZ', 'MARTINEZ', 'HERNANDEZ', 'LOPEZ', 'GONZALEZ', 'WILSON', 'ANDERSON',
            'THOMAS', 'TAYLOR', 'MOORE', 'JACKSON', 'MARTIN', 'LEE', 'PEREZ', 'THOMPSON',
            'WHITE', 'HARRIS', 'SANCHEZ', 'CLARK', 'RAMIREZ', 'LEWIS', 'ROBINSON', 'WALKER',
            'YOUNG', 'ALLEN', 'KING', 'WRIGHT', 'SCOTT', 'TORRES', 'NGUYEN', 'HILL', 'FLORES',
            'GREEN', 'ADAMS', 'NELSON', 'BAKER', 'HALL', 'RIVERA', 'CAMPBELL', 'MITCHELL',
            'CARTER', 'ROBERTS'
        ]

        # If it's a known common name
        if text in common_first_names or text in common_last_names:
            return True

        # For multi-word values, check if combination looks like full name
        if ' ' in text:
            parts = text.split()
            if len(parts) == 2:  # First Last format
                first, last = parts
                if first in common_first_names or last in common_last_names:
                    return True

                # Check for name-like patterns (both parts are capitalized words)
                if first.isalpha() and last.isalpha() and len(first) > 2 and len(last) > 2:
                    # Additional check: avoid city-like combinations
                    city_words = ['FORT', 'NORTH', 'SOUTH', 'EAST', 'WEST', 'SAINT', 'SAN', 'LOS']
                    if first not in city_words and last not in city_words:
                        return True

            elif len(parts) == 3:  # First Middle Last or First Last Jr/Sr
                first, middle, last = parts
                if (first in common_first_names or
                    last in common_last_names or
                    last in ['JR', 'SR', 'III', 'II']):
                    return True

        # Single word names - be more conservative
        else:
            # Must be a common name to be considered a single word name
            if text in common_first_names or text in common_last_names:
                return True

            # Or follow typical name patterns (but not city patterns)
            if (text.isalpha() and
                len(text) >= 3 and
                not text.endswith('TOWN') and
                not text.endswith('VILLE') and
                not text.endswith('CITY') and
                text not in ['HOLLYWOOD', 'MIAMI', 'ORLANDO', 'PARKLAND']):
                # Be conservative - only if it really looks like a name
                return len(text) <= 10 and not any(city_word in text for city_word in ['PARK', 'LAKE', 'HILL'])

        return False

    def _try_enhance_with_additional_info(self, base_address: str, row_data: dict) -> str:
        """
        Try to enhance a partial address with additional city/state info from other columns.
        For example: '789 NW 5TH AVE' + 'FORT LAUDERDALE FL 33301' -> '789 NW 5TH AVE, FORT LAUDERDALE FL'
        """
        # Look for city/state information in other columns
        city_state_info = ""

        for col_name, value in row_data.items():
            if pd.isna(value) or str(value).strip() == '':
                continue

            value_str = str(value).strip().upper()

            # Skip if this is the base address
            if value_str == base_address.upper():
                continue

            # Look for city/state patterns
            if any(indicator in value_str for indicator in ['FL', 'FLORIDA']) and not any(char.isdigit() for char in value_str.replace('FL', '').replace('FLORIDA', '')):
                # This looks like city + state (not zip code)
                if 'FL' in value_str:
                    parts = value_str.split()
                    if 'FL' in parts:
                        fl_index = parts.index('FL')
                        city_part = ' '.join(parts[:fl_index])
                        if len(city_part) >= 4:  # Reasonable city name length
                            city_state_info = f"{city_part} FL"
                            break

            # Look for known city names
            elif any(city in value_str for city in ['FORT LAUDERDALE', 'DEERFIELD BEACH', 'POMPANO BEACH', 'BOCA RATON', 'DELRAY BEACH', 'PARKLAND', 'CORAL SPRINGS', 'MARGATE', 'COCONUT CREEK', 'TAMARAC', 'SUNRISE', 'PLANTATION', 'DAVIE', 'COOPER CITY', 'HOLLYWOOD', 'HALLANDALE', 'AVENTURA', 'MIAMI', 'HIALEAH', 'HOMESTEAD']):
                # Extract the city and add FL
                for city in ['FORT LAUDERDALE', 'DEERFIELD BEACH', 'POMPANO BEACH', 'BOCA RATON', 'DELRAY BEACH', 'PARKLAND', 'CORAL SPRINGS', 'MARGATE', 'COCONUT CREEK', 'TAMARAC', 'SUNRISE', 'PLANTATION', 'DAVIE', 'COOPER CITY', 'HOLLYWOOD', 'HALLANDALE', 'AVENTURA', 'MIAMI', 'HIALEAH', 'HOMESTEAD']:
                    if city in value_str:
                        city_state_info = f"{city} FL"
                        break
                if city_state_info:
                    break

        # Combine base address with city/state if found
        if city_state_info:
            return f"{base_address}, {city_state_info}"
        else:
            # Try to enhance using the standard method
            return self._enhance_complete_address(base_address)

    def _construct_address_from_components(self, components: dict) -> str:
        """Construct a clean address from detected components"""
        parts = []

        # House number (remove .0 from floats)
        if components['house_number']:
            house_num = str(components['house_number']).replace('.0', '')
            parts.append(house_num)

        # Direction (pre) - combine N W to NW
        if components['direction']:
            direction = components['direction'].replace(' ', '')  # N W -> NW
            parts.append(direction)

        # Street name
        if components['street_name']:
            street = components['street_name']
            # Handle ordinal numbers (3RD, 4TH, etc.)
            if street.isdigit() or (len(street) <= 3 and street.replace('RD', '').replace('TH', '').replace('ST', '').replace('ND', '').isdigit()):
                # This is likely an ordinal street number
                if not street.endswith(('ST', 'ND', 'RD', 'TH')):
                    # Add ordinal suffix if missing
                    num = int(street) if street.isdigit() else int(street[0])
                    if num == 1:
                        street = f"{num}ST"
                    elif num == 2:
                        street = f"{num}ND"
                    elif num == 3:
                        street = f"{num}RD"
                    else:
                        street = f"{num}TH"
            parts.append(street)

        # Street type
        if components['street_type']:
            parts.append(components['street_type'])

        # Unit info
        if components['unit_type'] and components['unit_number']:
            parts.append(f"{components['unit_type']} {components['unit_number']}")

        # Build street address
        street_address = ' '.join(parts) if len(parts) >= 3 else ''

        # Add city and state if available for ZabaSearch compatibility
        full_address_parts = []
        if street_address:
            full_address_parts.append(street_address)

        # Add city for ZabaSearch parsing
        if components['city']:
            full_address_parts.append(components['city'])

        # Add state if available
        if components['state']:
            full_address_parts.append(components['state'])

        # Join with commas for proper ZabaSearch format
        if len(full_address_parts) > 1:
            return f"{full_address_parts[0]}, {' '.join(full_address_parts[1:])}"
        else:
            return street_address

    def _clean_address_for_zabasearch(self, address: str) -> str:
        """Clean address specifically for ZabaSearch matching"""
        if not address:
            return ''

        # Remove common prefixes that interfere with matching
        cleaned = address.strip()

        # Remove name prefixes (often found in property records)
        name_prefixes = [
            r'^[A-Z\s,&]+\s+ETAL\s+',  # "FUENTES, JOEBERT J&E FUENTES TR ETAL "
            r'^[A-Z\s,&]+\s+TR\s+',    # "VAZQUEZ FAM TR "
            r'^[A-Z\s,&]+\s+TRUST\s+', # "GASKINS TRUST "
            r'^[A-Z\s,&]+,\s+[A-Z\s]+\s+', # "LASTNAME, FIRSTNAME "
        ]

        for pattern in name_prefixes:
            cleaned = re.sub(pattern, '', cleaned)

        # For already well-formatted addresses with commas, preserve them
        if ',' in cleaned:
            # Basic cleanup but preserve the full address with city for ZabaSearch
            cleaned = re.sub(r'\.0\b', '', cleaned)  # Remove .0 from numbers
            cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces to single
            return cleaned.strip()

        # For complete addresses without commas (like "WILLIAMS ROBERT 999 NW 1ST ST PARKLAND FL 33067 954-123-4567")
        # We need to intelligently extract the address and city parts

        # Remove phone numbers first
        phone_pattern = r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b'
        cleaned = re.sub(phone_pattern, '', cleaned)

        # Remove obvious names at the beginning (First Last pattern)
        name_pattern = r'^[A-Z]+\s+[A-Z]+\s+'
        cleaned = re.sub(name_pattern, '', cleaned)

        # Clean up extra spaces
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()

        # Now try to extract street address + city + state from what's left
        # Pattern: "999 NW 1ST ST PARKLAND FL 33067"

        # Look for the full address pattern with city and state
        full_address_pattern = r'(\d+\.?\d*)\s+(?:(N|S|E|W|NE|NW|SE|SW)\s+)?([A-Z0-9\s]+?)\s+(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|CT|COURT|PL|PLACE|WAY|BLVD|BOULEVARD|LN|LANE)\s+([A-Z\s]+?)\s+(FL|FLORIDA)\s*(?:\d{5}(?:-\d{4})?)?'

        match = re.search(full_address_pattern, cleaned.upper())
        if match:
            house_num = match.group(1).replace('.0', '')  # Remove .0
            direction = match.group(2) or ''
            street_name = match.group(3).strip()
            street_type = match.group(4)
            city = match.group(5).strip()
            state = match.group(6)

            # Construct full address with city
            street_parts = [house_num]
            if direction:
                street_parts.append(direction)
            street_parts.append(street_name)
            street_parts.append(street_type)

            street_address = ' '.join(street_parts)

            # Return full address with city for ZabaSearch
            return f"{street_address}, {city} {state}"

        # Fallback: Look for just street address without city
        street_pattern = r'(\d+\.?\d*)\s+(?:(N|S|E|W|NE|NW|SE|SW)\s+)?([A-Z0-9\s]+?)\s+(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|CT|COURT|PL|PLACE|WAY|BLVD|BOULEVARD|LN|LANE)(?:\s|$)'

        match = re.search(street_pattern, cleaned.upper())
        if match:
            house_num = match.group(1).replace('.0', '')  # Remove .0
            direction = match.group(2) or ''
            street_name = match.group(3).strip()
            street_type = match.group(4)

            # Construct clean address
            parts = [house_num]
            if direction:
                parts.append(direction)
            parts.append(street_name)
            parts.append(street_type)

            return ' '.join(parts)

        # Final fallback: Basic cleaning
        cleaned = re.sub(r'\.0\b', '', cleaned)  # Remove .0 from numbers
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces to single

        return cleaned.strip()

    def _categorize_value(self, value: str, info: dict):
        """Intelligently categorize a value by analyzing its characteristics"""

        # Skip if empty or NAN
        if not value or value.strip() == '':
            return

        value = value.strip().upper()

        # Skip NAN, NULL, or obvious non-data values
        if value in ['NAN', 'NULL', 'NONE', 'UNNAMED:', '0', '0.0'] or value.startswith('UNNAMED:'):
            return

        # 1. ZIP CODE - Very distinctive pattern
        if re.match(r'^\d{5}(-\d{4})?$', value):
            info['zip_codes'].append(value)
            return

        # 2. STATE - Distinctive patterns
        if value in ['FL', 'FLORIDA', 'CA', 'CALIFORNIA', 'NY', 'NEW YORK', 'TX', 'TEXAS']:
            info['states'].append(value)
            return

        # 3. HOUSE NUMBER - Pure numbers or numbers with letters
        if re.match(r'^\d+\.?\d*[A-Z]?$', value) and not re.match(r'^\d{5}', value):
            info['house_numbers'].append(value.replace('.0', ''))
            return

        # 4. DIRECTION - Very specific set
        if value in ['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW', 'NORTH', 'SOUTH', 'EAST', 'WEST']:
            info['directions'].append(value)
            return

        # 5. STREET TYPE - Common street suffixes
        if value in ['ST', 'STREET', 'AVE', 'AVENUE', 'RD', 'ROAD', 'DR', 'DRIVE', 'CT', 'COURT',
                    'PL', 'PLACE', 'WAY', 'BLVD', 'BOULEVARD', 'LN', 'LANE', 'CIR', 'CIRCLE',
                    'TER', 'TERRACE', 'PKWY', 'PARKWAY', 'PT', 'POINT', 'LOOP', 'PATH', 'TRAIL']:
            info['street_types'].append(value)
            return

        # 6. PERSON NAMES - Check for names BEFORE cities to avoid confusion
        if self._looks_like_person_name(value):
            info['names'].append(value)
            return

        # 7. COMMON CITIES - Look for known city patterns
        city_patterns = [
            r'.*BEACH$',  # DEERFIELD BEACH, POMPANO BEACH
            r'^PARKLAND$',
            r'^HOLLYWOOD$',
            r'^MIAMI$',
            r'^ORLANDO$',
            r'.*SPRINGS$',
            r'.*LAKES$',
            r'.*HEIGHTS$',
            r'.*GARDENS$',
            r'^FORT\s+\w+$',  # FORT LAUDERDALE
            r'^BOCA\s+RATON$',
            r'^CORAL\s+SPRINGS$'
        ]

        for pattern in city_patterns:
            if re.match(pattern, value):
                info['cities'].append(value)
                return

        # 8. STREET NAMES - Look for ordinal numbers and typical street names
        if re.match(r'^\d+(ST|ND|RD|TH)$', value):  # 3RD, 4TH, 21ST
            info['street_names'].append(value)
            return

        # 9. Multi-word potential cities (if contains space and looks like city)
        if ' ' in value and len(value) > 5:
            # Could be city like "FORT LAUDERDALE" or "BOCA RATON"
            words = value.split()
            if len(words) >= 2 and all(word.isalpha() for word in words):
                # Only treat as city if it doesn't look like a person name
                if not self._looks_like_person_name(value):
                    info['cities'].append(value)
                    return

        # 10. Single word street names or other components
        if len(value) > 1 and value.isalpha():
            # Could be street name like "WATERSIDE", "MAIN", "CYPRESS"
            info['street_names'].append(value)
            return

        # 11. Everything else
        info['other'].append(value)

    def _construct_smart_address(self, info: dict) -> str:
        """Construct address from intelligently extracted components"""

        # Try to build: HOUSE_NUMBER DIRECTION STREET_NAME STREET_TYPE, CITY STATE
        address_parts = []

        # House number
        if info['house_numbers']:
            address_parts.append(info['house_numbers'][0])

        # Direction
        if info['directions']:
            address_parts.append(info['directions'][0])

        # Street name
        if info['street_names']:
            address_parts.append(info['street_names'][0])

        # Street type
        if info['street_types']:
            address_parts.append(info['street_types'][0])

        # Must have at least house number + street type or house number + street name
        if len(address_parts) < 2:
            print(f"⚠️ Insufficient address components: {address_parts}")
            return ''

        street_address = ' '.join(address_parts)

        # Add city and state with INTELLIGENT CITY SELECTION
        full_parts = [street_address]

        if info['cities']:
            # PRIORITIZE ACTUAL GEOGRAPHIC CITIES over trust/business names
            best_city = self._select_best_city(info['cities'])
            if best_city:
                full_parts.append(best_city)

        if info['states']:
            state = info['states'][0]
            if state in ['FL', 'FLORIDA']:
                full_parts.append('FL')

        # Join with proper formatting
        if len(full_parts) > 1:
            result = f"{full_parts[0]}, {' '.join(full_parts[1:])}"
        else:
            result = street_address

        print(f"🏗️ Constructed address: '{result}'")
        return result

    def _select_best_city(self, cities: list) -> str:
        """Select the best city from a list, prioritizing actual geographic cities"""

        if not cities:
            return ''

        print(f"🏙️ Selecting best city from: {cities}")

        # PRIORITY 1: Known Florida cities (exact matches)
        known_fl_cities = [
            'HOLLYWOOD', 'DAVIE', 'PLANTATION', 'FORT LAUDERDALE', 'LAUDERDALE',
            'DEERFIELD BEACH', 'POMPANO BEACH', 'CORAL SPRINGS', 'PARKLAND',
            'BOCA RATON', 'DELRAY BEACH', 'BOYNTON BEACH', 'WEST PALM BEACH',
            'AVENTURA', 'HALLANDALE', 'MIRAMAR', 'PEMBROKE PINES', 'WESTON',
            'SUNRISE', 'TAMARAC', 'LAUDERHILL', 'COCONUT CREEK', 'MARGATE'
        ]

        for city in cities:
            if city in known_fl_cities:
                print(f"✅ Selected known FL city: {city}")
                return city

        # PRIORITY 2: Cities ending with geographic indicators
        geographic_patterns = [
            r'.*BEACH$',    # Any BEACH
            r'.*SPRINGS$',  # Any SPRINGS
            r'.*LAKES$',    # Any LAKES
            r'.*HEIGHTS$',  # Any HEIGHTS
            r'.*GARDENS$'   # Any GARDENS
        ]

        for city in cities:
            for pattern in geographic_patterns:
                if re.match(pattern, city):
                    print(f"✅ Selected geographic city: {city}")
                    return city

        # PRIORITY 3: Cities with FORT prefix
        for city in cities:
            if city.startswith('FORT '):
                print(f"✅ Selected FORT city: {city}")
                return city

        # PRIORITY 4: Avoid trust/business names (filter out cities with these indicators)
        trust_indicators = ['TR', 'TRUST', 'REV', 'LLC', 'INC', 'CORP', 'REVOCABLE', 'FAMILY', 'LIVING']
        clean_cities = []

        for city in cities:
            has_trust_indicator = any(indicator in city for indicator in trust_indicators)
            if not has_trust_indicator:
                clean_cities.append(city)

        if clean_cities:
            print(f"✅ Selected clean city (no trust indicators): {clean_cities[0]}")
            return clean_cities[0]

        # FALLBACK: Return first city if no good options found
        print(f"⚠️ Using fallback city: {cities[0]}")
        return cities[0]

    def _try_enhance_address_with_row_data(self, base_address: str, all_values: list) -> str:
        """Try to enhance a partial address with additional city/state info from the row"""

        # If address already has city/state info, return as-is
        if ('BEACH' in base_address.upper() or 'HOLLYWOOD' in base_address.upper() or
            'PARKLAND' in base_address.upper() or 'LAUDERDALE' in base_address.upper() or
            re.search(r'\b(FL|FLORIDA)\b', base_address.upper())):
            return base_address

        # Look for city and state information in other values
        found_city = None
        found_state = None

        for value in all_values:
            if value == base_address:
                continue

            value_upper = value.upper()

            # Check for state
            if value_upper in ['FL', 'FLORIDA'] and not found_state:
                found_state = 'FL'
                continue

            # Check for cities
            city_patterns = [
                r'.*BEACH$',  # DEERFIELD BEACH, POMPANO BEACH
                r'^PARKLAND$',
                r'^HOLLYWOOD$',
                r'^FORT\s+LAUDERDALE$',
                r'^BOCA\s+RATON$',
                r'^CORAL\s+SPRINGS$'
            ]

            for pattern in city_patterns:
                if re.match(pattern, value_upper) and not found_city:
                    found_city = value
                    break

            # Check for combined city/state like "FORT LAUDERDALE FL"
            if ' FL' in value_upper or ' FLORIDA' in value_upper:
                parts = value_upper.split()
                if len(parts) >= 2 and parts[-1] in ['FL', 'FLORIDA']:
                    city_part = ' '.join(parts[:-1])
                    if len(city_part) >= 4 and not found_city:
                        found_city = city_part
                        found_state = 'FL'
                        break

        # Enhance the address if we found additional info
        if found_city or found_state:
            enhanced_parts = [base_address.strip()]

            if found_city:
                enhanced_parts.append(found_city)

            if found_state:
                enhanced_parts.append(found_state)

            if len(enhanced_parts) > 1:
                return f"{enhanced_parts[0]}, {' '.join(enhanced_parts[1:])}"

        return base_address

    def _looks_like_phone(self, text: str) -> bool:
        """Check if text looks like a phone number"""
        if not text:
            return False

        # Clean and check for phone patterns
        cleaned = re.sub(r'[^\d]', '', str(text))
        return len(cleaned) == 10 or len(cleaned) == 11

    def _clean_phone(self, text: str) -> str:
        """Clean and format phone number"""
        if not text:
            return ''

        cleaned = re.sub(r'[^\d]', '', str(text))
        if len(cleaned) == 11 and cleaned.startswith('1'):
            cleaned = cleaned[1:]

        if len(cleaned) == 10:
            return f"{cleaned[:3]}-{cleaned[3:6]}-{cleaned[6:]}"

        return str(text).strip()

    def _name_confidence_score(self, text: str) -> float:
        """Calculate confidence score for name detection"""
        if not text:
            return 0.0

        score = 0.0
        text_upper = text.upper()

        # Higher score for obvious name patterns
        if re.search(r'[A-Z][a-z]+\s+[A-Z][a-z]+', text):
            score += 0.8
        elif re.search(r'[A-Z]+,\s*[A-Z]+', text):
            score += 0.9
        elif any(indicator in text_upper for indicator in ['LLC', 'INC', 'TRUST', 'CORP']):
            score += 0.7
        elif '&' in text or ' AND ' in text_upper:
            score += 0.6
        elif re.search(r'[A-Z]{2,}', text):
            score += 0.4

        # Penalize obvious non-names
        if re.search(r'^\d+', text):
            score -= 0.5
        if len(text) < 3:
            score -= 0.3

        return max(0.0, min(1.0, score))

    def _clean_name_for_search(self, name: str) -> str:
        """Clean name specifically for search processing"""
        if not name:
            return ''

        # Basic cleaning
        cleaned = name.strip()

        # Remove common noise
        cleaned = re.sub(r'\s+', ' ', cleaned)  # Multiple spaces
        cleaned = re.sub(r'[^\w\s&,-]', '', cleaned)  # Keep only word chars, spaces, &, comma, dash

        # Handle common name formats
        if ',' in cleaned:
            # "LAST, FIRST" format - convert to "FIRST LAST"
            parts = [part.strip() for part in cleaned.split(',')]
            if len(parts) == 2:
                cleaned = f"{parts[1]} {parts[0]}"

        # Clean up business indicators
        business_words = ['LLC', 'INC', 'CORP', 'CO', 'TRUST', 'TR', 'ETAL', 'ESTATE']
        for word in business_words:
            cleaned = re.sub(rf'\b{word}\b', word, cleaned, flags=re.IGNORECASE)

        return cleaned.strip()

    def _address_confidence_score(self, text: str) -> float:
        """Calculate confidence score for address detection"""
        if not text:
            return 0.0

        score = 0.0
        text_upper = text.upper()

        # Higher score for obvious address patterns
        if re.search(r'\d+.*\b(ST|AVE|RD|DR|CT|PL|WAY|BLVD|LN)\b', text_upper):
            score += 0.9
        if re.search(r'\b(N|S|E|W|NE|NW|SE|SW)\b.*\d+', text_upper):
            score += 0.8
        if re.search(r'\b(BEACH|CITY|TOWN|PARK)\b', text_upper):
            score += 0.7
        if re.search(r'\b(FL|FLORIDA)\b', text_upper):
            score += 0.6
        if re.search(r'\d{5}', text):
            score += 0.5

        # Require minimum components
        components = len([part for part in text.split() if part.strip()])
        if components >= 5:
            score += 0.3
        elif components < 3:
            score -= 0.5

        return max(0.0, min(1.0, score))
        """Convert any CSV format to standardized search format"""
        try:
            # Detect format
            csv_format = self.detect_csv_format(csv_path)

            # Load CSV
            df = read_data_file(csv_path)
            logger.info(f"Loaded {len(df)} records from {csv_path}")

            # Create output path if not provided
            if not output_path:
                base_path = Path(csv_path)
                output_path = base_path.parent / f"{base_path.stem}_standardized.csv"

            processed_records = []

            if csv_format == 'broward_raw':
                logger.info("🏠 Processing Broward County raw format...")

                for index, row in df.iterrows():
                    # Process Owner Name 1
                    name1 = self.clean_name(row.get('Owner Name 1', ''))

                    # Combine address
                    full_address = self.combine_address_components(row)

                    # Get existing phone
                    existing_phone = str(row.get('Telephone Number', '')).strip()

                    # Create record for Owner Name 1
                    if name1:
                        record = {
                            'DirectName_Cleaned': name1,
                            'DirectName_Address': full_address,
                            'DirectName_Type': 'Person',
                            'DirectName_Phone_Primary': existing_phone if existing_phone and existing_phone != 'nan' else '',
                            'original_index': index,
                            'source': 'Owner Name 1'
                        }
                        processed_records.append(record)

            elif csv_format == 'search_ready':
                logger.info("🔍 CSV already in search-ready format - copying...")
                # Already in correct format, just ensure all required columns exist
                for index, row in df.iterrows():
                    record = {
                        'DirectName_Cleaned': row.get('DirectName_Cleaned', ''),
                        'DirectName_Address': row.get('DirectName_Address', ''),
                        'DirectName_Type': row.get('DirectName_Type', 'Person'),
                        'DirectName_Phone_Primary': row.get('DirectName_Phone_Primary', ''),
                        'original_index': row.get('original_index', index),
                        'source': row.get('source', 'Unknown')
                    }
                    processed_records.append(record)

            else:
                logger.info("📝 Processing generic/unknown format...")

                # Check if this is a file with "Unnamed" columns that matches Broward property format
                columns = list(df.columns)
                unnamed_cols = [col for col in columns if isinstance(col, str) and col.startswith('Unnamed:')]

                if len(unnamed_cols) >= 10:  # Likely a property file without headers
                    logger.info("🔍 Detected property file without headers - applying Broward format mapping")

                    # Map the Unnamed columns to proper Broward property format
                    # Based on the structure we saw: Owner1, HouseNum, PreDir, Street, Type, PostDir, Unit, UnitNum, City, State, Zip, Country, Phone
                    column_mapping = {
                        'Unnamed: 0': 'Owner Name 1',
                        'Unnamed: 1': 'House Number',
                        'Unnamed: 2': 'Prefix Direction',
                        'Unnamed: 3': 'Street Name',
                        'Unnamed: 4': 'Street Type',
                        'Unnamed: 5': 'Post Direction',
                        'Unnamed: 6': 'Unit Type',
                        'Unnamed: 7': 'Unit Number',
                        'Unnamed: 8': 'City Name',
                        'Unnamed: 9': 'State Abbreviation',
                        'Unnamed: 10': 'Zip Code',
                        'Unnamed: 11': 'Country',
                        'Unnamed: 12': 'Telephone Number'
                    }

                    # Rename columns to match expected format
                    df_renamed = df.rename(columns=column_mapping)
                    logger.info(f"✅ Mapped {len(unnamed_cols)} unnamed columns to Broward property format")

                    # Now process using the Broward format logic
                    for index, row in df_renamed.iterrows():
                        if pd.isna(row.get('Owner Name 1', '')) or not str(row.get('Owner Name 1', '')).strip():
                            continue

                        # Create address from components
                        address_parts = []
                        if pd.notna(row.get('House Number', '')):
                            address_parts.append(str(row.get('House Number', '')).strip())
                        if pd.notna(row.get('Prefix Direction', '')):
                            address_parts.append(str(row.get('Prefix Direction', '')).strip())
                        if pd.notna(row.get('Street Name', '')):
                            address_parts.append(str(row.get('Street Name', '')).strip())
                        if pd.notna(row.get('Street Type', '')):
                            address_parts.append(str(row.get('Street Type', '')).strip())
                        if pd.notna(row.get('Post Direction', '')):
                            address_parts.append(str(row.get('Post Direction', '')).strip())

                        address = ' '.join([part for part in address_parts if part])
                        city = str(row.get('City Name', '')).strip()
                        state = str(row.get('State Abbreviation', '')).strip()

                        if city and state:
                            address = f"{address}, {city}, {state}"

                        # Process Owner Name 1
                        name1 = self.clean_name(row.get('Owner Name 1', ''))
                        if name1 and address:
                            record = {
                                'DirectName_Cleaned': name1,
                                'DirectName_Address': address,
                                'DirectName_Type': 'Person',
                                'DirectName_Phone_Primary': str(row.get('Telephone Number', '')).strip(),
                                'DirectName_Phone_Secondary': '',
                                'original_index': index,
                                'source': 'Broward_Property_Unmapped'
                            }
                            processed_records.append(record)



                else:
                    # TRUE ROW-BY-ROW CONTENT SCANNER - IGNORES COLUMN STRUCTURE COMPLETELY
                    logger.info("🔍 Using universal row-by-row content scanner (structure-independent)")

                    # Phone number patterns
                    phone_patterns = [
                        r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',  # 561-776-8577 or 5617768577
                        r'\b\(\d{3}\)\s?\d{3}[-.]?\d{4}\b',  # (561) 776-8577
                        r'\b\d{10}\b'  # 5617768577
                    ]

                    # Address patterns
                    address_patterns = [
                        r'\d+\s+[\w\s]+(?:ST|AVE|RD|BLVD|DR|CT|LN|WAY|PL|CIR|PKWY|TER)[\w\s]*(?:,\s*[\w\s]+)*(?:,\s*[A-Z]{2})?\s*\d{5}?',
                        r'\d+\s+[\w\s]+(?:STREET|AVENUE|ROAD|BOULEVARD|DRIVE|COURT|LANE|PLACE)[\w\s]*(?:,\s*[\w\s]+)*(?:,\s*[A-Z]{2})?'
                    ]

                    for index, row in df.iterrows():
                        # Scan ALL cells in this row for content
                        row_names = []
                        row_addresses = []
                        row_phones = []

                        for col_name in df.columns:
                            cell_value = str(row[col_name]).strip()
                            if pd.isna(row[col_name]) or cell_value in ['', 'nan', 'NaN']:
                                continue

                            # Check if cell contains a phone number
                            for phone_pattern in phone_patterns:
                                if re.search(phone_pattern, cell_value):
                                    # Extract just the phone number
                                    phone_match = re.search(phone_pattern, cell_value)
                                    if phone_match:
                                        clean_phone = re.sub(r'[^\d]', '', phone_match.group())
                                        if len(clean_phone) == 10:
                                            formatted_phone = f"{clean_phone[:3]}-{clean_phone[3:6]}-{clean_phone[6:]}"
                                            row_phones.append(formatted_phone)
                                            continue  # Don't process this cell as name/address

                            # Check if cell contains an address
                            is_address = False
                            for addr_pattern in address_patterns:
                                if re.search(addr_pattern, cell_value, re.IGNORECASE):
                                    row_addresses.append(cell_value)
                                    is_address = True
                                    break

                            if is_address:
                                continue  # Don't process this cell as name

                            # If it's not a phone or address, and looks like a name
                            if len(cell_value) > 2 and len(cell_value.split()) >= 1:
                                # Basic name validation
                                if not re.search(r'^\d+$', cell_value):  # Not just numbers
                                    cleaned_name = self.clean_name(cell_value)
                                    if cleaned_name and len(cleaned_name) > 2:
                                        row_names.append(cleaned_name)

                        # Create records from discovered content
                        if row_names and row_addresses:
                            # Use first address found
                            main_address = row_addresses[0]

                            # Create record for each name found
                            for i, name in enumerate(row_names):
                                primary_phone = row_phones[0] if len(row_phones) > 0 else ''
                                secondary_phone = row_phones[1] if len(row_phones) > 1 else ''

                                record = {
                                    'DirectName_Cleaned': name,
                                    'DirectName_Address': main_address,
                                    'DirectName_Type': 'Person',
                                    'DirectName_Phone_Primary': primary_phone,
                                    'DirectName_Phone_Secondary': secondary_phone,
                                    'original_index': index,
                                    'source': f'Universal_Content_Scanner_Name_{i}'
                                }
                                processed_records.append(record)

                        elif row_names:
                            # Names without addresses - still create records
                            for i, name in enumerate(row_names):
                                primary_phone = row_phones[0] if len(row_phones) > 0 else ''
                                secondary_phone = row_phones[1] if len(row_phones) > 1 else ''

                                record = {
                                    'DirectName_Cleaned': name,
                                    'DirectName_Address': '',
                                    'DirectName_Type': 'Person',
                                    'DirectName_Phone_Primary': primary_phone,
                                    'DirectName_Phone_Secondary': secondary_phone,
                                    'original_index': index,
                                    'source': f'Universal_Content_Scanner_Name_Only_{i}'
                                }
                                processed_records.append(record)            # Save standardized CSV
            result_df = pd.DataFrame(processed_records)
            result_df.to_csv(output_path, index=False)

            logger.info(f"✅ Standardized {len(processed_records)} records")
            logger.info(f"💾 Saved to: {output_path}")

            return str(output_path)

        except Exception as e:
            logger.error(f"Error standardizing CSV: {e}")
            return None

    def filter_records_without_phones(self, csv_path: str) -> tuple:
        """Filter records that need phone numbers vs those that already have them"""
        try:
            df = read_data_file(csv_path)

            # Check for existing phone numbers
            phone_columns = [
                'DirectName_Phone_Primary',
                'DirectName_Phone_Secondary',
                'DirectName_Phone_All',
                'Radaris_Phone_Primary',
                'Radaris_Phone_Secondary',
                'phone',
                'Phone',
                'Telephone Number'
            ]

            records_with_phones = []
            records_without_phones = []

            for index, row in df.iterrows():
                has_phone = False

                for col in phone_columns:
                    if col in row and pd.notna(row[col]) and str(row[col]).strip() and str(row[col]).strip() != 'nan':
                        has_phone = True
                        break

                if has_phone:
                    records_with_phones.append(index)
                else:
                    records_without_phones.append(index)

            logger.info(f"📊 Phone status: {len(records_with_phones)} with phones, {len(records_without_phones)} without phones")

            return records_without_phones, records_with_phones

        except Exception as e:
            logger.error(f"Error filtering records: {e}")
            return [], []

def process_file(csv_path: str) -> pd.DataFrame:
    """
    Process file for Flask integration - returns standardized DataFrame

    Args:
        csv_path: Path to input CSV file

    Returns:
        pd.DataFrame: Processed dataframe
    """
    try:
        handler = CSVFormatHandler()

        # Read the file with encoding detection
        encodings = ['utf-8', 'latin1', 'cp1252']
        df = None

        for encoding in encodings:
            try:
                if csv_path.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(csv_path)
                else:
                    # First try reading normally
                    df = read_data_file(csv_path, encoding=encoding)

                    # Check if we got "Unnamed" columns (indicates missing headers)
                    unnamed_cols = [col for col in df.columns if isinstance(col, str) and col.startswith('Unnamed:')]
                    if len(unnamed_cols) > len(df.columns) * 0.5:  # More than half are unnamed
                        logger.info(f"⚠️ Detected file without headers - attempting to infer structure")

                        # Try reading without header and infer column names
                        df_no_header = read_data_file(csv_path, encoding=encoding, header=None)

                        # Check if first row looks like data (has names/addresses)
                        first_row = df_no_header.iloc[0].astype(str).tolist()
                        if any(re.search(r'[A-Za-z]{3,}', str(val)) for val in first_row[:3]):
                            # First row contains data, use Broward property format headers
                            num_cols = len(df_no_header.columns)

                            # Apply Broward property format column mapping (same as in standardize_to_search_format)
                            if num_cols >= 10:  # Likely a property file
                                logger.info("🔍 Applying Broward property format headers")
                                broward_headers = [
                                    'Owner Name 1',
                                    'House Number',
                                    'Prefix Direction',
                                    'Street Name',
                                    'Street Type',
                                    'Post Direction',
                                    'Unit Type',
                                    'Unit Number',
                                    'City Name',
                                    'State Abbreviation',
                                    'Zip Code',
                                    'Country',
                                    'Telephone Number'
                                ]

                                # Use Broward headers for available columns, generic for extras
                                final_headers = broward_headers[:num_cols]
                                if num_cols > len(broward_headers):
                                    final_headers.extend([f'Column_{i+1}' for i in range(len(broward_headers), num_cols)])

                                df_no_header.columns = final_headers
                                df = df_no_header
                                logger.info(f"✅ Applied Broward property format headers: {final_headers[:5]}...")
                            else:
                                # Fallback to generic inference for non-property files
                                inferred_headers = []
                                for i, col_data in enumerate(df_no_header.iloc[:, :min(10, num_cols)].columns):
                                    col_values = df_no_header.iloc[:5, i].astype(str).tolist()
                                    col_sample = ' '.join(col_values).upper()

                                    # Infer column type based on content
                                    if any(name_pattern in col_sample for name_pattern in ['LLC', 'INC', 'CORP', '&', 'TRUST', 'TR', 'ETAL']):
                                        base_name = 'Owner Name'
                                        count = len([h for h in inferred_headers if base_name in h]) + 1
                                        header_name = f'{base_name} {count}'
                                    elif re.search(r'\d+.*\b(ST|AVE|RD|DR|CT|PL|WAY|BLVD|LN)\b', col_sample):
                                        base_name = 'Street Address'
                                        count = len([h for h in inferred_headers if base_name in h])
                                        header_name = base_name if count == 0 else f'{base_name} {count + 1}'
                                    elif any(city in col_sample for city in ['BEACH', 'CITY', 'TOWN', 'PARK']):
                                        base_name = 'City'
                                        count = len([h for h in inferred_headers if base_name in h])
                                        header_name = base_name if count == 0 else f'{base_name} {count + 1}'
                                    elif re.search(r'\b(FL|CA|NY|TX)\b', col_sample):
                                        base_name = 'State'
                                        count = len([h for h in inferred_headers if base_name in h])
                                        header_name = base_name if count == 0 else f'{base_name} {count + 1}'
                                    elif re.search(r'\d{5}', col_sample):
                                        base_name = 'Zip Code'
                                        count = len([h for h in inferred_headers if base_name in h])
                                        header_name = base_name if count == 0 else f'{base_name} {count + 1}'
                                    else:
                                        header_name = f'Column_{i+1}'

                                    inferred_headers.append(header_name)

                                # Apply inferred headers
                                df_no_header.columns = inferred_headers + [f'Column_{i+1}' for i in range(len(inferred_headers), num_cols)]
                                df = df_no_header
                                logger.info(f"✅ Applied inferred headers: {inferred_headers[:5]}...")

                logger.info(f"Successfully read file with {encoding} encoding")
                break
            except:
                continue

        if df is None:
            raise ValueError("Could not read file with any standard encoding")

        logger.info(f"Loaded {len(df)} records from {csv_path}")
        return df

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise

def main():
    """Test the CSV format handler"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python csv_format_handler.py <input_csv>")
        return

    input_csv = sys.argv[1]

    handler = CSVFormatHandler()

    # Standardize the CSV
    standardized_path = handler.standardize_to_search_format(input_csv)

    if standardized_path:
        print(f"✅ CSV standardized successfully: {standardized_path}")

        # Check phone status
        without_phones, with_phones = handler.filter_records_without_phones(standardized_path)
        print(f"📞 {len(with_phones)} records already have phones")
        print(f"❌ {len(without_phones)} records need phone numbers")
    else:
        print("❌ Failed to standardize CSV")


if __name__ == "__main__":
    main()
