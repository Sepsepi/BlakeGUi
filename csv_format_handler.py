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
                'name_columns': ['Owner Name 1', 'Owner Name 2'],
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
            df = pd.read_csv(csv_path, nrows=5)
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

            name_cols = [col for col in columns if any(indicator in col.lower() for indicator in name_indicators)]
            addr_cols = [col for col in columns if any(indicator in col.lower() for indicator in address_indicators)]

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

        # Remove business indicators - skip businesses
        for suffix in business_suffixes:
            if suffix in name:
                if name.endswith(suffix) or f" {suffix}" in name:
                    return ""  # Skip businesses

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

    def standardize_to_search_format(self, csv_path: str, output_path: str = None) -> str:
        """Convert any CSV format to standardized search format"""
        try:
            # Detect format
            csv_format = self.detect_csv_format(csv_path)

            # Load CSV
            df = pd.read_csv(csv_path)
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
                    name2 = self.clean_name(row.get('Owner Name 2', ''))

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

                    # Create record for Owner Name 2 (if different)
                    if name2 and name2 != name1:
                        record = {
                            'DirectName_Cleaned': name2,
                            'DirectName_Address': full_address,
                            'DirectName_Type': 'Person',
                            'DirectName_Phone_Primary': existing_phone if existing_phone and existing_phone != 'nan' else '',
                            'original_index': index,
                            'source': 'Owner Name 2'
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
                # Try to auto-detect name and address columns
                columns = list(df.columns)

                # Find name columns
                name_cols = []
                for col in columns:
                    if any(word in col.lower() for word in ['name', 'owner', 'person', 'first', 'last']):
                        name_cols.append(col)

                # Find address columns
                addr_cols = []
                for col in columns:
                    if any(word in col.lower() for word in ['address', 'street', 'location', 'addr']):
                        addr_cols.append(col)

                if not name_cols:
                    logger.error("No name columns detected")
                    return None

                if not addr_cols:
                    logger.error("No address columns detected")
                    return None

                logger.info(f"Using name columns: {name_cols}")
                logger.info(f"Using address columns: {addr_cols}")

                for index, row in df.iterrows():
                    # Process each name column
                    for name_col in name_cols:
                        name = self.clean_name(row.get(name_col, ''))
                        if name:
                            # Use first address column or combine multiple
                            address = str(row.get(addr_cols[0], '')).strip()

                            record = {
                                'DirectName_Cleaned': name,
                                'DirectName_Address': address,
                                'DirectName_Type': 'Person',
                                'DirectName_Phone_Primary': '',
                                'original_index': index,
                                'source': name_col
                            }
                            processed_records.append(record)

            # Save standardized CSV
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
            df = pd.read_csv(csv_path)

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
