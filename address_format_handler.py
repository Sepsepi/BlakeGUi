#!/usr/bin/env python3
"""
Address Format Handler - Specialized for Reverse Address Extraction
Handles multiple CSV formats and standardizes addresses for BCPA searches
Separated from phone extraction logic for cleaner architecture
"""

import pandas as pd
import re
import logging
from datetime import datetime
from pathlib import Path
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'address_format_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class AddressFormatHandler:
    """Specialized handler for address formatting and standardization"""

    def __init__(self):
        """Initialize the address format handler"""
        self.supported_formats = {
            'pending_listings': {
                'description': 'Pending listings with positional columns',
                'address_column': 3,  # 0-indexed
                'city_column': 4,
                'identifier': 'positional_mls'
            },
            'broward_raw': {
                'description': 'Broward County raw property data',
                'name_columns': ['Owner Name 1', 'Owner Name 2'],
                'address_components': ['House Number', 'Street Name', 'Street Type', 'City Name'],
                'identifier': 'structured_broward'
            },
            'simple_address': {
                'description': 'Simple format with Address and City columns',
                'address_column': 'Address',
                'city_column': 'City',
                'identifier': 'simple_columns'
            }
        }

        # Broward County cities for geographic validation
        self.broward_cities = {
            'HOLLYWOOD', 'FORT LAUDERDALE', 'PEMBROKE PINES', 'CORAL SPRINGS',
            'MIRAMAR', 'SUNRISE', 'PLANTATION', 'DAVIE', 'WESTON', 'MARGATE',
            'TAMARAC', 'COCONUT CREEK', 'POMPANO BEACH', 'LAUDERHILL',
            'LAUDERDALE LAKES', 'WILTON MANORS', 'OAKLAND PARK', 'HALLANDALE BEACH',
            'COOPER CITY', 'DEERFIELD BEACH', 'LIGHTHOUSE POINT', 'NORTH LAUDERDALE',
            'PARKLAND', 'SEA RANCH LAKES', 'SOUTHWEST RANCHES', 'WEST PARK',
            'HILLSBORO BEACH', 'LAZY LAKE', 'PEMBROKE PARK', 'HIGHLAND BEACH'
        }

        # Street type standardization
        self.street_types = {
            'AVENUE': 'AVE', 'AV': 'AVE', 'AVE': 'AVE',
            'STREET': 'ST', 'STR': 'ST', 'ST': 'ST',
            'ROAD': 'RD', 'RD': 'RD',
            'DRIVE': 'DR', 'DRV': 'DR', 'DR': 'DR',
            'BOULEVARD': 'BLVD', 'BLVD': 'BLVD',
            'LANE': 'LN', 'LN': 'LN',
            'COURT': 'CT', 'CT': 'CT',
            'CIRCLE': 'CIR', 'CIR': 'CIR',
            'PLACE': 'PL', 'PL': 'PL',
            'TERRACE': 'TER', 'TER': 'TER',
            'WAY': 'WAY'
        }

    def detect_csv_format(self, csv_path: str) -> dict:
        """Detect the CSV format and return format information"""
        try:
            # Read first few rows to examine structure
            df = pd.read_csv(csv_path, nrows=10)
            columns = list(df.columns)

            logger.info(f"🔍 Analyzing CSV format with columns: {columns}")

            # Check for Broward raw format (structured with separated components)
            if all(col in columns for col in ['House Number', 'Street Name', 'City Name']):
                logger.info("📋 Detected: Broward County structured format")
                return {
                    'type': 'broward_raw',
                    'confidence': 'high',
                    'description': 'Broward County property data with separated address components',
                    'sample_data': df.head(3).to_dict('records')
                }

            # Check for pending listings format (positional columns, no proper headers)
            if (len(columns) >= 5 and
                str(columns[0]).startswith(('R', 'A', 'F')) and  # MLS ID pattern
                'pending' in str(columns[1]).lower()):

                logger.info("📋 Detected: Pending listings format (positional)")
                return {
                    'type': 'pending_listings',
                    'confidence': 'high',
                    'description': 'Pending listings with MLS IDs and positional address data',
                    'sample_data': df.head(3).to_dict('records')
                }

            # Check for simple address format
            address_indicators = ['address', 'street address', 'property address', 'addr']
            city_indicators = ['city', 'city name', 'municipality']

            has_address = any(any(indicator in col.lower() for indicator in address_indicators) for col in columns)
            has_city = any(any(indicator in col.lower() for indicator in city_indicators) for col in columns)

            if has_address and has_city:
                logger.info("📋 Detected: Simple address format")
                return {
                    'type': 'simple_address',
                    'confidence': 'medium',
                    'description': 'Simple format with address and city columns',
                    'sample_data': df.head(3).to_dict('records')
                }

            # Unknown format
            logger.warning("❓ Unknown CSV format detected")
            return {
                'type': 'unknown',
                'confidence': 'low',
                'description': 'Unknown format - will attempt generic processing',
                'sample_data': df.head(3).to_dict('records')
            }

        except Exception as e:
            logger.error(f"Error detecting CSV format: {e}")
            return {
                'type': 'error',
                'confidence': 'none',
                'description': f'Error reading file: {str(e)}',
                'sample_data': []
            }

    def extract_address_from_pending(self, row: pd.Series) -> dict:
        """Extract address from pending listings format (positional columns)"""
        try:
            # Column 3 (index 3) = Street Address, Column 4 (index 4) = City
            street_address = ''
            city = ''

            if len(row) > 3 and pd.notna(row.iloc[3]):
                street_address = str(row.iloc[3]).strip()

            if len(row) > 4 and pd.notna(row.iloc[4]):
                city = str(row.iloc[4]).strip().upper()

            # Create BCPA search format and check if it's in Broward County
            if street_address and city:
                bcpa_format = f"{street_address}, {city}"
                is_broward = city in self.broward_cities

                return {
                    'street_address': street_address,
                    'city': city,
                    'bcpa_search_format': bcpa_format,
                    'is_broward_county': is_broward,
                    'extraction_method': 'pending_positional'
                }

            return {'error': 'Missing address or city data'}

        except Exception as e:
            logger.error(f"Error extracting address from pending format: {e}")
            return {'error': str(e)}

    def extract_address_from_broward(self, row: pd.Series) -> dict:
        """Extract address from Broward structured format"""
        try:
            address_parts = []

            # Build address from components
            house_num = str(row.get('House Number', '')).strip()
            if house_num and house_num.lower() not in ['nan', 'none', '']:
                address_parts.append(house_num)

            # Add prefix direction if present
            prefix_dir = str(row.get('Prefix Direction', '')).strip()
            if prefix_dir and prefix_dir.lower() not in ['nan', 'none', '']:
                address_parts.append(prefix_dir)

            # Add street name
            street_name = str(row.get('Street Name', '')).strip()
            if street_name and street_name.lower() not in ['nan', 'none', '']:
                address_parts.append(street_name)

            # Add street type (standardized)
            street_type = str(row.get('Street Type', '')).strip().upper()
            if street_type and street_type.lower() not in ['nan', 'none', '']:
                standardized_type = self.street_types.get(street_type, street_type)
                address_parts.append(standardized_type)

            # Add post direction if present
            post_dir = str(row.get('Post Direction', '')).strip()
            if post_dir and post_dir.lower() not in ['nan', 'none', '']:
                address_parts.append(post_dir)

            street_address = " ".join(address_parts)

            # Get city
            city = str(row.get('City Name', '')).strip().upper()
            if city and city.lower() in ['nan', 'none', '']:
                city = ''

            if street_address and city:
                bcpa_format = f"{street_address}, {city}"
                is_broward = city in self.broward_cities

                return {
                    'street_address': street_address,
                    'city': city,
                    'bcpa_search_format': bcpa_format,
                    'is_broward_county': is_broward,
                    'extraction_method': 'broward_structured'
                }

            return {'error': 'Missing address components'}

        except Exception as e:
            logger.error(f"Error extracting address from Broward format: {e}")
            return {'error': str(e)}

    def extract_address_from_simple(self, row: pd.Series) -> dict:
        """Extract address from simple format with Address and City columns"""
        try:
            # Find address column
            address_col = None
            city_col = None

            for col in row.index:
                col_lower = str(col).lower()
                if any(indicator in col_lower for indicator in ['address', 'street', 'property']):
                    address_col = col
                elif any(indicator in col_lower for indicator in ['city', 'municipality']):
                    city_col = col

            if not address_col or not city_col:
                return {'error': 'Could not find address or city columns'}

            street_address = str(row.get(address_col, '')).strip()
            city = str(row.get(city_col, '')).strip().upper()

            if street_address and city and street_address.lower() not in ['nan', 'none', '']:
                bcpa_format = f"{street_address}, {city}"
                is_broward = city in self.broward_cities

                return {
                    'street_address': street_address,
                    'city': city,
                    'bcpa_search_format': bcpa_format,
                    'is_broward_county': is_broward,
                    'extraction_method': 'simple_columns'
                }

            return {'error': 'Missing address or city data'}

        except Exception as e:
            logger.error(f"Error extracting address from simple format: {e}")
            return {'error': str(e)}

    def process_csv_for_address_extraction(self, csv_path: str, output_path: str = None) -> str:
        """Process CSV file and standardize addresses for BCPA extraction"""
        try:
            # Detect format
            format_info = self.detect_csv_format(csv_path)
            logger.info(f"📋 Format detected: {format_info['type']} ({format_info['confidence']} confidence)")
            logger.info(f"📄 Description: {format_info['description']}")

            # Read full CSV
            df = pd.read_csv(csv_path)
            logger.info(f"📊 Loaded {len(df)} records from CSV")

            # Process based on detected format
            processed_records = []
            broward_count = 0
            non_broward_count = 0
            error_count = 0

            for index, row in df.iterrows():
                if format_info['type'] == 'pending_listings':
                    address_info = self.extract_address_from_pending(row)
                elif format_info['type'] == 'broward_raw':
                    address_info = self.extract_address_from_broward(row)
                elif format_info['type'] == 'simple_address':
                    address_info = self.extract_address_from_simple(row)
                else:
                    # Try to auto-detect from row content
                    address_info = self._try_auto_extract(row)

                if 'error' in address_info:
                    error_count += 1
                    # Keep original row with error flag
                    record = row.to_dict()
                    record['BCPA_Processing_Error'] = address_info['error']
                    record['BCPA_Search_Format'] = ''
                    record['BCPA_Eligible'] = False
                else:
                    # Add address standardization results
                    record = row.to_dict()
                    record['BCPA_Street_Address'] = address_info['street_address']
                    record['BCPA_City'] = address_info['city']
                    record['BCPA_Search_Format'] = address_info['bcpa_search_format']
                    record['BCPA_Eligible'] = address_info['is_broward_county']
                    record['BCPA_Extraction_Method'] = address_info['extraction_method']

                    if address_info['is_broward_county']:
                        broward_count += 1
                    else:
                        non_broward_count += 1

                processed_records.append(record)

            # Create output DataFrame
            processed_df = pd.DataFrame(processed_records)

            # Set output path
            if output_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                input_file = Path(csv_path)
                output_path = str(input_file.parent / f"{input_file.stem}_address_standardized_{timestamp}.csv")

            # Save processed file
            processed_df.to_csv(output_path, index=False, encoding='utf-8')

            # Log summary
            logger.info("=" * 60)
            logger.info("ADDRESS STANDARDIZATION COMPLETED")
            logger.info(f"📊 Total records processed: {len(df)}")
            logger.info(f"✅ Broward County eligible: {broward_count}")
            logger.info(f"⚠️ Non-Broward County: {non_broward_count}")
            logger.info(f"❌ Processing errors: {error_count}")
            logger.info(f"💾 Output saved: {output_path}")
            logger.info("=" * 60)

            return output_path

        except Exception as e:
            logger.error(f"Error processing CSV for address extraction: {e}")
            raise

    def _try_auto_extract(self, row: pd.Series) -> dict:
        """Try to automatically extract address from unknown format"""
        try:
            # Look for address-like data in any column
            for col in row.index:
                value = str(row[col]).strip()
                # Check if value looks like a street address
                if (len(value) > 10 and
                    any(indicator in value.upper() for indicator in ['DRIVE', 'STREET', 'AVENUE', 'ROAD', 'WAY']) and
                    any(char.isdigit() for char in value)):

                    # Try to split into address and city
                    if ',' in value:
                        parts = value.split(',')
                        if len(parts) >= 2:
                            street_address = parts[0].strip()
                            city = parts[1].strip().upper()

                            bcpa_format = f"{street_address}, {city}"
                            is_broward = city in self.broward_cities

                            return {
                                'street_address': street_address,
                                'city': city,
                                'bcpa_search_format': bcpa_format,
                                'is_broward_county': is_broward,
                                'extraction_method': 'auto_detected'
                            }

            return {'error': 'Could not auto-detect address format'}

        except Exception as e:
            return {'error': f'Auto-extraction failed: {str(e)}'}

    def filter_broward_only(self, csv_path: str, output_path: str = None) -> str:
        """Filter processed CSV to only include Broward County properties"""
        try:
            df = pd.read_csv(csv_path)

            # Filter for Broward County eligible records
            broward_df = df[df.get('BCPA_Eligible', False) == True].copy()

            if output_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                input_file = Path(csv_path)
                output_path = str(input_file.parent / f"{input_file.stem}_broward_only_{timestamp}.csv")

            broward_df.to_csv(output_path, index=False, encoding='utf-8')

            logger.info(f"🏠 Filtered to {len(broward_df)} Broward County properties")
            logger.info(f"💾 Broward-only file saved: {output_path}")

            return output_path

        except Exception as e:
            logger.error(f"Error filtering Broward properties: {e}")
            raise

def process_file_for_address_extraction(csv_path: str, output_path: str = None):
    """Main function for processing files for address extraction"""
    handler = AddressFormatHandler()
    return handler.process_csv_for_address_extraction(csv_path, output_path)

def main():
    """Command line interface for testing"""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python address_format_handler.py <csv_file> [output_file]")
        print("Example: python address_format_handler.py 'pending 8_9_2025 - Sheet1 (1).csv'")
        return

    csv_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    if not os.path.exists(csv_file):
        print(f"Error: File {csv_file} not found")
        return

    print("🚀 Starting address format processing...")

    handler = AddressFormatHandler()
    result_file = handler.process_csv_for_address_extraction(csv_file, output_file)

    print(f"✅ Processing completed: {result_file}")

if __name__ == "__main__":
    main()
