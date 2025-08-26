"""
Unified Address Format Handler for BCPA Integration
Uses the superior formatting logic from bcpa_flask_integration.py
Handles multiple CSV formats and standardizes them for BCPA searches
"""

import pandas as pd
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class UnifiedAddressFormatter:
    """
    Unified address formatter using the superior logic from bcpa_flask_integration.py
    Supports multiple CSV formats and optimizes addresses for BCPA searches
    """

    @staticmethod
    def detect_csv_format(df):
        """
        Detect the CSV format based on column names and structure
        Uses the proven logic from bcpa_flask_integration.py
        """
        columns = df.columns.tolist()
        logger.info(f"Analyzing columns: {columns}")

        # Format 1: Structured format with separate address components
        # ["Owner Name 1","Owner Name 2","House Number","Prefix Direction","Street Name","Street Type",...]
        structured_cols = ["House Number", "Street Name", "Street Type", "City Name"]
        if all(col in columns for col in structured_cols):
            logger.info("Detected structured address format")
            return "structured"

        # Format 2: Positional format (pending CSV style) - Check this FIRST before simple
        # This handles cases where column headers are actually data (no proper headers)
        if len(columns) >= 5:
            # Check if this looks like our pending CSV format by examining the data
            col3_sample = [str(df.iloc[i, 3]) for i in range(min(3, len(df))) if i < len(df)]
            col4_sample = [str(df.iloc[i, 4]) for i in range(min(3, len(df))) if i < len(df)]

            # Check if column 3 looks like street addresses and column 4 looks like cities
            has_street_indicators = any(any(word in str(val).lower() for word in ['drive', 'street', 'road', 'avenue', 'way', 'circle', 'dr', 'st', 'rd', 'ave', 'boulevard', 'blvd']) for val in col3_sample if pd.notna(val))
            has_numbers = any(any(char.isdigit() for char in str(val)) for val in col3_sample if pd.notna(val))

            # Also check if column headers look like data instead of headers
            headers_look_like_data = any(any(word in str(col).lower() for word in ['drive', 'street', 'road', 'avenue', 'way', 'circle']) for col in columns)
            has_mls_pattern = any(re.match(r'^[A-Z]\d{8}', str(col)) for col in columns)
            has_price_pattern = any('$' in str(col) for col in columns)

            if (has_street_indicators and has_numbers and len(col4_sample) > 0) or headers_look_like_data or has_mls_pattern or has_price_pattern:
                logger.info("Detected positional address format (pending CSV style)")
                return "positional"

        # Format 3: Simple format with address in single column
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
        """
        Format address from structured CSV format (Hollywood/Broward files)
        Uses the superior logic from bcpa_flask_integration.py
        """
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
        """
        Format address from simple CSV format (pending file)
        Uses the superior logic from bcpa_flask_integration.py
        """
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
    def format_positional_address(row):
        """
        Format address from positional CSV format (pending listings)
        Column 3 = Street Address, Column 4 = City
        """
        try:
            street_address = ''
            city = ''

            # Get street address from column 3 (index 3)
            if len(row) > 3:
                addr_val = row.iloc[3] if hasattr(row, 'iloc') else list(row.values())[3]
                if pd.notna(addr_val):
                    street_address = UnifiedAddressFormatter.format_simple_address(str(addr_val))

            # Get city from column 4 (index 4)
            if len(row) > 4:
                city_val = row.iloc[4] if hasattr(row, 'iloc') else list(row.values())[4]
                if pd.notna(city_val):
                    city = str(city_val).upper().strip()

            # Create BCPA search format
            if street_address and city:
                return f"{street_address}, {city}"
            elif street_address:
                return street_address
            else:
                return None

        except Exception as e:
            logger.error(f"Error formatting positional address: {e}")
            return None

    @staticmethod
    def extract_addresses_from_csv(df):
        """
        Extract and format addresses from a DataFrame
        Uses the superior logic from bcpa_flask_integration.py
        """
        csv_format = UnifiedAddressFormatter.detect_csv_format(df)
        addresses = []

        if csv_format == "structured":
            logger.info("Processing structured format...")
            for idx, row in df.iterrows():
                formatted_addr = UnifiedAddressFormatter.format_structured_address(row)
                # Add city to create complete BCPA format
                if formatted_addr:
                    city = str(row.get("City Name", "")).strip()
                    if city and city not in ["nan", "None", ""]:
                        formatted_addr = f"{formatted_addr}, {city.upper()}"
                addresses.append(formatted_addr)

        elif csv_format == "simple":
            logger.info("Processing simple format...")
            # Address is typically in the 4th column (index 3)
            address_col = df.columns[3]
            for idx, row in df.iterrows():
                formatted_addr = UnifiedAddressFormatter.format_simple_address(row[address_col])
                addresses.append(formatted_addr)

        elif csv_format == "positional":
            logger.info("Processing positional format...")
            for idx, row in df.iterrows():
                formatted_addr = UnifiedAddressFormatter.format_positional_address(row)
                addresses.append(formatted_addr)

        else:
            logger.error(f"Cannot process unknown CSV format")
            return None, None

        return addresses, csv_format

    @staticmethod
    def is_broward_county(city: str) -> bool:
        """
        Check if a city is in Broward County
        Enhanced geographic validation
        """
        if not city:
            return False

        city_clean = city.upper().strip()

        # Broward County cities
        broward_cities = {
            'HOLLYWOOD', 'FORT LAUDERDALE', 'PEMBROKE PINES', 'CORAL SPRINGS',
            'MIRAMAR', 'SUNRISE', 'PLANTATION', 'DAVIE', 'WESTON', 'MARGATE',
            'TAMARAC', 'COCONUT CREEK', 'POMPANO BEACH', 'LAUDERHILL',
            'LAUDERDALE LAKES', 'WILTON MANORS', 'OAKLAND PARK', 'HALLANDALE BEACH',
            'COOPER CITY', 'DEERFIELD BEACH', 'LIGHTHOUSE POINT', 'NORTH LAUDERDALE',
            'PARKLAND', 'SEA RANCH LAKES', 'SOUTHWEST RANCHES', 'WEST PARK',
            'HILLSBORO BEACH', 'LAZY LAKE', 'PEMBROKE PARK', 'HIGHLAND BEACH',
            'HOLLYWOOD BEACH', 'FORT LAUDERDALE BEACH', 'LAUDERDALE BY THE SEA'
        }

        # Remove common suffixes that might interfere
        suffixes_to_remove = ['BEACH', 'CITY', 'LAKES', 'PARK', 'GARDENS']
        city_base = city_clean
        for suffix in suffixes_to_remove:
            if city_clean.endswith(f' {suffix}'):
                city_base = city_clean.replace(f' {suffix}', '')
                break

        # Check both full name and base name
        if city_clean in broward_cities or city_base in broward_cities:
            return True

        # Common Broward area indicators
        broward_indicators = [
            'LAUDERDALE', 'HOLLYWOOD', 'PEMBROKE', 'CORAL', 'COCONUT',
            'PLANTATION', 'WESTON', 'MARGATE', 'SUNRISE', 'DAVIE'
        ]

        return any(indicator in city_clean for indicator in broward_indicators)

    @staticmethod
    def validate_and_filter_addresses(addresses, cities=None):
        """
        Validate addresses and filter for Broward County only
        Returns filtered addresses with geographic validation
        """
        valid_addresses = []
        filtered_addresses = []

        for i, address in enumerate(addresses):
            if not address or pd.isna(address) or str(address).strip() == "":
                valid_addresses.append(None)
                filtered_addresses.append(None)
                continue

            address_str = str(address).strip()
            valid_addresses.append(address_str)

            # Extract city from address or use provided cities
            city = None
            if cities and len(cities) > i and cities[i]:
                city = str(cities[i]).strip()
            elif ',' in address_str:
                city = address_str.split(',')[-1].strip()

            # Check if in Broward County
            if city and UnifiedAddressFormatter.is_broward_county(city):
                filtered_addresses.append(address_str)
                logger.info(f"✅ Valid Broward address: {address_str}")
            else:
                filtered_addresses.append(None)
                if city:
                    logger.warning(f"⚠️ Skipping non-Broward address: {address_str} (City: {city})")
                else:
                    logger.warning(f"⚠️ Skipping address without city: {address_str}")

        return valid_addresses, filtered_addresses


class BCPAIntegratedProcessor:
    """
    Integrated processor that combines the best of all BCPA scripts
    Uses UnifiedAddressFormatter + BCPA web scraping
    """

    def __init__(self):
        self.address_formatter = UnifiedAddressFormatter()

    def process_file_for_bcpa_lookup(self, input_file_path, max_records=25, broward_only=True):
        """
        Process file and prepare for BCPA owner lookup

        Args:
            input_file_path: Path to input CSV file
            max_records: Maximum records to process (0 = no limit)
            broward_only: Filter for Broward County addresses only

        Returns:
            tuple: (success: bool, processed_df: DataFrame, summary: dict)
        """
        try:
            logger.info(f"🏠 Processing file for BCPA lookup: {input_file_path}")

            # Read the CSV file
            df = pd.read_csv(input_file_path)
            logger.info(f"📄 Loaded {len(df)} records")

            if len(df) == 0:
                return False, None, {"error": "Empty CSV file"}

            # Limit records if specified
            if max_records > 0 and len(df) > max_records:
                df = df.head(max_records)
                logger.info(f"📝 Limited to {max_records} records for processing")

            # Extract addresses using unified formatter
            addresses, csv_format = self.address_formatter.extract_addresses_from_csv(df)

            if addresses is None:
                return False, None, {"error": "Could not parse CSV format"}

            # Get cities for geographic validation
            cities = []
            if csv_format == "structured":
                cities = [str(row.get("City Name", "")).strip() for _, row in df.iterrows()]
            elif csv_format == "positional" and len(df.columns) > 4:
                cities = [str(df.iloc[i, 4]).strip() if len(df) > i else "" for i in range(len(df))]

            # Validate and filter addresses
            valid_addresses, filtered_addresses = self.address_formatter.validate_and_filter_addresses(
                addresses, cities if cities else None
            )

            # Add processed columns to DataFrame
            df['BCPA_Extracted_Address'] = valid_addresses
            df['BCPA_Search_Ready'] = filtered_addresses if broward_only else valid_addresses
            df['CSV_Format_Detected'] = csv_format
            df['Broward_County_Filter'] = broward_only

            # Count valid addresses
            search_ready_count = sum(1 for addr in df['BCPA_Search_Ready'] if addr)

            summary = {
                "total_records": len(df),
                "addresses_extracted": len([a for a in addresses if a]),
                "search_ready_addresses": search_ready_count,
                "csv_format": csv_format,
                "broward_only": broward_only
            }

            logger.info(f"✅ File processed: {search_ready_count} addresses ready for BCPA search")
            return True, df, summary

        except Exception as e:
            logger.error(f"Error processing file for BCPA lookup: {e}")
            return False, None, {"error": str(e)}


def process_csv_for_address_extraction(input_file_path, output_dir="results", broward_only=True):
    """
    Process a CSV file for address extraction using the unified formatter

    Args:
        input_file_path: Path to input CSV file
        output_dir: Directory for output files
        broward_only: If True, filter for Broward County addresses only

    Returns:
        tuple: (success: bool, output_file_path: str, summary: dict)
    """
    try:
        # Use the integrated processor
        processor = BCPAIntegratedProcessor()
        success, processed_df, summary = processor.process_file_for_bcpa_lookup(
            input_file_path, max_records=0, broward_only=broward_only
        )

        if not success:
            return False, None, summary

        # Generate output filename
        input_name = Path(input_file_path).stem
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{timestamp}_{input_name}_address_extracted.csv"
        output_path = Path(output_dir) / output_filename

        # Create output directory
        Path(output_dir).mkdir(exist_ok=True)

        # Add processing metadata
        processed_df['Processing_Date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save results
        processed_df.to_csv(output_path, index=False)

        # Update summary with output path
        summary["output_file"] = str(output_path)

        logger.info(f"Address extraction completed. Output saved to: {output_path}")
        return True, str(output_path), summary

    except Exception as e:
        logger.error(f"Error in address extraction: {e}")
        return False, None, {"error": str(e)}


# Test function
def test_unified_address_system():
    """Test the unified address system"""
    logger.info("🧪 Testing Unified Address System")

    # Test structured format
    test_structured = {
        'House Number': ['5920', '5910'],
        'Prefix Direction': ['SW', 'SW'],
        'Street Name': ['33RD', '33RD'],
        'Street Type': ['AVE', 'AVE'],
        'City Name': ['HOLLYWOOD', 'HOLLYWOOD']
    }

    df_structured = pd.DataFrame(test_structured)
    addresses, format_type = UnifiedAddressFormatter.extract_addresses_from_csv(df_structured)
    print(f"✅ Structured test - Format: {format_type}, Addresses: {addresses}")

    # Test simple format
    df_simple = pd.DataFrame([
        ['R11112092', 'Pending', '$725,000', '8117 Florenza Drive', 'Boynton Beach'],
        ['A11838331', 'Pending', '$450,000', '1540 Cordova Rd', 'Fort Lauderdale']
    ])

    addresses, format_type = UnifiedAddressFormatter.extract_addresses_from_csv(df_simple)
    print(f"✅ Simple test - Format: {format_type}, Addresses: {addresses}")

    # Test Broward filtering
    valid_addresses, filtered_addresses = UnifiedAddressFormatter.validate_and_filter_addresses(addresses)
    print(f"✅ Filtered addresses: {filtered_addresses}")

    # Test integrated processor
    processor = BCPAIntegratedProcessor()
    print(f"✅ Integrated processor initialized successfully")


if __name__ == "__main__":
    test_unified_address_system()
