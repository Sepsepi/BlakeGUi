#!/usr/bin/env python3
"""
Enhanced Address Parser for BCPA Integration
Handles multiple address formats and standardizes them for BCPA searches

Supported formats:
1. Separated columns: House Number, Street Name, Street Type, City, State, Zip
2. Combined address strings: "1234 Main St, City, State 12345"
3. Partial addresses: "1234 Main St" + separate City field
4. Mixed formats within the same file
"""

import pandas as pd
import re
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class EnhancedAddressParser:
    """Enhanced address parsing for multiple formats"""
    
    def __init__(self):
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
            'POINT': 'PT', 'PT': 'PT',
            'WAY': 'WAY',
            'TRAIL': 'TRL', 'TRL': 'TRL',
            'PARKWAY': 'PKWY', 'PKWY': 'PKWY'
        }
        
        self.directions = ['N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW', 
                          'NORTH', 'SOUTH', 'EAST', 'WEST', 
                          'NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST']
    
    def detect_address_format(self, df: pd.DataFrame) -> str:
        """Detect the address format in the dataframe"""
        columns = [col.lower().strip() for col in df.columns]
        
        # Check for separated address format
        separated_indicators = ['house number', 'street name', 'street type', 'city name']
        has_separated = all(any(indicator in col for col in columns) for indicator in separated_indicators)
        
        if has_separated:
            return 'separated'
        
        # Check for combined address format
        combined_indicators = ['address', 'directname_address', 'full_address', 'street_address']
        has_combined = any(any(indicator in col for col in columns) for indicator in combined_indicators)
        
        if has_combined:
            return 'combined'
        
        # Check for mixed or unknown format
        return 'mixed'
    
    def parse_separated_address(self, row: pd.Series) -> Dict[str, str]:
        """Parse address from separated columns"""
        try:
            # Map common column variations
            column_mapping = {
                'house_number': ['house number', 'house_number', 'number', 'housenumber'],
                'prefix_direction': ['prefix direction', 'prefix_direction', 'pre_dir', 'predir'],
                'street_name': ['street name', 'street_name', 'streetname', 'street'],
                'street_type': ['street type', 'street_type', 'streettype', 'type'],
                'post_direction': ['post direction', 'post_direction', 'post_dir', 'postdir'],
                'unit_number': ['unit number', 'unit_number', 'unit', 'apt', 'apartment'],
                'city': ['city name', 'city_name', 'city', 'cityname'],
                'state': ['state abbreviation', 'state_abbreviation', 'state', 'st'],
                'zip_code': ['zip code', 'zip_code', 'zip', 'zipcode', 'postal_code']
            }
            
            # Extract values using flexible column matching
            address_parts = {}
            row_lower = {str(k).lower().strip(): v for k, v in row.items()}
            
            for field, possible_columns in column_mapping.items():
                value = None
                for col_name in possible_columns:
                    if col_name in row_lower:
                        raw_value = row_lower[col_name]
                        # Handle NaN, None, empty strings properly
                        if pd.isna(raw_value) or raw_value is None:
                            continue
                        value = str(raw_value).strip()
                        if value and value.lower() not in ['nan', 'none', '', 'null']:
                            break
                
                address_parts[field] = value or ''
            
            # Build the street address
            street_parts = []
            
            # House number
            if address_parts['house_number']:
                street_parts.append(address_parts['house_number'])
            
            # Prefix direction
            if address_parts['prefix_direction']:
                street_parts.append(address_parts['prefix_direction'].upper())
            
            # Street name
            if address_parts['street_name']:
                # Handle numeric street names (like "33RD")
                street_name = address_parts['street_name'].upper()
                street_parts.append(street_name)
            
            # Street type
            if address_parts['street_type']:
                street_type = address_parts['street_type'].upper()
                # Standardize street type
                street_type = self.street_types.get(street_type, street_type)
                street_parts.append(street_type)
            
            # Post direction
            if address_parts['post_direction']:
                street_parts.append(address_parts['post_direction'].upper())
            
            # Unit number (only if valid)
            if address_parts['unit_number'] and address_parts['unit_number'].lower() not in ['nan', 'none']:
                street_parts.append(f"#{address_parts['unit_number']}")
            
            street_address = " ".join(street_parts)
            city = address_parts['city'].upper() if address_parts['city'] else ''
            state = address_parts['state'].upper() if address_parts['state'] else 'FL'
            zip_code = address_parts['zip_code'] if address_parts['zip_code'] else ''
            
            return {
                'street_address': street_address,
                'city': city,
                'state': state,
                'zip_code': zip_code,
                'bcpa_search_format': f"{street_address}, {city}" if street_address and city else ""
            }
            
        except Exception as e:
            logger.error(f"Error parsing separated address: {e}")
            return {'street_address': '', 'city': '', 'state': '', 'zip_code': '', 'bcpa_search_format': ''}
    
    def parse_combined_address(self, address_string: str) -> Dict[str, str]:
        """Parse address from combined string format"""
        try:
            if not address_string or str(address_string).lower() in ['nan', 'none', '', 'null']:
                return {'street_address': '', 'city': '', 'state': '', 'zip_code': '', 'bcpa_search_format': ''}
            
            address_string = str(address_string).strip()
            
            # Handle formats like "10310 WATERSIDE CT, PARKLAND, FL, 33076"
            parts = [part.strip() for part in address_string.split(',')]
            
            if len(parts) >= 2:
                street_address = parts[0].strip()
                city = parts[1].strip().upper()
                
                state = 'FL'  # Default for Florida
                zip_code = ''
                
                # Extract state and zip from remaining parts
                for part in parts[2:]:
                    part = part.strip()
                    # Check if it's a state
                    if len(part) == 2 and part.isalpha():
                        state = part.upper()
                    # Check if it's a zip code
                    elif re.match(r'^\d{5}(-\d{4})?$', part):
                        zip_code = part
                    # Handle "FL 33076" format
                    elif ' ' in part:
                        sub_parts = part.split()
                        for sub_part in sub_parts:
                            if len(sub_part) == 2 and sub_part.isalpha():
                                state = sub_part.upper()
                            elif re.match(r'^\d{5}(-\d{4})?$', sub_part):
                                zip_code = sub_part
                
                return {
                    'street_address': street_address,
                    'city': city,
                    'state': state,
                    'zip_code': zip_code,
                    'bcpa_search_format': f"{street_address}, {city}"
                }
            
            # Handle single string without commas
            # Try to extract city from the end
            words = address_string.upper().split()
            if len(words) >= 3:
                # Look for known Florida cities at the end
                florida_cities = ['HOLLYWOOD', 'PARKLAND', 'FORT LAUDERDALE', 'MIAMI', 'PEMBROKE PINES', 
                                'CORAL SPRINGS', 'DAVIE', 'PLANTATION', 'SUNRISE', 'WESTON']
                
                for city in florida_cities:
                    city_words = city.split()
                    if len(words) >= len(city_words):
                        if words[-len(city_words):] == city_words:
                            street_address = " ".join(words[:-len(city_words)]).strip()
                            return {
                                'street_address': street_address,
                                'city': city,
                                'state': 'FL',
                                'zip_code': '',
                                'bcpa_search_format': f"{street_address}, {city}"
                            }
            
            # Fallback: treat entire string as street address
            return {
                'street_address': address_string,
                'city': '',
                'state': 'FL',
                'zip_code': '',
                'bcpa_search_format': address_string
            }
            
        except Exception as e:
            logger.error(f"Error parsing combined address: {e}")
            return {'street_address': '', 'city': '', 'state': '', 'zip_code': '', 'bcpa_search_format': ''}
    
    def standardize_addresses_for_bcpa(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize all addresses in the dataframe for BCPA searches"""
        try:
            logger.info("🏠 Standardizing addresses for BCPA search...")
            
            # Make a copy to avoid modifying the original
            result_df = df.copy()
            
            # Detect the address format
            format_type = self.detect_address_format(df)
            logger.info(f"📍 Detected address format: {format_type}")
            
            # Initialize new columns
            result_df['BCPA_Street_Address'] = ''
            result_df['BCPA_City'] = ''
            result_df['BCPA_State'] = ''
            result_df['BCPA_Zip'] = ''
            result_df['BCPA_Search_Format'] = ''
            
            if format_type == 'separated':
                # Process separated format
                for index, row in result_df.iterrows():
                    parsed = self.parse_separated_address(row)
                    result_df.at[index, 'BCPA_Street_Address'] = parsed['street_address']
                    result_df.at[index, 'BCPA_City'] = parsed['city']
                    result_df.at[index, 'BCPA_State'] = parsed['state']
                    result_df.at[index, 'BCPA_Zip'] = parsed['zip_code']
                    result_df.at[index, 'BCPA_Search_Format'] = parsed['bcpa_search_format']
            
            elif format_type == 'combined':
                # Find the address column
                address_columns = ['DirectName_Address', 'address', 'Address', 'full_address', 'street_address']
                address_col = None
                
                for col in address_columns:
                    if col in df.columns:
                        address_col = col
                        break
                
                if address_col:
                    logger.info(f"📍 Using combined address column: {address_col}")
                    for index, row in result_df.iterrows():
                        address_string = row[address_col]
                        parsed = self.parse_combined_address(address_string)
                        result_df.at[index, 'BCPA_Street_Address'] = parsed['street_address']
                        result_df.at[index, 'BCPA_City'] = parsed['city']
                        result_df.at[index, 'BCPA_State'] = parsed['state']
                        result_df.at[index, 'BCPA_Zip'] = parsed['zip_code']
                        result_df.at[index, 'BCPA_Search_Format'] = parsed['bcpa_search_format']
            
            else:
                # Mixed format - try both approaches
                logger.info("📍 Processing mixed format - trying multiple approaches")
                
                for index, row in result_df.iterrows():
                    parsed = None
                    
                    # First try separated format
                    separated_parsed = self.parse_separated_address(row)
                    if separated_parsed['bcpa_search_format']:
                        parsed = separated_parsed
                    else:
                        # Try to find any address-like column
                        for col_name, value in row.items():
                            col_name_str = str(col_name)
                            if ('address' in col_name_str.lower() or 'street' in col_name_str.lower()) and value:
                                combined_parsed = self.parse_combined_address(value)
                                if combined_parsed['bcpa_search_format']:
                                    parsed = combined_parsed
                                    break
                    
                    if parsed:
                        result_df.at[index, 'BCPA_Street_Address'] = parsed['street_address']
                        result_df.at[index, 'BCPA_City'] = parsed['city']
                        result_df.at[index, 'BCPA_State'] = parsed['state']
                        result_df.at[index, 'BCPA_Zip'] = parsed['zip_code']
                        result_df.at[index, 'BCPA_Search_Format'] = parsed['bcpa_search_format']
            
            # Count successful parsing
            valid_addresses = result_df['BCPA_Search_Format'].str.len() > 0
            success_count = valid_addresses.sum()
            total_count = len(result_df)
            
            logger.info(f"✅ Successfully parsed {success_count}/{total_count} addresses ({(success_count/total_count)*100:.1f}%)")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error standardizing addresses: {e}")
            return df

def process_file_for_bcpa(input_path: str) -> pd.DataFrame:
    """
    Process any file format and prepare it for BCPA searches
    
    Args:
        input_path: Path to input file (CSV or Excel)
        
    Returns:
        pd.DataFrame: Processed dataframe with standardized addresses
    """
    try:
        # Read the file
        if input_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(input_path)
        else:
            # Try different encodings for CSV
            encodings = ['utf-8', 'latin1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(input_path, encoding=encoding)
                    logger.info(f"✅ Successfully read CSV with {encoding} encoding")
                    break
                except:
                    continue
            
            if df is None:
                raise ValueError("Could not read CSV file with any standard encoding")
        
        logger.info(f"📄 Loaded {len(df)} records from {input_path}")
        
        # Initialize the address parser
        parser = EnhancedAddressParser()
        
        # Standardize addresses
        processed_df = parser.standardize_addresses_for_bcpa(df)
        
        return processed_df
        
    except Exception as e:
        logger.error(f"Error processing file for BCPA: {e}")
        raise

if __name__ == "__main__":
    # Test the address parser
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python enhanced_address_parser.py <input_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        result_df = process_file_for_bcpa(input_file)
        
        # Save result
        output_file = input_file.replace('.csv', '_bcpa_ready.csv').replace('.xlsx', '_bcpa_ready.csv')
        result_df.to_csv(output_file, index=False)
        
        print(f"✅ Processed file saved to: {output_file}")
        print(f"📊 Total records: {len(result_df)}")
        print(f"📍 Valid addresses: {(result_df['BCPA_Search_Format'].str.len() > 0).sum()}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
