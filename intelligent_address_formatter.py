#!/usr/bin/env python3
"""
AI Address Formatter - Intelligent BCPA Address Formatting
Uses DeepSeek AI with comprehensive instructions for one-time analysis and formula-based processing

Author: AI-Generated for Blake GUI system
Date: August 18, 2025
"""

import pandas as pd
import requests
import json
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
import sys

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from config import DEEPSEEK_API_KEY, DEEPSEEK_API_URL, AI_FORMATTER_MODEL, AI_FORMATTER_MAX_TOKENS, AI_FORMATTER_TEMPERATURE
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False

# Setup logging
log_folder = Path('logs')
log_folder.mkdir(exist_ok=True)
log_file = log_folder / f'ai_address_formatter_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class IntelligentAddressFormatter:
    """AI-powered address formatter using instruction-based analysis"""

    def __init__(self, api_key: Optional[str] = None):
        # Use config file API key if available and no key provided
        if api_key is None and CONFIG_AVAILABLE:
            api_key = DEEPSEEK_API_KEY
            logger.info("🔑 Using API key from config file")
        elif api_key is None:
            raise ValueError("API key must be provided either as parameter or in config.py")

        self.api_key = api_key
        self.api_url = DEEPSEEK_API_URL if CONFIG_AVAILABLE else "https://api.deepseek.com/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        # Load AI instructions
        self.instructions = self._load_instructions()

        # Broward County cities for validation (expanded list)
        self.broward_cities = {
            'HOLLYWOOD', 'FORT LAUDERDALE', 'PEMBROKE PINES', 'CORAL SPRINGS',
            'MIRAMAR', 'SUNRISE', 'PLANTATION', 'DAVIE', 'WESTON', 'MARGATE',
            'TAMARAC', 'COCONUT CREEK', 'POMPANO BEACH', 'LAUDERHILL',
            'LAUDERDALE LAKES', 'WILTON MANORS', 'OAKLAND PARK', 'HALLANDALE BEACH',
            'COOPER CITY', 'DEERFIELD BEACH', 'LIGHTHOUSE POINT', 'NORTH LAUDERDALE',
            'PARKLAND', 'SEA RANCH LAKES', 'SOUTHWEST RANCHES', 'WEST PARK',
            'HILLSBORO BEACH', 'LAZY LAKE', 'PEMBROKE PARK', 'HIGHLAND BEACH',
            'DANIA BEACH', 'DANIA', 'LAUDERDALE BY THE SEA', 'FORT LAUDERDALE BEACH'
        }

    def _load_instructions(self) -> str:
        """Load the AI instruction manual"""
        try:
            instructions_file = Path(__file__).parent / "AI_ADDRESS_FORMATTER_INSTRUCTIONS.md"
            if instructions_file.exists():
                return instructions_file.read_text(encoding='utf-8')
            else:
                logger.warning("AI instructions file not found, using basic instructions")
                return "You are an address formatter. Analyze CSV structure and create BCPA format addresses."
        except Exception as e:
            logger.error(f"Error loading instructions: {e}")
            return "Basic address formatting instructions."

    def _clean_and_format_bcpa_address(self, address: str, city: str) -> str:
        """Clean address and create standardized BCPA format"""
        import re

        if not address or not city:
            return ""

        # Clean the address
        address = str(address).strip()
        city = str(city).strip().upper()

        # Remove extra whitespace and normalize
        address = re.sub(r'\s+', ' ', address)

        # Remove state abbreviations and ZIP codes from address
        address = re.sub(r',?\s*(FL|Florida)\s*\d{5}[-\d]*.*$', '', address, flags=re.IGNORECASE)
        address = re.sub(r'\s+(FL|Florida)\s*\d{5}[-\d]*.*$', '', address, flags=re.IGNORECASE)

        # Remove ZIP codes without state
        address = re.sub(r',?\s*\d{5}[-\d]*\s*$', '', address)

        # Remove city duplication if city is already in address
        city_pattern = rf',?\s*{re.escape(city)}\s*,?\s*$'
        address = re.sub(city_pattern, '', address, flags=re.IGNORECASE)

        # Remove any remaining trailing commas and spaces
        address = re.sub(r',\s*$', '', address).strip()

        # Validate the city is in Broward County
        if city not in self.broward_cities:
            return ""

        # Create final BCPA format: "Street Address, CITY"
        bcpa_format = f"{address}, {city}"

        # Final validation - ensure format is clean
        if len(address) < 5 or len(city) < 3:  # Basic length checks
            return ""

        # Check for obvious formatting issues
        if address.count(',') > 2 or bcpa_format.count(city) > 1:
            return ""

        return bcpa_format

    def analyze_csv_structure(self, df: pd.DataFrame) -> Dict:
        """Single AI analysis to understand CSV structure and create extraction formula"""
        try:
            # Get structure info
            columns = list(df.columns)
            sample_data = df.head(3).to_dict('records')

            # Create analysis prompt with loaded instructions
            prompt = f"""
{self.instructions}

Now analyze this specific CSV file:

COLUMNS: {columns}

SAMPLE DATA (first 3 rows):
{json.dumps(sample_data, indent=2, default=str)}

Provide your analysis following the exact JSON format specified in the instructions.
Focus on creating a formula that will work for ALL {len(df)} rows in this file.
"""

            # Call AI for structure analysis
            response = self.call_deepseek_api(prompt, max_tokens=1500)

            if response:
                try:
                    # Try to extract JSON from response
                    json_start = response.find('{')
                    json_end = response.rfind('}') + 1
                    if json_start >= 0 and json_end > json_start:
                        json_text = response[json_start:json_end]
                        analysis = json.loads(json_text)
                        logger.info("✅ AI structure analysis completed successfully")
                        return analysis
                    else:
                        logger.error("No valid JSON found in AI response")
                        return self._create_fallback_analysis(df)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse AI response: {e}")
                    logger.debug(f"AI Response: {response[:500]}...")
                    return self._create_fallback_analysis(df)
            else:
                logger.warning("AI analysis failed, using fallback")
                return self._create_fallback_analysis(df)

        except Exception as e:
            logger.error(f"Error in AI analysis: {e}")
            return self._create_fallback_analysis(df)

    def call_deepseek_api(self, prompt: str, max_tokens: int = 1000) -> Optional[str]:
        """Call DeepSeek API with the given prompt"""
        try:
            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert address formatter. Always respond with valid JSON following the exact format specified in the instructions."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": max_tokens,
                "temperature": 0.1,
                "stream": False
            }

            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content']
            else:
                logger.error(f"DeepSeek API error: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            logger.error(f"Error calling DeepSeek API: {e}")
            return None

    def _create_fallback_analysis(self, df: pd.DataFrame) -> Dict:
        """Create fallback analysis when AI fails"""
        columns = [str(col).lower().strip() for col in df.columns]

        # Detect separated components format (like your Hallandale Beach file)
        has_house_number = any('house' in col and 'number' in col for col in columns)
        has_street_name = any('street' in col and 'name' in col for col in columns)
        has_city_name = any('city' in col and 'name' in col for col in columns)

        if has_house_number and has_street_name and has_city_name:
            # This looks like separated format
            return {
                "analysis": {
                    "format_type": "separated",
                    "confidence": "medium",
                    "columns_detected": {
                        "house_number": "House Number",
                        "prefix_direction": "Prefix Direction",
                        "street_name": "Street Name",
                        "street_type": "Street Type",
                        "post_direction": "Post Direction",
                        "city": "City Name"
                    }
                },
                "extraction_formula": {
                    "method": "separated_components",
                    "address_pattern": "Concatenate house number, prefix direction, street name, street type, post direction",
                    "city_source": "City Name column",
                    "validation_notes": "Fallback analysis - detected separated format"
                }
            }
        else:
            # Unknown format
            return {
                "analysis": {
                    "format_type": "unknown",
                    "confidence": "low",
                    "columns_detected": {}
                },
                "extraction_formula": {
                    "method": "best_guess",
                    "address_pattern": "Try to find address-like data",
                    "city_source": "unknown",
                    "validation_notes": "Fallback analysis - could not determine format"
                }
            }

    def apply_extraction_formula(self, df: pd.DataFrame, analysis: Dict) -> pd.DataFrame:
        """Apply the AI-created formula to all rows in the dataframe"""
        try:
            logger.info("🚀 Applying extraction formula to all rows...")

            result_df = df.copy()
            result_df['BCPA_Search_Format'] = ''
            result_df['BCPA_City'] = ''
            result_df['BCPA_Valid'] = False

            # Get the extraction method
            formula = analysis.get('extraction_formula', {})
            method = formula.get('method', 'best_guess')
            columns_detected = analysis.get('analysis', {}).get('columns_detected', {})

            logger.info(f"📋 Using method: {method}")
            logger.info(f"📋 Column mapping: {columns_detected}")

            valid_count = 0

            if method == "separated_components":
                # Handle separated address components (like Hallandale Beach file)
                for i, row in df.iterrows():
                    try:
                        address_parts = []

                        # Build address from components
                        component_order = ['house_number', 'prefix_direction', 'street_name', 'street_type', 'post_direction']

                        for component in component_order:
                            col_name = columns_detected.get(component)
                            if col_name and col_name in row.index:
                                value = row.get(col_name)
                                if pd.notna(value) and str(value).strip():
                                    address_parts.append(str(value).strip())

                        street_address = " ".join(address_parts)

                        # Get city
                        city_col = columns_detected.get('city')
                        city = ""
                        if city_col and city_col in row.index:
                            city_value = row.get(city_col)
                            if pd.notna(city_value):
                                city = str(city_value).strip().upper()

                        # Use new cleaning and formatting method
                        bcpa_format = self._clean_and_format_bcpa_address(street_address, city)
                        if bcpa_format:
                            result_df.at[i, 'BCPA_Search_Format'] = bcpa_format
                            result_df.at[i, 'BCPA_City'] = city
                            result_df.at[i, 'BCPA_Valid'] = True
                            valid_count += 1

                    except Exception as e:
                        logger.debug(f"Error processing row {i}: {e}")
                        continue

            elif method == "parse_combined":
                # Handle combined address format
                combined_col = columns_detected.get('combined_address')
                city_col = columns_detected.get('city')

                for i, row in df.iterrows():
                    try:
                        # Get combined address
                        address = ""
                        if combined_col and combined_col in row.index:
                            addr_value = row.get(combined_col)
                            if pd.notna(addr_value):
                                address = str(addr_value).strip()

                        # Get city
                        city = ""
                        if city_col and city_col in row.index:
                            city_value = row.get(city_col)
                            if pd.notna(city_value):
                                city = str(city_value).strip().upper()

                        # Use new cleaning and formatting method
                        bcpa_format = self._clean_and_format_bcpa_address(address, city)
                        if bcpa_format:
                            result_df.at[i, 'BCPA_Search_Format'] = bcpa_format
                            result_df.at[i, 'BCPA_City'] = city
                            result_df.at[i, 'BCPA_Valid'] = True
                            valid_count += 1

                    except Exception as e:
                        logger.debug(f"Error processing row {i}: {e}")
                        continue

            else:
                logger.warning(f"Unknown method: {method}, attempting best guess")
                # Best guess fallback
                for i, row in df.iterrows():
                    try:
                        # Try to find any address-like and city-like data
                        address_candidates = []
                        city_candidates = []

                        for col, value in row.items():
                            if pd.notna(value) and str(value).strip():
                                str_val = str(value).strip()

                                # Check if it looks like an address (has numbers and street words)
                                if any(char.isdigit() for char in str_val) and any(word in str_val.upper() for word in ['ST', 'AVE', 'RD', 'DR', 'BLVD', 'STREET', 'AVENUE', 'ROAD', 'DRIVE']):
                                    address_candidates.append(str_val)

                                # Check if it's a Broward city
                                elif str_val.upper() in self.broward_cities:
                                    city_candidates.append(str_val.upper())

                        # Use first valid combination with new cleaning method
                        if address_candidates and city_candidates:
                            address = address_candidates[0]
                            city = city_candidates[0]
                            bcpa_format = self._clean_and_format_bcpa_address(address, city)
                            if bcpa_format:
                                result_df.at[i, 'BCPA_Search_Format'] = bcpa_format
                                result_df.at[i, 'BCPA_City'] = city
                                result_df.at[i, 'BCPA_Valid'] = True
                                valid_count += 1

                    except Exception as e:
                        logger.debug(f"Error processing row {i}: {e}")
                        continue

            logger.info(f"✅ Formula applied successfully: {valid_count}/{len(df)} valid addresses")
            return result_df

        except Exception as e:
            logger.error(f"Error applying extraction formula: {e}")
            return df

    def process_file(self, input_path: str, output_path: Optional[str] = None) -> str:
        """Process CSV file with AI-guided address formatting"""
        try:
            logger.info("=" * 70)
            logger.info("🤖 INTELLIGENT AI ADDRESS FORMATTER")
            logger.info("=" * 70)

            # Read file
            logger.info(f"📄 Reading file: {input_path}")
            if input_path.endswith('.csv'):
                df = pd.read_csv(input_path)
            elif input_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(input_path)
            else:
                raise ValueError("Unsupported file format")

            logger.info(f"✅ Loaded {len(df)} records with {len(df.columns)} columns")

            # Single AI analysis
            logger.info("🧠 Running AI structure analysis...")
            analysis = self.analyze_csv_structure(df)

            # Log analysis results
            format_type = analysis.get('analysis', {}).get('format_type', 'unknown')
            confidence = analysis.get('analysis', {}).get('confidence', 'unknown')
            method = analysis.get('extraction_formula', {}).get('method', 'unknown')

            logger.info(f"📊 Analysis Results:")
            logger.info(f"  Format Type: {format_type}")
            logger.info(f"  Confidence: {confidence}")
            logger.info(f"  Method: {method}")

            # Apply formula to all rows
            result_df = self.apply_extraction_formula(df, analysis)

            # Set output path
            if output_path is None:
                input_file = Path(input_path)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path = str(input_file.parent / f"ai_formatted_{timestamp}.csv")

            # Save result
            result_df.to_csv(output_path, index=False)

            # Final statistics
            if 'BCPA_Valid' in result_df.columns:
                valid_count = result_df['BCPA_Valid'].sum()
                success_rate = (valid_count / len(result_df)) * 100
            else:
                valid_count = 0
                success_rate = 0

            logger.info("=" * 70)
            logger.info("✅ AI ADDRESS FORMATTING COMPLETED")
            logger.info(f"📊 Total records: {len(df)}")
            logger.info(f"📊 Valid BCPA addresses: {valid_count}")
            logger.info(f"📊 Success rate: {success_rate:.1f}%")
            logger.info(f"📄 Output file: {output_path}")
            logger.info("=" * 70)

            return output_path

        except Exception as e:
            logger.error(f"Error processing file: {e}")
            raise

def main():
    """CLI interface"""
    import argparse

    parser = argparse.ArgumentParser(description="Intelligent AI address formatter")
    parser.add_argument('input_file', help='Input CSV/Excel file')
    parser.add_argument('--output', help='Output CSV file path')
    parser.add_argument('--api-key', help='DeepSeek API key (optional if config.py exists)',
                        default=os.getenv('DEEPSEEK_API_KEY'))

    args = parser.parse_args()

    # Try to get API key from multiple sources
    api_key = args.api_key
    if not api_key and CONFIG_AVAILABLE:
        api_key = DEEPSEEK_API_KEY

    if not api_key:
        print("❌ Error: DeepSeek API key required. Use one of:")
        print("   1. --api-key parameter")
        print("   2. DEEPSEEK_API_KEY environment variable")
        print("   3. config.py file with DEEPSEEK_API_KEY")
        return

    try:
        formatter = IntelligentAddressFormatter(api_key)
        output_file = formatter.process_file(args.input_file, args.output)
        print(f"✅ Success! Formatted file saved to: {output_file}")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
