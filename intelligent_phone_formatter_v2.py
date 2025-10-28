#!/usr/bin/env python3
"""
Intelligent Phone Formatter - AI-Powered Input Preparation for ZabaSearch
Simple, AI-driven approach that reads comprehensive instructions and handles any CSV structure

Author: Enhanced for Blake GUI system
Date: August 18, 2025
"""

import os
import sys
import pandas as pd
import logging
import json
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

# Configuration imports
try:
    from config import DEEPSEEK_API_KEY, DEEPSEEK_API_URL, AI_FORMATTER_MODEL, AI_FORMATTER_MAX_TOKENS, AI_FORMATTER_TEMPERATURE
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False
    print("⚠️ config.py not found - using environment variables for API configuration")

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntelligentPhoneFormatter:
    """AI-powered phone data formatter that reads instructions and handles any CSV structure"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.ai_instructions = self._load_ai_instructions()

        # Setup DeepSeek API configuration
        if CONFIG_AVAILABLE:
            self.api_key = DEEPSEEK_API_KEY
            self.model = AI_FORMATTER_MODEL
            self.max_tokens = AI_FORMATTER_MAX_TOKENS
            self.temperature = AI_FORMATTER_TEMPERATURE
        else:
            self.api_key = os.getenv('DEEPSEEK_API_KEY')
            self.model = "deepseek-chat"
            self.max_tokens = 4000
            self.temperature = 0.1

        self.api_url = DEEPSEEK_API_URL if CONFIG_AVAILABLE else "https://api.deepseek.com/v1/chat/completions"

        if not self.api_key:
            self.logger.error("❌ DeepSeek API key not configured")

    def _load_ai_instructions(self) -> str:
        """Load AI instructions from markdown file"""
        instructions_file = project_root / "AI_PHONE_FORMATTER_INSTRUCTIONS.md"
        try:
            with open(instructions_file, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            self.logger.error(f"❌ Could not load AI instructions: {e}")
            return ""

    def call_deepseek_api(self, prompt: str, max_tokens: int = 1000) -> Optional[str]:
        """Call DeepSeek API with the given prompt"""
        try:
            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert data analyst that provides specific extraction formulas for standardizing CSV files."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": max_tokens,
                "temperature": self.temperature
            }

            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }

            # AI API calls use direct connection (no proxy) for cost efficiency
            self.logger.info("📡 Using direct connection for AI API call")
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=60)

            if response.status_code == 200:
                result = response.json()
                return result['choices'][0]['message']['content']
            else:
                self.logger.error(f"DeepSeek API error: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            self.logger.error(f"Error calling DeepSeek API: {e}")
            return None

    def analyze_csv_structure(self, df: pd.DataFrame) -> Dict:
        """Use AI to analyze CSV structure and create processing formula"""
        if not self.api_key or not self.ai_instructions:
            self.logger.error("❌ AI analysis not available")
            return {}

        # Prepare sample data for AI
        sample_data = self._prepare_sample_for_ai(df)

        prompt = f"""
{self.ai_instructions}

Please analyze this CSV structure for phone data preparation:

COLUMNS: {list(df.columns)}

SAMPLE DATA (first 3 rows):
{sample_data}

TOTAL RECORDS: {len(df)}

CRITICAL ANALYSIS REQUIREMENT:
Look at the ACTUAL DATA VALUES, not just column headers. The data might be shifted or misaligned.
For example, if column "House Number" is empty but column "Prefix Direction" contains numbers like "313",
then the data is shifted one column to the right.

PATTERN DETECTION RULES:
1. House numbers are typically 1-5 digit numbers (100, 313, 4523, etc.)
2. Prefix directions are N, S, E, W, NE, NW, SE, SW, NORTH, SOUTH, etc.
3. Street names are words like 4TH, MAIN, OAK, MAPLE, etc.
4. Street types are AVE, ST, BLVD, RD, WAY, DRIVE, LANE, etc.
5. Cities are proper names like HALLANDALE BEACH, MIAMI, NEW YORK, etc.
6. States are full names (Florida) or abbreviations (FL)

Examine the actual data values to determine which column contains which type of data, regardless of the column header names.

Provide your analysis in the exact JSON format specified in the instructions.
"""

        try:
            response = self.call_deepseek_api(prompt, max_tokens=1500)
            if not response:
                return {}

            content = response.strip()

            # Extract JSON from response
            if content.startswith('```json'):
                content = content[7:-3]
            elif content.startswith('```'):
                content = content[3:-3]

            analysis = json.loads(content)

            # Enhance the analysis with actual phone counting
            analysis = self._enhance_analysis_with_actual_counts(df, analysis)

            # Validate required structure
            if "analysis" in analysis and "extraction_formula" in analysis:
                self.logger.info("🤖 AI analysis completed successfully")
                return analysis
            else:
                self.logger.error("❌ AI response missing required structure")
                return {}

        except Exception as e:
            self.logger.error(f"❌ AI analysis failed: {e}")
            return {}

    def _enhance_analysis_with_actual_counts(self, df: pd.DataFrame, analysis: Dict) -> Dict:
        """Enhance AI analysis with actual phone counting from the full dataset"""
        import re
        import pandas as pd

        try:
            # Get detected phone columns from AI analysis
            columns_detected = analysis.get('analysis', {}).get('columns_detected', {})
            existing_phones = columns_detected.get('existing_phones', [])

            # If no phone columns detected, try to find them
            if not existing_phones:
                phone_patterns = ['phone', 'tel', 'telephone', 'cell', 'mobile', 'contact', 'number']
                for col in df.columns:
                    col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                    if any(pattern in col_lower for pattern in phone_patterns):
                        existing_phones.append(col)
                        break

            # Count actual records with phones
            actual_phone_count = 0
            processable_count = 0

            for _, row in df.iterrows():
                has_phone = False

                # Check for phones in detected columns
                for phone_col in existing_phones:
                    if phone_col in row.index:
                        value = row.get(phone_col)
                        # Handle pandas NaN properly
                        if pd.isna(value):
                            continue
                        value = str(value).strip()
                        if value and value.lower() not in ['nan', 'none', '', 'null']:
                            digits_only = re.sub(r'[^\d]', '', value)
                            if len(digits_only) >= 10:
                                has_phone = True
                                break

                if has_phone:
                    actual_phone_count += 1
                else:
                    # This record might be processable (no phone)
                    name_col = columns_detected.get('primary_name')
                    if name_col and name_col in row.index:
                        name = str(row.get(name_col, '')).strip()
                        if name and name.lower() not in ['nan', 'none', '']:
                            # Quick business check
                            if not any(biz in name.upper() for biz in ['LLC', 'INC', 'CORP', 'TRUST']):
                                processable_count += 1

            # Update the analysis with actual counts
            analysis['analysis']['records_with_phones'] = actual_phone_count
            analysis['analysis']['records_processable'] = processable_count

            # Update existing_phones in columns_detected
            if existing_phones:
                analysis['analysis']['columns_detected']['existing_phones'] = existing_phones

            self.logger.info(f"📊 Enhanced analysis - Phones: {actual_phone_count}, Processable: {processable_count}")

        except Exception as e:
            self.logger.warning(f"⚠️ Could not enhance analysis: {e}")

        return analysis

    def _prepare_sample_for_ai(self, df: pd.DataFrame) -> str:
        """Prepare sample data for AI analysis"""
        sample_rows = []

        # Take first few rows that aren't empty
        row_count = 0
        for idx, row in df.iterrows():
            if row_count >= 3:
                break

            # Skip empty rows
            if row.isna().all() or (row.astype(str).str.strip() == '').all():
                continue

            row_str = ', '.join([f'"{str(val)}"' for val in row.values])
            sample_rows.append(f"Row {row_count + 1}: {row_str}")
            row_count += 1

        return '\n'.join(sample_rows)

    def format_csv_for_phone_extraction(self, input_path: str, output_path: Optional[str] = None, max_records: Optional[int] = None) -> Dict:
        """
        Format CSV for phone extraction using AI analysis

        Args:
            input_path: Path to input CSV file
            output_path: Path for output file (optional)
            max_records: Maximum records to process (optional)

        Returns:
            Dict: Processing results and statistics
        """
        try:
            self.logger.info(f"🚀 Starting AI-powered phone data formatting")
            self.logger.info(f"📄 Input file: {input_path}")

            # Load CSV
            df = self._load_csv_with_encoding_detection(input_path)
            if df is None:
                return {'success': False, 'error': 'Could not load input file'}

            # AI analysis
            self.logger.info("🤖 Running AI analysis...")
            analysis = self.analyze_csv_structure(df)
            if not analysis:
                return {'success': False, 'error': 'AI analysis failed'}

            # Log analysis results
            self.logger.info(f"📊 Analysis complete:")
            self.logger.info(f"  📝 Total records: {analysis['analysis']['total_records']}")
            self.logger.info(f"  📞 Records with phones: {analysis['analysis']['records_with_phones']}")
            self.logger.info(f"  ✅ Records processable: {analysis['analysis']['records_processable']}")

            # Apply AI formula to format records
            formatted_records = []
            processed_count = 0
            skipped_count = 0

            for idx, row in df.iterrows():
                if max_records and processed_count >= max_records:
                    break

                # Apply AI-determined processing logic
                formatted_record = self._apply_ai_formula(row, analysis, df, original_index=idx)

                if formatted_record:
                    formatted_records.append(formatted_record)
                    processed_count += 1
                else:
                    skipped_count += 1

            # Create output
            if formatted_records:
                output_df = pd.DataFrame(formatted_records)

                # Create output path if not provided
                if output_path is None:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    base_name = Path(input_path).stem
                    output_path = f"phone_ready_{timestamp}.csv"

                # Save formatted data
                output_df.to_csv(output_path, index=False)
                self.logger.info(f"💾 Formatted data saved: {output_path}")

                return {
                    'success': True,
                    'output_path': output_path,
                    'total_input_records': len(df),
                    'records_processed': processed_count,
                    'records_skipped': skipped_count,
                    'success_rate': f"{(processed_count / len(df) * 100):.1f}%",
                    'analysis_used': analysis,
                    'formatted_columns': list(output_df.columns)
                }
            else:
                return {
                    'success': False,
                    'error': 'No records could be formatted',
                    'total_input_records': len(df),
                    'records_skipped': skipped_count
                }

        except Exception as e:
            self.logger.error(f"❌ Phone formatting failed: {e}")
            return {'success': False, 'error': str(e)}

    def _load_csv_with_encoding_detection(self, file_path: str) -> Optional[pd.DataFrame]:
        """Load CSV with multiple encoding attempts"""
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

        for encoding in encodings:
            try:
                if file_path.endswith('.csv'):
                    df = pd.read_csv(file_path, encoding=encoding)
                elif file_path.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(file_path)
                else:
                    df = pd.read_csv(file_path, encoding=encoding)  # Try CSV as fallback

                self.logger.info(f"✅ Loaded {len(df)} records with {encoding} encoding")
                return df
            except Exception:
                continue

        self.logger.error(f"❌ Could not load {file_path} with any encoding")
        return None

    def _apply_ai_formula(self, row: pd.Series, analysis: Dict, df: pd.DataFrame, original_index=None) -> Optional[Dict]:
        """Apply AI-determined formula to format a single record"""

        # Extract column mappings from AI analysis
        columns_detected = analysis.get('analysis', {}).get('columns_detected', {})
        extraction_formula = analysis.get('extraction_formula', {})

        # Enhance column detection with fallback pattern matching
        columns_detected = self._enhance_column_detection(df, columns_detected)

        # Check if record has phone - but PROCESS ALL RECORDS regardless
        has_existing_phone = self._record_has_phone(row, columns_detected.get('existing_phones', []))

        # Extract and clean name using AI guidance
        cleaned_name = self._extract_name_with_ai_guidance(row, columns_detected, extraction_formula)
        if not cleaned_name:
            return None

        # Extract and format address using AI guidance
        formatted_address = self._extract_address_with_ai_guidance(row, columns_detected, extraction_formula)
        if not formatted_address:
            return None

        # Extract city and state using AI guidance
        city = self._extract_city_with_ai_guidance(row, columns_detected, extraction_formula)
        state = self._extract_state_with_ai_guidance(row, columns_detected, extraction_formula)

        # Extract existing phone numbers if present
        existing_phones = self._extract_existing_phones(row, columns_detected.get('existing_phones', [])) if has_existing_phone else {'primary': '', 'secondary': '', 'all': ''}

        # Return ZabaSearch-ready format with ALL records processed and proper flags
        result = {
            'DirectName_Cleaned': cleaned_name,
            'DirectName_Address': formatted_address,
            'DirectName_City': city,
            'DirectName_State': state,
            'DirectName_Type': 'Person',
            'Original_Index': original_index,
            'Skip_ZabaSearch': has_existing_phone,  # Flag for ZabaSearch to skip
            'Primary_Phone': existing_phones['primary'],
            'Secondary_Phone': existing_phones['secondary'],
            'DirectName_Phone_Primary': existing_phones['primary'],
            'DirectName_Phone_Secondary': existing_phones['secondary'],
            'DirectName_Phone_All': existing_phones['all']
        }

        return result

    def _record_has_phone(self, row: pd.Series, phone_columns: list) -> bool:
        """Check if record already has phone data with comprehensive detection"""
        import re

        # First check AI-detected phone columns
        for col in phone_columns:
            if col in row.index:
                value = str(row.get(col, ''))
                # Check for phone patterns
                digits_only = re.sub(r'[^\d]', '', value)
                if len(digits_only) >= 10:
                    return True

        # Fallback: Check all columns for phone-like names and patterns
        phone_name_patterns = [
            'phone', 'tel', 'telephone', 'cell', 'mobile', 'contact',
            'number', 'primary', 'secondary', 'home', 'work', 'business'
        ]

        for col_name in row.index:
            col_lower = col_name.lower().replace('_', ' ').replace('-', ' ')

            # Check if column name suggests it might contain phone numbers
            if any(pattern in col_lower for pattern in phone_name_patterns):
                value = str(row.get(col_name, ''))
                digits_only = re.sub(r'[^\d]', '', value)
                if len(digits_only) >= 10:
                    return True

            # Also check for phone patterns in any column (even if name doesn't suggest it)
            value = str(row.get(col_name, ''))
            if re.search(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', value):
                return True

        return False

    def _extract_existing_phones(self, row: pd.Series, phone_columns: list) -> dict:
        """Extract existing phone numbers from a record"""
        import re
        phones = {'primary': '', 'secondary': '', 'all': ''}
        found_phones = []

        # First check AI-detected phone columns
        for col in phone_columns:
            if col in row.index:
                value = str(row.get(col, '')).strip()
                if value and value.lower() not in ['nan', 'none', '']:
                    # Check for phone patterns
                    digits_only = re.sub(r'[^\d]', '', value)
                    if len(digits_only) >= 10:
                        found_phones.append(value)

        # Fallback: Check all columns for phone-like patterns
        phone_name_patterns = [
            'phone', 'tel', 'telephone', 'cell', 'mobile', 'contact',
            'number', 'primary', 'secondary', 'home', 'work', 'business'
        ]

        for col_name in row.index:
            col_lower = col_name.lower().replace('_', ' ').replace('-', ' ')

            # Check if column name suggests it might contain phone numbers
            if any(pattern in col_lower for pattern in phone_name_patterns):
                value = str(row.get(col_name, '')).strip()
                if value and value.lower() not in ['nan', 'none', '']:
                    digits_only = re.sub(r'[^\d]', '', value)
                    if len(digits_only) >= 10 and value not in found_phones:
                        found_phones.append(value)

            # Also check for phone patterns in any column (even if name doesn't suggest it)
            value = str(row.get(col_name, '')).strip()
            if value and re.search(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', value):
                if value not in found_phones:
                    found_phones.append(value)

        # Assign phones to primary/secondary
        if found_phones:
            phones['primary'] = found_phones[0]
            if len(found_phones) > 1:
                phones['secondary'] = found_phones[1]
            phones['all'] = ', '.join(found_phones)

        return phones

    def _enhance_column_detection(self, df: pd.DataFrame, columns_detected: Dict) -> Dict:
        """Enhance AI column detection with fallback pattern matching"""
        enhanced = columns_detected.copy()

        # Name column fallback patterns
        name_patterns = ['name', 'owner', 'person', 'individual', 'borrower', 'buyer', 'seller', 'resident', 'tenant', 'customer', 'client', 'contact']

        # Address component fallback patterns
        house_patterns = ['house', 'street', 'number', 'num', 'addr', 'building', 'property']
        direction_patterns = ['direction', 'dir', 'prefix', 'pre']
        street_patterns = ['street', 'road', 'avenue', 'blvd', 'lane', 'drive', 'way', 'st', 'rd', 'ave']
        city_patterns = ['city', 'town', 'municipality', 'place', 'location']
        state_patterns = ['state', 'province', 'region', 'st']

        # If primary name not detected, try to find one
        if not enhanced.get('primary_name'):
            for col in df.columns:
                col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                if any(pattern in col_lower for pattern in name_patterns):
                    # Prefer columns with "1" or "primary" for primary name
                    if '1' in col_lower or 'primary' in col_lower or 'main' in col_lower:
                        enhanced['primary_name'] = col
                        break
                    elif not enhanced.get('primary_name'):  # Fallback to first match
                        enhanced['primary_name'] = col

        # If house number not detected, try to find one
        if not enhanced.get('house_number'):
            for col in df.columns:
                col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                if any(pattern in col_lower for pattern in house_patterns):
                    enhanced['house_number'] = col
                    break

        # If city not detected, try to find one
        if not enhanced.get('city'):
            for col in df.columns:
                col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                if any(pattern in col_lower for pattern in city_patterns):
                    enhanced['city'] = col
                    break

        # If state not detected, try to find one
        if not enhanced.get('state'):
            for col in df.columns:
                col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                if any(pattern in col_lower for pattern in state_patterns):
                    enhanced['state'] = col
                    break

        # If street name not detected, try to find one
        if not enhanced.get('street_name') and not enhanced.get('combined_address'):
            for col in df.columns:
                col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                if any(pattern in col_lower for pattern in street_patterns) and 'type' not in col_lower and 'suffix' not in col_lower:
                    enhanced['street_name'] = col
                    break
                # Also look for combined address patterns
                elif any(pattern in col_lower for pattern in ['street address', 'address', 'addr']):
                    enhanced['combined_address'] = col
                    break

        return enhanced

    def _extract_name_with_ai_guidance(self, row: pd.Series, columns_detected: Dict, extraction_formula: Dict) -> Optional[str]:
        """Extract and clean name using proven cleaning logic from CSV format handler"""

        # Get primary name column from AI analysis
        primary_name_col = columns_detected.get('primary_name')
        if not primary_name_col or primary_name_col not in row.index:
            return None

        name = str(row.get(primary_name_col, '')).strip()
        if not name or name.lower() in ['nan', 'none', '']:
            return None

        # Use proven name cleaning logic (same as csv_format_handler.py)
        cleaned_name = self._clean_name_for_zabasearch(name)
        return cleaned_name if cleaned_name else None

    def _clean_name_for_zabasearch(self, name: str) -> str:
        """Enhanced name cleaning to handle ALL possible name formats and get just 'FIRST LAST'"""
        import re
        import pandas as pd

        if pd.isna(name) or not name or name.strip() == "":
            return ""

        # Comprehensive lists for cleaning
        business_suffixes = ['LLC', 'INC', 'CORP', 'LTD', 'CO', 'COMPANY', 'TRUST', 'TR', 'REV TR', 'LIV TR', 'FAM TR',
                           'ESTATE', 'PROPERTIES', 'INVESTMENTS', 'ENTERPRISES', 'HOLDINGS', 'GROUP', 'ASSOCIATES']
        titles = ['MR', 'MRS', 'MS', 'DR', 'PROF', 'REV', 'FATHER', 'SISTER', 'BROTHER', 'PASTOR', 'MINISTER']
        suffixes = ['JR', 'SR', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', '1ST', '2ND', '3RD', '4TH', '5TH',
                   'JUNIOR', 'SENIOR', 'ESQ', 'ESQUIRE', 'PHD', 'MD', 'DDS', 'DO', 'RN', 'CPA']
        middle_indicators = ['NMI', 'NMN', 'NONE', 'N/A', 'NA']

        name = str(name).strip().upper()

        # Remove extra whitespace and clean punctuation
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'["\']', '', name)  # Remove quotes
        name = re.sub(r'[-_]', ' ', name)  # Convert dashes/underscores to spaces

        # Business entity detection - skip if clearly a business
        business_patterns = [
            r'^.*(LLC|INC|CORP|LTD|COMPANY|TRUST|ESTATE|PROPERTIES|INVESTMENTS).*$',
            r'^(THE|A)\s+.*',  # "THE SMITH TRUST", "A COMPANY"
            r'.*\b(FAMILY\s+TRUST|REV\s+TR|LIV\s+TR)\b.*'
        ]

        for pattern in business_patterns:
            if re.match(pattern, name):
                # Check for person indicators within business names
                person_indicators = r'\b(JOHN|JANE|ROBERT|MARY|JAMES|PATRICIA|MICHAEL|LINDA|WILLIAM|ELIZABETH|DAVID|BARBARA|RICHARD|SUSAN|JOSEPH|JESSICA|THOMAS|SARAH|CHARLES|KAREN|CHRISTOPHER|NANCY|DANIEL|LISA|MATTHEW|BETTY|ANTHONY|HELEN|MARK|SANDRA|DONALD|DONNA|STEVEN|CAROL|PAUL|RUTH|ANDREW|SHARON|JOSHUA|MICHELLE|KENNETH|LAURA|KEVIN|BRIAN|KIMBERLY|GEORGE|DEBORAH|TIMOTHY|DOROTHY|RONALD|JASON|EDWARD|JEFFREY|RYAN|JACOB|GARY|NICHOLAS|ERIC|JONATHAN|STEPHEN|LARRY|JUSTIN|SCOTT|BRANDON|BENJAMIN|SAMUEL|GREGORY|ALEXANDER|FRANK|RAYMOND|JACK|DENNIS|JERRY|TYLER|AARON|JOSE|HENRY|ADAM|DOUGLAS|NATHAN|PETER|ZACHARY|KYLE|WALTER|HAROLD|CARL)\b'
                if not re.search(person_indicators, name):
                    return ""

        # Remove business suffixes first
        for suffix in business_suffixes:
            name = re.sub(rf'\b{suffix}\b', '', name, flags=re.IGNORECASE)

        # Remove titles
        for title in titles:
            name = re.sub(rf'\b{title}\.?\s*', '', name)

        # Remove suffixes
        for suffix in suffixes:
            name = re.sub(rf'\b{suffix}\.?\b', '', name)

        # Remove common name prefixes to simplify names for better search results
        name_prefixes_to_remove = ['DE', 'DEL', 'DER', 'LA', 'LE', 'VAN', 'VON', 'MAC', 'MC', 'O\'', 'ST', 'SAN', 'SANTA']
        for prefix in name_prefixes_to_remove:
            name = re.sub(rf'\b{prefix}\.?\s+', '', name)

        # Remove middle name indicators
        for indicator in middle_indicators:
            name = re.sub(rf'\b{indicator}\b', '', name)

        # Remove single letter middle initials ONLY (not full words)
        # This regex only removes isolated single letters with optional periods
        name = re.sub(r'\s+[A-Z]\.(?=\s+|$)', ' ', name)  # Remove single letter with period (middle initial)
        name = re.sub(r'\s+(?<!\w)[A-Z](?=\s+[A-Z][a-z]|\s*$)', ' ', name)  # Remove isolated single letters at end

        # Clean up extra spaces
        name = re.sub(r'\s+', ' ', name).strip()

        # Handle different name formats

        # Format 1: "LAST, FIRST MIDDLE" or "LAST, FIRST"
        if ',' in name:
            parts = name.split(',', 1)
            if len(parts) == 2:
                last_part = parts[0].strip()
                first_part = parts[1].strip()

                # Handle multiple owners: "SMITH, JOHN & MARY" -> take first person
                if any(sep in first_part for sep in [' & ', ' AND ', '&', 'AND']):
                    for sep in [' & ', ' AND ', '&', 'AND']:
                        if sep in first_part:
                            first_part = first_part.split(sep)[0].strip()
                            break

                # Extract first name (first word) and last name (first word of last part)
                first_words = first_part.split()
                last_words = last_part.split()

                if first_words and last_words:
                    first_name = first_words[0]
                    last_name = last_words[0]
                    return f"{first_name} {last_name}"
                elif first_words:
                    return first_words[0]

        # Format 2: "FIRST MIDDLE LAST" or "FIRST LAST"
        elif ' ' in name:
            # Handle multiple owners first
            if any(sep in name for sep in [' & ', ' AND ', '&']):
                for sep in [' & ', ' AND ', '&']:
                    if sep in name:
                        name = name.split(sep)[0].strip()
                        break

            words = name.split()
            # Filter out remaining business words and clean words
            clean_words = []
            business_words = ['LIV', 'TR', 'REV', 'TRUST', 'LLC', 'INC', 'CORP', 'FAM', 'ETAL', 'FAMILY']

            for word in words:
                if (word not in business_words and
                    word not in suffixes and
                    word not in titles and
                    word not in middle_indicators and
                    len(word) > 1):  # Skip single letters
                    clean_words.append(word)

            # Extract first and last name intelligently
            # Format: "FIRSTNAME LASTNAME" for most cases without commas
            if len(clean_words) >= 2:
                # For 2 words: need to detect if it's "FIRSTNAME LASTNAME" or "LASTNAME FIRSTNAME"
                if len(clean_words) == 2:
                    # SMART DETECTION: Check if first word is a common surname
                    # Common surnames that appear in property records
                    common_surnames = {
                        'SMITH', 'JOHNSON', 'WILLIAMS', 'BROWN', 'JONES', 'GARCIA', 'MILLER', 'DAVIS', 'RODRIGUEZ', 'MARTINEZ',
                        'HERNANDEZ', 'LOPEZ', 'GONZALEZ', 'WILSON', 'ANDERSON', 'THOMAS', 'TAYLOR', 'MOORE', 'JACKSON', 'MARTIN',
                        'LEE', 'PEREZ', 'THOMPSON', 'WHITE', 'HARRIS', 'SANCHEZ', 'CLARK', 'RAMIREZ', 'LEWIS', 'ROBINSON',
                        'WALKER', 'YOUNG', 'ALLEN', 'KING', 'WRIGHT', 'SCOTT', 'TORRES', 'NGUYEN', 'HILL', 'FLORES',
                        'GREEN', 'ADAMS', 'NELSON', 'BAKER', 'HALL', 'RIVERA', 'CAMPBELL', 'MITCHELL', 'CARTER', 'ROBERTS',
                        'GOMEZ', 'PHILLIPS', 'EVANS', 'TURNER', 'DIAZ', 'PARKER', 'CRUZ', 'EDWARDS', 'COLLINS', 'REYES',
                        'STEWART', 'MORRIS', 'MORALES', 'MURPHY', 'COOK', 'ROGERS', 'GUTIERREZ', 'ORTIZ', 'MORGAN', 'COOPER',
                        'PETERSON', 'BAILEY', 'REED', 'KELLY', 'HOWARD', 'RAMOS', 'KIM', 'COX', 'WARD', 'RICHARDSON',
                        'WATSON', 'BROOKS', 'CHAVEZ', 'WOOD', 'JAMES', 'BENNETT', 'GRAY', 'MENDOZA', 'RUIZ', 'HUGHES',
                        'PRICE', 'ALVAREZ', 'CASTILLO', 'SANDERS', 'PATEL', 'MYERS', 'LONG', 'ROSS', 'FOSTER', 'JIMENEZ',
                        # Additional common surnames from Florida property records
                        'WELTY', 'DUTIL', 'SIVONGSAY', 'PEDERSEN', 'ALMANZAR', 'NUNEZ', 'MASTERS', 'SAUTEL', 'KRISHNA',
                        'OCONNOR', 'MCDONALD', 'OLEARY', 'SULLIVAN', 'OBRIEN', 'KENNEDY', 'MURPHY', 'RYAN', 'WALSH', 'BYRNE'
                    }

                    # Common first names for additional validation
                    common_first_names = {
                        'JAMES', 'JOHN', 'ROBERT', 'MICHAEL', 'WILLIAM', 'DAVID', 'RICHARD', 'JOSEPH', 'THOMAS', 'CHARLES',
                        'CHRISTOPHER', 'DANIEL', 'MATTHEW', 'ANTHONY', 'MARK', 'DONALD', 'STEVEN', 'PAUL', 'ANDREW', 'JOSHUA',
                        'KENNETH', 'KEVIN', 'BRIAN', 'GEORGE', 'EDWARD', 'RONALD', 'TIMOTHY', 'JASON', 'JEFFREY', 'RYAN',
                        'JACOB', 'GARY', 'NICHOLAS', 'ERIC', 'JONATHAN', 'STEPHEN', 'LARRY', 'JUSTIN', 'SCOTT', 'BRANDON',
                        'BENJAMIN', 'SAMUEL', 'RAYMOND', 'GREGORY', 'ALEXANDER', 'PATRICK', 'JACK', 'DENNIS', 'JERRY', 'TYLER',
                        'MARY', 'PATRICIA', 'JENNIFER', 'LINDA', 'BARBARA', 'ELIZABETH', 'SUSAN', 'JESSICA', 'SARAH', 'KAREN',
                        'NANCY', 'LISA', 'BETTY', 'MARGARET', 'SANDRA', 'ASHLEY', 'KIMBERLY', 'EMILY', 'DONNA', 'MICHELLE',
                        'DOROTHY', 'CAROL', 'AMANDA', 'MELISSA', 'DEBORAH', 'STEPHANIE', 'REBECCA', 'SHARON', 'LAURA', 'CYNTHIA',
                        'NELSON', 'BRANDON', 'LEONARD', 'SANDRA', 'CHRISTINA', 'ART', 'JOSE', 'DONNALEE', 'JUAN', 'DEBRA'
                    }

                    first_word = clean_words[0]
                    second_word = clean_words[1]

                    # Detection logic with multiple heuristics:
                    # 1. If first word is common surname AND second word is common first name → REVERSE
                    # 2. If first word is common surname AND second word is NOT a surname → REVERSE
                    # 3. Otherwise keep original order (FIRSTNAME LASTNAME)

                    is_reversed_format = False

                    # Check 1: First word is surname AND second word is first name
                    if first_word in common_surnames and second_word in common_first_names:
                        is_reversed_format = True
                    # Check 2: First word is surname AND second word is not a common surname
                    elif first_word in common_surnames and second_word not in common_surnames:
                        is_reversed_format = True
                    # Check 3: Second word is clearly a surname AND first word is not
                    elif second_word in common_surnames and first_word not in common_surnames and first_word in common_first_names:
                        is_reversed_format = False  # Normal order

                    if is_reversed_format:
                        # DETECTED: "LASTNAME FIRSTNAME" format - REVERSE IT
                        first_name = second_word  # Second word is the first name
                        last_name = first_word    # First word is the last name
                        return f"{first_name} {last_name}"
                    else:
                        # Normal "FIRSTNAME LASTNAME" format
                        first_name = first_word
                        last_name = second_word
                        return f"{first_name} {last_name}"

                # For 3+ words: need to determine where last name ends and first name begins
                # Common patterns:
                # - "DE SAUTEL EDWARD J" → last name is "DE SAUTEL", first name is "EDWARD"
                # - "MASTERS MICHAEL JAY" → last name is "MASTERS", first name is "MICHAEL"
                # - "ALVAREZ NELSON JOSE" → last name is "ALVAREZ", first name is "NELSON" (reversed)

                # Reuse the surname detection from above
                common_surnames_3word = {
                    'SMITH', 'JOHNSON', 'WILLIAMS', 'BROWN', 'JONES', 'GARCIA', 'MILLER', 'DAVIS', 'RODRIGUEZ', 'MARTINEZ',
                    'ALVAREZ', 'WELTY', 'DUTIL', 'SIVONGSAY', 'PEDERSEN', 'ALMANZAR', 'NUNEZ', 'MASTERS', 'ALLEN', 'NELSON'
                }

                # Strategy: Look for common last name prefixes and handle multi-word last names
                last_name_prefixes = ['DE', 'DEL', 'DER', 'LA', 'LE', 'VAN', 'VON', 'MAC', 'MC', 'O', 'ST', 'SAN', 'SANTA']

                # Check for compound prefixes like "DE LA", "VAN DER", etc.
                if (len(clean_words) >= 4 and
                    clean_words[0] in last_name_prefixes and
                    clean_words[1] in last_name_prefixes):
                    # Compound prefix like "DE LA CRUZ MARIA ELENA"
                    last_name = f"{clean_words[0]} {clean_words[1]} {clean_words[2]}"
                    first_name = clean_words[3]
                    return f"{first_name} {last_name}"
                elif clean_words[0] in last_name_prefixes and len(clean_words) >= 3:
                    # Single prefix like "DE SAUTEL EDWARD"
                    last_name = f"{clean_words[0]} {clean_words[1]}"
                    first_name = clean_words[2]
                    return f"{first_name} {last_name}"
                # SMART DETECTION for 3+ words: Check if first word is a surname
                elif clean_words[0] in common_surnames_3word:
                    # Likely "LASTNAME FIRSTNAME MIDDLE" format
                    # Example: "ALVAREZ NELSON JOSE" → "NELSON ALVAREZ"
                    last_name = clean_words[0]
                    first_name = clean_words[1]
                    return f"{first_name} {last_name}"
                else:
                    # No prefix - assume last word is last name (less common)
                    # Example: "MICHAEL JAY MASTERS" → "MICHAEL MASTERS"
                    last_name = clean_words[-1]  # Last word is surname
                    first_name = clean_words[0]  # First word is first name
                    return f"{first_name} {last_name}"

            elif len(clean_words) == 1:
                return clean_words[0]

        # Format 3: Single name
        else:
            # Check if it's a valid single name (not business/title)
            if (name not in business_suffixes and
                name not in titles and
                name not in suffixes and
                len(name) > 1):
                return name

        return ""

    def _extract_address_with_ai_guidance(self, row: pd.Series, columns_detected: Dict, extraction_formula: Dict) -> Optional[str]:
        """Extract and format address using AI guidance"""
        import re

        address_method = extraction_formula.get('address_method', '')

        if 'separated_components' in address_method:
            # Build from separated components
            components = []

            # House number
            house_col = columns_detected.get('house_number')
            if house_col and house_col in row.index:
                house_num = str(row.get(house_col, '')).strip()
                if house_num and house_num.lower() not in ['nan', 'none']:
                    components.append(house_num)

            # Prefix direction
            prefix_col = columns_detected.get('prefix_direction')
            if prefix_col and prefix_col in row.index:
                prefix = str(row.get(prefix_col, '')).strip()
                if prefix and prefix.lower() not in ['nan', 'none']:
                    components.append(prefix)

            # Street name
            street_col = columns_detected.get('street_name')
            if street_col and street_col in row.index:
                street = str(row.get(street_col, '')).strip()
                if street and street.lower() not in ['nan', 'none']:
                    components.append(street)

            # Street type
            type_col = columns_detected.get('street_type')
            if type_col and type_col in row.index:
                street_type = str(row.get(type_col, '')).strip()
                if street_type and street_type.lower() not in ['nan', 'none']:
                    components.append(street_type)

            # City and State
            city_col = columns_detected.get('city')
            state_col = columns_detected.get('state')

            if city_col and city_col in row.index:
                city = str(row.get(city_col, '')).strip().upper()
                if city and city.lower() not in ['nan', 'none']:
                    if len(components) >= 2:  # At least house number and street
                        street_address = ' '.join(components)

                        # Add state if available
                        if state_col and state_col in row.index:
                            state = str(row.get(state_col, '')).strip().upper()
                            if state and state.lower() not in ['nan', 'none']:
                                return f"{street_address}, {city}, {state}"

                        # If no separate state column, return with just city
                        return f"{street_address}, {city}"

        elif 'parse_combined' in address_method or columns_detected.get('combined_address'):
            # Handle combined address with separate city/state
            combined_col = columns_detected.get('combined_address')
            if combined_col and combined_col in row.index:
                address = str(row.get(combined_col, '')).strip().upper()
                if address and address.lower() not in ['nan', 'none']:
                    # Get city and state if available
                    city_col = columns_detected.get('city')
                    state_col = columns_detected.get('state')

                    city = None
                    state = None

                    if city_col and city_col in row.index:
                        city_value = str(row.get(city_col, '')).strip()
                        if city_value and city_value.lower() not in ['nan', 'none']:
                            city = city_value.upper()

                    if state_col and state_col in row.index:
                        state_value = str(row.get(state_col, '')).strip()
                        if state_value and state_value.lower() not in ['nan', 'none']:
                            state = state_value.upper()

                    # Combine address with city and state
                    if city and state:
                        return f"{address}, {city}, {state}"
                    elif city:
                        return f"{address}, {city}"
                    else:
                        return address

        # Fallback: Try to combine any available address components
        else:
            # Look for common address column patterns
            street_address = None
            city = None
            state = None

            # Find street address column
            for col in row.index:
                col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                if any(pattern in col_lower for pattern in ['street', 'address', 'addr']) and 'city' not in col_lower and 'state' not in col_lower:
                    value = str(row.get(col, '')).strip()
                    if value and value.lower() not in ['nan', 'none']:
                        street_address = value.upper()
                        break

            # Find city column
            city_col = columns_detected.get('city')
            if city_col and city_col in row.index:
                city_value = str(row.get(city_col, '')).strip()
                if city_value and city_value.lower() not in ['nan', 'none']:
                    city = city_value.upper()
            else:
                # Fallback city detection
                for col in row.index:
                    col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                    if 'city' in col_lower:
                        value = str(row.get(col, '')).strip()
                        if value and value.lower() not in ['nan', 'none']:
                            city = value.upper()
                            break

            # Find state column
            state_col = columns_detected.get('state')
            if state_col and state_col in row.index:
                state_value = str(row.get(state_col, '')).strip()
                if state_value and state_value.lower() not in ['nan', 'none']:
                    state = state_value.upper()
            else:
                # Fallback state detection
                for col in row.index:
                    col_lower = col.lower().replace('_', ' ').replace('-', ' ')
                    if 'state' in col_lower:
                        value = str(row.get(col, '')).strip()
                        if value and value.lower() not in ['nan', 'none']:
                            state = value.upper()
                            break

            # Combine available components
            if street_address:
                if city and state:
                    return f"{street_address}, {city}, {state}"
                elif city:
                    return f"{street_address}, {city}"
                else:
                    return street_address

        return None

    def _extract_city_with_ai_guidance(self, row: pd.Series, columns_detected: Dict, extraction_formula: Dict) -> str:
        """Extract city using AI guidance"""
        # Find city column using AI detection
        city_col = columns_detected.get('city')
        if city_col and city_col in row.index:
            city_value = str(row.get(city_col, '')).strip()
            if city_value and city_value.lower() not in ['nan', 'none']:
                return city_value.upper()

        # Fallback city detection
        for col in row.index:
            col_lower = col.lower().replace('_', ' ').replace('-', ' ')
            if 'city' in col_lower:
                value = str(row.get(col, '')).strip()
                if value and value.lower() not in ['nan', 'none']:
                    return value.upper()

        # Default fallback
        return 'HALLANDALE BEACH'

    def _extract_state_with_ai_guidance(self, row: pd.Series, columns_detected: Dict, extraction_formula: Dict) -> str:
        """Extract state using AI guidance"""
        # Find state column using AI detection
        state_col = columns_detected.get('state')
        if state_col and state_col in row.index:
            state_value = str(row.get(state_col, '')).strip()
            if state_value and state_value.lower() not in ['nan', 'none']:
                # Convert abbreviations to full state name
                if state_value.upper() in ['FL', 'FLORIDA']:
                    return 'Florida'
                return state_value.title()  # Proper case for state names

        # Fallback state detection
        for col in row.index:
            col_lower = col.lower().replace('_', ' ').replace('-', ' ')
            if 'state' in col_lower:
                value = str(row.get(col, '')).strip()
                if value and value.lower() not in ['nan', 'none']:
                    # Convert abbreviations to full state name
                    if value.upper() in ['FL', 'FLORIDA']:
                        return 'Florida'
                    return value.title()

        # Default fallback
        return 'Florida'

    def _clean_combined_address(self, address: str) -> Optional[str]:
        """Clean combined address to ZabaSearch format"""
        import re

        if not address or len(address.strip()) < 5:
            return None

        address = address.strip().upper()

        # Remove state and zip
        address = re.sub(r',?\s*(FL|FLORIDA|CA|CALIFORNIA|NY|NEW YORK)\s*\d{5}(-\d{4})?', '', address)
        address = re.sub(r',?\s*\d{5}(-\d{4})?', '', address)
        address = re.sub(r',?\s*(FL|FLORIDA|CA|CALIFORNIA|NY|NEW YORK)\s*$', '', address)

        # If already has comma, validate format
        if ',' in address:
            parts = address.split(',')
            if len(parts) >= 2:
                street = parts[0].strip()
                city = parts[1].strip()
                if street and city:
                    return f"{street}, {city}"

        return address.strip() if address.strip() else None


def main():
    """Command line interface"""
    if len(sys.argv) < 2:
        print("Usage: python intelligent_phone_formatter_v2.py <csv_file> [output_file] [max_records]")
        print("  csv_file: Path to input CSV or Excel file")
        print("  output_file: Path for output file (optional)")
        print("  max_records: Maximum records to process (optional)")
        return

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    max_records = int(sys.argv[3]) if len(sys.argv) > 3 else None

    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return

    print(f"\n🤖 Intelligent Phone Data Formatter (AI-Powered)")
    print(f"📄 Input file: {input_file}")
    if output_file:
        print(f"📁 Output file: {output_file}")
    if max_records:
        print(f"🔢 Max records: {max_records}")
    print("=" * 60)

    # Process the file
    formatter = IntelligentPhoneFormatter()

    # Handle default values
    if output_file is None:
        output_file = input_file.replace('.csv', '_zabasearch_ready.csv').replace('.xlsx', '_zabasearch_ready.csv')

    # Process all records by default (no limit)
    if max_records is None:
        max_records = None  # No limit - process entire file

    results = formatter.format_csv_for_phone_extraction(input_file, output_file, max_records)

    if results['success']:
        print(f"\n✅ Phone formatting completed successfully!")
        print(f"📊 Results Summary:")
        print(f"  📝 Total input records: {results['total_input_records']}")
        print(f"  ✅ Records processed: {results['records_processed']}")
        print(f"  ⏭️ Records skipped: {results['records_skipped']}")
        print(f"  📈 Success rate: {results['success_rate']}")
        print(f"  📞 Output columns: {results['formatted_columns']}")
        print(f"💾 Formatted file: {results['output_path']}")
    else:
        print(f"❌ Phone formatting failed: {results['error']}")
        if 'total_input_records' in results:
            print(f"📊 Records analyzed: {results['total_input_records']}")
            print(f"⏭️ Records skipped: {results.get('records_skipped', 0)}")

if __name__ == "__main__":
    main()
