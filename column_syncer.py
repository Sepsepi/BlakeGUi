#!/usr/bin/env python3
"""
Phone Number Formatter
Advanced mobile phone validation and formatting using AI
Processes Primary_Phone and Secondary_Phone columns with intelligent row handling
"""

import pandas as pd
import json
import logging
import re
from pathlib import Path
from datetime import datetime
import requests
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ColumnSyncer:
    def __init__(self):
        """Initialize the Phone Number Formatter with DeepSeek API configuration"""
        from config import DEEPSEEK_API_KEY, DEEPSEEK_API_URL
        self.api_key = DEEPSEEK_API_KEY
        self.api_url = DEEPSEEK_API_URL
        self.model = "deepseek-chat"

        # Load AI instructions
        self.ai_instructions = self._load_ai_instructions()

        # Phone validation patterns
        self.phone_patterns = [
            r'\(\d{3}\)\s*\d{3}-\d{4}',  # (123) 456-7890
            r'\d{3}-\d{3}-\d{4}',        # 123-456-7890
            r'\d{10}',                   # 1234567890
            r'\d{3}\.\d{3}\.\d{4}',      # 123.456.7890
            r'\+1\s*\d{3}\s*\d{3}\s*\d{4}' # +1 123 456 7890
        ]

    def _load_ai_instructions(self) -> str:
        """Load AI instructions from markdown file"""
        try:
            instructions_path = Path("AI_COLUMN_SYNCER_INSTRUCTIONS.md")
            if instructions_path.exists():
                with open(instructions_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                logger.warning("AI instructions file not found, using fallback instructions")
                return self._get_fallback_instructions()
        except Exception as e:
            logger.error(f"Error loading AI instructions: {e}")
            return self._get_fallback_instructions()

    def _get_fallback_instructions(self) -> str:
        """Fallback AI instructions if file not found"""
        return """
        You are a phone number validator. Analyze phone numbers and return JSON:
        {
          "phone_number": "(formatted or empty)",
          "is_valid": true/false,
          "type": "MOBILE/LANDLINE/UNKNOWN",
          "confidence": 1-100,
          "reason": "explanation"
        }
        Only validate MOBILE numbers. Reject landlines.
        """

    def _clean_phone_number(self, phone: str) -> str:
        """Clean and normalize phone number for analysis"""
        if pd.isna(phone) or phone == "":
            return ""

        # Convert to string and clean
        phone_str = str(phone).strip()

        # Remove common separators but keep the digits
        phone_clean = re.sub(r'[^\d]', '', phone_str)

        # Handle different formats
        if len(phone_clean) == 11 and phone_clean.startswith('1'):
            phone_clean = phone_clean[1:]  # Remove country code
        elif len(phone_clean) == 10:
            pass  # Perfect
        else:
            return ""  # Invalid length

        # Format as (XXX) XXX-XXXX for analysis
        if len(phone_clean) == 10:
            return f"({phone_clean[:3]}) {phone_clean[3:6]}-{phone_clean[6:]}"

        return ""

    def _validate_phone_with_ai(self, phone: str) -> Dict:
        """Validate a single phone number using AI"""
        if not phone:
            return {
                "phone_number": "",
                "is_valid": False,
                "type": "UNKNOWN",
                "confidence": 0,
                "reason": "Empty phone number"
            }

        try:
            # Prepare AI request
            messages = [
                {"role": "system", "content": self.ai_instructions},
                {"role": "user", "content": f"Analyze this phone number: {phone}"}
            ]

            response = requests.post(
                self.api_url,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.model,
                    "messages": messages,
                    "max_tokens": 200,
                    "temperature": 0.1
                },
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                ai_response = result['choices'][0]['message']['content'].strip()

                # Parse JSON response
                try:
                    phone_analysis = json.loads(ai_response)
                    return phone_analysis
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON from AI: {ai_response}")
                    return self._fallback_analysis(phone)
            else:
                logger.error(f"AI API error: {response.status_code}")
                return self._fallback_analysis(phone)

        except Exception as e:
            logger.error(f"Error validating phone {phone}: {e}")
            return self._fallback_analysis(phone)

    def _fallback_analysis(self, phone: str) -> Dict:
        """Fallback phone analysis when AI fails"""
        # Simple pattern-based validation
        area_code = phone[1:4] if phone.startswith('(') else phone[:3]

        # Mobile-first area codes
        mobile_area_codes = ['321', '407', '689', '754', '786']

        if area_code in mobile_area_codes:
            return {
                "phone_number": phone,
                "is_valid": True,
                "type": "MOBILE",
                "confidence": 75,
                "reason": f"Area code {area_code} is mobile-first"
            }
        else:
            return {
                "phone_number": "",
                "is_valid": False,
                "type": "UNKNOWN",
                "confidence": 50,
                "reason": "Fallback analysis - uncertain type"
            }

    def _analyze_phone_batch(self, phone_numbers: List[str], column_name: str) -> List[str]:
        """
        Analyze multiple phone numbers with automatic chunking for large datasets

        Args:
            phone_numbers (List[str]): List of phone numbers to analyze
            column_name (str): Name of the column being processed

        Returns:
            List[str]: List of analysis results ['mobile', 'landline', 'invalid']
        """
        if not phone_numbers:
            return []

        # Filter out empty numbers but keep track of original positions
        valid_phones = []
        phone_positions = []

        for i, phone in enumerate(phone_numbers):
            if phone and phone.strip():
                valid_phones.append(phone.strip())
                phone_positions.append(i)

        if not valid_phones:
            return ['invalid'] * len(phone_numbers)

        # Calculate optimal batch size based on token limits
        # Each phone uses ~10 tokens, safe limit is 800 phones per batch (8000 tokens)
        max_phones_per_batch = 800

        if len(valid_phones) <= max_phones_per_batch:
            # Process as single batch
            return self._process_single_batch(phone_numbers, valid_phones, phone_positions, column_name)
        else:
            # Process in multiple batches
            logger.info(f"📦 Large dataset detected: {len(valid_phones)} phones. Processing in batches of {max_phones_per_batch}...")
            return self._process_multiple_batches(phone_numbers, valid_phones, phone_positions, column_name, max_phones_per_batch)

    def _process_single_batch(self, phone_numbers: List[str], valid_phones: List[str], phone_positions: List[int], column_name: str) -> List[str]:
        """Process a single batch of phone numbers"""
        try:
            # Create batch input for AI
            phone_list = '\n'.join([f"{i+1}. {phone}" for i, phone in enumerate(valid_phones)])

            logger.info(f"🚀 Sending {len(valid_phones)} {column_name} numbers to AI for batch analysis...")

            response = requests.post(
                self.api_url,
                headers={
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json'
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [
                        {
                            "role": "system",
                            "content": self.ai_instructions
                        },
                        {
                            "role": "user",
                            "content": f"Analyze these {column_name} phone numbers:\n\n{phone_list}"
                        }
                    ],
                    "temperature": 0.1,
                    "max_tokens": min(len(valid_phones) * 10, 8000)
                },
                timeout=120
            )

            if response.status_code == 200:
                result = response.json()
                ai_response = result['choices'][0]['message']['content'].strip()

                # Parse AI response - expect simple format like "mobile\nlandline\nmobile"
                results = ['invalid'] * len(phone_numbers)
                lines = ai_response.split('\n')

                for i, line in enumerate(lines):
                    line = line.strip().lower()
                    if not line:
                        continue

                    # Map line position to original phone position
                    if i < len(phone_positions):
                        original_pos = phone_positions[i]

                        if 'mobile' in line:
                            results[original_pos] = 'mobile'
                        elif 'landline' in line:
                            results[original_pos] = 'landline'
                        else:
                            results[original_pos] = 'invalid'

                logger.info(f"✅ {column_name} batch analysis complete")
                return results
            else:
                logger.error(f"API error {response.status_code}: {response.text}")
                return ['invalid'] * len(phone_numbers)

        except Exception as e:
            logger.error(f"Error analyzing {column_name} phones: {e}")
            return ['invalid'] * len(phone_numbers)

    def _process_multiple_batches(self, phone_numbers: List[str], valid_phones: List[str], phone_positions: List[int], column_name: str, batch_size: int) -> List[str]:
        """Process phone numbers in multiple batches to handle large datasets"""
        results = ['invalid'] * len(phone_numbers)
        total_batches = (len(valid_phones) + batch_size - 1) // batch_size

        logger.info(f"📊 Processing {len(valid_phones)} phones in {total_batches} batches...")

        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(valid_phones))

            # Create batch data
            batch_phones = valid_phones[start_idx:end_idx]
            batch_positions = phone_positions[start_idx:end_idx]

            logger.info(f"🔄 Processing batch {batch_num + 1}/{total_batches} ({len(batch_phones)} phones)...")

            try:
                # Create batch input for AI
                phone_list = '\n'.join([f"{i+1}. {phone}" for i, phone in enumerate(batch_phones)])

                response = requests.post(
                    self.api_url,
                    headers={
                        'Authorization': f'Bearer {self.api_key}',
                        'Content-Type': 'application/json'
                    },
                    json={
                        "model": "deepseek-chat",
                        "messages": [
                            {
                                "role": "system",
                                "content": self.ai_instructions
                            },
                            {
                                "role": "user",
                                "content": f"Analyze these {column_name} phone numbers:\n\n{phone_list}"
                            }
                        ],
                        "temperature": 0.1,
                        "max_tokens": min(len(batch_phones) * 10, 8000)
                    },
                    timeout=120
                )

                if response.status_code == 200:
                    result = response.json()
                    ai_response = result['choices'][0]['message']['content'].strip()

                    # Parse AI response for this batch
                    lines = ai_response.split('\n')

                    for i, line in enumerate(lines):
                        line = line.strip().lower()
                        if not line:
                            continue

                        # Map line position to original phone position
                        if i < len(batch_positions):
                            original_pos = batch_positions[i]

                            if 'mobile' in line:
                                results[original_pos] = 'mobile'
                            elif 'landline' in line:
                                results[original_pos] = 'landline'
                            else:
                                results[original_pos] = 'invalid'

                    logger.info(f"✅ Batch {batch_num + 1}/{total_batches} complete")

                else:
                    logger.error(f"❌ Batch {batch_num + 1} failed - API error {response.status_code}: {response.text}")
                    # Keep default 'invalid' for this batch

            except Exception as e:
                logger.error(f"❌ Error processing batch {batch_num + 1}: {e}")
                # Keep default 'invalid' for this batch

            # Small delay between batches to be respectful to the API
            import time
            time.sleep(0.5)

        logger.info(f"🎉 All {total_batches} batches processed for {column_name}")
        return results

    def _process_phone_pair(self, primary: str, secondary: str) -> Tuple[List[str], str]:
        """
        Process a pair of phone numbers and determine row handling

        Returns:
            Tuple[List[str], str]: (list_of_mobile_numbers, processing_action)
        """
        # Clean both numbers
        primary_clean = self._clean_phone_number(primary)
        secondary_clean = self._clean_phone_number(secondary)

        mobile_numbers = []

        # Validate primary phone
        if primary_clean:
            primary_analysis = self._validate_phone_with_ai(primary_clean)
            if primary_analysis['is_valid'] and primary_analysis['type'] == 'MOBILE':
                mobile_numbers.append(primary_analysis['phone_number'])

        # Validate secondary phone
        if secondary_clean:
            secondary_analysis = self._validate_phone_with_ai(secondary_clean)
            if secondary_analysis['is_valid'] and secondary_analysis['type'] == 'MOBILE':
                mobile_numbers.append(secondary_analysis['phone_number'])

        # Determine processing action
        if len(mobile_numbers) == 0:
            return [], "skip"  # No mobile numbers found
        elif len(mobile_numbers) == 1:
            return mobile_numbers, "single"  # One mobile number
        else:
            return mobile_numbers, "duplicate"  # Two mobile numbers - duplicate row

    def process_file(self, file_path: str, output_dir: str = "results") -> Dict:
        """
        Process a CSV file with Primary_Phone and Secondary_Phone columns
        Uses only 2 API calls total for optimal performance

        Args:
            file_path (str): Path to input CSV file
            output_dir (str): Output directory for results

        Returns:
            Dict: Processing results summary
        """
        logger.info(f"🔍 Starting phone number formatting for: {file_path}")

        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            logger.info(f"📊 Loaded {len(df)} records from CSV")

            # Validate required columns
            required_columns = ['Primary_Phone', 'Secondary_Phone']
            missing_columns = [col for col in required_columns if col not in df.columns]

            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            # Clean all phone numbers first
            logger.info("🧹 Cleaning phone numbers...")
            df['Primary_Clean'] = df['Primary_Phone'].fillna('').apply(self._clean_phone_number)
            df['Secondary_Clean'] = df['Secondary_Phone'].fillna('').apply(self._clean_phone_number)

            # Collect all unique phone numbers for batch processing
            primary_phones = df['Primary_Clean'].tolist()
            secondary_phones = df['Secondary_Clean'].tolist()

            logger.info("📱 Processing ALL phone numbers with AI validation (2 API calls only)...")

            # BATCH API CALL 1: Analyze ALL Primary phones
            primary_results = self._analyze_phone_batch(primary_phones, "Primary_Phone")

            # BATCH API CALL 2: Analyze ALL Secondary phones
            secondary_results = self._analyze_phone_batch(secondary_phones, "Secondary_Phone")

            # Process results
            processed_rows = []
            stats = {
                'total_input_rows': len(df),
                'rows_with_mobiles': 0,
                'rows_skipped': 0,
                'rows_duplicated': 0,
                'total_output_rows': 0,
                'mobile_numbers_found': 0
            }

            logger.info("� Processing results and applying mobile-only logic...")

            for index, row in df.iterrows():
                primary_type = primary_results[index] if index < len(primary_results) else 'invalid'
                secondary_type = secondary_results[index] if index < len(secondary_results) else 'invalid'

                mobile_numbers = []

                # Check primary phone
                if primary_type == 'mobile' and row['Primary_Clean']:
                    # Primary_Clean is already formatted as (XXX) XXX-XXXX
                    mobile_numbers.append(row['Primary_Clean'])

                # Check secondary phone
                if secondary_type == 'mobile' and row['Secondary_Clean']:
                    # Secondary_Clean is already formatted as (XXX) XXX-XXXX
                    # Only add if different from primary
                    if row['Secondary_Clean'] not in mobile_numbers:
                        mobile_numbers.append(row['Secondary_Clean'])

                # Apply mobile-only logic
                if len(mobile_numbers) == 0:
                    # No mobile numbers found - skip row
                    stats['rows_skipped'] += 1
                    logger.debug(f"⏭️  Row {index + 1}: No mobile numbers found")

                elif len(mobile_numbers) == 1:
                    # One mobile number found - single row
                    new_row = row.copy()
                    new_row['Phone_Number'] = mobile_numbers[0]
                    processed_rows.append(new_row)
                    stats['rows_with_mobiles'] += 1
                    stats['mobile_numbers_found'] += 1
                    logger.debug(f"📱 Row {index + 1}: Single mobile - {mobile_numbers[0]}")

                elif len(mobile_numbers) == 2:
                    # Two mobile numbers found - duplicate row
                    for mobile_num in mobile_numbers:
                        new_row = row.copy()
                        new_row['Phone_Number'] = mobile_num
                        processed_rows.append(new_row)
                    stats['rows_with_mobiles'] += 1
                    stats['rows_duplicated'] += 1
                    stats['mobile_numbers_found'] += 2
                    logger.debug(f"📱📱 Row {index + 1}: Dual mobile - duplicated for {mobile_numbers}")

            # Create output DataFrame
            if processed_rows:
                output_df = pd.DataFrame(processed_rows)

                # Remove original phone columns and temp columns
                columns_to_remove = ['Primary_Phone', 'Secondary_Phone', 'Primary_Clean', 'Secondary_Clean']
                output_df = output_df.drop(columns=[col for col in columns_to_remove if col in output_df.columns])

                stats['total_output_rows'] = len(output_df)

                # Generate output filename with timestamp
                input_filename = Path(file_path).stem
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_filename = f"Mobile_Formatted_{input_filename}_{timestamp}.csv"
                output_path = Path(output_dir) / output_filename

                # Ensure output directory exists
                output_path.parent.mkdir(parents=True, exist_ok=True)

                # Save the processed file
                output_df.to_csv(output_path, index=False)

                logger.info(f"💾 Mobile formatted file saved: {output_path}")

                # Log processing summary
                logger.info("📊 PROCESSING SUMMARY:")
                logger.info(f"  📥 Input rows: {stats['total_input_rows']}")
                logger.info(f"  📱 Rows with mobiles: {stats['rows_with_mobiles']}")
                logger.info(f"  ⏭️  Rows skipped: {stats['rows_skipped']}")
                logger.info(f"  📱📱 Rows duplicated: {stats['rows_duplicated']}")
                logger.info(f"  📤 Output rows: {stats['total_output_rows']}")
                logger.info(f"  📱 Mobile numbers found: {stats['mobile_numbers_found']}")

                return {
                    'success': True,
                    'output_file': str(output_path),
                    'stats': stats,
                    'message': f"Successfully processed {stats['total_input_rows']} rows, found {stats['mobile_numbers_found']} mobile numbers"
                }
            else:
                logger.warning("⚠️  No mobile numbers found in the entire file")
                return {
                    'success': False,
                    'message': "No mobile numbers found in the file",
                    'stats': stats
                }

        except Exception as e:
            logger.error(f"❌ Error processing file: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': f"Failed to process file: {e}"
            }

def main():
    """Test the phone formatter with sample data"""
    formatter = PhoneNumberFormatter()

    # Test with a sample file if it exists
    test_files = [
        "results/Merged_BROWARD_Built_1950-1951.csv",
        "results/phone_extraction_*.csv"
    ]

    for pattern in test_files:
        matching_files = list(Path(".").glob(pattern))
        if matching_files:
            test_file = matching_files[0]
            logger.info(f"🧪 Testing with file: {test_file}")
            result = formatter.process_file(str(test_file))
            logger.info(f"✅ Test result: {result}")
            break
    else:
        logger.info("📝 No test files found. Phone formatter is ready for use.")

if __name__ == "__main__":
    main()
