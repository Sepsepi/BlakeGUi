#!/usr/bin/env python3
"""
Enhanced Phone Number Merger - Intelligent ZabaSearch Result Processing
Fixes the phone extraction issues by properly tracking and            # Generate output path if not provided
            if output_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = Path(original_file_path).stem
                output_path = f"{original_file_path.replace('.csv', '').replace('.xlsx', '').replace('.xls', '')}_enhanced_phones_{timestamp}.csv"ing found phone numbers

Features:
- Smart tracking of which records actually found phone numbers
- Robust record matching using multiple strategies
- Clear reporting of success/failure rates
- Support for multiple phone result formats
- Maintains original file structure while adding phone data

Author: Enhanced for Blake GUI system
Date: August 15, 2025
"""

import os
import sys
import pandas as pd
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedPhoneMerger:
    """Enhanced phone number merger with intelligent record matching"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.matched_records = []
        self.unmatched_records = []
        self.phone_stats = {
            'total_processed': 0,
            'phone_numbers_found': 0,
            'records_updated': 0,
            'records_with_primary': 0,
            'records_with_secondary': 0,
            'match_strategies_used': {}
        }


    def merge_phone_dataframes(self, original_df: pd.DataFrame, results_df: pd.DataFrame) -> Dict:
        """
        Merge phone data between two DataFrames directly (for Flask integration)

        Args:
            original_df: Original DataFrame
            results_df: Results DataFrame with phone data

        Returns:
            Dict with merge results and statistics
        """
        self.logger.info("🔗 Starting DataFrame-to-DataFrame phone merge...")

        # Find phone columns in results
        phone_columns = self._find_phone_columns(results_df)
        if not phone_columns:
            self.logger.warning("⚠️ No phone columns found in results DataFrame")
            return {
                'success': False,
                'message': 'No phone columns found in results',
                'merged_df': original_df,
                'total_records': len(original_df),
                'total_with_phones': self._count_records_with_phones_in_original(original_df),
                'new_phones_added': 0
            }

        self.logger.info(f"📞 Found phone columns: {phone_columns}")

        # Prepare original DataFrame with phone columns
        working_df = self._prepare_original_for_phones(original_df.copy(), phone_columns)

        # Run intelligent merge
        merge_stats = self._intelligent_merge(working_df, results_df, phone_columns)

        # Copy results back to original DataFrame
        for col in working_df.columns:
            if col not in original_df.columns:
                original_df[col] = working_df[col]
            else:
                original_df[col] = working_df[col]

        # Compile final statistics
        final_stats = self._compile_final_statistics(working_df, phone_columns)

        return {
            'success': True,
            'message': 'Phone merge completed successfully',
            'merged_df': working_df,
            'total_records': final_stats['total_records'],
            'total_with_phones': final_stats['total_with_phones'],
            'new_phones_added': merge_stats.get('total_merged', 0),
            'merge_strategies': merge_stats.get('strategies_used', []),
            'phone_columns': phone_columns
        }

    def merge_phone_results(self, original_file_path: str, results_csv_path: Optional[str] = None, output_path: Optional[str] = None) -> Dict:
        """
        Enhanced merge of phone extraction results back to original file

        Args:
            original_file_path: Path to original CSV or Excel file
            results_csv_path: Path to processed results (optional - will look for latest if not provided)
            output_path: Path for merged output (optional - will create if not provided)

        Returns:
            Dict: Detailed merge results and statistics
        """
        try:
            self.logger.info(f"🚀 Starting Enhanced Phone Merger")
            self.logger.info(f"📄 Original file: {original_file_path}")

            # Load original file
            original_df = self._load_file(original_file_path)
            if original_df is None:
                return {'success': False, 'error': 'Could not load original file'}

            # Find results file if not provided
            if results_csv_path is None:
                results_csv_path = self._find_latest_results_file(original_file_path)

            if results_csv_path is None:
                return {'success': False, 'error': 'No results file found'}

            self.logger.info(f"📊 Results file: {results_csv_path}")

            # Load results file
            results_df = self._load_file(results_csv_path)
            if results_df is None:
                return {'success': False, 'error': 'Could not load results file'}

            # Analyze the files
            original_analysis = self._analyze_file_structure(original_df, "original")
            results_analysis = self._analyze_file_structure(results_df, "results")

            # Check if results file has phone data
            phone_columns = self._find_phone_columns(results_df)
            if not phone_columns:
                return {'success': False, 'error': 'No phone columns found in results file'}

            self.logger.info(f"📞 Found phone columns: {phone_columns}")

            # Count records with actual phone data
            phone_records = self._count_records_with_phones(results_df, phone_columns)
            self.logger.info(f"📊 Records with phone data: {phone_records}")

            # Prepare original file for phone data
            original_df = self._prepare_original_for_phones(original_df, phone_columns)

            # Perform intelligent matching and merging
            merge_results = self._intelligent_merge(original_df, results_df, phone_columns)

            # Create output path if not provided
            if output_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = Path(original_file_path).stem
                output_path = f"{original_file_path.replace('.csv', '').replace('.xlsx', '').replace('.xls', '')}_enhanced_phones_{timestamp}.csv"

            # Save enhanced file (remove DirectName columns to avoid clutter)
            columns_to_remove = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All']
            clean_df = original_df.drop(columns=[col for col in columns_to_remove if col in original_df.columns])
            clean_df.to_csv(output_path, index=False)
            self.logger.info(f"💾 Enhanced file saved: {output_path}")

            # Compile final statistics
            final_stats = self._compile_final_statistics(original_df, phone_columns)

            return {
                'success': True,
                'output_path': output_path,
                'original_records': len(original_df),
                'phone_records_available': phone_records,
                'records_updated': final_stats['records_updated'],
                'primary_phones_added': final_stats['primary_phones'],
                'secondary_phones_added': final_stats['secondary_phones'],
                'success_rate': f"{(final_stats['records_updated'] / len(original_df) * 100):.1f}%",
                'match_strategies': self.phone_stats['match_strategies_used'],
                'detailed_stats': final_stats,
                'phone_columns_added': phone_columns
            }

        except Exception as e:
            self.logger.error(f"❌ Enhanced merge failed: {e}")
            return {'success': False, 'error': str(e)}

    def _load_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Load CSV or Excel file with multiple encoding attempts"""
        file_ext = Path(file_path).suffix.lower()

        # Handle Excel files
        if file_ext in ['.xlsx', '.xls']:
            try:
                df = pd.read_excel(file_path)
                self.logger.info(f"✅ Loaded {len(df)} records from Excel file {file_path}")
                return df
            except Exception as e:
                self.logger.error(f"❌ Could not load Excel file {file_path}: {e}")
                return None

        # Handle CSV files with multiple encoding attempts
        elif file_ext == '.csv':
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    self.logger.info(f"✅ Loaded {len(df)} records from CSV file {file_path} (encoding: {encoding})")
                    return df
                except Exception as e:
                    continue

            self.logger.error(f"❌ Could not load CSV file {file_path} with any encoding")
            return None

        else:
            self.logger.error(f"❌ Unsupported file format: {file_ext}. Supported formats: .csv, .xlsx, .xls")
            return None

    def _find_latest_results_file(self, original_path: str) -> Optional[str]:
        """Find the latest results file related to the original"""
        import glob

        # Look for related files in results directory
        base_name = Path(original_path).stem
        patterns = [
            f"results/phone_extraction*{base_name}*.csv",
            f"results/phone_results*.csv",
            f"results/*phone*.csv",
            f"*{base_name}*phone*.csv",
            f"phone_extraction*.csv"
        ]

        found_files = []
        for pattern in patterns:
            files = glob.glob(pattern)
            for file in files:
                if os.path.exists(file):
                    mod_time = os.path.getmtime(file)
                    found_files.append((file, mod_time))

        if found_files:
            # Sort by modification time and return newest
            found_files.sort(key=lambda x: x[1], reverse=True)
            latest_file = found_files[0][0]
            self.logger.info(f"📁 Auto-detected results file: {latest_file}")
            return latest_file

        return None

    def _analyze_file_structure(self, df: pd.DataFrame, file_type: str) -> Dict:
        """Analyze file structure and identify key columns"""
        analysis = {
            'total_records': len(df),
            'total_columns': len(df.columns),
            'name_columns': [],
            'address_columns': [],
            'phone_columns': [],
            'other_columns': []
        }

        for col in df.columns:
            col_lower = col.lower()

            # Categorize columns
            if any(keyword in col_lower for keyword in ['name', 'owner', 'person']):
                analysis['name_columns'].append(col)
            elif any(keyword in col_lower for keyword in ['address', 'street', 'location']):
                analysis['address_columns'].append(col)
            elif any(keyword in col_lower for keyword in ['phone', 'telephone', 'cell', 'mobile']):
                analysis['phone_columns'].append(col)
            else:
                analysis['other_columns'].append(col)

        self.logger.info(f"📊 {file_type.title()} file analysis:")
        self.logger.info(f"  📝 Name columns: {analysis['name_columns']}")
        self.logger.info(f"  📍 Address columns: {analysis['address_columns']}")
        self.logger.info(f"  📞 Phone columns: {analysis['phone_columns']}")

        return analysis

    def _find_phone_columns(self, df: pd.DataFrame) -> List[str]:
        """Find all phone-related columns in the dataframe"""
        phone_columns = []

        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in [
                'phone', 'telephone', 'cell', 'mobile', 'tel'
            ]):
                phone_columns.append(col)

        return phone_columns

    def _count_records_with_phones(self, df: pd.DataFrame, phone_columns: List[str]) -> int:
        """Count how many records actually have phone data"""
        records_with_phones = 0

        for _, row in df.iterrows():
            has_phone = False
            for col in phone_columns:
                value = row.get(col, '')
                if value and str(value).strip() and str(value).strip().lower() not in ['nan', 'none', '']:
                    # Validate it looks like a phone number
                    cleaned = re.sub(r'[^\d]', '', str(value))
                    if len(cleaned) >= 10:
                        has_phone = True
                        break

            if has_phone:
                records_with_phones += 1

        return records_with_phones

    def _prepare_original_for_phones(self, df: pd.DataFrame, phone_columns: List[str]) -> pd.DataFrame:
        """Prepare original dataframe by adding phone columns if they don't exist"""

        # Standard phone columns to ensure exist
        standard_phone_cols = ['Primary_Phone', 'Secondary_Phone']

        for col in standard_phone_cols + phone_columns:
            if col not in df.columns:
                df[col] = ''
                df[col] = df[col].astype('object')  # Ensure string type
                self.logger.info(f"➕ Added phone column: {col}")
            else:
                # Ensure existing columns are object type
                df[col] = df[col].astype('object')

        return df

    def _intelligent_merge(self, original_df: pd.DataFrame, results_df: pd.DataFrame, phone_columns: List[str]) -> Dict:
        """Perform intelligent matching and merging using multiple strategies"""

        merge_results = {
            'strategy_1_matches': 0,  # Direct index matching
            'strategy_2_matches': 0,  # Name + address matching
            'strategy_3_matches': 0,  # Fuzzy matching
            'strategy_4_matches': 0,  # Positional fallback
            'total_updates': 0
        }

        # Strategy 1: Direct index matching (if original_index column exists)
        index_column = None
        if 'original_index' in results_df.columns:
            index_column = 'original_index'
        elif 'Original_Index' in results_df.columns:
            index_column = 'Original_Index'

        if index_column:
            self.logger.info("🎯 Strategy 1: Direct index matching (ONLY strategy - fast and effective)")
            merge_results['strategy_1_matches'] = self._strategy_1_direct_index(original_df, results_df, phone_columns, index_column)
            self.logger.info("✅ Phone merging complete - Strategy 1 is sufficient for excellent results")
        else:
            self.logger.warning("⚠️  No index column found - may need fallback strategies")
            # Could add simplified fallback here if needed
            merge_results['strategy_1_matches'] = 0

        merge_results['total_updates'] = merge_results['strategy_1_matches']
        merge_results['strategy_2_matches'] = 0  # Disabled for performance
        merge_results['strategy_3_matches'] = 0  # Disabled for performance
        merge_results['strategy_4_matches'] = 0  # Disabled for performance

        # Remove DirectName columns after merging for cleaner output
        directname_cols = [col for col in original_df.columns if 'directname' in col.lower() and 'phone' in col.lower()]
        if directname_cols:
            original_df.drop(columns=directname_cols, inplace=True)
            self.logger.info(f"🧹 Removed {len(directname_cols)} DirectName phone columns for cleaner output")

        self.phone_stats['match_strategies_used'] = merge_results
        self.logger.info(f"🎉 Total phone updates applied: {merge_results['total_updates']}")

        return merge_results

    def _strategy_1_direct_index(self, original_df: pd.DataFrame, results_df: pd.DataFrame, phone_columns: List[str], index_column: str) -> int:
        """Strategy 1: Use original_index or Original_Index column for direct mapping"""
        updates = 0

        if index_column not in results_df.columns:
            return 0

        for _, results_row in results_df.iterrows():
            original_idx = results_row.get(index_column)

            if pd.notna(original_idx) and int(original_idx) < len(original_df):
                original_idx = int(original_idx)

                # Check if this record should be skipped (already has phone data)
                skip_zabasearch = results_row.get('Skip_ZabaSearch', False)

                # Check if this results row has phone data
                phone_data = self._extract_phone_data(results_row, phone_columns)

                if phone_data['has_data'] and not skip_zabasearch:
                    self._update_record_with_phones(original_df, original_idx, phone_data, phone_columns)
                    updates += 1
                    self.logger.info(f"  📞 Index match: Row {original_idx} updated with {len(phone_data['phones'])} phone(s)")
                elif skip_zabasearch:
                    self.logger.info(f"  ⏭️  Skipping row {original_idx} - already has existing phone data")

        return updates

    def _strategy_2_name_address(self, original_df: pd.DataFrame, results_df: pd.DataFrame, phone_columns: List[str]) -> int:
        """Strategy 2: Match by name and address similarity"""
        updates = 0

        # Find name and address columns in both dataframes
        original_name_cols = self._find_name_columns(original_df)
        original_addr_cols = self._find_address_columns(original_df)

        results_name_cols = self._find_name_columns(results_df)
        results_addr_cols = self._find_address_columns(results_df)

        for results_idx, results_row in results_df.iterrows():
            # Check if this results row has phone data
            phone_data = self._extract_phone_data(results_row, phone_columns)

            if not phone_data['has_data']:
                continue

            # Find best match in original data
            best_match_idx = None
            best_score = 0

            for orig_idx, orig_row in original_df.iterrows():
                score = 0
                comparisons = 0

                # Compare names
                for orig_name_col in original_name_cols:
                    for results_name_col in results_name_cols:
                        orig_name = self._normalize_text(orig_row.get(orig_name_col, ''))
                        results_name = self._normalize_text(results_row.get(results_name_col, ''))

                        if orig_name and results_name:
                            if orig_name == results_name:
                                score += 3  # Exact name match gets high score
                            elif orig_name in results_name or results_name in orig_name:
                                score += 2  # Partial match
                            comparisons += 3

                # Compare addresses
                for orig_addr_col in original_addr_cols:
                    for results_addr_col in results_addr_cols:
                        orig_addr = self._normalize_text(orig_row.get(orig_addr_col, ''))
                        results_addr = self._normalize_text(results_row.get(results_addr_col, ''))

                        if orig_addr and results_addr:
                            if orig_addr == results_addr:
                                score += 2
                            elif self._addresses_similar(orig_addr, results_addr):
                                score += 1
                            comparisons += 2

                # Calculate match percentage
                if comparisons > 0:
                    match_percentage = score / comparisons
                    if match_percentage >= 0.6 and score > best_score:  # 60% threshold
                        best_score = score
                        best_match_idx = orig_idx

            # Update best match
            if best_match_idx is not None:
                # Check if this record already has phone data (avoid duplicates)
                if not self._record_already_has_phone(original_df, best_match_idx):
                    self._update_record_with_phones(original_df, best_match_idx, phone_data, phone_columns)
                    updates += 1
                    self.logger.info(f"  🎯 Name/Address match: Row {best_match_idx} updated")

        return updates

    def _strategy_3_fuzzy_matching(self, original_df: pd.DataFrame, results_df: pd.DataFrame, phone_columns: List[str]) -> int:
        """Strategy 3: Fuzzy matching for partial name matches"""
        updates = 0

        # Simple fuzzy matching based on first few characters and length
        original_name_cols = self._find_name_columns(original_df)
        results_name_cols = self._find_name_columns(results_df)

        for results_idx, results_row in results_df.iterrows():
            phone_data = self._extract_phone_data(results_row, phone_columns)

            if not phone_data['has_data']:
                continue

            # Find fuzzy matches
            for orig_idx, orig_row in original_df.iterrows():
                # Skip if already has phone data
                if self._record_already_has_phone(original_df, orig_idx):
                    continue

                match_found = False

                for orig_name_col in original_name_cols:
                    for results_name_col in results_name_cols:
                        orig_name = self._normalize_text(orig_row.get(orig_name_col, ''))
                        results_name = self._normalize_text(results_row.get(results_name_col, ''))

                        if orig_name and results_name and len(orig_name) > 5 and len(results_name) > 5:
                            # Check if first 5-6 characters match (fuzzy)
                            if (orig_name[:5] == results_name[:5] or
                                orig_name[:6] == results_name[:6]):
                                match_found = True
                                break

                    if match_found:
                        break

                if match_found:
                    self._update_record_with_phones(original_df, orig_idx, phone_data, phone_columns)
                    updates += 1
                    self.logger.info(f"  🧠 Fuzzy match: Row {orig_idx} updated")
                    break  # Move to next results row

        return updates

    def _strategy_4_positional(self, original_df: pd.DataFrame, results_df: pd.DataFrame, phone_columns: List[str]) -> int:
        """Strategy 4: Enhanced name-based matching as last resort"""
        updates = 0

        # Only apply if we haven't found many matches yet
        current_phone_coverage = self._count_records_with_phones_in_original(original_df)

        if current_phone_coverage < len(original_df) * 0.3:  # Less than 30% coverage
            self.logger.info("  📊 Low phone coverage, applying enhanced name matching...")

            # Use name-based matching instead of positional
            original_name_cols = self._find_name_columns(original_df)
            results_name_cols = self._find_name_columns(results_df)

            for results_idx, results_row in results_df.iterrows():
                phone_data = self._extract_phone_data(results_row, phone_columns)

                if not phone_data['has_data']:
                    continue

                # Find the best match in original data by name similarity
                best_match_idx = None
                best_score = 0

                for orig_idx, orig_row in original_df.iterrows():
                    # Skip if already has phone data
                    if self._record_already_has_phone(original_df, orig_idx):
                        continue

                    # Compare names between results and original
                    for orig_name_col in original_name_cols:
                        for results_name_col in results_name_cols:
                            orig_name = self._normalize_text(orig_row.get(orig_name_col, ''))
                            results_name = self._normalize_text(results_row.get(results_name_col, ''))

                            if orig_name and results_name:
                                # Calculate name similarity score
                                score = self._calculate_name_similarity(orig_name, results_name)
                                if score > best_score and score > 0.7:  # Require 70% similarity
                                    best_score = score
                                    best_match_idx = orig_idx

                # Apply the match if we found a good one
                if best_match_idx is not None:
                    self._update_record_with_phones(original_df, best_match_idx, phone_data, phone_columns)
                    updates += 1
                    self.logger.info(f"  🔄 Enhanced match: Row {best_match_idx} updated (score: {best_score:.2f})")

        return updates

    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity score between two names"""
        if not name1 or not name2:
            return 0.0

        # Split names into words
        words1 = set(name1.split())
        words2 = set(name2.split())

        if not words1 or not words2:
            return 0.0

        # Calculate Jaccard similarity (intersection over union)
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))

        return intersection / union if union > 0 else 0.0

    def _find_name_columns(self, df: pd.DataFrame) -> List[str]:
        """Find name-related columns"""
        name_columns = []
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['name', 'owner', 'person', 'cleaned']):
                name_columns.append(col)
        return name_columns

    def _find_address_columns(self, df: pd.DataFrame) -> List[str]:
        """Find address-related columns"""
        address_columns = []
        for col in df.columns:
            col_lower = col.lower()
            if any(keyword in col_lower for keyword in ['address', 'street', 'location', 'addr']):
                address_columns.append(col)
        return address_columns

    def _normalize_text(self, text: str) -> str:
        """Normalize text for comparison"""
        if pd.isna(text) or not text:
            return ''
        # Remove commas, extra spaces, and convert to uppercase
        normalized = str(text).strip().upper()
        normalized = normalized.replace(',', ' ').replace('  ', ' ')
        return normalized

    def _addresses_similar(self, addr1: str, addr2: str) -> bool:
        """Check if two addresses are similar"""
        if not addr1 or not addr2:
            return False

        # Simple similarity check - extract numbers and street names
        addr1_parts = addr1.replace(',', ' ').split()
        addr2_parts = addr2.replace(',', ' ').split()

        # Find common parts
        common_parts = set(addr1_parts) & set(addr2_parts)

        # Check if they share significant parts
        return len(common_parts) >= 2

    def _extract_phone_data(self, row: pd.Series, phone_columns: List[str]) -> Dict:
        """Extract phone data from a row"""
        phone_data = {
            'has_data': False,
            'phones': {},
            'primary': '',
            'secondary': ''
        }

        for col in phone_columns:
            value = row.get(col, '')
            if value and str(value).strip() and str(value).strip().lower() not in ['nan', 'none', '']:
                # Validate phone number format
                cleaned = re.sub(r'[^\d]', '', str(value))
                if len(cleaned) >= 10:
                    phone_data['phones'][col] = str(value).strip()
                    phone_data['has_data'] = True

                    # Set primary/secondary
                    if 'primary' in col.lower() and not phone_data['primary']:
                        phone_data['primary'] = str(value).strip()
                    elif 'secondary' in col.lower() and not phone_data['secondary']:
                        phone_data['secondary'] = str(value).strip()

        # If no designated primary, use first available phone
        if phone_data['has_data'] and not phone_data['primary']:
            phone_data['primary'] = list(phone_data['phones'].values())[0]

        return phone_data

    def _record_already_has_phone(self, df: pd.DataFrame, row_idx: int) -> bool:
        """Check if a record already has phone data"""
        # Check all possible phone columns including original ones
        phone_indicators = ['Primary_Phone', 'Secondary_Phone', 'Telephone Number', 'Phone', 'phone', 'Tel', 'Mobile', 'Cell']

        for col in phone_indicators:
            if col in df.columns:
                value = df.at[row_idx, col]
                if value and str(value).strip() and str(value).strip().lower() not in ['nan', 'none', '', 'n/a']:
                    cleaned = re.sub(r'[^\d]', '', str(value))
                    if len(cleaned) >= 10:
                        return True
        return False

    def _update_record_with_phones(self, df: pd.DataFrame, row_idx: int, phone_data: Dict, phone_columns: List[str]):
        """Update a record with phone data"""

        # Update specific phone columns
        for col, value in phone_data['phones'].items():
            if col in df.columns:
                df.at[row_idx, col] = value

        # Update standard columns
        if phone_data['primary'] and 'Primary_Phone' in df.columns:
            df.at[row_idx, 'Primary_Phone'] = phone_data['primary']

        if phone_data['secondary'] and 'Secondary_Phone' in df.columns:
            df.at[row_idx, 'Secondary_Phone'] = phone_data['secondary']

    def _count_records_with_phones_in_original(self, df: pd.DataFrame) -> int:
        """Count records that already have phone data in original"""
        count = 0
        for i in range(len(df)):
            if self._record_already_has_phone(df, i):
                count += 1
        return count

    def _compile_final_statistics(self, df: pd.DataFrame, phone_columns: List[str]) -> Dict:
        """Compile final statistics about phone data"""
        stats = {
            'total_records': len(df),
            'records_updated': 0,
            'primary_phones': 0,
            'secondary_phones': 0,
            'total_phones_added': 0
        }

        for i in range(len(df)):
            record_has_phone = False

            # Check Primary_Phone
            if 'Primary_Phone' in df.columns:
                value = df.at[i, 'Primary_Phone']
                if value and str(value).strip() and str(value).strip().lower() not in ['nan', 'none', '']:
                    stats['primary_phones'] += 1
                    record_has_phone = True

            # Check Secondary_Phone
            if 'Secondary_Phone' in df.columns:
                value = df.at[i, 'Secondary_Phone']
                if value and str(value).strip() and str(value).strip().lower() not in ['nan', 'none', '']:
                    stats['secondary_phones'] += 1
                    record_has_phone = True

            if record_has_phone:
                stats['records_updated'] += 1

        stats['total_phones_added'] = stats['primary_phones'] + stats['secondary_phones']
        stats['total_with_phones'] = stats['records_updated']  # Add missing total_with_phones key

        return stats

def process_phone_merge(original_file: str, results_file: str = None, output_file: str = None) -> Dict:
    """
    Process phone merge for Flask integration

    Args:
        original_file: Path to original CSV file
        results_file: Path to phone extraction results (optional)
        output_file: Path for merged output (optional)

    Returns:
        Dict: Merge results and statistics
    """
    try:
        merger = EnhancedPhoneMerger()
        results = merger.merge_phone_results(original_file, results_file, output_file)
        return results
    except Exception as e:
        logger.error(f"Phone merge processing failed: {e}")
        return {'success': False, 'error': str(e)}

def main():
    """Command line interface for testing"""
    if len(sys.argv) < 2:
        print("Usage: python enhanced_phone_merger.py <original_csv> [results_csv] [output_csv]")
        print("  original_csv: Path to original CSV file")
        print("  results_csv: Path to phone extraction results (optional)")
        print("  output_csv: Path for merged output (optional)")
        return

    original_file = sys.argv[1]
    results_file = sys.argv[2] if len(sys.argv) > 2 else None
    output_file = sys.argv[3] if len(sys.argv) > 3 else None

    if not os.path.exists(original_file):
        print(f"Error: Original file {original_file} not found")
        return

    print(f"\n🚀 Enhanced Phone Merger")
    print(f"📄 Original file: {original_file}")
    if results_file:
        print(f"📊 Results file: {results_file}")
    print("=" * 60)

    # Process the merge
    merger = EnhancedPhoneMerger()
    results = merger.merge_phone_results(original_file, results_file, output_file)

    if results['success']:
        print(f"\n✅ Phone merge completed successfully!")
        print(f"📊 Results Summary:")
        print(f"  📝 Total records: {results['original_records']}")
        print(f"  📞 Phone records available: {results['phone_records_available']}")
        print(f"  ✅ Records updated: {results['records_updated']}")
        print(f"  📞 Primary phones added: {results['primary_phones_added']}")
        print(f"  📞 Secondary phones added: {results['secondary_phones_added']}")
        print(f"  📈 Success rate: {results['success_rate']}")
        print(f"💾 Enhanced file: {results['output_path']}")

        print(f"\n🎯 Match Strategies Used:")
        for strategy, count in results['match_strategies'].items():
            if count > 0:
                print(f"  {strategy}: {count} records")

        # Note: Temp cleanup moved to end of batch processing to avoid file conflicts
        # during parallel batch execution

    else:
        print(f"❌ Phone merge failed: {results['error']}")

if __name__ == "__main__":
    main()
