#!/usr/bin/env python3
"""
Phone Search Pipeline - Enhanced ZabaSearch Automation
Direct processing with improved error handling and robust                             # 🚀 SIMPLIFIED BATCH SIZING for optimal resource management
                            if records_needing_processing < 100:
                                batch_count = 4  # Small files: 4 batches
                                self.logger.info("📊 Using 4 batches for small file (<100 records)")
                            else:
                                batch_count = 10  # Large files: 10 batches
                                self.logger.info("📊 Using 10 batches for large file (100+ records)")ssing

Features:
- Direct CSV processing without job queuing
- Improved address parsing and city detection
- Enhanced error handling and retry logic
- Support for multiple CSV formats (Broward, generic)
- Real-time logging and progress tracking
- Format-preserving output (maintains column structure)
"""

import os
import sys
import asyncio
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional

# Import proxy manager (AI will use direct connection, ZabaSearch will use proxies)
try:
    from proxy_manager import proxy_manager
    print("🔒 Proxy manager loaded - Smart proxy routing enabled")
    print("📡 AI APIs: Direct connection | 🕸️ ZabaSearch: Proxy connection")
except ImportError:
    print("⚠️ Proxy manager not available - all connections direct")
    proxy_manager = None

def read_data_file(filepath, encoding='utf-8', sheet_name=0):
    """
    Universal file reader for CSV, Excel (.xlsx), and Excel (.xls) files

    Args:
        filepath: Path to the file
        encoding: Encoding for CSV files (default: utf-8)
        sheet_name: Sheet name or index for Excel files (default: 0 - first sheet)

    Returns:
        pd.DataFrame: Loaded data
    """
    try:
        if filepath.endswith('.csv'):
            return pd.read_csv(filepath, encoding=encoding)
        elif filepath.endswith(('.xlsx', '.xls')):
            return pd.read_excel(filepath, sheet_name=sheet_name)
        else:
            # Fallback to CSV
            logging.warning(f"Unknown file extension for {filepath}, trying CSV format")
            return pd.read_csv(filepath, encoding=encoding)
    except Exception as e:
        logging.error(f"Error reading file {filepath}: {e}")
        raise

# Import our processing modules
try:
    from intelligent_phone_formatter_v2 import IntelligentPhoneFormatter
    AI_FORMATTER_AVAILABLE = True
    print("✅ AI Phone Formatter: Available")
except ImportError as e:
    print(f"❌ AI Phone Formatter: Not available - {e}")
    AI_FORMATTER_AVAILABLE = False

# Setup logging to logs folder
log_folder = Path('logs')
log_folder.mkdir(exist_ok=True)
log_file = log_folder / f'phone_search_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class PhoneSearchPipeline:
    """Enhanced phone search pipeline with direct processing"""

    def __init__(self, user_config: Optional[dict] = None):
        self.logger = logging.getLogger(__name__)
        self.ai_formatter = IntelligentPhoneFormatter() if AI_FORMATTER_AVAILABLE else None
        
        # Set up paths - use user-specific if provided, otherwise defaults
        if user_config:
            self.results_folder = user_config.get('RESULTS_FOLDER', 'results')
            self.temp_folder = user_config.get('TEMP_FOLDER', 'temp')
            self.logs_folder = user_config.get('LOGS_FOLDER', 'logs')
        else:
            self.results_folder = 'results'
            self.temp_folder = 'temp'
            self.logs_folder = 'logs'

        # Ensure directories exist
        os.makedirs(self.results_folder, exist_ok=True)
        os.makedirs(self.temp_folder, exist_ok=True)
        os.makedirs(self.logs_folder, exist_ok=True)

        self.logger.info(f"Phone Search Pipeline initialized with results: {self.results_folder}, temp: {self.temp_folder}")

    def process_csv_direct(self, csv_path: str, output_path: str, max_records: Optional[int] = None) -> bool:
        """
        Process CSV file directly with ZabaSearch automation
        For large files (>100 records needing processing), splits into batches for parallel processing

        Args:
            csv_path: Path to input CSV file
            output_path: Path for output file
            max_records: Maximum number of records to process

        Returns:
            bool: True if processing completed successfully
        """
        try:
            self.logger.info(f"🚀 Starting direct processing: {csv_path}")
            self.logger.info(f"📊 Max records to process: {max_records}")

            # Read and format with AI-powered formatter
            if self.ai_formatter:
                self.logger.info("🤖 Running AI-powered phone data formatting...")

                # Use AI formatter to process the file and prepare ZabaSearch-ready data
                format_result = self.ai_formatter.format_csv_for_phone_extraction(csv_path)

                if format_result.get('success'):
                    self.logger.info("✅ AI formatting completed successfully")
                    self.logger.info(f"📊 Records processed: {format_result.get('records_processed', 0)}")
                    self.logger.info(f"📊 Records skipped: {format_result.get('records_skipped', 0)}")

                    # Read the formatted data
                    formatted_path = format_result.get('output_path')
                    if formatted_path and os.path.exists(formatted_path):
                        # Check if we need batch processing
                        df = pd.read_csv(formatted_path)
                        records_needing_processing = len(df[df.get('Skip_ZabaSearch', False) != True])

                        if records_needing_processing > 100:
                            self.logger.info(f"🔄 Large dataset detected: {records_needing_processing} records need processing")
                            
                            # 🚀 OPTIMIZED BATCH SIZING: 1 batch for ≤30 records, 15 batches for >30
                            if records_needing_processing <= 30:
                                batch_count = 1  # Small files: Single batch (fastest, no overhead)
                                self.logger.info("📊 Using 1 batch for small file (≤30 records) - single thread processing")
                            else:
                                batch_count = 15  # Large files: 15 batches (maximum parallel processing)
                                self.logger.info("📊 Using 15 batches for large file (>30 records) - maximum parallelization")

                            self.logger.info("🚀 Initiating multi-terminal batch processing...")
                            return self._process_in_batches(formatted_path, output_path, records_needing_processing, csv_path, batch_count)
                        else:
                            self.logger.info(f"📊 Standard processing: {records_needing_processing} records")
                            # Continue with normal processing
                        df = read_data_file(formatted_path)
                        self.logger.info(f"✅ Loaded {len(df)} AI-formatted records for ZabaSearch processing")
                        process_df = df  # Use all AI-formatted records
                    else:
                        self.logger.warning("⚠️ AI formatter succeeded but no output file found")
                        df = read_data_file(csv_path)
                        process_df = df.copy()  # Use all records
                else:
                    self.logger.warning(f"⚠️ AI formatting failed: {format_result.get('error', 'Unknown error')}")
                    self.logger.info("📄 Falling back to direct file processing...")
                    df = read_data_file(csv_path)
                    process_df = df.copy()  # Use all records
            else:
                self.logger.info("📄 AI formatter not available, reading file directly...")
                df = read_data_file(csv_path)
                process_df = df.copy()  # Use all records

            if df is None or len(df) == 0:
                self.logger.error("❌ No data found in file")
                return False

            self.logger.info(f"📊 Total records available for processing: {len(process_df)}")

            # Run ZabaSearch processing with the prepared data
            # Pass the original csv_path so merger can access the full original file
            success = self._run_zabasearch_processing(process_df, output_path, csv_path)

            if success:
                self.logger.info(f"✅ Processing completed successfully: {output_path}")
                
                # Clean up temp folder after single file processing
                try:
                    from file_cleanup import cleanup_temp_folder
                    temp_cleanup_result = cleanup_temp_folder()
                    if temp_cleanup_result['files_deleted'] > 0:
                        self.logger.info(f"🧹 Temp cleanup: {temp_cleanup_result['files_deleted']} files deleted, {temp_cleanup_result['size_freed_mb']:.2f} MB freed")
                except Exception as cleanup_error:
                    self.logger.warning(f"⚠️ Temp cleanup failed: {cleanup_error}")
                
                return True
            else:
                self.logger.error("❌ ZabaSearch processing failed")
                # Save processed data anyway
                columns_to_drop = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All']
                final_process_df = process_df.drop(columns=[col for col in columns_to_drop if col in process_df.columns])
                final_process_df.to_csv(output_path, index=False)
                return True

        except Exception as e:
            self.logger.error(f"❌ Pipeline processing failed: {e}")
            return False

    def _process_in_batches(self, formatted_path: str, output_path: str, total_records: int, original_csv_path: str, batch_count: int = 10) -> bool:
        """
        Process large files in batches using multiple terminals

        Args:
            formatted_path: Path to the formatted CSV file
            output_path: Final output path
            total_records: Total number of records needing processing
            original_csv_path: Path to the original input CSV file

        Returns:
            bool: True if batch processing completed successfully
        """
        try:
            import subprocess
            import threading
            import time

            self.logger.info("🔄 Setting up batch processing...")

            # Read the full dataset
            df = pd.read_csv(formatted_path)

            # FIXED: Don't filter here - let ZabaSearch handle Skip_ZabaSearch logic
            # This preserves Original_Index alignment for proper merging
            self.logger.info(f"📊 Total records for batch processing: {len(df)}")
            records_needing_processing = len(df[df.get('Skip_ZabaSearch', False) != True])
            self.logger.info(f"📞 Records that need phone extraction: {records_needing_processing}")
            self.logger.info(f"⏭️  Records to skip (already have phones): {len(df) - records_needing_processing}")

            # Split ALL records (including skipped ones) into dynamic batches to preserve indexing
            batch_size = len(df) // batch_count
            batches = []

            for i in range(batch_count):
                start_idx = i * batch_size
                if i == batch_count - 1:  # Last batch gets remainder
                    end_idx = len(df)
                else:
                    end_idx = (i + 1) * batch_size

                batch_df = df.iloc[start_idx:end_idx]
                if len(batch_df) > 0:
                    batches.append(batch_df)

            self.logger.info(f"📊 Created {len(batches)} batches for parallel processing")

            # Create batch files and run headless processing
            batch_files = []
            batch_outputs = []

            for i, batch_df in enumerate(batches):
                batch_filename = f"batch_{i+1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                batch_path = os.path.join(self.temp_folder, batch_filename)

                # Save batch file
                batch_df.to_csv(batch_path, index=False)
                batch_files.append(batch_path)

                # Create output path for this batch using user-specific results folder
                batch_output = os.path.join(self.results_folder, f"phone_results_batch_{i+1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                batch_outputs.append(batch_output)

            # Run batches using headless processing (import zabasearch module directly)
            self.logger.info("🚀 Starting headless batch processing...")
            success = self._run_headless_batches(batch_files, batch_outputs)

            # ENHANCED: Combine successful batch results even if some batches failed
            self.logger.info("🔄 Combining available batch results...")
            combined_df = pd.DataFrame()
            successful_batches = 0

            for i, (batch_file, batch_output) in enumerate(zip(batch_files, batch_outputs)):
                # Try to read from output file first, then from batch file (processed in-place)
                result_file = None
                if os.path.exists(batch_output):
                    result_file = batch_output
                    self.logger.info(f"   📂 Batch {i+1}: Using output file {batch_output}")
                elif os.path.exists(batch_file):
                    result_file = batch_file
                    self.logger.info(f"   📂 Batch {i+1}: Using processed batch file {batch_file}")
                else:
                    # FALLBACK: Look for backup files in temp folder for interrupted processing
                    batch_name = os.path.basename(batch_file).replace('.csv', '')
                    temp_pattern = os.path.join(self.temp_folder, f"{batch_name}_backup_after_*.csv")
                    import glob
                    backup_files = glob.glob(temp_pattern)
                    if backup_files:
                        # Use the latest backup file
                        latest_backup = max(backup_files, key=os.path.getmtime)
                        result_file = latest_backup
                        self.logger.info(f"   🔄 Batch {i+1}: Using backup file {latest_backup}")
                
                if result_file:
                    try:
                        batch_result = pd.read_csv(result_file)
                        if len(batch_result) > 0:  # Only combine if batch has data
                            combined_df = pd.concat([combined_df, batch_result], ignore_index=True)
                            successful_batches += 1
                            self.logger.info(f"   ✅ Combined Batch {i+1}: {len(batch_result)} records")
                        else:
                            self.logger.warning(f"   ⚠️ Batch {i+1} is empty - skipping")
                    except Exception as e:
                        self.logger.error(f"   ❌ Failed to read Batch {i+1}: {e}")
                else:
                    self.logger.warning(f"   ⚠️ Batch {i+1} - no result files found - skipping")

            # Save combined results if we have any successful batches
            if len(combined_df) > 0:
                self.logger.info(f"✅ Combined results from {successful_batches}/{len(batch_outputs)} batches")
                self.logger.info(f"✅ Total records combined: {len(combined_df)}")
                
                # AUTO-MERGE: Apply enhanced phone merger for batch results
                try:
                    from enhanced_phone_merger import EnhancedPhoneMerger
                    self.logger.info("🔗 Auto-merging batch phone data with enhanced merger...")

                    # Read original data for merging
                    original_df = read_data_file(original_csv_path)

                    # Use enhanced merger to merge DataFrames directly
                    merger = EnhancedPhoneMerger()
                    merge_result = merger.merge_phone_dataframes(original_df, combined_df)

                    if merge_result and merge_result.get('success'):
                        self.logger.info(f"✅ Enhanced phone merger applied successfully:")
                        self.logger.info(f"   📞 Total records with phones: {merge_result.get('total_with_phones', 0)}")
                        self.logger.info(f"   🆕 New phone numbers found: {merge_result.get('new_phones_added', 0)}")
                        self.logger.info(f"   📊 Records processed: {merge_result.get('total_records', 0)}")

                        # Save the merged DataFrame
                        merged_df = merge_result.get('merged_df')
                        if merged_df is not None:
                            # KEEP DirectName_Phone and IndirectName_Phone columns (needed by Radaris)
                            # DO NOT drop these columns - Radaris checks them to avoid reprocessing
                            merged_df.to_csv(output_path, index=False)
                            self.logger.info(f"✅ Enhanced merged results saved to: {output_path}")
                        else:
                            # Fallback: Save combined results without enhanced merging
                            self.logger.warning("⚠️ Enhanced merger returned None, using combined results")
                            combined_df.to_csv(output_path, index=False)
                    else:
                        # Fallback: Save combined results without enhanced merging
                        self.logger.warning("⚠️ Enhanced merger didn't complete successfully, using combined results")
                        combined_df.to_csv(output_path, index=False)

                except Exception as merge_error:
                    self.logger.error(f"❌ Enhanced phone merger failed: {merge_error}")
                    self.logger.info("📞 Falling back to combined batch results...")
                    # Fallback: Save combined results without enhanced merging
                    combined_df.to_csv(output_path, index=False)

                # RADARIS FOLLOW-UP STAGE: Process records without phones
                self.logger.info("\n" + "="*80)
                self.logger.info("🔍 RADARIS FOLLOW-UP STAGE")
                self.logger.info("="*80)
                self.logger.info("📋 Checking for records that still need phone numbers...")

                try:
                    radaris_success = self._run_radaris_batches(output_path, original_csv_path)
                    if radaris_success:
                        self.logger.info("✅ Radaris follow-up stage completed successfully")
                    else:
                        self.logger.warning("⚠️ Radaris follow-up stage completed with warnings")
                except Exception as radaris_error:
                    self.logger.error(f"❌ Radaris follow-up stage failed: {radaris_error}")
                    self.logger.info("📞 Continuing with ZabaSearch results only...")

                # Cleanup batch files
                for batch_file in batch_files:
                    if os.path.exists(batch_file):
                        os.remove(batch_file)
                for batch_output in batch_outputs:
                    if os.path.exists(batch_output):
                        os.remove(batch_output)

                # Clean up temp folder after all batches are complete
                try:
                    from file_cleanup import cleanup_temp_folder
                    temp_cleanup_result = cleanup_temp_folder()
                    if temp_cleanup_result['files_deleted'] > 0:
                        self.logger.info(f"🧹 Final temp cleanup: {temp_cleanup_result['files_deleted']} files deleted, {temp_cleanup_result['size_freed_mb']:.2f} MB freed")
                except Exception as cleanup_error:
                    self.logger.warning(f"⚠️ Temp cleanup failed: {cleanup_error}")

                return True
            else:
                self.logger.error("❌ No successful batches to combine")
                return False

        except Exception as e:
            self.logger.error(f"❌ Batch processing failed: {e}")
            return False

    def _run_headless_batches(self, batch_files: list, batch_outputs: list) -> bool:
        """
        Run ZabaSearch processing on multiple batches using headless automation

        Args:
            batch_files: List of batch file paths to process
            batch_outputs: List of output file paths for results

        Returns:
            bool: True if all batches processed successfully
        """
        try:
            import threading
            import time

            def process_batch(batch_file, output_path, batch_num):
                """Process a single batch file"""
                try:
                    self.logger.info(f"   🖥️ Batch {batch_num}: Starting ZabaSearch processing...")

                    # Import and use zabasearch module directly
                    import importlib.util

                    # Load zabasearch module
                    if os.path.exists('zabasearch_batch1_records_1_15.py'):
                        spec = importlib.util.spec_from_file_location("zabasearch", 'zabasearch_batch1_records_1_15.py')
                        if spec and spec.loader:
                            zaba_module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(zaba_module)

                            if hasattr(zaba_module, 'ZabaSearchExtractor'):
                                # Create headless scraper and run the full processing
                                scraper = zaba_module.ZabaSearchExtractor(headless=True)

                                # Use the full async processing method with timeout
                                import asyncio

                                async def run_batch():
                                    try:
                                        # No timeout - let batches complete naturally (can take 4+ hours for large files)
                                        await scraper.process_csv_with_sessions(batch_file)
                                    except Exception as e:
                                        self.logger.error(f"   ❌ Batch {batch_num}: ZabaSearch processing failed: {e}")
                                        raise

                                # Run the async processing without timeout
                                try:
                                    asyncio.run(run_batch())
                                except Exception as e:
                                    error_msg = str(e).lower()
                                    self.logger.error(f"   ❌ Batch {batch_num}: ZabaSearch processing failed: {e}")
                                    
                                    # Check for specific error types
                                    if "broken pipe" in error_msg or "errno 32" in error_msg:
                                        self.logger.warning(f"   🔄 Batch {batch_num}: Connection error - likely proxy or network issue")
                                        # Try to continue with batch file if it exists and has some data
                                        if os.path.exists(batch_file):
                                            try:
                                                # Check if batch file has any processed data
                                                import pandas as pd
                                                test_df = pd.read_csv(batch_file)
                                                if len(test_df) > 0:
                                                    self.logger.info(f"   🔄 Batch {batch_num}: Found partial results, using available data")
                                                    # Copy partial results to output
                                                    if batch_file != output_path:
                                                        import shutil
                                                        shutil.copy2(batch_file, output_path)
                                                    return True
                                            except Exception as read_error:
                                                self.logger.error(f"   ❌ Batch {batch_num}: Cannot read partial results: {read_error}")
                                    
                                    return False

                                # The results should be written back to the batch_file
                                # Copy to output_path if different
                                if batch_file != output_path and os.path.exists(batch_file):
                                    import shutil
                                    shutil.copy2(batch_file, output_path)
                                    self.logger.info(f"   ✅ Batch {batch_num}: Results saved to {output_path}")
                                    return True
                                elif os.path.exists(output_path):
                                    self.logger.info(f"   ✅ Batch {batch_num}: Processing completed")
                                    return True
                                else:
                                    self.logger.error(f"   ❌ Batch {batch_num}: No results generated")
                                    return False
                            else:
                                self.logger.error(f"   ❌ Batch {batch_num}: ZabaSearchExtractor not found in module")
                                return False
                        else:
                            self.logger.error(f"   ❌ Batch {batch_num}: Failed to load module")
                            return False
                    else:
                        self.logger.error(f"   ❌ Batch {batch_num}: zabasearch_batch1_records_1_15.py not found")
                        return False

                except Exception as e:
                    self.logger.error(f"   ❌ Batch {batch_num}: Exception: {e}")
                    return False

            # Create and start threads with staggered starts to prevent proxy conflicts
            threads = []
            results = {}

            self.logger.info(f"🚀 Starting {len(batch_files)} batches with staggered proxy sessions...")

            for i, (batch_file, output_path) in enumerate(zip(batch_files, batch_outputs)):
                def run_batch(bf=batch_file, op=output_path, bn=i+1):
                    results[bn] = process_batch(bf, op, bn)

                thread = threading.Thread(target=run_batch)
                threads.append(thread)
                thread.start()

                # Staggered start: 2 second delay between batches to prevent proxy conflicts
                if i < len(batch_files) - 1:  # Don't delay after the last one
                    self.logger.info(f"   ⏸️ Waiting 2 seconds before starting batch {i+2} to prevent proxy conflicts...")
                    time.sleep(2)

            # Wait for all threads to complete
            self.logger.info("⏳ Waiting for all batches to complete...")
            for i, thread in enumerate(threads):
                thread.join()
                self.logger.info(f"   ✅ Batch {i+1} thread completed")

            # Check if all batches succeeded
            all_success = all(results.values())
            if all_success:
                self.logger.info("✅ All headless batches completed successfully")
            else:
                self.logger.error("❌ Some headless batches failed")

            return all_success

        except Exception as e:
            self.logger.error(f"❌ Headless batch processing failed: {e}")
            return False

    def _run_zabasearch_processing(self, df: pd.DataFrame, output_path: str, original_csv_path: str) -> bool:
        """
        Run ZabaSearch processing on the dataframe

        Args:
            df: DataFrame to process
            output_path: Path for output file

        Returns:
            bool: True if processing completed successfully
        """
        try:
            self.logger.info("🔍 Starting ZabaSearch processing...")

            # Import ZabaSearch module
            try:
                # Try to import the specific ZabaSearch script we have
                import importlib.util

                # Look for ZabaSearch scripts
                zaba_scripts = [
                    'zabasearch_batch1_records_1_15.py',
                    'zabasearch_enhanced.py',
                    'zabasearch_automation.py'
                ]

                zaba_module = None
                for script_name in zaba_scripts:
                    if os.path.exists(script_name):
                        spec = importlib.util.spec_from_file_location("zabasearch", script_name)
                        if spec and spec.loader:
                            zaba_module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(zaba_module)
                            self.logger.info(f"✅ Loaded ZabaSearch module: {script_name}")
                            break

                if not zaba_module:
                    self.logger.error("❌ No ZabaSearch module found")
                    return False

                # Run the ZabaSearch processing
                if hasattr(zaba_module, 'ZabaSearchExtractor'):
                    self.logger.info("🤖 Running ZabaSearch automation...")
                    scraper = zaba_module.ZabaSearchExtractor(headless=True)

                    # Create a temporary CSV for processing using user-specific temp folder
                    temp_csv = os.path.join(self.temp_folder, f"temp_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")

                    # 🔧 COLUMN FORMAT FIX: Ensure ZabaSearch format compatibility
                    self.logger.info("🔧 Ensuring ZabaSearch format compatibility...")

                    # Create a copy of the DataFrame for ZabaSearch processing
                    zaba_df = df.copy()

                    # Check if we need to map columns from standardized format to ZabaSearch format
                    if 'DirectName_Cleaned' in zaba_df.columns:
                        self.logger.info("✅ File already has DirectName format - no column mapping needed")
                    else:
                        self.logger.info("🔧 Converting standardized format to ZabaSearch format...")

                        # Map standardized columns to ZabaSearch expected format
                        column_mapping = {}
                        if 'Name' in zaba_df.columns:
                            column_mapping['Name'] = 'DirectName_Cleaned'
                        if 'Address' in zaba_df.columns:
                            column_mapping['Address'] = 'DirectName_Address'
                        if 'Phone' in zaba_df.columns:
                            column_mapping['Phone'] = 'DirectName_Phone_Primary'

                        # Rename columns
                        if column_mapping:
                            zaba_df = zaba_df.rename(columns=column_mapping)
                            self.logger.info(f"📋 Mapped columns: {column_mapping}")

                        # Add required DirectName_Type column (ZabaSearch expects this)
                        if 'DirectName_Type' not in zaba_df.columns:
                            zaba_df['DirectName_Type'] = 'Person'

                    # Save the mapped DataFrame to temp CSV
                    zaba_df.to_csv(temp_csv, index=False)
                    self.logger.info(f"✅ Created ZabaSearch-compatible temp file: {temp_csv}")

                    # Run async processing with correct method
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    try:
                        results = loop.run_until_complete(
                            scraper.process_csv_with_sessions(temp_csv)
                        )

                        # Check if ZabaSearch completed successfully by reading the temp file
                        if os.path.exists(temp_csv):
                            self.logger.info(f"🔍 Reading ZabaSearch results from: {temp_csv}")

                            # Read the processed CSV to get the results
                            try:
                                processed_df = pd.read_csv(temp_csv)
                                self.logger.info(f"📊 Processed file has {len(processed_df)} records")

                                # Debug: Show all columns
                                self.logger.info(f"📋 All columns: {list(processed_df.columns)}")

                                # Check for phone data in the processed file
                                phone_cols = [col for col in processed_df.columns if 'phone' in col.lower() or 'Phone' in col]
                                self.logger.info(f"📞 Phone columns found: {phone_cols}")

                                has_phone_data = False
                                phone_record_count = 0

                                if phone_cols:
                                    for col in phone_cols:
                                        col_count = processed_df[col].notna().sum()
                                        self.logger.info(f"   {col}: {col_count} records with data")
                                        if col_count > 0:
                                            has_phone_data = True
                                            phone_record_count += col_count

                                self.logger.info(f"🎯 Total phone data entries found: {phone_record_count}")

                            except Exception as read_error:
                                self.logger.error(f"❌ Error reading temp file: {read_error}")
                                has_phone_data = False

                            if has_phone_data:
                                self.logger.info(f"✅ ZabaSearch found phone data in columns: {phone_cols}")

                                # COLUMN FORMAT FIX: Map ZabaSearch columns to expected format
                                self.logger.info("🔧 Fixing column format mismatch...")

                                # Create standardized phone columns
                                if 'Primary_Phone' not in processed_df.columns:
                                    processed_df['Primary_Phone'] = ''
                                if 'Secondary_Phone' not in processed_df.columns:
                                    processed_df['Secondary_Phone'] = ''

                                # Map ZabaSearch phone data to standard columns
                                for idx, row in processed_df.iterrows():
                                    # Map primary phone
                                    if pd.notna(row.get('DirectName_Phone_Primary')) and str(row.get('DirectName_Phone_Primary')).strip():
                                        processed_df.at[idx, 'Primary_Phone'] = str(row.get('DirectName_Phone_Primary')).strip()

                                    # Map secondary phone
                                    if pd.notna(row.get('DirectName_Phone_Secondary')) and str(row.get('DirectName_Phone_Secondary')).strip():
                                        processed_df.at[idx, 'Secondary_Phone'] = str(row.get('DirectName_Phone_Secondary')).strip()
                                    elif pd.notna(row.get('Secondary_Phone')) and str(row.get('Secondary_Phone')).strip():
                                        # Secondary_Phone might already be correctly named
                                        processed_df.at[idx, 'Secondary_Phone'] = str(row.get('Secondary_Phone')).strip()

                                # Count fixed phone data
                                primary_count = processed_df['Primary_Phone'].apply(lambda x: bool(str(x).strip()) and str(x) != 'nan').sum()
                                secondary_count = processed_df['Secondary_Phone'].apply(lambda x: bool(str(x).strip()) and str(x) != 'nan').sum()

                                self.logger.info(f"✅ Column format fixed - Primary_Phone: {primary_count}, Secondary_Phone: {secondary_count}")

                                results_df = processed_df

                                # AUTO-MERGE: Apply enhanced phone merger automatically
                                try:
                                    from enhanced_phone_merger import EnhancedPhoneMerger
                                    self.logger.info("🔗 Auto-merging phone data with enhanced merger...")

                                    # Read original data for merging
                                    original_df = read_data_file(original_csv_path)

                                    # Use enhanced merger to merge DataFrames directly
                                    merger = EnhancedPhoneMerger()
                                    merge_result = merger.merge_phone_dataframes(original_df, results_df)

                                    if merge_result and merge_result.get('success'):
                                        self.logger.info(f"✅ Enhanced phone merger applied successfully:")
                                        self.logger.info(f"   📞 Total records with phones: {merge_result.get('total_with_phones', 0)}")
                                        self.logger.info(f"   🆕 New phone numbers found: {merge_result.get('new_phones_added', 0)}")
                                        self.logger.info(f"   📊 Records processed: {merge_result.get('total_records', 0)}")

                                        # Save the merged DataFrame
                                        merged_df = merge_result.get('merged_df')
                                        if merged_df is not None:
                                            # Remove DirectName_Phone columns from merged output too
                                            columns_to_drop = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All']
                                            final_merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])
                                            final_merged_df.to_csv(output_path, index=False)
                                            return True
                                    else:
                                        self.logger.warning("⚠️ Enhanced merger didn't complete successfully")

                                except Exception as merge_error:
                                    self.logger.error(f"❌ Enhanced phone merger failed: {merge_error}")
                                    self.logger.info("📞 Falling back to standard ZabaSearch results...")

                                # Save ZabaSearch results directly
                                # Remove DirectName_Phone columns from output (keep only Primary_Phone and Secondary_Phone)
                                columns_to_drop = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All']
                                final_results_df = results_df.drop(columns=[col for col in columns_to_drop if col in results_df.columns])
                                final_results_df.to_csv(output_path, index=False)
                                self.logger.info(f"✅ ZabaSearch processing completed: {len(results_df)} results")

                                # 🔍 RADARIS FOLLOW-UP: Try Radaris for records where ZabaSearch didn't find phones
                                self.logger.info("🔄 Starting Radaris follow-up for records without phones...")
                                self._run_radaris_followup(final_results_df, output_path, original_csv_path)

                                return True
                            else:
                                self.logger.warning("⚠️ ZabaSearch completed but no phone data found")
                                # Save original data with proper headers when no phone data found
                                columns_to_drop = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All']
                                final_processed_df = processed_df.drop(columns=[col for col in columns_to_drop if col in processed_df.columns])
                                final_processed_df.to_csv(output_path, index=False)
                                self.logger.info(f"✅ Saved processed data: {len(processed_df)} records")
                                return True
                        else:
                            self.logger.error("❌ ZabaSearch temp file not found")
                            return False

                    finally:
                        loop.close()
                        # Keep temp file for debugging - don't delete it immediately
                        if os.path.exists(temp_csv):
                            self.logger.info(f"🗂️ Temp file preserved for analysis: {temp_csv}")
                            # os.remove(temp_csv)  # Commented out to preserve ZabaSearch results

                else:
                    self.logger.error("❌ ZabaSearchExtractor class not found in module")
                    return False

            except Exception as e:
                self.logger.error(f"❌ Failed to import/run ZabaSearch module: {e}")
                # Try Radaris fallback when ZabaSearch completely fails
                self.logger.info("🔄 ZabaSearch failed completely, trying Radaris fallback...")
                return self._run_radaris_fallback(df, output_path)

        except Exception as e:
            self.logger.error(f"❌ ZabaSearch processing failed: {e}")
            return False

    def _run_radaris_followup(self, df: pd.DataFrame, output_path: str, original_csv_path: str) -> bool:
        """
        Run Radaris processing for records where ZabaSearch didn't find phones

        This runs AFTER ZabaSearch completes, targeting only records that still need phones.
        Uses the same address matching logic as ZabaSearch for consistency.

        Args:
            df: DataFrame with ZabaSearch results
            output_path: Path for output file
            original_csv_path: Path to original CSV for merging

        Returns:
            bool: True if processing completed successfully
        """
        try:
            self.logger.info("🔍 Starting Radaris follow-up processing for records without phones...")

            # Find records that still don't have phone numbers after ZabaSearch
            records_needing_radaris = []

            for idx, row in df.iterrows():
                # Check if Primary_Phone is empty/missing
                primary_phone = row.get('Primary_Phone', '')
                has_phone = bool(str(primary_phone).strip()) and str(primary_phone) != 'nan'

                if not has_phone:
                    # This record needs Radaris
                    records_needing_radaris.append(idx)

            if not records_needing_radaris:
                self.logger.info("✅ All records already have phone numbers - Radaris not needed")
                return True

            self.logger.info(f"📞 Found {len(records_needing_radaris)} records needing Radaris lookup")

            # Import Radaris module
            try:
                import importlib.util

                radaris_script = 'radaris_phone_scraper.py'

                if not os.path.exists(radaris_script):
                    self.logger.warning("⚠️ Radaris script not found, skipping Radaris processing")
                    return True

                spec = importlib.util.spec_from_file_location("radaris", radaris_script)
                if spec and spec.loader:
                    radaris_module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(radaris_module)
                    self.logger.info(f"✅ Loaded Radaris module: {radaris_script}")

                    # Run the Radaris processing
                    if hasattr(radaris_module, 'RadarisPhoneScraper'):
                        self.logger.info("🤖 Running Radaris automation on remaining records...")

                        # Create a temporary CSV with only records needing Radaris
                        temp_csv = os.path.join(self.temp_folder, f"temp_radaris_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                        df_radaris = df.loc[records_needing_radaris].copy()
                        df_radaris.to_csv(temp_csv, index=False)

                        self.logger.info(f"📄 Created Radaris temp file with {len(df_radaris)} records")

                        # Initialize Radaris scraper with our pipeline settings
                        scraper = radaris_module.RadarisPhoneScraper(temp_csv, temp_csv)  # Process in-place

                        # Run async processing
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                        try:
                            # Run the scraper using process_csv method
                            loop.run_until_complete(
                                scraper.process_csv(start_row=0, max_rows=len(df_radaris))
                            )

                            # Read Radaris results
                            if os.path.exists(temp_csv):
                                radaris_results = pd.read_csv(temp_csv)
                                self.logger.info(f"📊 Radaris processing completed: {len(radaris_results)} results")

                                # Merge Radaris results back into main DataFrame
                                phones_found = 0
                                for idx, radaris_row in radaris_results.iterrows():
                                    original_idx = records_needing_radaris[idx]

                                    # Use Radaris phone columns (Phone_Primary, Phone_Secondary, Phone_All)
                                    radaris_primary = radaris_row.get('Phone_Primary', '')
                                    radaris_secondary = radaris_row.get('Phone_Secondary', '')
                                    radaris_all = radaris_row.get('Phone_All', '')

                                    if radaris_primary and str(radaris_primary).strip() and str(radaris_primary) != 'nan':
                                        df.at[original_idx, 'Primary_Phone'] = radaris_primary
                                        phones_found += 1
                                        self.logger.info(f"  ✅ Row {original_idx}: Found phone via Radaris: {radaris_primary}")

                                    if radaris_secondary and str(radaris_secondary).strip() and str(radaris_secondary) != 'nan':
                                        df.at[original_idx, 'Secondary_Phone'] = radaris_secondary

                                    # Store Radaris-specific metadata
                                    if radaris_all and str(radaris_all).strip() and str(radaris_all) != 'nan':
                                        df.at[original_idx, 'Radaris_Phone_All'] = radaris_all

                                    address_match = radaris_row.get('Address_Match', '')
                                    if address_match and str(address_match).strip():
                                        df.at[original_idx, 'Radaris_Address_Match'] = address_match

                                    search_status = radaris_row.get('Search_Status', '')
                                    if search_status and str(search_status).strip():
                                        df.at[original_idx, 'Radaris_Search_Status'] = search_status

                                self.logger.info(f"🎉 Radaris found phones for {phones_found}/{len(records_needing_radaris)} remaining records")

                                # Save updated results
                                df.to_csv(output_path, index=False)
                                self.logger.info(f"✅ Updated results saved to: {output_path}")

                                return True
                            else:
                                self.logger.warning("⚠️ Radaris did not create output file")
                                return True

                        finally:
                            loop.close()
                            # Clean up temp file
                            if os.path.exists(temp_csv):
                                os.remove(temp_csv)

                    else:
                        self.logger.error("❌ RadarisPhoneScraper class not found in module")
                        return True
                else:
                    self.logger.error("❌ Failed to load Radaris module spec")
                    return True

            except Exception as e:
                self.logger.error(f"❌ Failed to import/run Radaris module: {e}")
                return True

        except Exception as e:
            self.logger.error(f"❌ Radaris follow-up processing failed: {e}")
            return True

    def _run_radaris_batches(self, output_path: str, original_csv_path: str) -> bool:
        """
        Run Radaris processing in batches for records where ZabaSearch didn't find phones

        This creates 15 batches from records without phones and processes them with Radaris.
        Uses the same batch processing pattern as ZabaSearch for consistency.

        Args:
            output_path: Path to the merged CSV file (after ZabaSearch)
            original_csv_path: Path to original CSV for reference

        Returns:
            bool: True if processing completed successfully
        """
        try:
            # Read the merged results
            if not os.path.exists(output_path):
                self.logger.error(f"❌ Output file not found: {output_path}")
                return False

            df = pd.read_csv(output_path)
            self.logger.info(f"📊 Loaded {len(df)} records from merged results")

            # Filter records that don't have phone numbers
            records_without_phones = df[
                (df['Primary_Phone'].isna()) |
                (df['Primary_Phone'] == '') |
                (df['Primary_Phone'].astype(str).str.strip() == '') |
                (df['Primary_Phone'].astype(str) == 'nan')
            ].copy()

            if len(records_without_phones) == 0:
                self.logger.info("✅ All records already have phone numbers - Radaris not needed")
                return True

            self.logger.info(f"📞 Found {len(records_without_phones)} records without phones")
            self.logger.info(f"   🎯 {(len(records_without_phones)/len(df)*100):.1f}% of records need Radaris lookup")

            # Create 15 batches (consistent with large batch processing)
            batch_count = 15
            self.logger.info(f"📦 Dividing into {batch_count} batches for Radaris processing")

            # Create batch files in temp folder
            batch_files = []
            batch_outputs = []

            for batch_num in range(batch_count):
                # Calculate batch indices
                batch_size = len(records_without_phones) // batch_count
                start_idx = batch_num * batch_size

                if batch_num == batch_count - 1:
                    # Last batch gets remaining records
                    end_idx = len(records_without_phones)
                else:
                    end_idx = start_idx + batch_size

                batch_df = records_without_phones.iloc[start_idx:end_idx].copy()

                if len(batch_df) == 0:
                    continue

                # Create batch file
                batch_file = os.path.join(self.temp_folder, f"radaris_batch_{batch_num + 1}_of_{batch_count}.csv")
                batch_output = os.path.join(self.temp_folder, f"radaris_batch_{batch_num + 1}_output.csv")

                batch_df.to_csv(batch_file, index=False)
                batch_files.append(batch_file)
                batch_outputs.append(batch_output)

                self.logger.info(f"   📄 Batch {batch_num + 1}: {len(batch_df)} records -> {os.path.basename(batch_file)}")

            if not batch_files:
                self.logger.warning("⚠️ No batches created")
                return True

            self.logger.info(f"\n🚀 Starting Radaris headless automation on {len(batch_files)} batches...")

            # Run Radaris processing on batches
            radaris_success = self._run_headless_radaris_batches(batch_files, batch_outputs)

            if not radaris_success:
                self.logger.warning("⚠️ Some Radaris batches failed - continuing with available results")

            # Combine all Radaris batch results
            # NOTE: Radaris modifies batch_file IN PLACE, not batch_output
            radaris_results = []
            successful_batches = 0

            for batch_num, (batch_file, batch_output) in enumerate(zip(batch_files, batch_outputs), 1):
                # Try batch_output first (in case implementation changes), then batch_file (actual location)
                result_file = None
                if os.path.exists(batch_output):
                    result_file = batch_output
                elif os.path.exists(batch_file):
                    result_file = batch_file
                    self.logger.info(f"   📂 Batch {batch_num}: Reading from batch file (modified in-place)")

                if result_file:
                    try:
                        batch_df = pd.read_csv(result_file)
                        radaris_results.append(batch_df)
                        successful_batches += 1
                        self.logger.info(f"   ✅ Batch {batch_num}: {len(batch_df)} results loaded")
                    except Exception as e:
                        self.logger.warning(f"   ⚠️ Batch {batch_num}: Failed to read results - {e}")
                else:
                    self.logger.warning(f"   ⚠️ Batch {batch_num}: No result file found")

            if not radaris_results:
                self.logger.warning("⚠️ No Radaris results to merge")
                return True

            # Combine all Radaris results
            combined_radaris_df = pd.concat(radaris_results, ignore_index=True)
            self.logger.info(f"📊 Combined {len(combined_radaris_df)} Radaris results from {successful_batches} batches")

            # Merge Radaris results back into main DataFrame
            # OPTIMIZED: Build lookup dictionaries first to avoid O(n*m) complexity
            phones_found = 0

            # Build fast lookup dictionaries for both prefixes (O(n) instead of O(n*m))
            lookup_dicts = {}
            for prefix in ['DirectName', 'IndirectName']:
                name_col = f"{prefix}_Cleaned"
                address_col = f"{prefix}_Address"

                # Create dictionary: (name, address) -> row_index
                lookup_dicts[prefix] = {}
                for idx, row in df.iterrows():
                    name = row.get(name_col, '')
                    addr = row.get(address_col, '')
                    if name and addr:  # Only add valid entries
                        # Convert to string and strip for consistent matching
                        key = (str(name).strip(), str(addr).strip())
                        lookup_dicts[prefix][key] = idx

            self.logger.info(f"📇 Built lookup indexes: {sum(len(d) for d in lookup_dicts.values())} entries")

            # Now merge using fast dictionary lookups (O(k) where k = Radaris records)
            for _, radaris_row in combined_radaris_df.iterrows():
                for prefix in ['DirectName', 'IndirectName']:
                    name_col = f"{prefix}_Cleaned"
                    address_col = f"{prefix}_Address"
                    phone_col = f"{prefix}_Phone_Primary"

                    # Get values from Radaris row
                    full_name = radaris_row.get(name_col, '')
                    address = radaris_row.get(address_col, '')
                    radaris_phone = radaris_row.get(phone_col, '')

                    # Skip if no data for this prefix
                    if not full_name or not address:
                        continue

                    # Fast O(1) dictionary lookup instead of O(n) mask operation
                    key = (str(full_name).strip(), str(address).strip())
                    idx = lookup_dicts[prefix].get(key)

                    if idx is not None:
                        # Update phone from Radaris if found
                        if radaris_phone and str(radaris_phone).strip() and str(radaris_phone) != 'nan':
                            # Update the specific prefix phone column
                            df.at[idx, phone_col] = radaris_phone

                            # Also update Primary_Phone if not already set
                            if pd.isna(df.at[idx, 'Primary_Phone']) or not str(df.at[idx, 'Primary_Phone']).strip():
                                df.at[idx, 'Primary_Phone'] = radaris_phone

                            phones_found += 1
                            self.logger.info(f"  📞 Updated {phone_col} for {full_name} at {address}")

            self.logger.info(f"🎉 Radaris found phones for {phones_found}/{len(records_without_phones)} records")
            self.logger.info(f"   📈 Success rate: {(phones_found/len(records_without_phones)*100):.1f}%")

            # Clean up DirectName/IndirectName phone columns from FINAL output (keep only Primary_Phone and Secondary_Phone)
            # These columns were kept during processing for Radaris to check, but now we remove them for cleaner output
            columns_to_drop = [
                'DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All',
                'IndirectName_Phone_Primary', 'IndirectName_Phone_Secondary', 'IndirectName_Phone_All'
            ]
            final_df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

            # Save updated results with clean columns
            final_df.to_csv(output_path, index=False)
            self.logger.info(f"✅ Updated results saved to: {output_path}")
            self.logger.info(f"🧹 Removed {len([c for c in columns_to_drop if c in df.columns])} internal columns for cleaner output")

            # Cleanup Radaris batch files
            for batch_file in batch_files:
                if os.path.exists(batch_file):
                    os.remove(batch_file)
            for batch_output in batch_outputs:
                if os.path.exists(batch_output):
                    os.remove(batch_output)

            self.logger.info("🧹 Radaris batch files cleaned up")

            return True

        except Exception as e:
            self.logger.error(f"❌ Radaris batch processing failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

    def _run_headless_radaris_batches(self, batch_files: list, batch_outputs: list) -> bool:
        """
        Run Radaris processing on multiple batches using headless automation with THREADING (SAME AS ZABA)

        Args:
            batch_files: List of batch file paths to process
            batch_outputs: List of output file paths for results

        Returns:
            bool: True if all batches processed successfully
        """
        try:
            import threading
            import time

            def process_radaris_batch(batch_file, output_path, batch_num):
                """Process a single Radaris batch file"""
                try:
                    self.logger.info(f"   🖥️ Radaris Batch {batch_num}: Starting processing...")

                    # Import and use radaris module directly
                    import importlib.util

                    # Load radaris module
                    radaris_script = 'radaris_phone_scraper.py'
                    if os.path.exists(radaris_script):
                        spec = importlib.util.spec_from_file_location("radaris", radaris_script)
                        if spec and spec.loader:
                            radaris_module = importlib.util.module_from_spec(spec)
                            spec.loader.exec_module(radaris_module)

                            if hasattr(radaris_module, 'RadarisPhoneScraper'):
                                # Create scraper instance (just for accessing the session method)
                                scraper = radaris_module.RadarisPhoneScraper(
                                    csv_path=batch_file,
                                    output_path=output_path,
                                    headless=True
                                )

                                # Run async processing with SESSION-BASED METHOD (SAME AS ZABA)
                                # Each record gets its own browser session for maximum stealth
                                import asyncio

                                async def run_radaris_batch():
                                    try:
                                        # Use process_csv_with_sessions - 1 record = 1 browser session
                                        await scraper.process_csv_with_sessions(batch_file)
                                    except Exception as e:
                                        self.logger.error(f"   ❌ Radaris Batch {batch_num}: Processing failed: {e}")
                                        raise

                                # Run the async processing
                                try:
                                    asyncio.run(run_radaris_batch())
                                except Exception as e:
                                    self.logger.error(f"   ❌ Radaris Batch {batch_num}: Exception: {e}")
                                    return False

                                # Results are saved directly to batch_file by process_csv_with_sessions
                                # Copy to output if different
                                if batch_file != output_path and os.path.exists(batch_file):
                                    import shutil
                                    shutil.copy2(batch_file, output_path)
                                    self.logger.info(f"   ✅ Radaris Batch {batch_num}: Results saved to {output_path}")
                                    return True
                                elif os.path.exists(output_path):
                                    self.logger.info(f"   ✅ Radaris Batch {batch_num}: Processing completed")
                                    return True
                                else:
                                    self.logger.error(f"   ❌ Radaris Batch {batch_num}: No results generated")
                                    return False

                            else:
                                self.logger.error(f"   ❌ Radaris Batch {batch_num}: RadarisPhoneScraper class not found")
                                return False
                        else:
                            self.logger.error(f"   ❌ Radaris Batch {batch_num}: Failed to load module")
                            return False
                    else:
                        self.logger.error(f"   ❌ Radaris Batch {batch_num}: {radaris_script} not found")
                        return False

                except Exception as e:
                    self.logger.error(f"   ❌ Radaris Batch {batch_num} failed: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                    return False

            # Create and start threads with staggered starts (SAME AS ZABA)
            threads = []
            results = {}

            self.logger.info(f"🚀 Starting {len(batch_files)} Radaris batches with staggered proxy sessions (PARALLEL THREADING)...")

            for i, (batch_file, output_path) in enumerate(zip(batch_files, batch_outputs)):
                def run_batch(bf=batch_file, op=output_path, bn=i+1):
                    results[bn] = process_radaris_batch(bf, op, bn)

                thread = threading.Thread(target=run_batch)
                threads.append(thread)
                thread.start()

                # Staggered start: 2 second delay between batches to prevent proxy conflicts (SAME AS ZABA)
                if i < len(batch_files) - 1:  # Don't delay after the last one
                    self.logger.info(f"   ⏸️ Waiting 2 seconds before starting Radaris batch {i+2} to prevent proxy conflicts...")
                    time.sleep(2)

            # Wait for all threads to complete
            self.logger.info("⏳ Waiting for all Radaris batches to complete...")
            for i, thread in enumerate(threads):
                thread.join()
                self.logger.info(f"   ✅ Radaris Batch {i+1} thread completed")

            # Check results
            all_success = all(results.values())
            if all_success:
                self.logger.info("✅ All Radaris headless batches completed successfully")
            else:
                failed_batches = [k for k, v in results.items() if not v]
                self.logger.warning(f"⚠️ Some Radaris batches failed: {failed_batches}")

            return all_success or any(results.values())  # Return True if at least one succeeded

        except Exception as e:
            self.logger.error(f"❌ Radaris headless batch processing failed: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

    def _run_radaris_fallback(self, df: pd.DataFrame, output_path: str) -> bool:
        """
        DEPRECATED: Use _run_radaris_followup instead

        This is kept for backward compatibility but redirects to the new method
        """
        self.logger.warning("⚠️ Using deprecated _run_radaris_fallback - consider using _run_radaris_followup")
        return self._run_radaris_followup(df, output_path, "")

    def analyze_csv(self, csv_path: str) -> dict:
        """
        Analyze CSV file for phone number statistics

        Args:
            csv_path: Path to CSV file

        Returns:
            dict: Analysis results
        """
        try:
            # For analysis, just use basic file reading since AI formatter is designed for processing
            df = read_data_file(csv_path)
            if df is None or len(df) == 0:
                return {'error': 'No data found in file'}

            total_records = len(df)
            phone_columns = [
                col for col in df.columns
                if any(keyword in col.lower() for keyword in [
                    'phone', 'telephone', 'tel', 'mobile', 'cell',
                    'radaris_phone', 'directname_phone', 'zaba_phone'
                ])
            ]

            if phone_columns:
                phone_col = phone_columns[0]
                phone_series = df[phone_col].fillna('').astype(str).str.strip()
                has_phone = int((phone_series != '').sum())
                no_phone = total_records - has_phone
                phone_percentage = (has_phone / total_records * 100) if total_records > 0 else 0

                return {
                    'total_records': total_records,
                    'has_phone': has_phone,
                    'no_phone': no_phone,
                    'phone_percentage': round(phone_percentage, 2),
                    'phone_column': phone_col,
                    'columns': list(df.columns),
                    'ai_analysis': self.ai_formatter is not None
                }
            else:
                return {
                    'total_records': total_records,
                    'has_phone': 0,
                    'no_phone': total_records,
                    'phone_percentage': 0.0,
                    'phone_column': 'None found',
                    'columns': list(df.columns),
                    'ai_analysis': self.ai_formatter is not None
                }

        except Exception as e:
            self.logger.error(f"CSV analysis failed: {e}")
            return {'error': str(e)}

# Flask integration functions
def process_phone_extraction(csv_path: str, max_records: Optional[int] = None, user_config: Optional[dict] = None):
    """
    Process phone extraction for Flask integration

    Args:
        csv_path: Path to input CSV file
        max_records: Maximum records to process (optional)
        user_config: User-specific configuration with TEMP_FOLDER, RESULTS_FOLDER paths

    Returns:
        str: Path to output file or None if failed
    """
    try:
        pipeline = PhoneSearchPipeline(user_config=user_config)

        # Create output filename with user-specific results folder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"phone_extraction_{timestamp}.csv"
        
        # Use user-specific results folder if provided
        if user_config and user_config.get('RESULTS_FOLDER'):
            output_path = os.path.join(user_config['RESULTS_FOLDER'], output_filename)
        else:
            output_path = os.path.join('results', output_filename)

        # Process the file
        success = pipeline.process_csv_direct(csv_path, output_path, max_records)

        if success and os.path.exists(output_path):
            return output_path
        else:
            return None

    except Exception as e:
        logging.error(f"Phone extraction failed: {e}")
        return None

def main():
    """Command line interface with argument parsing"""
    import argparse

    parser = argparse.ArgumentParser(description='AI-Powered Phone Search Pipeline')
    parser.add_argument('csv_file', help='Path to input CSV file')
    parser.add_argument('--max-records', type=int, default=None, help='Maximum records to process (default: unlimited)')
    parser.add_argument('--output', help='Output file path (optional)')

    args = parser.parse_args()

    if not os.path.exists(args.csv_file):
        print(f"❌ Error: File '{args.csv_file}' not found")
        return

    pipeline = PhoneSearchPipeline()

    # Analyze first
    print("\n📊 Analyzing CSV file...")
    analysis = pipeline.analyze_csv(args.csv_file)
    print(f"Analysis results: {analysis}")

    # Process
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = args.output if args.output else f"results/phone_results_{timestamp}.csv"

    print(f"\n🚀 Starting processing...")
    success = pipeline.process_csv_direct(args.csv_file, output_file, args.max_records)

    if success:
        print(f"✅ Processing completed: {output_file}")
    else:
        print("❌ Processing failed")

if __name__ == "__main__":
    main()
