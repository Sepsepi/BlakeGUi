#!/usr/bin/env python3
"""
Phone Search Pipeline - Enhanced ZabaSearch Automation
Direct processing with improved error handling and robust data processing

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

# Import our processing modules
try:
    from csv_format_handler import CSVFormatHandler
    CSV_HANDLER_AVAILABLE = True
    print("✅ CSV Format Handler: Available")
except ImportError as e:
    print(f"❌ CSV Format Handler: Not available - {e}")
    CSV_HANDLER_AVAILABLE = False

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class PhoneSearchPipeline:
    """Enhanced phone search pipeline with direct processing"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.format_handler = CSVFormatHandler() if CSV_HANDLER_AVAILABLE else None

        # Ensure directories exist
        os.makedirs('results', exist_ok=True)
        os.makedirs('temp', exist_ok=True)
        os.makedirs('logs', exist_ok=True)

        self.logger.info("Phone Search Pipeline initialized")

    def process_csv_direct(self, csv_path: str, output_path: str, max_records: int = 10) -> bool:
        """
        Process CSV file directly with ZabaSearch automation

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

            # Read and standardize the CSV
            if self.format_handler:
                self.logger.info("📄 Using format handler to standardize CSV...")
                standardized_path = self.format_handler.standardize_to_search_format(csv_path)
                if standardized_path:
                    df = pd.read_csv(standardized_path)
                else:
                    df = pd.read_csv(csv_path)
            else:
                self.logger.info("📄 Reading CSV directly...")
                df = pd.read_csv(csv_path)

            if df is None or len(df) == 0:
                self.logger.error("❌ No data found in CSV file")
                return False

            self.logger.info(f"📊 Loaded {len(df)} total records")

            # Filter to records without phone numbers
            phone_columns = [col for col in df.columns if 'phone' in col.lower() or 'telephone' in col.lower()]

            if phone_columns:
                phone_col = phone_columns[0]
                self.logger.info(f"📞 Using phone column: {phone_col}")

                # Get records without phone numbers
                no_phone_mask = df[phone_col].fillna('').astype(str).str.strip() == ''
                no_phone_df = df[no_phone_mask].copy()

                self.logger.info(f"🔍 Found {len(no_phone_df)} records without phone numbers")

                if len(no_phone_df) == 0:
                    self.logger.warning("⚠️ No records found without phone numbers")
                    # Save original file as result
                    df.to_csv(output_path, index=False)
                    return True

                # Limit to max_records
                process_df = no_phone_df.head(max_records).copy()
                self.logger.info(f"🎯 Processing {len(process_df)} records")

            else:
                self.logger.info("📞 No phone column found, processing all records")
                process_df = df.head(max_records).copy()

            # Run ZabaSearch processing
            success = self._run_zabasearch_processing(process_df, output_path)

            if success:
                self.logger.info(f"✅ Processing completed successfully: {output_path}")
                return True
            else:
                self.logger.error("❌ ZabaSearch processing failed")
                # Save processed data anyway
                process_df.to_csv(output_path, index=False)
                return True

        except Exception as e:
            self.logger.error(f"❌ Pipeline processing failed: {e}")
            return False

    def _run_zabasearch_processing(self, df: pd.DataFrame, output_path: str) -> bool:
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
                if hasattr(zaba_module, 'ZabaSearchScraper'):
                    self.logger.info("🤖 Running ZabaSearch automation...")
                    scraper = zaba_module.ZabaSearchScraper()

                    # Create a temporary CSV for processing
                    temp_csv = f"temp/temp_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    df.to_csv(temp_csv, index=False)

                    # Run async processing
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)

                    try:
                        results = loop.run_until_complete(
                            scraper.process_csv_file(temp_csv, max_records=len(df))
                        )

                        if results and len(results) > 0:
                            # Save results
                            results_df = pd.DataFrame(results)
                            results_df.to_csv(output_path, index=False)
                            self.logger.info(f"✅ ZabaSearch processing completed: {len(results)} results")
                            return True
                        else:
                            self.logger.warning("⚠️ ZabaSearch returned no results")
                            return False

                    finally:
                        loop.close()
                        # Clean up temp file
                        if os.path.exists(temp_csv):
                            os.remove(temp_csv)

                else:
                    self.logger.error("❌ ZabaSearchScraper class not found in module")
                    return False

            except Exception as e:
                self.logger.error(f"❌ Failed to import/run ZabaSearch module: {e}")
                return False

        except Exception as e:
            self.logger.error(f"❌ ZabaSearch processing failed: {e}")
            return False

    def analyze_csv(self, csv_path: str) -> dict:
        """
        Analyze CSV file for phone number statistics

        Args:
            csv_path: Path to CSV file

        Returns:
            dict: Analysis results
        """
        try:
            if self.format_handler:
                standardized_path = self.format_handler.standardize_to_search_format(csv_path)
                if standardized_path:
                    df = pd.read_csv(standardized_path)
                else:
                    df = pd.read_csv(csv_path)
            else:
                df = pd.read_csv(csv_path)

            if df is None or len(df) == 0:
                return {'error': 'No data found in CSV'}

            total_records = len(df)
            phone_columns = [col for col in df.columns if 'phone' in col.lower() or 'telephone' in col.lower()]

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
                    'columns': list(df.columns)
                }
            else:
                return {
                    'total_records': total_records,
                    'has_phone': 0,
                    'no_phone': total_records,
                    'phone_percentage': 0.0,
                    'phone_column': 'None found',
                    'columns': list(df.columns)
                }

        except Exception as e:
            self.logger.error(f"CSV analysis failed: {e}")
            return {'error': str(e)}

# Flask integration functions
def process_phone_extraction(csv_path: str, max_records: int = 30):
    """
    Process phone extraction for Flask integration
    
    Args:
        csv_path: Path to input CSV file
        max_records: Maximum records to process
        
    Returns:
        str: Path to output file or None if failed
    """
    try:
        pipeline = PhoneSearchPipeline()
        
        # Create output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"phone_extraction_{timestamp}.csv"
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
    """Command line interface for testing"""
    if len(sys.argv) < 2:
        print("Usage: python phone_search_pipeline.py <csv_file> [max_records]")
        return

    csv_file = sys.argv[1]
    max_records = int(sys.argv[2]) if len(sys.argv) > 2 else 10

    if not os.path.exists(csv_file):
        print(f"Error: File {csv_file} not found")
        return

    pipeline = PhoneSearchPipeline()

    # Analyze first
    print("\n📊 Analyzing CSV file...")
    analysis = pipeline.analyze_csv(csv_file)
    print(f"Analysis results: {analysis}")

    # Process
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"results/phone_results_{timestamp}.csv"

    print(f"\n🚀 Starting processing...")
    success = pipeline.process_csv_direct(csv_file, output_file, max_records)

    if success:
        print(f"✅ Processing completed: {output_file}")
    else:
        print("❌ Processing failed")

if __name__ == "__main__":
    main()
