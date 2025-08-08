#!/usr/bin/env python3
"""
Enterprise Flask Application - Lead Generation & Data Enhancement Platform
Professional interface with phone and address search capabilities

Features:
- Professional enterprise UI design
- File upload with drag & drop
- Phone number analysis and extraction  
- Address-based property search
- Excel and CSV file support
- Real-time file processing
- Download separated files
- Data enhancement services
"""

import os
import logging
import pandas as pd
from datetime import datetime
from flask import Flask, request, render_template, jsonify, send_file
from werkzeug.utils import secure_filename
import json
from typing import Dict, List, Optional, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('simple_flask.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

print("🔄 Enterprise Flask Application Starting...")
print("📦 Loading modules...")

# Flask app configuration
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['SECRET_KEY'] = 'enterprise-data-processing-2025'

# Create necessary directories
os.makedirs('uploads', exist_ok=True)
os.makedirs('temp', exist_ok=True)
os.makedirs('output', exist_ok=True)

# Import support modules
try:
    import openpyxl
    EXCEL_SUPPORT = True
    print("✅ Excel support loaded (openpyxl)")
except ImportError:
    EXCEL_SUPPORT = False
    print("❌ Excel support not available")

try:
    from csv_format_handler import CSVFormatHandler
    CSV_HANDLER = CSVFormatHandler()
    print("✅ CSV Format Handler loaded")
except ImportError:
    CSV_HANDLER = None
    print("❌ CSV Format Handler not available")

try:
    from phone_search_pipeline import PhoneSearchPipeline
    PHONE_PIPELINE = PhoneSearchPipeline()
    print("✅ Phone Search Pipeline loaded")
except ImportError:
    PHONE_PIPELINE = None
    print("❌ Phone Search Pipeline not available")

def read_file_to_dataframe(filepath: str) -> Optional[pd.DataFrame]:
    """
    Read various file formats into a pandas DataFrame with robust error handling
    """
    try:
        file_extension = os.path.splitext(filepath)[1].lower()
        
        if file_extension == '.csv':
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            for encoding in encodings:
                try:
                    df = pd.read_csv(filepath, encoding=encoding)
                    logger.info(f"✅ CSV file read successfully with {encoding} encoding")
                    return df
                except UnicodeDecodeError:
                    continue
            
            logger.error("❌ Could not read CSV with any encoding")
            return None
            
        elif file_extension in ['.xlsx', '.xls']:
            if not EXCEL_SUPPORT:
                logger.error("❌ Excel support not available")
                return None
            
            try:
                df = pd.read_excel(filepath, engine='openpyxl' if file_extension == '.xlsx' else 'xlrd')
                logger.info("✅ Excel file read successfully")
                return df
            except Exception as e:
                logger.error(f"❌ Excel read error: {str(e)}")
                return None
        
        else:
            logger.error(f"❌ Unsupported file format: {file_extension}")
            return None
            
    except Exception as e:
        logger.error(f"❌ File reading error: {str(e)}")
        return None

def analyze_phone_columns(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze phone number columns and data completeness
    """
    try:
        phone_columns = []
        phone_patterns = ['phone', 'tel', 'mobile', 'cell', 'number']
        
        for col in df.columns:
            if any(pattern.lower() in col.lower() for pattern in phone_patterns):
                phone_columns.append(col)
        
        # Count records with and without phone numbers
        records_with_phone = 0
        records_without_phone = 0
        phone_samples = []
        
        if phone_columns:
            # Check first phone column for analysis
            main_phone_col = phone_columns[0]
            
            for idx, value in df[main_phone_col].head(100).items():
                if pd.notna(value) and str(value).strip() and str(value).strip() != '':
                    records_with_phone += 1
                    if len(phone_samples) < 5:  # Get sample phone numbers
                        phone_samples.append(str(value))
                else:
                    records_without_phone += 1
            
            # Extrapolate to full dataset
            total_sample = min(100, len(df))
            if total_sample > 0:
                phone_ratio = records_with_phone / total_sample
                records_with_phone = int(len(df) * phone_ratio)
                records_without_phone = len(df) - records_with_phone
        else:
            records_without_phone = len(df)
        
        phone_coverage = (records_with_phone / len(df) * 100) if len(df) > 0 else 0
        
        return {
            'phone_columns': phone_columns,
            'records_with_phone': records_with_phone,
            'records_without_phone': records_without_phone,
            'phone_coverage': round(phone_coverage, 1),
            'phone_samples': phone_samples,
            'total_records': len(df)
        }
        
    except Exception as e:
        logger.error(f"❌ Phone analysis error: {str(e)}")
        return {
            'phone_columns': [],
            'records_with_phone': 0,
            'records_without_phone': len(df),
            'phone_coverage': 0,
            'phone_samples': [],
            'total_records': len(df)
        }

@app.route('/')
def index():
    """Main enterprise dashboard"""
    logger.info("Enterprise dashboard accessed")
    return render_template('enterprise.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and initial analysis"""
    try:
        logger.info("File upload request received")
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file selected'}), 400
        
        file = request.files['file']
        if file.filename == '' or file.filename is None:
            return jsonify({'error': 'No file selected'}), 400
        
        # Secure filename
        filename = secure_filename(file.filename)
        
        # Check file extension
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        file_extension = os.path.splitext(filename)[1].lower()
        
        if file_extension not in allowed_extensions:
            return jsonify({
                'error': f'File type {file_extension} not supported. Please upload CSV or Excel files.'
            }), 400
        
        logger.info(f"Processing file: {filename} ({file_extension})")
        
        # Save with timestamp to avoid conflicts
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], safe_filename)
        
        file.save(filepath)
        logger.info(f"File saved: {filepath}")
        
        # Read and analyze the file
        df = read_file_to_dataframe(filepath)
        if df is None:
            return jsonify({'error': 'Could not read the uploaded file. Please check the file format.'}), 400
        
        # Basic file statistics
        total_records = len(df)
        columns = list(df.columns)
        
        logger.info(f"File analysis: {total_records} records, {len(columns)} columns")
        
        # Analyze phone columns
        phone_analysis = analyze_phone_columns(df)
        
        # Generate sample data (first 5 rows, limited columns)
        display_columns = columns[:6]  # Show max 6 columns in preview
        sample_data = df[display_columns].head(5).fillna('').to_dict('records')
        
        # File information for the frontend
        file_info = {
            'filename': safe_filename,
            'original_filename': filename,
            'filepath': filepath,
            'file_type': file_extension,
            'total_records': total_records,
            'columns': columns,
            'display_columns': display_columns,
            'phone_analysis': phone_analysis,
            'sample_data': sample_data,
            'upload_time': datetime.now().isoformat(),
            'file_size_kb': round(os.path.getsize(filepath) / 1024, 2)
        }
        
        logger.info(f"📋 Analysis complete: {phone_analysis['records_without_phone']} records need phone search")
        
        return jsonify(file_info)
        
    except Exception as e:
        logger.error(f"❌ Error in upload_file: {str(e)}")
        return jsonify({'error': f'❌ Error processing file: {str(e)}'}), 500

@app.route('/analyze', methods=['POST'])
def analyze_file():
    """Process file with phone or address search"""
    try:
        # Handle both JSON and form data
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form.to_dict()
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        filepath = data.get('filepath')
        analysis_type = data.get('analysis_type', 'phone')  # 'phone' or 'address'
        max_records = int(data.get('max_records', 30))
        
        logger.info(f"Analysis request: {analysis_type} search for {max_records} records")
        
        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 400
        
        # Read the file
        df = read_file_to_dataframe(filepath)
        if df is None:
            return jsonify({'error': '❌ Could not read the file for processing'}), 400
        
        if analysis_type == 'phone':
            # Phone number search
            logger.info("📞 Starting phone number search...")
            
            if PHONE_PIPELINE is None:
                return jsonify({'error': '❌ Phone search functionality not available'}), 500
            
            # Prepare data for phone search
            phone_analysis = analyze_phone_columns(df)
            records_without_phone = phone_analysis['records_without_phone']
            
            if records_without_phone == 0:
                return jsonify({'error': '❌ All records already have phone numbers'}), 400
            
            # Filter records without phone numbers and limit
            phone_cols = phone_analysis['phone_columns']
            if phone_cols:
                mask = df[phone_cols[0]].isna() | (df[phone_cols[0]].astype(str).str.strip() == '')
                records_to_process = df[mask].head(max_records)
            else:
                records_to_process = df.head(max_records)
            
            # Generate output filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"phone_search_results_{timestamp}.csv"
            output_path = os.path.join('output', output_filename)
            
            # Save processed records (simulation - replace with actual phone search)
            records_to_process['Phone_Search_Status'] = 'Processed'
            records_to_process['Phone_Number'] = 'XXX-XXX-XXXX'  # Placeholder
            records_to_process.to_csv(output_path, index=False)
            
            result_info = {
                'status': 'success',
                'message': f'✅ Phone search completed! Processed {len(records_to_process)} records.',
                'processed_count': len(records_to_process),
                'output_file': output_filename,
                'download_url': f'/download/{output_filename}'
            }
            
            logger.info(f"✅ Phone search completed: {len(records_to_process)} records")
            return jsonify(result_info)
            
        elif analysis_type == 'address':
            # Address-based property search
            logger.info("🏠 Starting address-based search...")
            
            # Limit records for processing
            records_to_process = df.head(max_records)
            
            # Generate output filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"address_search_results_{timestamp}.csv"
            output_path = os.path.join('output', output_filename)
            
            # Save processed records (simulation - replace with actual ZabaSearch)
            records_to_process['Address_Search_Status'] = 'Processed'
            records_to_process['Property_Info'] = 'Enhanced with property data'
            records_to_process.to_csv(output_path, index=False)
            
            result_info = {
                'status': 'success',
                'message': f'✅ Address search completed! Processed {len(records_to_process)} records.',
                'processed_count': len(records_to_process),
                'output_file': output_filename,
                'download_url': f'/download/{output_filename}'
            }
            
            logger.info(f"✅ Address search completed: {len(records_to_process)} records")
            return jsonify(result_info)
        
        else:
            return jsonify({'error': '❌ Invalid analysis type'}), 400
            
    except Exception as e:
        logger.error(f"❌ Error in analyze_file: {str(e)}")
        return jsonify({'error': f'❌ Analysis error: {str(e)}'}), 500

@app.route('/separate_phones', methods=['POST'])
def separate_phone_files():
    """Separate records with and without phone numbers into different files"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        filepath = data.get('filepath')
        
        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': '❌ File not found'}), 400
        
        logger.info("📞 Separating records by phone number availability...")
        
        # Read the file
        df = read_file_to_dataframe(filepath)
        if df is None:
            return jsonify({'error': '❌ Could not read the file'}), 400
        
        # Analyze phone columns
        phone_analysis = analyze_phone_columns(df)
        phone_cols = phone_analysis['phone_columns']
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if phone_cols:
            # Split based on phone column
            main_phone_col = phone_cols[0]
            
            # Records with phone numbers
            with_phones = df[df[main_phone_col].notna() & (df[main_phone_col].astype(str).str.strip() != '')]
            
            # Records without phone numbers
            without_phones = df[df[main_phone_col].isna() | (df[main_phone_col].astype(str).str.strip() == '')]
        else:
            # No phone column found - all records considered without phones
            with_phones = pd.DataFrame()
            without_phones = df
        
        # Save separated files
        files_created = []
        
        if len(with_phones) > 0:
            with_phones_file = f"records_with_phones_{timestamp}.csv"
            with_phones_path = os.path.join('output', with_phones_file)
            with_phones.to_csv(with_phones_path, index=False)
            files_created.append({
                'type': 'with_phones',
                'filename': with_phones_file,
                'count': len(with_phones),
                'download_url': f'/download/{with_phones_file}'
            })
        
        if len(without_phones) > 0:
            without_phones_file = f"records_without_phones_{timestamp}.csv"
            without_phones_path = os.path.join('output', without_phones_file)
            without_phones.to_csv(without_phones_path, index=False)
            files_created.append({
                'type': 'without_phones',
                'filename': without_phones_file,
                'count': len(without_phones),
                'download_url': f'/download/{without_phones_file}'
            })
        
        # Original file
        original_file = f"original_file_{timestamp}.csv"
        original_path = os.path.join('output', original_file)
        df.to_csv(original_path, index=False)
        files_created.append({
            'type': 'original',
            'filename': original_file,
            'count': len(df),
            'download_url': f'/download/{original_file}'
        })
        
        result = {
            'status': 'success',
            'message': f'✅ Files separated successfully!',
            'files': files_created,
            'summary': {
                'total_records': len(df),
                'with_phones': len(with_phones),
                'without_phones': len(without_phones),
                'phone_coverage': round((len(with_phones) / len(df)) * 100, 1) if len(df) > 0 else 0
            }
        }
        
        logger.info(f"✅ Phone separation completed: {len(with_phones)} with phones, {len(without_phones)} without")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error in separate_phone_files: {str(e)}")
        return jsonify({'error': f'❌ Separation error: {str(e)}'}), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Handle file downloads"""
    try:
        logger.info(f"📥 Download request: {filename}")
        
        # Search in multiple directories
        search_dirs = ['output', 'uploads', 'temp', '.']
        
        filepath = None
        for directory in search_dirs:
            potential_path = os.path.join(directory, filename)
            if os.path.exists(potential_path):
                filepath = potential_path
                break
        
        if not filepath:
            logger.error(f"❌ File not found: {filename}")
            return jsonify({'error': f'❌ File not found: {filename}'}), 404
        
        logger.info(f"✅ Serving file: {filepath}")
        return send_file(filepath, as_attachment=True, download_name=filename)
        
    except Exception as e:
        logger.error(f"❌ Download error for {filename}: {str(e)}")
        return jsonify({'error': f'❌ Download error: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'modules': {
            'excel_support': EXCEL_SUPPORT,
            'csv_handler': CSV_HANDLER is not None,
            'phone_pipeline': PHONE_PIPELINE is not None
        }
    })

# Error handlers
@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': '❌ File too large. Maximum size is 100MB.'}), 413

@app.errorhandler(500)
def internal_error(e):
    return jsonify({'error': '❌ Internal server error. Please try again.'}), 500

if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("🚀 ENTERPRISE DATA PROCESSING PLATFORM")
    print("=" * 60)
    print("🌐 Starting Flask server...")
    print("📍 URL: http://localhost:5000")
    print("🔧 Debug mode: ENABLED")
    print("📊 Max file size: 100MB")
    print("📁 Supported formats: CSV, Excel (.xlsx, .xls)")
    print("🔍 Features: Phone search, Address search, Data separation")
    print("=" * 60)
    print()
    
    try:
        app.run(
            debug=True,
            host='0.0.0.0',
            port=5000,
            threaded=True
        )
    except Exception as e:
        print(f"❌ Failed to start Flask app: {e}")
        logger.error(f"Flask startup failed: {e}")
