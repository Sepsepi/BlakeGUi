from flask import Flask, render_template, request, jsonify, send_file, url_for, make_response, session
import pandas as pd
import os
import logging
import zipfile
import re
import glob
import time
import json
import uuid
from datetime import datetime
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
import csv_format_handler
import phone_search_pipeline
import bcpa_owner_search
import file_cleanup
from intelligent_address_formatter import IntelligentAddressFormatter
from column_syncer import ColumnSyncer

# Create Flask application
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['SECRET_KEY'] = 'blakegui-multi-user-session-key-2025'  # For session encryption

# Base directories for user isolation
app.config['BASE_UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'uploads')
app.config['BASE_RESULTS_FOLDER'] = os.path.join(os.getcwd(), 'results')
app.config['BASE_TEMP_FOLDER'] = os.path.join(os.getcwd(), 'temp')
app.config['LOGS_FOLDER'] = os.path.join(os.getcwd(), 'logs')

# Backward compatibility - some parts still expect these
app.config['UPLOAD_FOLDER'] = app.config['BASE_UPLOAD_FOLDER']
app.config['RESULTS_FOLDER'] = app.config['BASE_RESULTS_FOLDER']

# Create base directories
os.makedirs(app.config['BASE_UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['BASE_RESULTS_FOLDER'], exist_ok=True)
os.makedirs(app.config['BASE_TEMP_FOLDER'], exist_ok=True)
os.makedirs(app.config['LOGS_FOLDER'], exist_ok=True)
os.makedirs('output', exist_ok=True)

# Session Management Functions
def get_user_id():
    """Get or create a unique user ID for the session"""
    if 'user_id' not in session:
        session['user_id'] = str(uuid.uuid4())
        session.permanent = True  # Make session persistent
        logger.info(f"🆔 New user session created: {session['user_id']}")
    return session['user_id']

def get_user_directories(user_id):
    """Get user-specific directory paths"""
    user_upload_dir = os.path.join(app.config['BASE_UPLOAD_FOLDER'], user_id)
    user_results_dir = os.path.join(app.config['BASE_RESULTS_FOLDER'], user_id)
    user_temp_dir = os.path.join(app.config['BASE_TEMP_FOLDER'], user_id)
    
    # Create user directories if they don't exist
    os.makedirs(user_upload_dir, exist_ok=True)
    os.makedirs(user_results_dir, exist_ok=True)
    os.makedirs(user_temp_dir, exist_ok=True)
    
    return {
        'upload': user_upload_dir,
        'results': user_results_dir,
        'temp': user_temp_dir
    }

def get_user_config(user_id):
    """Get user-specific Flask config"""
    user_dirs = get_user_directories(user_id)
    return {
        'UPLOAD_FOLDER': user_dirs['upload'],
        'RESULTS_FOLDER': user_dirs['results'],
        'TEMP_FOLDER': user_dirs['temp']
    }

@app.before_request
def ensure_user_session():
    """Ensure every request has a user session"""
    user_id = get_user_id()
    # Create user directories on every request to ensure they exist
    get_user_directories(user_id)

# Set up logging to logs folder
log_file = os.path.join(app.config['LOGS_FOLDER'], 'enterprise_flask.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Terminal log storage for real-time monitoring
import threading
from collections import deque

terminal_logs = deque(maxlen=200)  # Keep last 200 log entries
log_lock = threading.Lock()

class TerminalLogHandler(logging.Handler):
    """Custom handler to capture logs for terminal feed"""
    def emit(self, record):
        try:
            # Format the log record
            log_entry = {
                'timestamp': datetime.fromtimestamp(record.created).strftime('%H:%M:%S'),
                'level': record.levelname,
                'message': self.format(record)
            }

            # Add to terminal logs thread-safely
            with log_lock:
                terminal_logs.append(log_entry)
        except Exception:
            pass  # Silently ignore errors to avoid recursion

# Add terminal handler to root logger and Flask logger
terminal_handler = TerminalLogHandler()
terminal_handler.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger().addHandler(terminal_handler)  # Root logger
app.logger.addHandler(terminal_handler)  # Flask logger

# Also capture werkzeug logs (Flask's built-in server logs)
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.addHandler(terminal_handler)

# Try to import BCPA integration (optional)
try:
    import bcpa_flask_integration
    BCPA_AVAILABLE = True
    logger.info("BCPA integration module loaded successfully")
except ImportError:
    BCPA_AVAILABLE = False
    logger.warning("BCPA integration module not available")

# Try to import Column Syncer (should always be available)
try:
    from column_syncer import ColumnSyncer
    COLUMN_SYNCER_AVAILABLE = True
    logger.info("✅ Column Syncer: Available")
except ImportError:
    COLUMN_SYNCER_AVAILABLE = False
    logger.warning("❌ Column Syncer: Not available")

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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
            logger.warning(f"Unknown file extension for {filepath}, trying CSV format")
            return pd.read_csv(filepath, encoding=encoding)
    except Exception as e:
        logger.error(f"Error reading file {filepath}: {e}")
        raise

def analyze_phone_coverage(df):
    """Analyze phone number coverage in the dataset - UNIVERSAL APPROACH."""

    # Check if this is AI-formatted data (has DirectName columns)
    if 'DirectName_Cleaned' in df.columns:
        logger.info("📊 Detected AI-formatted data - analyzing phone extraction readiness")

        # For AI-formatted data, all records are ready for phone extraction
        # The AI formatter already filtered out records with existing phones
        records_with_phone = 0  # AI formatter removes records that already have phones
        records_without_phone = len(df)  # All remaining records need phone extraction
        phone_coverage = 0  # 0% because AI formatter specifically outputs records WITHOUT phones

        # Check for any existing phone columns that might have been added by ZabaSearch
        phone_columns = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'Primary_Phone', 'Secondary_Phone']
        existing_phone_col = None

        for col in phone_columns:
            if col in df.columns:
                existing_phone_col = col
                # Count actual phone numbers in this column
                valid_phones = df[col].dropna().astype(str).str.strip()
                valid_phones = valid_phones[valid_phones != '']

                actual_phones = []
                for phone in valid_phones:
                    if _looks_like_phone_number(phone):
                        actual_phones.append(phone)

                if actual_phones:
                    records_with_phone = len(actual_phones)
                    records_without_phone = len(df) - records_with_phone
                    phone_coverage = round((records_with_phone / len(df)) * 100, 1) if len(df) > 0 else 0
                    break

        return {
            'phone_column': existing_phone_col,
            'records_with_phone': records_with_phone,
            'records_without_phone': records_without_phone,
            'phone_coverage': phone_coverage
        }

    # Original logic for non-AI-formatted data
    # First try standard column names
    phone_columns = ['phone', 'Phone', 'PHONE', 'phone_number', 'Phone Number', 'PHONE_NUMBER',
                    'cell', 'Cell', 'CELL', 'mobile', 'Mobile', 'MOBILE', 'telephone', 'Telephone']

    phone_col = None
    for col in phone_columns:
        if col in df.columns:
            phone_col = col
            break

    # If no standard phone column found, scan ALL columns for phone numbers
    if phone_col is None:
        logger.info("🔍 No standard phone column found, scanning all columns for phone numbers...")

        best_phone_col = None
        max_phone_count = 0

        for col in df.columns:
            try:
                # Count how many valid phone numbers are in this column
                phone_count = 0
                for value in df[col].dropna().astype(str):
                    if _looks_like_phone_number(value.strip()):
                        phone_count += 1

                # If this column has more phone numbers than our current best, use it
                if phone_count > max_phone_count:
                    max_phone_count = phone_count
                    best_phone_col = col

            except Exception:
                continue

        if best_phone_col and max_phone_count > 0:
            phone_col = best_phone_col
            logger.info(f"✅ Found phone numbers in column: {phone_col} ({max_phone_count} phone numbers)")

    if phone_col:
        # Count non-empty phone numbers
        valid_phones = df[phone_col].dropna().astype(str).str.strip()
        valid_phones = valid_phones[valid_phones != '']

        # Further filter to only valid phone number patterns
        actual_phones = []
        for phone in valid_phones:
            if _looks_like_phone_number(phone):
                actual_phones.append(phone)

        records_with_phone = len(actual_phones)
        records_without_phone = len(df) - records_with_phone
        phone_coverage = round((records_with_phone / len(df)) * 100, 1) if len(df) > 0 else 0
    else:
        records_with_phone = 0
        records_without_phone = len(df)
        phone_coverage = 0

    return {
        'phone_column': phone_col,
        'records_with_phone': records_with_phone,
        'records_without_phone': records_without_phone,
        'phone_coverage': phone_coverage
    }

def _looks_like_phone_number(text):
    """Check if text looks like a phone number."""
    if not text or len(str(text).strip()) < 7:
        return False

    # Remove common separators and check if it's mostly digits
    cleaned = re.sub(r'[^\d]', '', str(text))

    # Valid phone number should have 10 or 11 digits
    if len(cleaned) == 10 or (len(cleaned) == 11 and cleaned.startswith('1')):
        return True

    return False

def ensure_phone_columns_exist(df):
    """Ensure dataframe has standard phone columns - REMOVED: Primary_Phone and Secondary_Phone no longer needed"""
    # This function has been disabled - phone columns are no longer required
    return df, False

def analyze_address_coverage(df):
    """Analyze address coverage in the dataset for reverse address extraction."""
    # First analyze name coverage
    name_analysis = analyze_name_coverage(df)

    # Check if we have BCPA processed columns first
    if 'BCPA_Search_Format' in df.columns:
        # Enhanced analysis for AI-formatted files
        valid_addresses = df['BCPA_Search_Format'].dropna().astype(str).str.strip()
        valid_addresses = valid_addresses[(valid_addresses != '') & (valid_addresses != 'nan')]

        records_with_address = len(valid_addresses)
        records_without_address = len(df) - records_with_address
        address_coverage = round((records_with_address / len(df)) * 100, 1) if len(df) > 0 else 0

        # Additional AI formatter statistics
        ai_stats = {}
        if 'BCPA_Valid' in df.columns:
            broward_valid = df['BCPA_Valid'].sum() if df['BCPA_Valid'].dtype == 'bool' else (df['BCPA_Valid'] == True).sum()
            ai_stats['broward_county_addresses'] = int(broward_valid)
            ai_stats['broward_coverage'] = round((broward_valid / len(df)) * 100, 1) if len(df) > 0 else 0

        if 'BCPA_City' in df.columns:
            cities = df['BCPA_City'].dropna().value_counts()
            ai_stats['top_cities'] = cities.head(5).to_dict()

        result = {
            'address_column': 'BCPA_Search_Format (AI Enhanced)',
            'records_with_address': records_with_address,
            'records_without_address': records_without_address,
            'address_coverage': address_coverage,
            'ai_formatted': True,
            'ai_stats': ai_stats
        }

        # Add name analysis
        result.update(name_analysis)
        return result

    # For files without BCPA_Search_Format, look for standard address columns
    address_columns = [
        'Address', 'address', 'ADDRESS',
        'Street Address', 'street_address', 'STREET_ADDRESS',
        'DirectName_Address', 'full_address', 'Full Address',
        'street', 'Street'
    ]

    # Also check for separated address components
    separated_columns = [
        'House Number', 'house_number', 'HOUSE_NUMBER',
        'Street Name', 'street_name', 'STREET_NAME'
    ]

    address_col = None

    # First try combined address columns
    for col in address_columns:
        if col in df.columns:
            address_col = col
            break

    # If no combined address, check for separated components
    if not address_col:
        house_col = None
        street_col = None

        for col in ['House Number', 'house_number', 'HOUSE_NUMBER']:
            if col in df.columns:
                house_col = col
                break

        for col in ['Street Name', 'street_name', 'STREET_NAME']:
            if col in df.columns:
                street_col = col
                break

        # If we have both house and street components, estimate address coverage
        if house_col and street_col:
            valid_addresses = 0
            for idx, row in df.iterrows():
                house = str(row[house_col]).strip() if pd.notna(row[house_col]) else ''
                street = str(row[street_col]).strip() if pd.notna(row[street_col]) else ''
                if house and street and house != 'nan' and street != 'nan':
                    valid_addresses += 1

            records_with_address = valid_addresses
            records_without_address = len(df) - records_with_address
            address_coverage = round((records_with_address / len(df)) * 100, 1) if len(df) > 0 else 0

            result = {
                'address_column': f'{house_col} + {street_col}',
                'records_with_address': records_with_address,
                'records_without_address': records_without_address,
                'address_coverage': address_coverage
            }
        else:
            # No address columns found - estimate based on pattern
            result = {
                'address_column': 'Address columns detected',
                'records_with_address': len(df),  # Assume all have addresses if no name columns
                'records_without_address': 0,
                'address_coverage': 100.0
            }
    else:
        # We found a combined address column
        valid_addresses = df[address_col].dropna().astype(str).str.strip()
        valid_addresses = valid_addresses[(valid_addresses != '') & (valid_addresses != 'nan')]

        records_with_address = len(valid_addresses)
        records_without_address = len(df) - records_with_address
        address_coverage = round((records_with_address / len(df)) * 100, 1) if len(df) > 0 else 0

        result = {
            'address_column': address_col,
            'records_with_address': records_with_address,
            'records_without_address': records_without_address,
            'address_coverage': address_coverage
        }

    # Add name analysis to the result
    result.update(name_analysis)
    return result

def analyze_name_coverage(df):
    """Analyze how many records have names vs just addresses."""
    # Comprehensive name columns - covers most common variations
    name_columns = [
        # Basic name variations
        'Name', 'name', 'NAME',
        'First Name', 'first_name', 'FIRST_NAME', 'FirstName', 'First_Name',
        'Last Name', 'last_name', 'LAST_NAME', 'LastName', 'Last_Name',
        'Full Name', 'full_name', 'FULL_NAME', 'FullName', 'Full_Name',

        # Owner variations
        'Owner Name', 'owner_name', 'OWNER_NAME', 'OwnerName', 'Owner_Name',
        'Owner Name 1', 'owner_name_1', 'OWNER_NAME_1', 'OwnerName1', 'Owner_Name_1',

        # Property-specific variations
        'Property Owner', 'property_owner', 'PROPERTY_OWNER', 'PropertyOwner', 'Property_Owner',
        'Taxpayer Name', 'taxpayer_name', 'TAXPAYER_NAME', 'TaxpayerName', 'Taxpayer_Name',
        'Taxpayer', 'taxpayer', 'TAXPAYER',

        # Real estate specific terms
        'Deed Holder', 'deed_holder', 'DEED_HOLDER', 'DeedHolder', 'Deed_Holder',
        'Legal Owner', 'legal_owner', 'LEGAL_OWNER', 'LegalOwner', 'Legal_Owner',
        'Title Holder', 'title_holder', 'TITLE_HOLDER', 'TitleHolder', 'Title_Holder',

        # Contact variations
        'Contact Name', 'contact_name', 'CONTACT_NAME', 'ContactName', 'Contact_Name',
        'Primary Contact', 'primary_contact', 'PRIMARY_CONTACT', 'PrimaryContact', 'Primary_Contact',
        'Owner Contact', 'owner_contact', 'OWNER_CONTACT', 'OwnerContact', 'Owner_Contact',

        # Business/entity variations
        'Company Name', 'company_name', 'COMPANY_NAME', 'CompanyName', 'Company_Name',
        'Entity Name', 'entity_name', 'ENTITY_NAME', 'EntityName', 'Entity_Name',
        'Business Name', 'business_name', 'BUSINESS_NAME', 'BusinessName', 'Business_Name',
        'Corporation', 'corporation', 'CORPORATION',
        'LLC', 'llc', 'Inc', 'INC', 'Corp', 'CORP',

        # Processed file variations
        'DirectName_FirstName', 'DirectName_LastName', 'DirectName_Cleaned',
        'Processed_Name', 'processed_name', 'PROCESSED_NAME',

        # Person/Individual indicators
        'Individual', 'individual', 'INDIVIDUAL',
        'Person', 'person', 'PERSON',
        'Client', 'client', 'CLIENT',
        'Customer', 'customer', 'CUSTOMER'
    ]

    # Find name columns that exist in the data
    existing_name_cols = [col for col in name_columns if col in df.columns]

    logger.info(f"🔍 Name detection - Found {len(existing_name_cols)} name columns: {existing_name_cols}")

    if not existing_name_cols:
        # No name columns found - all records are without names
        logger.info("📋 No name columns detected - all records are address-only targets")
        return {
            'has_names': False,
            'records_with_names': 0,
            'records_without_names': len(df),
            'name_coverage': 0.0,
            'target_records_for_extraction': len(df),  # All records are targets
            'name_columns_found': []
        }

    # Enhanced name validation function
    def is_valid_name(value):
        """Enhanced name validation with better detection"""
        if not value or pd.isna(value):
            return False

        value_str = str(value).strip()

        # Empty or placeholder values
        if not value_str or len(value_str) < 2:
            return False

        # Common non-name values (case insensitive)
        non_names = {
            'n/a', 'na', 'none', 'null', 'blank', 'empty', 'unknown', 'not available',
            'not provided', 'no name', 'noname', 'no data', 'missing', 'tbd', 'tba',
            'pending', 'temp', 'temporary', 'test', 'example', 'sample', 'placeholder',
            '---', '--', '-', 'nil', 'void', 'invalid', 'error', 'default'
        }

        if value_str.lower() in non_names:
            return False

        # Check for purely numeric values
        if value_str.replace('.', '').replace(',', '').replace('-', '').replace(' ', '').isdigit():
            return False

        # Check for email-like patterns (likely not names)
        if '@' in value_str and '.' in value_str:
            return False

        # Check for URL-like patterns
        if any(protocol in value_str.lower() for protocol in ['http://', 'https://', 'www.', '.com', '.org', '.net']):
            return False

        # Must contain at least one letter
        if not re.search(r'[A-Za-z]', value_str):
            return False

        # Check for realistic name patterns
        # Single letter (likely initial, not full name)
        if len(value_str) == 1:
            return False

        # Very long strings (likely addresses or descriptions)
        if len(value_str) > 100:
            return False

        # Check for address-like patterns
        address_indicators = [
            r'\d+\s+(st|street|ave|avenue|rd|road|blvd|boulevard|dr|drive|ln|lane|ct|court|pl|place|way|ter|terrace)',
            r'(apt|apartment|unit|suite|ste|#)\s*\d+',
            r'\d{5}(-\d{4})?$',  # ZIP codes
            r'(north|south|east|west|n|s|e|w)\s+(st|street|ave|avenue|rd|road)',
            r'po\s*box\s*\d+',
            r'\d+\s+[a-z]+\s+(st|ave|rd|dr|ln|ct|pl|way|blvd)'
        ]

        for pattern in address_indicators:
            if re.search(pattern, value_str, re.IGNORECASE):
                return False

        # Check for phone number patterns
        phone_patterns = [
            r'\(\d{3}\)\s*\d{3}-\d{4}',
            r'\d{3}-\d{3}-\d{4}',
            r'\d{3}\.\d{3}\.\d{4}',
            r'\+?\d{10,15}'
        ]

        for pattern in phone_patterns:
            if re.search(pattern, value_str):
                return False

        # Enhanced name patterns - look for realistic names
        name_patterns = [
            r'^[A-Za-z]+\s+[A-Za-z]+',  # First Last
            r'^[A-Za-z]+,\s*[A-Za-z]+',  # Last, First
            r'^[A-Za-z]+\s+[A-Z]\.\s*[A-Za-z]+',  # First M. Last
            r'^[A-Za-z-\']+\s+[A-Za-z-\']+',  # Names with hyphens/apostrophes
        ]

        # Must match at least one name pattern OR be a single word that looks like a name
        has_name_pattern = any(re.search(pattern, value_str) for pattern in name_patterns)
        is_single_word_name = len(value_str.split()) == 1 and value_str.isalpha() and len(value_str) > 2

        if not (has_name_pattern or is_single_word_name):
            return False

        # Additional business entity checks
        business_indicators = [
            'llc', 'inc', 'corp', 'corporation', 'company', 'co.', 'ltd', 'limited',
            'partnership', 'trust', 'foundation', 'association', 'society', 'group',
            'enterprises', 'holdings', 'investments', 'properties', 'development',
            'management', 'services', 'solutions', 'systems', 'technologies'
        ]

        # Business entities are valid names too
        if any(indicator in value_str.lower() for indicator in business_indicators):
            return True

        return True

    # Check for records with actual name data
    records_with_names = 0
    records_details = []  # For debugging

    for idx, row in df.iterrows():
        has_name = False
        row_names = []  # Track what names were found for this row

        for col in existing_name_cols:
            value = row[col]

            if is_valid_name(value):
                has_name = True
                value_str = str(value).strip()
                row_names.append(f"{col}='{value_str[:30]}..'" if len(value_str) > 30 else f"{col}='{value_str}'")

        if has_name:
            records_with_names += 1
            if idx < 5:  # Log first 5 records for debugging
                logger.info(f"✅ Record {idx+1} HAS names: {', '.join(row_names)}")
                records_details.append(f"Row {idx+1}: {', '.join(row_names)}")
        else:
            if idx < 5:  # Log first 5 records for debugging
                logger.info(f"❌ Record {idx+1} NO names detected")
                records_details.append(f"Row {idx+1}: No valid names")

    logger.info(f"📊 Enhanced name analysis complete: {records_with_names}/{len(df)} records have names")
    if records_details:
        logger.info(f"🔍 Sample analysis: {'; '.join(records_details[:3])}")

    records_without_names = len(df) - records_with_names
    name_coverage = round((records_with_names / len(df)) * 100, 1) if len(df) > 0 else 0

    result = {
        'has_names': records_with_names > 0,
        'records_with_names': records_with_names,
        'records_without_names': records_without_names,
        'name_coverage': name_coverage,
        'target_records_for_extraction': records_without_names,  # Only target records without names
        'name_columns_found': existing_name_cols,
        'total_records': len(df)
    }

    logger.info(f"🎯 Final enhanced result: {records_with_names} with names, {records_without_names} targets for extraction")
    return result

    # Look for common address columns
    address_columns = [
        'Address', 'address', 'ADDRESS',
        'Street Address', 'street_address', 'STREET_ADDRESS',
        'DirectName_Address', 'full_address', 'Full Address',
        'BCPA_Street_Address', 'street', 'Street'
    ]

    # Also check for separated address components
    separated_columns = [
        'House Number', 'house_number', 'HOUSE_NUMBER',
        'Street Name', 'street_name', 'STREET_NAME'
    ]

    address_col = None

    # First try combined address columns
    for col in address_columns:
        if col in df.columns:
            address_col = col
            break

    # If no combined address, check for separated components
    if not address_col:
        house_col = None
        street_col = None

        for col in ['House Number', 'house_number', 'HOUSE_NUMBER']:
            if col in df.columns:
                house_col = col
                break

        for col in ['Street Name', 'street_name', 'STREET_NAME']:
            if col in df.columns:
                street_col = col
                break

        if house_col and street_col:
            # Count records with both house number and street name
            house_data = df[house_col].dropna().astype(str).str.strip()
            street_data = df[street_col].dropna().astype(str).str.strip()

            valid_house = (house_data != '') & (house_data != 'nan')
            valid_street = (street_data != '') & (street_data != 'nan')
            valid_addresses = valid_house & valid_street

            records_with_address = valid_addresses.sum()
            records_without_address = len(df) - records_with_address
            address_coverage = round((records_with_address / len(df)) * 100, 1) if len(df) > 0 else 0

            return {
                'address_column': f'{house_col} + {street_col} (Separated)',
                'records_with_address': records_with_address,
                'records_without_address': records_without_address,
                'address_coverage': address_coverage
            }

    if address_col:
        # Count non-empty addresses
        valid_addresses = df[address_col].dropna().astype(str).str.strip()
        valid_addresses = valid_addresses[(valid_addresses != '') & (valid_addresses != 'nan')]

        records_with_address = len(valid_addresses)
        records_without_address = len(df) - records_with_address
        address_coverage = round((records_with_address / len(df)) * 100, 1) if len(df) > 0 else 0

        return {
            'address_column': address_col,
            'records_with_address': records_with_address,
            'records_without_address': records_without_address,
            'address_coverage': address_coverage
        }
    else:
        return {
            'address_column': 'No address columns found',
            'records_with_address': 0,
            'records_without_address': len(df),
            'address_coverage': 0
        }

@app.route('/')
def index():
    """Main enterprise dashboard."""
    logger.info("Enterprise dashboard accessed")
    response = make_response(render_template('enterprise_fixed.html'))
    # Add cache-busting headers to ensure fresh content
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and initial analysis with user session isolation."""
    try:
        # Get user session and directories
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        logger.info(f"📁 File upload request from user: {user_id}")

        # Check if the post request has the file part
        if 'file' not in request.files:
            logger.error(f"❌ No file part in request for user: {user_id}")
            return jsonify({'error': 'No file part in request'}), 400

        file = request.files['file']

        # If user does not select file, browser submits empty part without filename
        if file.filename == '':
            logger.error(f"❌ No file selected for user: {user_id}")
            return jsonify({'error': 'No file selected'}), 400

        if file and file.filename and allowed_file(file.filename):
            logger.info(f"🔄 Processing file: {file.filename} for user: {user_id}")
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{filename}"
            filepath = os.path.join(user_config['UPLOAD_FOLDER'], filename)

            # Save the uploaded file to user-specific directory
            file.save(filepath)
            logger.info(f"💾 File saved to user directory: {filepath}")

            try:
                # Get tab type from form data
                tab_type = request.form.get('tabType', 'phone')  # Default to phone if not specified
                logger.info(f"🎯 Processing for tab type: {tab_type} (User: {user_id})")

                # FIRST: Analyze the ORIGINAL uploaded file (before any processing)
                original_df = read_data_file(filepath)
                logger.info(f"📊 Analyzing original uploaded file: {len(original_df)} records (User: {user_id})")

                if tab_type == 'phone':
                    original_analysis = analyze_phone_coverage(original_df)
                    logger.info(f"📊 Original file phone analysis: {original_analysis} (User: {user_id})")
                elif tab_type == 'columnSync':
                    # For Column Syncer, analyze phone coverage
                    original_analysis = analyze_phone_coverage(original_df)
                    logger.info(f"📊 Original file Column Syncer analysis: {original_analysis} (User: {user_id})")
                else:
                    original_analysis = analyze_address_coverage(original_df)
                    logger.info(f"📊 Original file address analysis: {original_analysis} (User: {user_id})")

                # Route processing based on tab type
                if tab_type == 'phone':
                    # Use ONLY AI-powered phone formatter for phone tab
                    from intelligent_phone_formatter_v2 import IntelligentPhoneFormatter
                    phone_formatter = IntelligentPhoneFormatter()

                    # Process with AI phone formatter
                    result = phone_formatter.format_csv_for_phone_extraction(filepath)

                    if result['success']:
                        processed_df = pd.read_csv(result['output_path'])
                        logger.info(f"✅ AI phone formatting completed - {len(processed_df)} records (User: {user_id})")
                        # Copy the AI-formatted file to user's results folder
                        import shutil
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        results_filename = f"phone_formatted_{timestamp}.csv"
                        results_path = os.path.join(user_config['RESULTS_FOLDER'], results_filename)
                        shutil.copy2(result['output_path'], results_path)
                    else:
                        # If AI formatter fails, return error instead of using buggy fallback
                        logger.error(f"❌ AI phone formatter failed: {result.get('error', 'Unknown error')} (User: {user_id})")
                        return jsonify({
                            'error': f"AI phone formatter failed: {result.get('error', 'Please check your file format and try again')}"
                        }), 400

                elif tab_type == 'columnSync':
                    # Use Column Syncer for mobile validation - immediate processing and download
                    from column_syncer import ColumnSyncer
                    logger.info(f"🔧 Starting Column Syncer processing... (User: {user_id})")

                    syncer = ColumnSyncer()
                    result = syncer.process_file(filepath)

                    if result['success']:
                        # Read the processed file
                        processed_df = pd.read_csv(result['output_file'])
                        logger.info(f"✅ Column Syncer complete: {len(processed_df)} records processed (User: {user_id})")

                        # Save with Cleaned_ prefix + original filename to user's results folder
                        original_name = os.path.splitext(filename)[0]
                        original_ext = os.path.splitext(filename)[1]
                        clean_filename = f"Cleaned_{original_name}{original_ext}"
                        results_path = os.path.join(user_config['RESULTS_FOLDER'], clean_filename)
                        processed_df.to_csv(results_path, index=False)
                        logger.info(f"💾 Column Syncer results saved: {clean_filename} (User: {user_id})")

                        # Return immediate download response instead of analysis
                        stats = result.get('stats', {})
                        return jsonify({
                            'success': True,
                            'message': f'Processing complete! Found {stats.get("mobile_numbers_found", 0)} mobile numbers.',
                            'download': True,
                            'download_filename': clean_filename,
                            'download_url': f'/download/{clean_filename}',
                            'stats_summary': f'Processed {stats.get("total_input_rows", 0)} records, found {stats.get("mobile_numbers_found", 0)} mobile numbers'
                        })
                    else:
                        logger.error(f"❌ Column Syncer processing failed: {result.get('error', result.get('message', 'Unknown error'))}")
                        return jsonify({'error': f"Column Syncer processing failed: {result.get('error', result.get('message', 'Please check your file format and try again'))}"}), 400

                else:
                    # Use original method for address tab
                    try:
                        handler = csv_format_handler.CSVFormatHandler()
                        standardized_file = handler.standardize_to_search_format(filepath)

                        if standardized_file:
                            # Read the standardized file back as DataFrame
                            processed_df = pd.read_csv(standardized_file)
                            logger.info(f"File processed with universal scanner - {len(processed_df)} records")
                        else:
                            # Fallback to old method if new one fails
                            processed_df = csv_format_handler.process_file(filepath)
                            logger.info(f"File processed with fallback method - {len(processed_df)} records")
                    except Exception as e:
                        logger.error(f"❌ Address processing error: {e}")
                        processed_df = csv_format_handler.process_file(filepath)
                        logger.info(f"File processed with fallback method - {len(processed_df)} records")

                # ENSURE PHONE COLUMNS EXIST - Critical for phone number extraction
                processed_df, phone_columns_added = ensure_phone_columns_exist(processed_df)
                if phone_columns_added:
                    logger.info("📱 Standard phone columns added to file")

                # Use AI-powered intelligent formatter for address tab
                if tab_type == 'address':
                    # Import and use intelligent AI address formatter
                    try:
                        formatter = IntelligentAddressFormatter()
                        formatted_file = formatter.process_file(filepath)
                        processed_df = pd.read_csv(formatted_file)
                        logger.info(f"AI address formatting completed - {len(processed_df)} records")

                        # Re-ensure phone columns after address processing
                        processed_df, phone_columns_added = ensure_phone_columns_exist(processed_df)

                    except Exception as e:
                        logger.warning(f"Enhanced address parser failed, using basic processing: {e}")

                # Use the ORIGINAL file analysis for display (show all records)
                try:
                    if tab_type == 'columnSync':
                        # Column Syncer analysis was already set above
                        pass
                    else:
                        analysis = original_analysis  # Use the original file analysis we computed earlier
                        analysis_key = 'phone_analysis' if tab_type == 'phone' else 'address_analysis'
                    logger.info(f"Using analysis for display: {analysis}")
                except Exception as analysis_error:
                    logger.error(f"Analysis failed: {analysis_error}")
                    import traceback
                    traceback.print_exc()
                    # Provide fallback analysis
                    analysis = {
                        'total_records': len(original_df),
                        'error': f'Analysis failed: {str(analysis_error)}'
                    }
                    if tab_type == 'phone':
                        analysis_key = 'phone_analysis'
                    else:
                        analysis_key = 'address_analysis'

                # Save processed version for later use
                try:
                    processed_filepath = filepath.replace('.csv', '_processed.csv').replace('.xlsx', '_processed.csv').replace('.xls', '_processed.csv')
                    processed_df.to_csv(processed_filepath, index=False, encoding='utf-8')
                    logger.info(f"Processed file saved to: {processed_filepath}")
                except Exception as save_error:
                    logger.error(f"Failed to save processed file: {save_error}")
                    # Continue without processed file path
                    processed_filepath = filepath

                response_data = {
                    'message': 'File uploaded and processed successfully',
                    'filename': filename,
                    'filepath': processed_filepath,  # Use PROCESSED file for data display in frontend
                    'processed_filepath': processed_filepath,  # Keep processed file for analysis
                    'original_filepath': filepath,  # Keep original file reference for merging
                    'original_filename': file.filename,
                    'file_size_kb': round(os.path.getsize(filepath) / 1024, 2),
                    'file_type': os.path.splitext(filepath)[1],
                    'total_records': len(original_df),  # Use ORIGINAL file count for frontend stats
                    'original_total_records': len(original_df),  # Keep original count for reference
                    'processed_records': len(processed_df),  # Show processed records count
                    'columns': list(processed_df.columns),
                    'tab_type': tab_type
                }
                response_data[analysis_key] = analysis

                # Debug logging to see what's being sent to browser
                logger.info(f"🐛 DEBUG: Sending response to browser:")
                logger.info(f"🐛   Total records: {response_data['total_records']}")
                logger.info(f"🐛   Analysis key: {analysis_key}")
                logger.info(f"🐛   Analysis data: {analysis}")

                if analysis_key == 'address_analysis':
                    logger.info(f"🐛   Records without names: {analysis.get('records_without_names', 'NOT FOUND')}")
                    logger.info(f"🐛   Target records: {analysis.get('target_records_for_extraction', 'NOT FOUND')}")

                # Create response with cache-busting headers
                response = jsonify(response_data)
                response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
                response.headers['Pragma'] = 'no-cache'
                response.headers['Expires'] = '0'
                return response

            except Exception as e:
                logger.error(f"Error processing file: {str(e)}")
                return jsonify({'error': f'Error processing file: {str(e)}'}), 500

        else:
            logger.error(f"File type not allowed: {file.filename}")
            return jsonify({'error': 'File type not allowed. Please upload CSV or Excel files only.'}), 400

    except RequestEntityTooLarge:
        return jsonify({'error': 'File too large. Maximum size is 100MB.'}), 413
    except Exception as e:
        logger.error(f"Unexpected error in upload: {str(e)}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

@app.route('/force_fresh_analysis', methods=['POST'])
def force_fresh_analysis():
    """Force a fresh analysis bypassing all caches"""
    try:
        data = request.get_json()
        filepath = data.get('filepath', 'HALLANDALE BEACH-All built 1920-2025.csv')

        # Force read the file fresh
        df = read_data_file(filepath)
        logger.info(f"🔄 FORCE FRESH: Reading {filepath} with {len(df)} records")

        # Run fresh analysis
        analysis = analyze_address_coverage(df)
        logger.info(f"🔄 FORCE FRESH: Analysis result: {analysis.get('records_without_names')} without names")

        response_data = {
            'message': 'Fresh analysis completed',
            'total_records': len(df),
            'address_analysis': analysis,
            'timestamp': pd.Timestamp.now().isoformat()
        }

        # Force no-cache headers
        response = jsonify(response_data)
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response

    except Exception as e:
        logger.error(f"Force fresh analysis error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/test_name_recognition', methods=['GET'])
def test_name_recognition():
    """Test endpoint to verify name recognition is working correctly"""
    try:
        # Test with the HALLANDALE BEACH file
        df = read_data_file('HALLANDALE BEACH-All built 1920-2025.csv')
        result = analyze_address_coverage(df)

        return jsonify({
            'message': 'Direct test of name recognition function',
            'total_records': len(df),
            'analysis_result': result,
            'expected_records_without_names': 49,
            'actual_records_without_names': result.get('records_without_names'),
            'is_working_correctly': result.get('records_without_names') == 49
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/separate_phones', methods=['POST'])
def separate_records():
    """Separate records based on phone numbers or addresses depending on context."""
    try:
        data = request.get_json()
        filepath = data.get('filepath')
        tab_type = data.get('tabType', 'phone')  # Get tab type to determine separation logic

        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404

        # Load the processed file
        df = read_data_file(filepath, encoding='utf-8')

        if tab_type == 'phone':
            # Separate by phone numbers
            phone_columns = ['phone', 'Phone', 'PHONE', 'phone_number', 'Phone Number', 'PHONE_NUMBER']
            phone_col = None
            for col in phone_columns:
                if col in df.columns:
                    phone_col = col
                    break

            if phone_col:
                # Separate records
                with_data = df[df[phone_col].notna() & (df[phone_col].astype(str).str.strip() != '')]
                without_data = df[df[phone_col].isna() | (df[phone_col].astype(str).str.strip() == '')]

                with_label = 'with_phones'
                without_label = 'without_phones'
                with_description = 'Records With Phone'
                without_description = 'Records Without Phone'
            else:
                with_data = pd.DataFrame()
                without_data = df.copy()
                with_label = 'with_phones'
                without_label = 'without_phones'
                with_description = 'Records With Phone'
                without_description = 'Records Without Phone'

        else:  # address tab
            # Separate by addresses
            if 'BCPA_Search_Format' in df.columns:
                # Use enhanced address format
                valid_addresses = df['BCPA_Search_Format'].notna() & \
                                (df['BCPA_Search_Format'].astype(str).str.strip() != '') & \
                                (df['BCPA_Search_Format'].astype(str).str.strip() != 'nan')

                with_data = df[valid_addresses]
                without_data = df[~valid_addresses]
            else:
                # Look for other address columns
                address_columns = ['Address', 'address', 'ADDRESS', 'Street Address', 'street_address']
                address_col = None
                for col in address_columns:
                    if col in df.columns:
                        address_col = col
                        break

                if address_col:
                    with_data = df[df[address_col].notna() & (df[address_col].astype(str).str.strip() != '')]
                    without_data = df[df[address_col].isna() | (df[address_col].astype(str).str.strip() == '')]
                else:
                    with_data = pd.DataFrame()
                    without_data = df.copy()

            with_label = 'with_addresses'
            without_label = 'without_addresses'
            with_description = 'Records With Valid Addresses'
            without_description = 'Records Without Valid Addresses'

        # Save separated files in user-specific directory
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        base_name = os.path.splitext(os.path.basename(filepath))[0]

        with_file = os.path.join(user_config['RESULTS_FOLDER'], f"{base_name}_{with_label}.csv")
        without_file = os.path.join(user_config['RESULTS_FOLDER'], f"{base_name}_{without_label}.csv")

        files_created = []

        if not with_data.empty:
            with_data.to_csv(with_file, index=False, encoding='utf-8')
            files_created.append({
                'type': with_label,
                'count': len(with_data),
                'description': with_description,
                'download_url': url_for('download_file', filename=os.path.basename(with_file))
            })

        if not without_data.empty:
            without_data.to_csv(without_file, index=False, encoding='utf-8')
            files_created.append({
                'type': without_label,
                'count': len(without_data),
                'description': without_description,
                'download_url': url_for('download_file', filename=os.path.basename(without_file))
            })

        return jsonify({
            'message': 'Files separated successfully',
            'files': files_created
        })

    except Exception as e:
        logger.error(f"Error separating records: {str(e)}")
        return jsonify({'error': f'Error separating files: {str(e)}'}), 500

@app.route('/analyze', methods=['POST'])
def analyze():
    """Process files based on analysis type (phone, address, bcpa) with user session isolation."""
    try:
        # Get user session and directories
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        
        data = request.get_json()
        filepath = data.get('filepath')
        analysis_type = data.get('analysis_type', 'phone')
        max_records = int(data.get('max_records', 0))  # 0 means unlimited

        if not filepath or not os.path.exists(filepath):
            logger.error(f"❌ File not found: {filepath} (User: {user_id})")
            return jsonify({'error': 'File not found'}), 404

        logger.info(f"🔄 Starting analysis: {analysis_type} for {filepath} (User: {user_id})")

        # The filepath sent from frontend is typically the PROCESSED file
        # We need to determine the original file and processed file paths
        if '_processed.csv' in filepath:
            # Frontend sent processed file - derive original file
            # Extract clean filename by removing timestamp prefix (e.g., "20250822_100515_ballz_processed.csv" -> "ballz")
            processed_basename = os.path.basename(filepath)
            if processed_basename.startswith('202') and '_' in processed_basename:
                # Remove timestamp prefix like "20250822_100515_"
                parts = processed_basename.split('_')
                if len(parts) >= 3:
                    clean_name = '_'.join(parts[2:]).replace('_processed.csv', '')
                else:
                    clean_name = processed_basename.replace('_processed.csv', '')
            else:
                clean_name = processed_basename.replace('_processed.csv', '')

            # Try different extensions for the clean original file
            # First, try to find with timestamp prefix (most common case)
            upload_dir = user_config['UPLOAD_FOLDER']  # Use user-specific upload directory
            timestamp_pattern = processed_basename.split('_')[0] + '_' + processed_basename.split('_')[1]  # Extract timestamp like "20250822_111415"

            # Try different combinations: timestamped and clean names with different extensions
            possible_files = [
                f"{timestamp_pattern}_{clean_name}.xlsx",  # 20250822_111415_ballz.xlsx
                f"{timestamp_pattern}_{clean_name}.csv",   # 20250822_111415_ballz.csv
                f"{timestamp_pattern}_{clean_name}.xls",   # 20250822_111415_ballz.xls
                f"{clean_name}.xlsx",                      # ballz.xlsx
                f"{clean_name}.csv",                       # ballz.csv
                f"{clean_name}.xls"                        # ballz.xls
            ]

            original_filepath = None
            for possible_file in possible_files:
                test_path = os.path.join(upload_dir, possible_file)
                if os.path.exists(test_path):
                    original_filepath = test_path
                    break

            # Fallback if no file found
            if original_filepath is None:
                original_filepath = filepath.replace(processed_basename, f"{clean_name}.xlsx")
                logger.warning(f"⚠️ Original file not found, using fallback: {original_filepath}")

            analysis_filepath = filepath  # Use processed file for analysis
            logger.info(f"🔍 Frontend sent processed file: {filepath}")
            logger.info(f"🔍 Extracted clean name: {clean_name}")
            logger.info(f"🔍 Derived original file: {original_filepath}")
            logger.info(f"🔍 Original file exists: {os.path.exists(original_filepath) if original_filepath else False}")
            logger.info(f"🔍 Using processed file for analysis: {analysis_filepath}")
        else:
            # Frontend sent original file - derive processed file
            processed_filepath = filepath.replace('.csv', '_processed.csv').replace('.xlsx', '_processed.csv').replace('.xls', '_processed.csv')
            if os.path.exists(processed_filepath):
                analysis_filepath = processed_filepath  # Use processed file for actual analysis
                original_filepath = filepath  # Original file for merging
                logger.info(f"🔍 Using original file for display: {filepath}")
                logger.info(f"🔍 Using processed file for analysis: {analysis_filepath}")
                logger.info(f"🔍 Using original file for merging: {original_filepath}")
            else:
                # No processed file found, use original for both
                analysis_filepath = filepath
                original_filepath = filepath
                logger.info(f"🔍 No processed file found, using original for all operations: {filepath}")

        # Load the data
        df = read_data_file(analysis_filepath, encoding='utf-8')  # Use analysis file

        if analysis_type == 'phone':
            # Phone extraction pipeline - use analysis file with timeout
            import threading
            import time

            result_container = {'result': None, 'error': None, 'completed': False}

            def run_phone_processing():
                try:
                    result_container['result'] = phone_search_pipeline.process_phone_extraction(analysis_filepath, max_records, user_config)
                    result_container['completed'] = True
                except Exception as e:
                    result_container['error'] = str(e)
                    result_container['completed'] = True

            # Start phone processing in a separate thread
            processing_thread = threading.Thread(target=run_phone_processing)
            processing_thread.daemon = True
            processing_thread.start()

            # Wait for completion - no timeout for 3-4 hour processing
            processing_thread.join()

            # No timeout check needed - unlimited processing time

            if result_container['error']:
                logger.error(f"❌ Phone processing failed: {result_container['error']}")
                return jsonify({'error': f'Phone processing failed: {result_container["error"]}'}), 500

            if not result_container['completed']:
                logger.error("❌ Phone processing did not complete properly")
                return jsonify({'error': 'Phone processing did not complete properly'}), 500

            result_file = result_container['result']

            # CRITICAL FIX: Check for batch result files and combined results
            # First check if combined results already exist in user's results folder
            combined_pattern = os.path.join(user_config['RESULTS_FOLDER'], 'phone_extraction_*.csv')
            combined_files = glob.glob(combined_pattern)

            # Filter for files that might contain batch results (look for newer files)
            recent_combined = [f for f in combined_files if os.path.getctime(f) > (time.time() - 300)]  # Last 5 minutes

            if recent_combined:
                # Find the most recent combined result file
                latest_combined = max(recent_combined, key=os.path.getctime)
                logger.info(f"🔍 Found recent combined results file: {latest_combined} (User: {user_id})")

                # Check if it has phone data (columns with phones and actual data)
                try:
                    temp_df = pd.read_csv(latest_combined)
                    phone_cols = [col for col in temp_df.columns if 'Phone' in col]
                    phone_data_found = any(temp_df[col].notna().any() for col in phone_cols if col in temp_df.columns)

                    if phone_data_found:
                        logger.info(f"✅ Combined file has phone data - using existing results")
                        result_file = latest_combined

                        # ENHANCED MERGE: Merge phone results back with original file
                        logger.info(f"🔗 Starting enhanced merge with combined results...")
                        try:
                            # Use Enhanced Phone Merger for automatic merging
                            from enhanced_phone_merger import EnhancedPhoneMerger
                            merger = EnhancedPhoneMerger()

                            # Create merged filename with simple naming - ALWAYS CSV
                            original_basename = os.path.basename(original_filepath)
                            base_name = os.path.splitext(original_basename)[0]
                            merged_filename = f"Merged_{base_name}.csv"
                            merged_filepath = os.path.join(user_config['RESULTS_FOLDER'], merged_filename)

                            # Perform the merge using original file + combined results
                            merge_result = merger.merge_phone_results(original_filepath, latest_combined, merged_filepath)

                            if merge_result.get('success'):
                                logger.info(f"🎉 Enhanced merge completed successfully! (User: {user_id})")
                                logger.info(f"📞 Records updated: {merge_result.get('records_updated', 0)} (User: {user_id})")

                                # Return the merged file as the main result
                                result_file = merged_filepath

                                # Prepare response for immediate return
                                download_url = url_for('download_file', filename=os.path.basename(result_file))
                                logger.info(f"📥 Auto-downloading merged file: {os.path.basename(result_file)} (User: {user_id})")

                                response = jsonify({
                                    'message': f'Process completed with enhanced merge (User: {user_id[:8]})',
                                    'download_url': download_url,
                                    'output_file': os.path.basename(result_file),
                                    'records_updated': merge_result.get('records_updated', 0),
                                    'success_rate': merge_result.get('success_rate', 'N/A'),
                                    'auto_download': True,
                                    'user_id': user_id[:8]  # Show first 8 chars of user ID
                                })

                                # CRITICAL: Return immediately to prevent double JSON response
                                return response
                            else:
                                logger.error(f"❌ Enhanced merge failed: {merge_result.get('error', 'Unknown error')}")
                                # Continue with just the combined results file

                        except Exception as merge_error:
                            logger.error(f"❌ Enhanced merge failed with exception: {merge_error}")
                            # Continue with just the combined results file
                    else:
                        logger.info("⚠️ Combined file found but no phone data detected")
                        # Continue to temp file check
                        pass

                except Exception as e:
                    logger.error(f"Error reading combined file: {e}")
                    # Continue to temp file check
                    pass

            # If no good combined file, check for temp files
            temp_files = []  # Initialize to avoid UnboundLocalError
            if not result_file or not os.path.exists(result_file):
                temp_pattern = os.path.join(user_config['TEMP_FOLDER'], 'temp_processing_*.csv')
                logger.info(f"🔍 Searching for temp files with pattern: {temp_pattern}")
                temp_files = glob.glob(temp_pattern)
                logger.info(f"🔍 Found {len(temp_files)} temp files: {temp_files}")
            else:
                logger.info(f"🔍 Using existing result file: {result_file}")

            if temp_files:
                # Find the most recent temp file
                latest_temp = max(temp_files, key=os.path.getctime)
                logger.info(f"🔍 Found temp file with phone data: {latest_temp}")

                # Check if it actually has phone data
                try:
                    temp_df = pd.read_csv(latest_temp)
                    phone_cols = [col for col in temp_df.columns if 'Phone' in col and temp_df[col].notna().any()]

                    if phone_cols:
                        logger.info(f"✅ Temp file has phone data in columns: {phone_cols}")

                        # Copy temp file to results folder with proper name
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        results_filename = f"phone_extraction_{timestamp}.csv"
                        results_path = os.path.join(app.config['RESULTS_FOLDER'], results_filename)

                        temp_df.to_csv(results_path, index=False, encoding='utf-8')
                        result_file = results_path
                        logger.info(f"📞 Phone data copied to results: {results_path}")

                        # AUTOMATIC MERGE: Automatically merge the phone data back to original file
                        logger.info(f"🔗 Starting automatic merge after phone extraction...")
                        try:
                            # Load both files
                            original_df = read_data_file(original_filepath)  # Use original file
                            processed_df = pd.read_csv(results_path)

                            # Use Enhanced Phone Merger for automatic merging
                            from enhanced_phone_merger import EnhancedPhoneMerger
                            merger = EnhancedPhoneMerger()

                            # Create merged filename with simple naming - ALWAYS CSV
                            # Use the ORIGINAL uploaded filename, not the processed filename
                            original_basename = os.path.basename(original_filepath)  # Use ORIGINAL uploaded file
                            base_name = os.path.splitext(original_basename)[0]
                            merged_filename = f"Merged_{base_name}.csv"  # Simple naming: Merged_originalname.csv
                            merged_filepath = os.path.join(app.config['RESULTS_FOLDER'], merged_filename)

                            # Perform the merge using our enhanced merger
                            merge_result = merger.merge_phone_results(original_filepath, results_path, merged_filepath)  # Use original file

                            if merge_result.get('success'):
                                logger.info(f"🎉 Automatic merge completed successfully!")
                                logger.info(f"📞 Records updated: {merge_result.get('records_updated', 0)}")

                                # Return the merged file as the main result
                                result_file = merged_filepath

                                # Update response to indicate automatic merge completed
                                download_url = url_for('download_file', filename=os.path.basename(result_file))

                                # Add auto-download trigger
                                logger.info(f"📥 Auto-downloading merged file: {os.path.basename(result_file)}")

                                response = jsonify({
                                    'message': f'Process completed',
                                    'download_url': download_url,
                                    'output_file': os.path.basename(result_file),
                                    'records_updated': merge_result.get('records_updated', 0),
                                    'success_rate': merge_result.get('success_rate', 'N/A'),
                                    'auto_download': True  # Single flag for frontend auto-download
                                })

                                # CRITICAL: Return immediately to prevent double JSON response
                                return response
                            else:
                                logger.error(f"❌ Automatic merge failed: {merge_result.get('error', 'Unknown error')}")
                                # Fall back to just returning the phone extraction results
                                result_file = results_path  # Set result_file for fallback

                        except Exception as merge_error:
                            logger.error(f"❌ Automatic merge failed with exception: {merge_error}")
                            # Fall back to just returning the phone extraction results
                            result_file = results_path  # Set result_file for fallback

                    else:
                        logger.warning("⚠️ Temp file found but no phone data detected")
                        # Save original data with proper headers when no phone data found
                        columns_to_drop = ['DirectName_Phone_Primary', 'DirectName_Phone_Secondary', 'DirectName_Phone_All']
                        final_processed_df = temp_df.drop(columns=[col for col in columns_to_drop if col in temp_df.columns])
                        fallback_file = os.path.join(app.config['RESULTS_FOLDER'], f"phone_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                        final_processed_df.to_csv(fallback_file, index=False)
                        result_file = fallback_file

                except Exception as e:
                    logger.error(f"Error processing temp file: {e}")
                    # Create basic fallback result
                    df = read_data_file(analysis_filepath)
                    fallback_file = os.path.join(app.config['RESULTS_FOLDER'], f"phone_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                    df.to_csv(fallback_file, index=False)
                    result_file = fallback_file
            else:
                logger.warning(f"⚠️ No temp files found - searched in: {user_config['TEMP_FOLDER']}")
                logger.info(f"🔍 Temp folder contents: {os.listdir(user_config['TEMP_FOLDER']) if os.path.exists(user_config['TEMP_FOLDER']) else 'Folder does not exist'}")
                logger.info("⚠️ Creating result file from processed data")
                # Create basic result file from processed data
                df = read_data_file(analysis_filepath)
                fallback_file = os.path.join(user_config['RESULTS_FOLDER'], f"phone_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                df.to_csv(fallback_file, index=False)
                result_file = fallback_file

        elif analysis_type == 'address':
            # Enhanced address processing with BCPA and phone separation
            result_file = process_enhanced_address_pipeline(filepath, max_records)

        else:
            return jsonify({'error': 'Invalid analysis type'}), 400

        if result_file and os.path.exists(result_file):
            download_url = url_for('download_file', filename=os.path.basename(result_file))
            return jsonify({
                'message': f'{analysis_type.title()} processing completed successfully',
                'download_url': download_url,
                'output_file': os.path.basename(result_file),
                'auto_download': True  # Enable auto-download and notification for all completions
            })
        else:
            return jsonify({'error': 'Processing failed to generate results'}), 500

    except Exception as e:
        logger.error(f"Error in analyze: {str(e)}")
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500

def process_enhanced_address_pipeline(filepath, max_records):
    """Process file with enhanced address parsing and BCPA owner lookup, then separate by phone status."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Step 1: Filter for records without names (reverse address extraction targets)
        logger.info(f"🔍 Filtering for records without names for reverse address extraction")

        df = read_data_file(filepath, encoding='utf-8')

        # Filter for records without names using the enhanced comprehensive logic
        name_columns = [
            # Basic name variations
            'Name', 'name', 'NAME',
            'First Name', 'first_name', 'FIRST_NAME', 'FirstName', 'First_Name',
            'Last Name', 'last_name', 'LAST_NAME', 'LastName', 'Last_Name',
            'Full Name', 'full_name', 'FULL_NAME', 'FullName', 'Full_Name',

            # Owner variations
            'Owner Name', 'owner_name', 'OWNER_NAME', 'OwnerName', 'Owner_Name',
            'Owner Name 1', 'owner_name_1', 'OWNER_NAME_1', 'OwnerName1', 'Owner_Name_1',

            # Property-specific variations
            'Property Owner', 'property_owner', 'PROPERTY_OWNER', 'PropertyOwner', 'Property_Owner',
            'Taxpayer Name', 'taxpayer_name', 'TAXPAYER_NAME', 'TaxpayerName', 'Taxpayer_Name',
            'Taxpayer', 'taxpayer', 'TAXPAYER',

            # Real estate specific terms
            'Deed Holder', 'deed_holder', 'DEED_HOLDER', 'DeedHolder', 'Deed_Holder',
            'Legal Owner', 'legal_owner', 'LEGAL_OWNER', 'LegalOwner', 'Legal_Owner',
            'Title Holder', 'title_holder', 'TITLE_HOLDER', 'TitleHolder', 'Title_Holder',

            # Contact variations
            'Contact Name', 'contact_name', 'CONTACT_NAME', 'ContactName', 'Contact_Name',
            'Primary Contact', 'primary_contact', 'PRIMARY_CONTACT', 'PrimaryContact', 'Primary_Contact',
            'Owner Contact', 'owner_contact', 'OWNER_CONTACT', 'OwnerContact', 'Owner_Contact',

            # Business/entity variations
            'Company Name', 'company_name', 'COMPANY_NAME', 'CompanyName', 'Company_Name',
            'Entity Name', 'entity_name', 'ENTITY_NAME', 'EntityName', 'Entity_Name',
            'Business Name', 'business_name', 'BUSINESS_NAME', 'BusinessName', 'Business_Name',
            'Corporation', 'corporation', 'CORPORATION',
            'LLC', 'llc', 'Inc', 'INC', 'Corp', 'CORP',

            # Processed file variations
            'DirectName_FirstName', 'DirectName_LastName', 'DirectName_Cleaned',
            'Processed_Name', 'processed_name', 'PROCESSED_NAME',

            # Person/Individual indicators
            'Individual', 'individual', 'INDIVIDUAL',
            'Person', 'person', 'PERSON',
            'Client', 'client', 'CLIENT',
            'Customer', 'customer', 'CUSTOMER'
        ]

        existing_name_cols = [col for col in name_columns if col in df.columns]

        # Enhanced name validation function (same as in analyze_name_coverage)
        def is_valid_name(value):
            """Enhanced name validation with better detection"""
            if not value or pd.isna(value):
                return False

            value_str = str(value).strip()

            # Empty or placeholder values
            if not value_str or len(value_str) < 2:
                return False

            # Common non-name values (case insensitive)
            non_names = {
                'n/a', 'na', 'none', 'null', 'blank', 'empty', 'unknown', 'not available',
                'not provided', 'no name', 'noname', 'no data', 'missing', 'tbd', 'tba',
                'pending', 'temp', 'temporary', 'test', 'example', 'sample', 'placeholder',
                '---', '--', '-', 'nil', 'void', 'invalid', 'error', 'default'
            }

            if value_str.lower() in non_names:
                return False

            # Check for purely numeric values
            if value_str.replace('.', '').replace(',', '').replace('-', '').replace(' ', '').isdigit():
                return False

            # Check for email-like patterns (likely not names)
            if '@' in value_str and '.' in value_str:
                return False

            # Check for URL-like patterns
            if any(protocol in value_str.lower() for protocol in ['http://', 'https://', 'www.', '.com', '.org', '.net']):
                return False

            # Must contain at least one letter
            if not re.search(r'[A-Za-z]', value_str):
                return False

            # Check for realistic name patterns
            # Single letter (likely initial, not full name)
            if len(value_str) == 1:
                return False

            # Very long strings (likely addresses or descriptions)
            if len(value_str) > 100:
                return False

            # Check for address-like patterns
            address_indicators = [
                r'\d+\s+(st|street|ave|avenue|rd|road|blvd|boulevard|dr|drive|ln|lane|ct|court|pl|place|way|ter|terrace)',
                r'(apt|apartment|unit|suite|ste|#)\s*\d+',
                r'\d{5}(-\d{4})?$',  # ZIP codes
                r'(north|south|east|west|n|s|e|w)\s+(st|street|ave|avenue|rd|road)',
                r'po\s*box\s*\d+',
                r'\d+\s+[a-z]+\s+(st|ave|rd|dr|ln|ct|pl|way|blvd)'
            ]

            for pattern in address_indicators:
                if re.search(pattern, value_str, re.IGNORECASE):
                    return False

            # Check for phone number patterns
            phone_patterns = [
                r'\(\d{3}\)\s*\d{3}-\d{4}',
                r'\d{3}-\d{3}-\d{4}',
                r'\d{3}\.\d{3}\.\d{4}',
                r'\+?\d{10,15}'
            ]

            for pattern in phone_patterns:
                if re.search(pattern, value_str):
                    return False

            # Enhanced name patterns - look for realistic names
            name_patterns = [
                r'^[A-Za-z]+\s+[A-Za-z]+',  # First Last
                r'^[A-Za-z]+,\s*[A-Za-z]+',  # Last, First
                r'^[A-Za-z]+\s+[A-Z]\.\s*[A-Za-z]+',  # First M. Last
                r'^[A-Za-z-\']+\s+[A-Za-z-\']+',  # Names with hyphens/apostrophes
            ]

            # Must match at least one name pattern OR be a single word that looks like a name
            has_name_pattern = any(re.search(pattern, value_str) for pattern in name_patterns)
            is_single_word_name = len(value_str.split()) == 1 and value_str.isalpha() and len(value_str) > 2

            if not (has_name_pattern or is_single_word_name):
                return False

            # Additional business entity checks
            business_indicators = [
                'llc', 'inc', 'corp', 'corporation', 'company', 'co.', 'ltd', 'limited',
                'partnership', 'trust', 'foundation', 'association', 'society', 'group',
                'enterprises', 'holdings', 'investments', 'properties', 'development',
                'management', 'services', 'solutions', 'systems', 'technologies'
            ]

            # Business entities are valid names too
            if any(indicator in value_str.lower() for indicator in business_indicators):
                return True

            return True

        # Filter records without names using enhanced validation
        records_without_names = []
        records_with_names = []

        for idx, row in df.iterrows():
            has_name = False
            for col in existing_name_cols:
                value = row[col]
                if is_valid_name(value):
                    has_name = True
                    break

            if not has_name:
                records_without_names.append(idx)
            else:
                records_with_names.append(idx)

        if not records_without_names:
            logger.warning("No records without names found - all records already have name information")
            # Create a summary file showing why no processing is needed
            summary_file = os.path.join(app.config['RESULTS_FOLDER'], f"no_processing_needed_{timestamp}.csv")

            # Create a small summary showing the name coverage
            summary_data = {
                'Message': ['All records already have owner names'],
                'Total Records': [len(df)],
                'Records with Names': [len(df)],
                'Records without Names': [0],
                'Name Columns Found': [', '.join(existing_name_cols[:5])],  # Show first 5 name columns
                'Recommendation': ['No reverse address extraction needed - all records have owner information']
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv(summary_file, index=False)
            logger.info(f"Created summary file: {summary_file}")
            return summary_file

        # Get only records without names
        df_no_names = df.iloc[records_without_names].copy()
        logger.info(f"📋 Found {len(df_no_names)} records without names (target records)")

        # Limit records if specified
        if max_records > 0 and len(df_no_names) > max_records:
            df_limited = df_no_names.head(max_records)
            logger.info(f"Limited processing to {max_records} records without names")
        else:
            df_limited = df_no_names.copy()

        # Step 2: Use BCPA to process addresses and get owner information
        logger.info(f"🏠 Processing {len(df_limited)} addresses with BCPA for reverse extraction")

        # Get user session for user-specific directories
        user_id = get_user_id()
        user_config = get_user_config(user_id)

        # Create temp file with filtered records for BCPA processing
        temp_input = os.path.join(user_config['TEMP_FOLDER'], f"temp_reverse_address_{timestamp}.csv")
        df_limited.to_csv(temp_input, index=False)

        # Use BCPA to process the addresses
        bcpa_result_file = bcpa_owner_search.process_bcpa_lookup(temp_input, max_records)

        if not bcpa_result_file or not os.path.exists(bcpa_result_file):
            logger.error("BCPA processing failed or no results file created")
            return None

        # Step 2: Load BCPA results and separate by phone status
        logger.info(f"📞 Separating BCPA results by phone number status")
        bcpa_df = read_data_file(bcpa_result_file, encoding='utf-8')

        # Find phone column(s)
        phone_columns = ['phone', 'Phone', 'PHONE', 'phone_number', 'Phone Number', 'PHONE_NUMBER',
                        'cell', 'Cell', 'CELL', 'mobile', 'Mobile', 'MOBILE', 'telephone', 'Telephone']

        phone_col = None
        for col in phone_columns:
            if col in bcpa_df.columns:
                phone_col = col
                break

        if phone_col:
            # Separate records with and without phone numbers
            with_phones = bcpa_df[bcpa_df[phone_col].notna() & (bcpa_df[phone_col].astype(str).str.strip() != '')]
            without_phones = bcpa_df[bcpa_df[phone_col].isna() | (bcpa_df[phone_col].astype(str).str.strip() == '')]
        else:
            # No phone column found, all records are considered without phones
            with_phones = pd.DataFrame()
            without_phones = bcpa_df.copy()

        # Step 3: Create separated output files in user-specific directory
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        base_name = f"reverse_address_extracted_{timestamp}"

        # Save the main enhanced file (with BCPA owner data for records without names)
        main_output_file = os.path.join(user_config['RESULTS_FOLDER'], f"{base_name}_with_owners.csv")
        bcpa_df.to_csv(main_output_file, index=False, encoding='utf-8')

        # Save separated files
        files_created = []

        if not with_phones.empty:
            with_phones_file = os.path.join(user_config['RESULTS_FOLDER'], f"{base_name}_with_phones.csv")
            with_phones.to_csv(with_phones_file, index=False, encoding='utf-8')
            files_created.append({
                'type': 'with_phones',
                'count': len(with_phones),
                'path': with_phones_file
            })
            logger.info(f"✅ Created with_phones file: {len(with_phones)} records")

        if not without_phones.empty:
            without_phones_file = os.path.join(user_config['RESULTS_FOLDER'], f"{base_name}_without_phones.csv")
            without_phones.to_csv(without_phones_file, index=False, encoding='utf-8')
            files_created.append({
                'type': 'without_phones',
                'count': len(without_phones),
                'path': without_phones_file
            })
            logger.info(f"✅ Created without_phones file: {len(without_phones)} records")

        # Step 4: Create a summary file with all results
        summary_file = os.path.join(user_config['RESULTS_FOLDER'], f"{base_name}_summary.csv")

        # Add processing summary columns
        bcpa_df['Processing_Summary'] = 'Address Enhanced with BCPA'
        bcpa_df['Has_Phone_Number'] = bcpa_df[phone_col].notna() & (bcpa_df[phone_col].astype(str).str.strip() != '') if phone_col else False
        bcpa_df['Records_With_Phones'] = len(with_phones)
        bcpa_df['Records_Without_Phones'] = len(without_phones)
        bcpa_df['Total_Records_Processed'] = len(bcpa_df)

        bcpa_df.to_csv(summary_file, index=False, encoding='utf-8')

        # Clean up temp file
        if os.path.exists(temp_input):
            os.remove(temp_input)

        logger.info(f"🎉 Address processing completed: {len(bcpa_df)} total records")
        logger.info(f"📞 With phones: {len(with_phones)}, Without phones: {len(without_phones)}")

        # Return the main enhanced file
        return main_output_file

    except Exception as e:
        logger.error(f"Enhanced address processing failed: {e}")
        return None

@app.route('/merge_files', methods=['POST'])
def merge_files():
    """Smart merge: Replace/update original records with enhanced processed data."""
    try:
        data = request.get_json()
        original_filepath = data.get('original_filepath')
        processed_filepath = data.get('processed_filepath')

        if not original_filepath or not os.path.exists(original_filepath):
            return jsonify({'error': 'Original file not found'}), 404

        if not processed_filepath or not os.path.exists(processed_filepath):
            return jsonify({'error': 'Processed file not found'}), 404

        logger.info(f"🔄 Smart merging: Replacing original records with enhanced data")

        # Load both files with explicit fresh read (no caching)
        original_df = read_data_file(original_filepath)
        processed_df = pd.read_csv(processed_filepath, encoding='utf-8')

        logger.info(f"Original file: {len(original_df)} records")
        logger.info(f"Processed file: {len(processed_df)} records")

        # Create merged file with simple naming - ALWAYS CSV
        original_basename = os.path.basename(original_filepath)
        # Remove file extension and add "Merged_" prefix - Force CSV extension
        base_name = os.path.splitext(original_basename)[0]
        merged_filename = f"Merged_{base_name}.csv"  # Simple naming: Merged_originalname.csv
        merged_filepath = os.path.join(app.config['RESULTS_FOLDER'], merged_filename)

        # Strategy: REPLACE records that were processed with enhanced versions
        # Step 1: Identify matching key for records (address-based matching)

        # Try different address matching strategies
        address_key_original = None
        address_key_processed = None

        # Look for BCPA_Search_Format first (most reliable)
        if 'BCPA_Search_Format' in original_df.columns and 'BCPA_Search_Format' in processed_df.columns:
            address_key_original = 'BCPA_Search_Format'
            address_key_processed = 'BCPA_Search_Format'
            logger.info("Using BCPA_Search_Format for record matching")
        elif 'Address' in original_df.columns and 'Address' in processed_df.columns:
            address_key_original = 'Address'
            address_key_processed = 'Address'
            logger.info("Using Address column for record matching")
        else:
            # Try to create composite address key
            address_parts_original = []
            address_parts_processed = []

            for addr_part in ['Street Name', 'House Number', 'City Name', 'City']:
                if addr_part in original_df.columns:
                    address_parts_original.append(addr_part)
                if addr_part in processed_df.columns:
                    address_parts_processed.append(addr_part)

            if address_parts_original and address_parts_processed:
                # Create composite address key
                original_df['_MERGE_KEY'] = original_df[address_parts_original].astype(str).agg(' '.join, axis=1)
                processed_df['_MERGE_KEY'] = processed_df[address_parts_processed].astype(str).agg(' '.join, axis=1)
                address_key_original = '_MERGE_KEY'
                address_key_processed = '_MERGE_KEY'
                logger.info(f"Created composite address key from: {address_parts_original}")

        if not address_key_original or not address_key_processed:
            # FALLBACK: Smart phone data merge for ZabaSearch results
            logger.warning("No address columns found - attempting smart phone data merge")

            # Check if this is a phone extraction merge (has phone columns)
            phone_cols_in_processed = [col for col in processed_df.columns if 'Phone' in col or 'phone' in col]

            if phone_cols_in_processed:
                logger.info(f"🔍 Detected phone extraction merge with columns: {phone_cols_in_processed}")

                # Add phone columns to original if they don't exist
                for col in phone_cols_in_processed:
                    if col not in original_df.columns:
                        original_df[col] = ''
                        logger.info(f"➕ Added phone column: {col}")

                # SMART MERGE: Try to match records by name or address similarity
                records_updated = 0

                logger.info(f"🔍 Starting smart matching...")
                logger.info(f"📊 Original columns: {list(original_df.columns)}")
                logger.info(f"📊 Processed columns: {list(processed_df.columns)}")

                # Try to find matching records by comparing available data
                for processed_idx, processed_row in processed_df.iterrows():
                    best_match_idx = None
                    best_match_score = 0

                    # More flexible column matching - look for ANY name or address columns
                    name_columns = [col for col in processed_df.columns if any(keyword in col.lower() for keyword in ['name', 'owner', 'person'])]
                    address_columns = [col for col in processed_df.columns if any(keyword in col.lower() for keyword in ['address', 'street', 'location'])]

                    logger.info(f"🔍 Row {processed_idx}: Looking for matches using name columns: {name_columns}, address columns: {address_columns}")

                    # Look for matching record in original data
                    for orig_idx, orig_row in original_df.iterrows():
                        match_score = 0
                        total_comparisons = 0

                        # Compare name columns
                        for name_col in name_columns:
                            if name_col in original_df.columns:
                                processed_val = str(processed_row.get(name_col, '')).strip().lower()
                                orig_val = str(orig_row.get(name_col, '')).strip().lower()

                                if processed_val and orig_val and processed_val == orig_val:
                                    match_score += 2  # Names get double weight
                                    logger.info(f"  ✅ Name match: {name_col} = '{processed_val}'")
                                total_comparisons += 2

                        # Compare address columns
                        for addr_col in address_columns:
                            if addr_col in original_df.columns:
                                processed_val = str(processed_row.get(addr_col, '')).strip().lower()
                                orig_val = str(orig_row.get(addr_col, '')).strip().lower()

                                if processed_val and orig_val and processed_val == orig_val:
                                    match_score += 1
                                    logger.info(f"  ✅ Address match: {addr_col} = '{processed_val}'")
                                total_comparisons += 1

                        # Also try partial matches for names (first few characters)
                        for name_col in name_columns:
                            if name_col in original_df.columns:
                                processed_val = str(processed_row.get(name_col, '')).strip().lower()
                                orig_val = str(orig_row.get(name_col, '')).strip().lower()

                                if processed_val and orig_val and len(processed_val) > 5 and len(orig_val) > 5:
                                    if processed_val[:5] == orig_val[:5]:  # First 5 characters match
                                        match_score += 1
                                        logger.info(f"  ✅ Partial name match: {name_col} = '{processed_val[:10]}...'")

                        # Calculate match percentage
                        if total_comparisons > 0:
                            match_percentage = match_score / total_comparisons
                            logger.info(f"  📊 Record {orig_idx}: {match_score}/{total_comparisons} = {match_percentage:.2%}")

                            # Lower threshold for matching (50% instead of 70%)
                            if match_percentage >= 0.5 and match_score > best_match_score:
                                best_match_score = match_score
                                best_match_idx = orig_idx

                    # Update the best match with phone data
                    if best_match_idx is not None:
                        logger.info(f"🎯 Updating record {best_match_idx} with phone data from processed row {processed_idx}")
                        for phone_col in phone_cols_in_processed:
                            phone_value = processed_row.get(phone_col, '')
                            if phone_value and str(phone_value).strip():
                                original_df.at[best_match_idx, phone_col] = phone_value
                                logger.info(f"  📞 Added {phone_col}: {phone_value}")
                        records_updated += 1
                    else:
                        logger.warning(f"❌ No match found for processed row {processed_idx}")

                # FALLBACK: If very few records updated, try row-index matching for smaller dataset
                if records_updated < len(phone_cols_in_processed) / 2 and len(original_df) <= len(processed_df):
                    logger.warning(f"⚠️ Low match rate ({records_updated} updated), trying row-index fallback...")

                    # CRITICAL: Use original_index if available for direct mapping
                    if 'original_index' in processed_df.columns:
                        logger.info("🎯 Using original_index for direct mapping...")
                        for _, processed_row in processed_df.iterrows():
                            orig_idx = processed_row.get('original_index')
                            if pd.notna(orig_idx) and int(orig_idx) < len(original_df):
                                orig_idx = int(orig_idx)

                                # Copy all phone columns that have data
                                for phone_col in phone_cols_in_processed:
                                    phone_value = processed_row.get(phone_col, '')
                                    if phone_value and str(phone_value).strip().lower() not in ['nan', 'none', '']:

                                        # Add column if not exists
                                        if phone_col not in original_df.columns:
                                            original_df[phone_col] = ''
                                            logger.info(f"➕ Added phone column: {phone_col}")

                                        # Ensure column is object type and convert value to string
                                        if original_df[phone_col].dtype != 'object':
                                            original_df[phone_col] = original_df[phone_col].astype('object')

                                        original_df.at[orig_idx, phone_col] = str(phone_value)
                                        logger.info(f"📞 Index mapping: Added {phone_col} to row {orig_idx}: {phone_value}")
                                        records_updated += 1
                    else:
                        # Original positional fallback
                        logger.info("🔄 Using positional fallback matching...")
                        for orig_idx in range(min(len(original_df), len(processed_df))):
                            if orig_idx < len(processed_df):
                                processed_row = processed_df.iloc[orig_idx]

                            # Check if this row has VALID phone data (not nan or empty)
                            has_phone_data = any(
                                processed_row.get(col, '') and
                                str(processed_row.get(col, '')).strip() and
                                str(processed_row.get(col, '')).strip().lower() not in ['nan', 'none', '']
                                for col in phone_cols_in_processed
                            )

                            if has_phone_data:
                                for phone_col in phone_cols_in_processed:
                                    phone_value = processed_row.get(phone_col, '')
                                    if (phone_value and
                                        str(phone_value).strip() and
                                        str(phone_value).strip().lower() not in ['nan', 'none', '']):

                                        # Only update if original doesn't have phone data
                                        original_col_value = str(original_df.at[orig_idx, phone_col]).strip()
                                        if not original_col_value or original_col_value.lower() in ['nan', 'none', '']:
                                            # Ensure column is object type and convert value to string
                                            if phone_col not in original_df.columns or original_df[phone_col].dtype != 'object':
                                                original_df[phone_col] = original_df[phone_col].astype('object') if phone_col in original_df.columns else ''
                                            original_df.at[orig_idx, phone_col] = str(phone_value)
                                            logger.info(f"🔄 Fallback: Added {phone_col} to row {orig_idx}: {phone_value}")
                                            records_updated += 1

                # Save merged file
                original_df.to_csv(merged_filepath, index=False, encoding='utf-8')

                logger.info(f"🎉 Smart phone merge completed: {records_updated} records updated")

                # Create download URL
                download_url = url_for('download_file', filename=merged_filename)

                return jsonify({
                    'success': True,
                    'message': f'Phone data merged successfully! Updated {records_updated} records',
                    'merged_file': merged_filename,
                    'download_url': download_url,
                    'records_total': len(original_df),
                    'records_updated': records_updated,
                    'phone_columns_added': len(phone_cols_in_processed),
                    'auto_download': True,  # Enable auto-download and notification
                    'summary': {
                        'records_updated': records_updated,
                        'total_final_records': len(original_df),
                        'records_unchanged': len(original_df) - records_updated,
                        'phone_columns_added': len(phone_cols_in_processed),
                        'merge_type': 'phone_extraction'
                    }
                })

            # Regular row-by-row merge for same row count
            elif len(original_df) == len(processed_df):
                logger.info("Same row count - performing direct row-by-row merge")

                # Add new columns from processed file
                processed_only_columns = [col for col in processed_df.columns if col not in original_df.columns]
                for col in processed_only_columns:
                    original_df[col] = ''

                # Copy data row by row
                for idx in range(len(original_df)):
                    for col in processed_only_columns:
                        if idx < len(processed_df):
                            original_df.at[idx, col] = processed_df.iloc[idx][col]

                # Save merged file
                original_df.to_csv(merged_filepath, index=False, encoding='utf-8')

                return jsonify({
                    'success': True,
                    'message': 'Files merged successfully using row-by-row method',
                    'merged_file': merged_filename,
                    'records_total': len(original_df),
                    'new_columns_added': len(processed_only_columns)
                })
            else:
                return jsonify({'error': f'Cannot merge: Original has {len(original_df)} records, processed has {len(processed_df)} records - no matching strategy available'}), 400

        # Step 2: Prepare enhanced columns from processed data
        processed_only_columns = [col for col in processed_df.columns if col not in original_df.columns]
        logger.info(f"New columns from processing: {processed_only_columns}")

        # Add new columns to original dataframe with empty values
        for col in processed_only_columns:
            original_df[col] = ''

        # Step 3: Smart merge logic
        records_updated = 0
        records_not_found = 0

        # Create a mapping of processed records by address key
        processed_dict = {}
        for idx, row in processed_df.iterrows():
            key = str(row[address_key_processed]).strip().lower()
            if key and key != 'nan':
                processed_dict[key] = row

        logger.info(f"Created lookup dictionary with {len(processed_dict)} processed records")

        # Update original records with processed data
        for idx, orig_row in original_df.iterrows():
            key = str(orig_row[address_key_original]).strip().lower()

            if key in processed_dict:
                # Found matching processed record - UPDATE with enhanced data
                processed_row = processed_dict[key]

                # Update all columns from processed data (prioritize processed values)
                for col in processed_df.columns:
                    if col in original_df.columns:
                        # Replace original value with processed value if processed has data
                        processed_value = processed_row[col]
                        if pd.notna(processed_value) and str(processed_value).strip() != '':
                            original_df.at[idx, col] = processed_value

                # Mark as updated
                original_df.at[idx, 'Processing_Status'] = 'Updated_with_BCPA_Data'
                records_updated += 1
            else:
                # No processed version found - keep original
                original_df.at[idx, 'Processing_Status'] = 'Original_Unchanged'
                records_not_found += 1

        # Step 4: Add any completely new records from processed file
        new_records = []
        original_keys = set(str(row[address_key_original]).strip().lower() for _, row in original_df.iterrows())

        for idx, processed_row in processed_df.iterrows():
            key = str(processed_row[address_key_processed]).strip().lower()
            if key not in original_keys:
                # This is a completely new record
                new_record = {}
                for col in original_df.columns:
                    if col in processed_df.columns:
                        new_record[col] = processed_row[col]
                    else:
                        new_record[col] = ''
                new_record['Processing_Status'] = 'New_from_Processing'
                new_records.append(new_record)

        # Add new records to the dataframe
        if new_records:
            new_df = pd.DataFrame(new_records)
            original_df = pd.concat([original_df, new_df], ignore_index=True)

        # Clean up temporary merge key if created
        if '_MERGE_KEY' in original_df.columns:
            original_df = original_df.drop('_MERGE_KEY', axis=1)

        # Save updated file
        original_df.to_csv(merged_filepath, index=False, encoding='utf-8')

        # Create summary statistics
        summary_stats = {
            'original_records': len(original_df) - len(new_records),
            'records_updated': records_updated,
            'records_unchanged': records_not_found,
            'new_records_added': len(new_records),
            'total_final_records': len(original_df),
            'new_columns_added': len(processed_only_columns)
        }

        logger.info(f"✅ Smart merge completed: {len(original_df)} total records in updated file")
        logger.info(f"   📊 Updated: {records_updated}, Unchanged: {records_not_found}, New: {len(new_records)}")

        download_url = url_for('download_file', filename=merged_filename)
        return jsonify({
            'message': f'Files merged successfully - {records_updated} records updated with new owner data',
            'download_url': download_url,
            'output_file': merged_filename,
            'auto_download': True,  # Enable auto-download and notification
            'summary': summary_stats
        })

    except Exception as e:
        logger.error(f"Error in merge_files: {str(e)}")
        return jsonify({'error': f'Merge failed: {str(e)}'}), 500

@app.route('/ai_format_addresses', methods=['POST'])
def ai_format_addresses():
    """Format addresses using AI for better BCPA compatibility"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')

        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 400

        logger.info(f"Starting AI address formatting for: {file_path}")

        # Use intelligent formatter
        try:
            formatter = IntelligentAddressFormatter()
            output_file = formatter.process_file(file_path)

            # Get formatting statistics
            df = pd.read_csv(output_file)
            total_records = len(df)
            valid_addresses = df['BCPA_Valid'].sum() if 'BCPA_Valid' in df.columns else 0
            success_rate = (valid_addresses / total_records * 100) if total_records > 0 else 0

            summary = {
                'total_records': total_records,
                'valid_addresses': int(valid_addresses),
                'success_rate': round(success_rate, 1),
                'broward_county_only': True
            }

            logger.info(f"AI formatting completed: {summary}")
            return jsonify({
                'success': True,
                'message': 'AI address formatting completed',
                'output_file': os.path.basename(output_file),
                'file_path': output_file,
                'summary': summary
            })

        except Exception as e:
            logger.error(f"AI formatting failed: {str(e)}")
            return jsonify({'error': f'AI formatting failed: {str(e)}'}), 500

    except Exception as e:
        logger.error(f"Error in AI format addresses: {str(e)}")
        return jsonify({'error': f'AI formatting failed: {str(e)}'}), 500

@app.route('/bcpa_reverse_search', methods=['POST'])
def bcpa_reverse_search():
    """Perform reverse address search using BCPA website"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        max_addresses = data.get('max_addresses', 25)

        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 400

        logger.info(f"Starting BCPA reverse search for: {file_path}")

        # Import BCPA integration modules
        try:
            from bcpa_flask_integration import process_bcpa_reverse_search
            from unified_address_handler import BCPAIntegratedProcessor, process_csv_for_address_extraction
        except ImportError:
            return jsonify({'error': 'BCPA integration module not available'}), 500

        # Get user session for user-specific directories
        user_id = get_user_id()
        user_config = get_user_config(user_id)

        # Process the file
        success, output_file, summary = process_bcpa_reverse_search(
            file_path,
            output_dir=user_config['RESULTS_FOLDER'],
            max_addresses=max_addresses
        )

        if success:
            logger.info(f"BCPA search completed: {summary}")
            return jsonify({
                'success': True,
                'message': 'BCPA reverse address search completed',
                'output_file': output_file,
                'summary': summary
            })
        else:
            logger.error(f"BCPA search failed: {summary}")
            return jsonify({
                'success': False,
                'error': summary.get('error', 'Unknown error'),
                'summary': summary
            }), 500

    except Exception as e:
        logger.error(f"Error in BCPA reverse search: {str(e)}")
        return jsonify({'error': f'BCPA search failed: {str(e)}'}), 500

@app.route('/column_sync', methods=['POST'])
def column_sync():
    """Perform mobile phone validation using Column Syncer"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        max_records = data.get('max_records', 50)

        if not file_path or not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 400

        if not COLUMN_SYNCER_AVAILABLE:
            return jsonify({'error': 'Column Syncer module not available'}), 500

        logger.info(f"🔄 Starting Column Syncer processing for: {file_path}")

        # Initialize Column Syncer
        syncer = ColumnSyncer()

        # Process the file
        results = syncer.process_file(file_path)

        if results['success']:
            logger.info(f"✅ Column Syncer completed: {results['message']}")
            return jsonify({
                'success': True,
                'message': 'Mobile phone validation completed',
                'output_file': results['output_file'],
                'summary': results.get('message', ''),
                'stats': {
                    'total_processed': results['stats'].get('total_input_rows', 0),
                    'mobile_phones_found': results['stats'].get('mobile_numbers_found', 0),
                    'records_duplicated': results['stats'].get('rows_duplicated', 0),
                    'success_rate': f"{(results['stats'].get('mobile_numbers_found', 0) / max(results['stats'].get('total_input_rows', 1), 1)) * 100:.1f}%"
                }
            })
        else:
            logger.error(f"❌ Column Syncer failed: {results.get('error', results.get('message', 'Unknown error'))}")
            return jsonify({
                'success': False,
                'error': results.get('error', results.get('message', 'Unknown error')),
                'summary': results.get('message', '')
            }), 500

    except Exception as e:
        logger.error(f"❌ Error in Column Syncer: {str(e)}")
        return jsonify({'error': f'Column Syncer failed: {str(e)}'}), 500


def cleanup_session_batch_files(user_id: str, completed_filename: str):
    """
    Clean up session-specific batch files after successful download completion.
    Only removes batch files belonging to the same user session.
    
    Args:
        user_id: User session ID
        completed_filename: The final output file that was downloaded
    """
    try:
        user_config = get_user_config(user_id)
        temp_folder = user_config['TEMP_FOLDER']
        
        if not os.path.exists(temp_folder):
            return
        
        # Look for batch files with the session ID in the filename
        batch_patterns = [
            f"*batch*{user_id}*.csv",
            f"*temp*{user_id}*.csv", 
            f"{user_id}_batch*.csv",
            f"batch*{user_id}*.csv"
        ]
        
        cleaned_files = []
        total_size = 0
        
        for pattern in batch_patterns:
            batch_files = glob.glob(os.path.join(temp_folder, pattern))
            
            for batch_file in batch_files:
                try:
                    file_size = os.path.getsize(batch_file)
                    os.remove(batch_file)
                    cleaned_files.append(os.path.basename(batch_file))
                    total_size += file_size
                    logger.info(f"🧹 Removed batch file: {os.path.basename(batch_file)}")
                except OSError as e:
                    logger.warning(f"⚠️ Could not remove batch file {batch_file}: {e}")
        
        if cleaned_files:
            size_mb = total_size / (1024 * 1024)
            logger.info(f"✅ Batch cleanup complete for user {user_id}: {len(cleaned_files)} files, {size_mb:.2f} MB freed")
            logger.info(f"📄 Completed file: {completed_filename}")
        else:
            logger.info(f"🧹 No batch files found for cleanup (User: {user_id})")
            
    except Exception as e:
        logger.error(f"❌ Error during batch cleanup for user {user_id}: {e}")

@app.route('/download/<filename>')
def download_file(filename):
    """Download processed files from user-specific directories with automatic batch cleanup."""
    try:
        # Get user session and directories
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        
        # Look for the file in user's uploads folder first
        file_path = os.path.join(user_config['UPLOAD_FOLDER'], filename)
        if os.path.exists(file_path):
            logger.info(f"📥 File download: {filename} from uploads (User: {user_id})")
            
            # Perform session-specific batch cleanup after successful download
            cleanup_session_batch_files(user_id, filename)
            
            return send_file(file_path, as_attachment=True, download_name=filename)

        # Also check user's results folder
        file_path = os.path.join(user_config['RESULTS_FOLDER'], filename)
        if os.path.exists(file_path):
            logger.info(f"📥 File download: {filename} from results (User: {user_id})")
            
            # Perform session-specific batch cleanup after successful download
            cleanup_session_batch_files(user_id, filename)
            
            return send_file(file_path, as_attachment=True, download_name=filename)

        # Check user's temp folder as fallback
        file_path = os.path.join(user_config['TEMP_FOLDER'], filename)
        if os.path.exists(file_path):
            logger.info(f"📥 File download: {filename} from temp (User: {user_id})")
            
            # Perform session-specific batch cleanup after successful download
            cleanup_session_batch_files(user_id, filename)
            
            return send_file(file_path, as_attachment=True, download_name=filename)

        logger.warning(f"❌ File not found: {filename} (User: {user_id})")
        return jsonify({'error': 'File not found'}), 404

    except Exception as e:
        logger.error(f"❌ Error downloading file: {str(e)} (User: {get_user_id()})")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'File too large. Maximum size is 100MB.'}), 413

@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {str(e)}")
    return jsonify({'error': 'Internal server error occurred'}), 500

@app.route('/user-info')
def user_info():
    """Get current user session information"""
    try:
        user_id = get_user_id()
        user_dirs = get_user_directories(user_id)
        
        # Count files in each directory
        upload_files = len([f for f in os.listdir(user_dirs['upload']) if os.path.isfile(os.path.join(user_dirs['upload'], f))])
        result_files = len([f for f in os.listdir(user_dirs['results']) if os.path.isfile(os.path.join(user_dirs['results'], f))])
        temp_files = len([f for f in os.listdir(user_dirs['temp']) if os.path.isfile(os.path.join(user_dirs['temp'], f))])
        
        return jsonify({
            'user_id': user_id,
            'session_active': True,
            'directories': user_dirs,
            'file_counts': {
                'uploads': upload_files,
                'results': result_files,
                'temp': temp_files
            },
            'session_time': session.get('_created_time', 'N/A')
        })
    except Exception as e:
        logger.error(f"❌ Error getting user info: {str(e)}")
        return jsonify({'error': f'Failed to get user info: {str(e)}'}), 500

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    """Fetch data from uploaded file with pagination"""
    try:
        data = request.get_json()
        filepath = data.get('filepath')
        page = data.get('page', 1)
        per_page = data.get('per_page', 25)  # Reduced from 50 to 25 rows

        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404

        # Load the data directly without processing to preserve original structure
        if filepath.endswith('.csv'):
            # Read CSV directly to preserve column names and structure
            df = pd.read_csv(filepath)
        elif filepath.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(filepath)
        else:
            return jsonify({'error': 'Unsupported file format'}), 400

        # Clean up column names - replace unnamed columns with more descriptive names
        clean_columns = []
        for i, col in enumerate(df.columns):
            if pd.isna(col) or str(col).startswith('Unnamed:') or str(col).strip() == '':
                # Try to infer column type from data
                sample_data = df.iloc[:5, i].dropna().astype(str).str.strip()
                if len(sample_data) > 0:
                    first_val = sample_data.iloc[0]
                    # Try to identify common patterns
                    if 'BCPA_Search_Format' in df.columns:
                        bcpa_loc = df.columns.get_loc('BCPA_Search_Format')
                        if isinstance(bcpa_loc, int) and i == bcpa_loc - 1:
                            clean_columns.append('Telephone Number')
                        elif isinstance(bcpa_loc, int) and i == bcpa_loc + 1:
                            clean_columns.append('BCPA_City')
                        else:
                            clean_columns.append(f'Column_{i+1}')
                    elif any(x in first_val.upper() for x in ['STREET', 'AVE', 'BLVD', 'DR', 'CT', 'PL']):
                        clean_columns.append('Address')
                    elif first_val.replace('$', '').replace(',', '').replace('.', '').isdigit():
                        clean_columns.append('Price')
                    elif len(first_val) <= 5 and first_val.isdigit():
                        clean_columns.append('Units')
                    else:
                        clean_columns.append(f'Column_{i+1}')
                else:
                    clean_columns.append(f'Column_{i+1}')
            else:
                clean_columns.append(str(col))

        # Apply cleaned column names - ensure we have same number of columns
        if len(clean_columns) == len(df.columns):
            df.columns = clean_columns
        else:
            # Fallback: just clean existing names without changing count
            new_columns = []
            for col in df.columns:
                if pd.isna(col) or str(col).startswith('Unnamed:') or str(col).strip() == '':
                    new_columns.append(f'Column_{len(new_columns)+1}')
                else:
                    new_columns.append(str(col))
            df.columns = new_columns

        # Limit and prioritize columns for better display
        important_columns = []
        all_columns = df.columns.tolist()

        # Priority order for display
        priority_patterns = [
            'Owner Name', 'Property Owner', 'Name',  # Owner info
            'BCPA_Search_Format', 'Address', 'Street',  # Address info
            'BCPA_City', 'City', 'Location',  # City info
            'Phone', 'Telephone',  # Phone info
            'Price', 'Value', 'Amount',  # Price info
            'BCPA_Valid', 'Valid',  # Status info
        ]

        # Add important columns first
        for pattern in priority_patterns:
            for col in all_columns:
                if pattern.lower() in col.lower() and col not in important_columns:
                    important_columns.append(col)

        # Exclude unwanted columns from display
        unwanted_columns = ['BCPA_Multiple_Owners', 'Owner Name 2', 'Primary_Phone', 'Secondary_Phone', 'BCPA_Skip_Reason']
        important_columns = [col for col in important_columns if col not in unwanted_columns]
        all_columns = [col for col in all_columns if col not in unwanted_columns]

        # Add remaining columns up to a limit (max 8 columns for readability)
        for col in all_columns:
            if col not in important_columns and len(important_columns) < 8:
                important_columns.append(col)

        # Use only the important columns
        display_df = df[important_columns] if important_columns else df

        # Calculate pagination
        total_records = len(df)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page

        # Get page data from filtered dataframe
        page_data = display_df.iloc[start_idx:end_idx]

        # Convert to dict format for JSON with better formatting
        columns = display_df.columns.tolist()
        rows = []
        for _, row in page_data.iterrows():
            row_data = {}
            for col in columns:
                value = row[col]
                # Handle NaN values and improve formatting
                if pd.isna(value):
                    row_data[col] = ""
                else:
                    # Truncate long text for better display
                    str_value = str(value)
                    if len(str_value) > 50:
                        str_value = str_value[:47] + "..."
                    row_data[col] = str_value
            rows.append(row_data)

        return jsonify({
            'columns': columns,
            'rows': rows,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_records': total_records,
                'total_pages': (total_records + per_page - 1) // per_page,
                'has_next': end_idx < total_records,
                'has_prev': page > 1
            }
        })

    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        return jsonify({'error': f'Error fetching data: {str(e)}'}), 500

@app.route('/cleanup_files', methods=['POST'])
def cleanup_files():
    """Manually trigger file cleanup"""
    try:
        data = request.get_json() or {}
        # For manual cleanup, default to 0 days (delete all files)
        max_age_days = data.get('max_age_days', 0)
        force_cleanup = data.get('force', True)  # Force cleanup by default for manual trigger

        logger.info(f"🔥 Manual file cleanup triggered - max age: {max_age_days} days, force: {force_cleanup}")

        # Ensure we're in the correct working directory
        import os
        from pathlib import Path
        original_cwd = os.getcwd()
        script_dir = os.path.dirname(os.path.abspath(__file__))
        logger.info(f"🔍 Original CWD: {original_cwd}")
        logger.info(f"🔍 Script dir: {script_dir}")

        # List files before cleanup
        uploads_before = len(list(Path('uploads').glob('*'))) if Path('uploads').exists() else 0
        results_before = len(list(Path('results').glob('*'))) if Path('results').exists() else 0
        temp_before = len(list(Path('temp').glob('*'))) if Path('temp').exists() else 0
        logger.info(f"📊 Files before cleanup - uploads: {uploads_before}, results: {results_before}, temp: {temp_before}")

        os.chdir(script_dir)

        try:
            if force_cleanup:
                # Force cleanup deletes all files regardless of age
                result = file_cleanup.cleanup_old_files(max_age_days=0)
                logger.info("🔥 Force cleanup completed")
            else:
                result = file_cleanup.cleanup_old_files(max_age_days=max_age_days)
        finally:
            # Always restore original working directory
            os.chdir(original_cwd)

        # List files after cleanup
        uploads_after = len(list(Path(script_dir + '/uploads').glob('*'))) if Path(script_dir + '/uploads').exists() else 0
        results_after = len(list(Path(script_dir + '/results').glob('*'))) if Path(script_dir + '/results').exists() else 0
        temp_after = len(list(Path(script_dir + '/temp').glob('*'))) if Path(script_dir + '/temp').exists() else 0
        logger.info(f"📊 Files after cleanup - uploads: {uploads_after}, results: {results_after}, temp: {temp_after}")

        return jsonify({
            'message': f'Cleanup completed: {result["files_deleted"]} files deleted, {result["size_freed_mb"]:.2f} MB freed',
            'result': result,
            'debug': {
                'before': {'uploads': uploads_before, 'results': results_before, 'temp': temp_before},
                'after': {'uploads': uploads_after, 'results': results_after, 'temp': temp_after}
            }
        })
    except Exception as e:
        logger.error(f"Cleanup error: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return jsonify({'error': f'Cleanup failed: {str(e)}'}), 500

@app.route('/recent_files', methods=['GET'])
def get_recent_files():
    """Get recent processed files for current user session - FINAL OUTPUTS ONLY"""
    try:
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        
        # Get user-specific recent files by scanning user directories
        import glob
        
        # 🎯 FILTER FOR FINAL OUTPUT FILES ONLY
        # Look for final output files and merge files in user's results directory
        final_output_patterns = [
            os.path.join(user_config['RESULTS_FOLDER'], "*_processed.csv"),         # Final processed files
            os.path.join(user_config['RESULTS_FOLDER'], "*_with_phones.csv"),      # Files with phone numbers
            os.path.join(user_config['RESULTS_FOLDER'], "merge_*.csv"),            # Merge files
            os.path.join(user_config['RESULTS_FOLDER'], "*merged*.csv"),           # Other merged files
            os.path.join(user_config['RESULTS_FOLDER'], "*_final.csv"),            # Final output files
            os.path.join(user_config['RESULTS_FOLDER'], "*_complete.csv"),         # Complete files
            os.path.join(user_config['RESULTS_FOLDER'], "Cleaned_*.csv"),          # Validator app outputs
            os.path.join(user_config['RESULTS_FOLDER'], "Merged_*.csv")            # Phone extractor app outputs
        ]
        
        all_final_files = []
        for pattern in final_output_patterns:
            files = glob.glob(pattern)
            all_final_files.extend(files)
        
        # Remove duplicates and sort by modification time (newest first)
        all_final_files = list(set(all_final_files))
        all_final_files.sort(key=os.path.getmtime, reverse=True)
        
        # Get file objects with metadata for display
        def get_file_info_list(file_paths):
            """Convert file paths to file info objects with metadata"""
            file_info_list = []
            for file_path in file_paths:
                try:
                    stat = os.stat(file_path)
                    filename = os.path.basename(file_path)
                    
                    # Calculate age in hours
                    age_seconds = time.time() - stat.st_mtime
                    age_hours = age_seconds / 3600
                    
                    file_info = {
                        'name': filename,
                        'size': stat.st_size,
                        'modified': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(stat.st_mtime)),
                        'age_hours': age_hours,
                        'download_url': url_for('download_file', filename=filename)
                    }
                    file_info_list.append(file_info)
                except (OSError, IOError) as e:
                    logger.warning(f"Could not get info for file {file_path}: {e}")
                    continue
            return file_info_list
        
        # Convert to file info objects with metadata
        final_file_info = get_file_info_list(all_final_files[:20])  # Limit to 20 most recent
        
        # Separate merge files for special handling
        merge_files = [f for f in final_file_info if 'merge_' in f['name'].lower() or 'merged' in f['name'].lower()]
        other_final_files = [f for f in final_file_info if f not in merge_files]
        
        logger.info(f"📁 Final files filter: {len(final_file_info)} total final files")
        logger.info(f"� Merge files: {[f['name'] for f in merge_files]}")
        logger.info(f"📄 Other final files: {[f['name'] for f in other_final_files[:5]]}")  # Log first 5
        
        return jsonify({
            'final_files': final_file_info,
            'merge_files': merge_files,
            'other_files': other_final_files,
            'total_files': len(final_file_info),
            'total_merge': len(merge_files),
            'total_other': len(other_final_files),
            'user_id': user_id  # For debugging
        })
    except Exception as e:
        logger.error(f"Error getting recent files: {str(e)}")
        return jsonify({'error': f'Error getting recent files: {str(e)}'}), 500

@app.route('/cleanup_status', methods=['GET'])
def get_cleanup_status():
    """Get automatic cleanup status"""
    try:
        status = file_cleanup.get_cleanup_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting cleanup status: {str(e)}")
        return jsonify({'error': f'Error getting cleanup status: {str(e)}'}), 500

@app.route('/cleanup_batch_files', methods=['POST'])
def cleanup_batch_files():
    """Manual cleanup of session-specific batch files"""
    try:
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        temp_folder = user_config['TEMP_FOLDER']
        
        # Look for batch files with session-specific patterns
        batch_patterns = [
            f"*batch_*.csv",
            f"*{user_id}*batch*.csv",
            f"batch_*_{user_id}_*.csv",
            f"temp_batch_*.csv"
        ]
        
        cleaned_files = 0
        freed_space = 0
        cleaned_file_list = []
        
        for pattern in batch_patterns:
            pattern_path = os.path.join(temp_folder, pattern)
            for batch_file in glob.glob(pattern_path):
                try:
                    batch_filename = os.path.basename(batch_file)
                    if (user_id in batch_filename or 
                        'batch_' in batch_filename.lower() or 
                        batch_filename.startswith('temp_batch')):
                        
                        file_size = os.path.getsize(batch_file)
                        os.remove(batch_file)
                        cleaned_files += 1
                        freed_space += file_size
                        cleaned_file_list.append(batch_filename)
                        logger.info(f"🧹 Manual cleanup: {batch_filename} (User: {user_id})")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Could not remove batch file {batch_file}: {str(e)}")
        
        freed_mb = freed_space / (1024 * 1024)
        result = {
            'success': True,
            'files_removed': cleaned_files,
            'space_freed_mb': round(freed_mb, 2),
            'cleaned_files': cleaned_file_list,
            'message': f'Cleaned {cleaned_files} batch files, freed {freed_mb:.2f} MB'
        }
        
        logger.info(f"✅ Manual batch cleanup complete: {cleaned_files} files removed, {freed_mb:.2f} MB freed (User: {user_id})")
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"❌ Error during manual batch cleanup: {str(e)} (User: {get_user_id()})")
        return jsonify({'error': f'Cleanup failed: {str(e)}'}), 500

@app.route('/system_info', methods=['GET'])
def get_system_info():
    """Get system information including storage usage"""
    try:
        import shutil

        # Get disk usage
        total, used, free = shutil.disk_usage('.')

        # Get folder sizes
        folder_sizes = {}
        for folder in ['uploads', 'results', 'logs', 'temp', 'output']:
            if os.path.exists(folder):
                size = sum(
                    os.path.getsize(os.path.join(dirpath, filename))
                    for dirpath, dirnames, filenames in os.walk(folder)
                    for filename in filenames
                )
                folder_sizes[folder] = size

        # Get cleanup status
        cleanup_status = file_cleanup.get_cleanup_status()

        return jsonify({
            'disk_usage': {
                'total_gb': total / (1024**3),
                'used_gb': used / (1024**3),
                'free_gb': free / (1024**3),
                'used_percent': (used / total) * 100
            },
            'folder_sizes_mb': {k: v / (1024**2) for k, v in folder_sizes.items()},
            'uptime_info': {
                'server_start': datetime.now().isoformat(),
                'cleanup_recommended': any(size > 100*1024*1024 for size in folder_sizes.values())  # 100MB threshold
            },
            'automatic_cleanup': cleanup_status
        })
    except Exception as e:
        logger.error(f"Error getting system info: {str(e)}")
        return jsonify({'error': f'Error getting system info: {str(e)}'}), 500

@app.route('/verify_file_data/<filename>')
def verify_file_data(filename):
    """Verify that the file actually contains the phone data - helps with frontend display issues."""
    try:
        # Get user session for user-specific directories
        user_id = get_user_id()
        user_config = get_user_config(user_id)
        
        # Look for the file in user's results folder first, then uploads
        file_path = os.path.join(user_config['RESULTS_FOLDER'], filename)
        if not os.path.exists(file_path):
            file_path = os.path.join(user_config['UPLOAD_FOLDER'], filename)

        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404

        # Read the file and analyze phone data
        df = read_data_file(file_path, encoding='utf-8')

        # Look for phone columns
        phone_columns = [col for col in df.columns if 'Phone' in col or 'phone' in col]

        # Count records with actual phone data
        phone_data_summary = {}
        sample_phones = []

        for col in phone_columns:
            if col in df.columns:
                valid_phones = df[col].dropna().astype(str).str.strip()
                valid_phones = valid_phones[(valid_phones != '') & (valid_phones != 'nan')]
                phone_data_summary[col] = len(valid_phones)

                # Get sample phone numbers (first 3)
                if len(valid_phones) > 0:
                    sample_phones.extend(valid_phones.head(3).tolist())

        return jsonify({
            'success': True,
            'filename': filename,
            'total_records': len(df),
            'phone_columns': phone_columns,
            'phone_data_counts': phone_data_summary,
            'sample_phones': sample_phones[:5],  # Show first 5 sample phones
            'has_phone_data': len(sample_phones) > 0,
            'verification_timestamp': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Error verifying file data: {str(e)}")
        return jsonify({'error': f'Verification failed: {str(e)}'}), 500

@app.route('/logs/<log_type>')
def view_logs(log_type):
    """View real-time logs for debugging"""
    try:
        log_files = {
            'enterprise': 'logs/enterprise_flask.log',
            'phone': f'logs/phone_search_{datetime.now().strftime("%Y%m%d")}*.log',
            'bcpa': f'logs/bcpa_search_{datetime.now().strftime("%Y%m%d")}*.log',
            'address': f'logs/ai_address_formatter_{datetime.now().strftime("%Y%m%d")}*.log'
        }

        if log_type not in log_files:
            return f"Available logs: {', '.join(log_files.keys())}", 400

        log_pattern = log_files[log_type]

        # Handle wildcard patterns
        if '*' in log_pattern:
            import glob
            matching_files = glob.glob(log_pattern)
            if not matching_files:
                return f"No {log_type} log files found for today", 404
            log_file = max(matching_files, key=os.path.getctime)  # Get most recent
        else:
            log_file = log_pattern

        if not os.path.exists(log_file):
            return f"Log file not found: {log_file}", 404

        # Read last 100 lines of log
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                last_lines = lines[-100:] if len(lines) > 100 else lines

            log_content = ''.join(last_lines)

            # Return as HTML with auto-refresh
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>{log_type.title()} Logs - BlakeGUI</title>
                <meta charset="UTF-8">
                <meta http-equiv="refresh" content="5">
                <style>
                    body {{ font-family: 'Courier New', monospace; background: #1a1a1a; color: #00ff00; margin: 20px; }}
                    .log-container {{ background: #000; padding: 20px; border-radius: 8px; }}
                    .log-header {{ color: #ffff00; font-size: 18px; margin-bottom: 15px; }}
                    .log-content {{ white-space: pre-wrap; font-size: 12px; line-height: 1.4; }}
                    .error {{ color: #ff6b6b; }}
                    .warning {{ color: #ffa500; }}
                    .info {{ color: #87ceeb; }}
                    .success {{ color: #90ee90; }}
                </style>
            </head>
            <body>
                <div class="log-container">
                    <div class="log-header">📊 {log_type.upper()} LOGS - {os.path.basename(log_file)}</div>
                    <div class="log-header">🔄 Auto-refreshing every 5 seconds | Last {len(last_lines)} lines</div>
                    <hr style="border-color: #333;">
                    <div class="log-content">{log_content}</div>
                </div>

                <script>
                    // Color-code log levels
                    document.addEventListener('DOMContentLoaded', function() {{
                        const content = document.querySelector('.log-content');
                        let html = content.innerHTML;

                        html = html.replace(/(ERROR|❌|CRITICAL)/g, '<span class="error">$1</span>');
                        html = html.replace(/(WARNING|⚠️|WARN)/g, '<span class="warning">$1</span>');
                        html = html.replace(/(INFO|📊|🔍|🚀)/g, '<span class="info">$1</span>');
                        html = html.replace(/(SUCCESS|✅|COMPLETED)/g, '<span class="success">$1</span>');

                        content.innerHTML = html;
                    }});
                </script>
            </body>
            </html>
            """

            return html_content

        except Exception as e:
            return f"Error reading log file: {str(e)}", 500

    except Exception as e:
        return f"Error accessing logs: {str(e)}", 500

@app.route('/terminal_feed')
def terminal_feed():
    """Enhanced server-sent events for real-time terminal feed with external log monitoring"""
    def generate():
        import time
        import glob
        import os
        from pathlib import Path

        # Send initial connection message
        initial_data = {'type': 'success', 'message': '🔌 Enhanced Terminal feed connected - Monitoring ALL system activity...', 'timestamp': time.strftime('%H:%M:%S')}
        yield f"data: {json.dumps(initial_data)}\n\n"

        # Track external log files
        log_files_tracking = {}
        last_sent = 0

        try:
            while True:
                # Get new Flask app log entries
                with log_lock:
                    current_logs = list(terminal_logs)
                    new_logs = current_logs[last_sent:]
                    last_sent = len(current_logs)

                # Check for external log files (ZabaSearch, Phone Pipeline, etc.)
                log_patterns = [
                    'logs/phone_search_*.log',
                    'logs/bcpa_search_*.log',
                    'logs/ai_address_formatter_*.log',
                    'logs/zabasearch_*.log',
                    'logs/column_syncer_*.log'
                ]

                external_logs = []
                for pattern in log_patterns:
                    for log_file in glob.glob(pattern):
                        try:
                            # Get file modification time
                            mtime = os.path.getmtime(log_file)

                            # Check if this is a new file or has been modified
                            if log_file not in log_files_tracking or log_files_tracking[log_file] < mtime:
                                # Read new lines from the file
                                try:
                                    with open(log_file, 'r', encoding='utf-8') as f:
                                        lines = f.readlines()

                                        # If new file, start from last 5 lines
                                        if log_file not in log_files_tracking:
                                            start_line = max(0, len(lines) - 5)
                                        else:
                                            # Continue from where we left off (approximate)
                                            start_line = log_files_tracking.get(f"{log_file}_line", 0)

                                        # Process new lines
                                        for i, line in enumerate(lines[start_line:], start_line):
                                            if line.strip():
                                                # Parse log line and extract relevant info
                                                log_type = 'info'
                                                if 'ERROR' in line or '❌' in line:
                                                    log_type = 'error'
                                                elif 'WARNING' in line or '⚠️' in line:
                                                    log_type = 'warning'
                                                elif 'SUCCESS' in line or '✅' in line:
                                                    log_type = 'success'
                                                elif 'ZabaSearch' in line or 'ZABA' in line:
                                                    log_type = 'processing'

                                                # Create log entry
                                                external_logs.append({
                                                    'type': log_type,
                                                    'message': f"📁 {Path(log_file).stem}: {line.strip()}",
                                                    'timestamp': time.strftime('%H:%M:%S'),
                                                    'source': 'external'
                                                })

                                        # Update tracking
                                        log_files_tracking[f"{log_file}_line"] = len(lines)

                                except Exception as read_error:
                                    pass  # Skip problematic files

                                # Update modification time tracking
                                log_files_tracking[log_file] = mtime

                        except Exception as stat_error:
                            pass  # Skip files that can't be accessed

                # Send Flask app logs first
                for log_entry in new_logs:
                    # Format the message with emoji based on level
                    if log_entry['level'] == 'ERROR':
                        emoji = '❌'
                    elif log_entry['level'] == 'WARNING':
                        emoji = '⚠️'
                    elif 'INFO' in log_entry['level']:
                        emoji = '🔍'
                    else:
                        emoji = '📝'

                    formatted_message = f"{emoji} Flask: {log_entry['message']}"
                    data = {'type': log_entry['level'].lower(), 'message': formatted_message, 'timestamp': log_entry['timestamp']}
                    yield f"data: {json.dumps(data)}\n\n"

                # Send external log entries
                for log_entry in external_logs[-10:]:  # Limit to last 10 external logs per cycle
                    yield f"data: {json.dumps(log_entry)}\n\n"

                # No heartbeat needed - only send actual log data

                time.sleep(1)  # Check more frequently for better responsiveness

        except Exception as e:
            error_data = {'type': 'error', 'message': f'❌ Enhanced terminal feed error: {str(e)}', 'timestamp': time.strftime('%H:%M:%S')}
            yield f"data: {json.dumps(error_data)}\n\n"

    from flask import Response
    return Response(generate(), mimetype='text/event-stream')

if __name__ == '__main__':
    try:
        # Initialize modules
        logger.info("Initializing Enterprise Flask Application...")

        # Check if Excel support is available
        try:
            import openpyxl
            logger.info("Excel support available")
        except ImportError:
            logger.warning("Excel support not available - install openpyxl for .xlsx support")

        # Test CSV format handler
        if hasattr(csv_format_handler, 'process_file'):
            logger.info("CSV Format Handler loaded successfully")
        else:
            logger.warning("CSV Format Handler may not be properly configured")

        # Test phone search pipeline
        if hasattr(phone_search_pipeline, 'process_phone_extraction'):
            logger.info("Phone Search Pipeline loaded successfully")
        else:
            logger.warning("Phone Search Pipeline may not be properly configured")

        # Test BCPA module
        if hasattr(bcpa_owner_search, 'process_bcpa_lookup'):
            logger.info("BCPA Owner Search loaded successfully")
        else:
            logger.warning("BCPA Owner Search may not be properly configured")

        # Perform automatic cleanup on startup
        logger.info("Performing automatic file cleanup on startup...")
        try:
            cleanup_result = file_cleanup.cleanup_old_files(max_age_days=7)
            logger.info(f"Startup cleanup completed: {cleanup_result['files_deleted']} files deleted, {cleanup_result['size_freed_mb']:.2f} MB freed")
        except Exception as e:
            logger.warning(f"Startup cleanup failed: {e}")

        # Start automatic weekly cleanup
        try:
            file_cleanup.start_automatic_weekly_cleanup()
            logger.info("Automatic weekly cleanup system started")
        except Exception as e:
            logger.warning(f"Failed to start automatic weekly cleanup: {e}")

        print("\n" + "="*80)
        print("SEPSEPISEP TOOL HUB")
        print("="*80)
        print(f"Server starting on: http://localhost:8080")
        print(f"Network access: http://0.0.0.0:8080")
        print(f"Upload folder: {app.config['UPLOAD_FOLDER']}")
        print(f"Results folder: {app.config['RESULTS_FOLDER']}")
        print("Features available:")
        print("  - Phone Number Extraction")
        print("  - Reverse Address Extraction (Name-less Records)")
        print("  - Column Syncer (Mobile Phone Validation)")
        print("  - BCPA Property Owner Lookup")
        print("  - Multi-format File Support (CSV, Excel)")
        print("="*80)

        # Check if running locally or on server
        import socket
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)

        print(f"Access the application at:")
        print(f"  Local: http://localhost:8080")
        print(f"  Local: http://127.0.0.1:8080")
        print(f"  Network: http://{local_ip}:8080")
        print("="*80)

        app.run(debug=False, host='0.0.0.0', port=8080, threaded=True)

    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        print(f"Error starting application: {str(e)}")