from flask import Flask, render_template, request, jsonify, send_file, url_for
import pandas as pd
import os
import logging
import zipfile
import re
from datetime import datetime
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge
import csv_format_handler
import phone_search_pipeline
import bcpa_owner_search

# Create Flask application
app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size
app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'uploads')
app.config['RESULTS_FOLDER'] = os.path.join(os.getcwd(), 'results')

# Create necessary directories
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['RESULTS_FOLDER'], exist_ok=True)

# Set up logging with Windows-safe formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enterprise_flask.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}

def allowed_file(filename):
    """Check if the uploaded file has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def analyze_phone_coverage(df):
    """Analyze phone number coverage in the dataset."""
    phone_columns = ['phone', 'Phone', 'PHONE', 'phone_number', 'Phone Number', 'PHONE_NUMBER',
                    'cell', 'Cell', 'CELL', 'mobile', 'Mobile', 'MOBILE', 'telephone', 'Telephone']

    phone_col = None
    for col in phone_columns:
        if col in df.columns:
            phone_col = col
            break

    if phone_col:
        # Count non-empty phone numbers
        valid_phones = df[phone_col].dropna().astype(str).str.strip()
        valid_phones = valid_phones[valid_phones != '']

        records_with_phone = len(valid_phones)
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

def analyze_address_coverage(df):
    """Analyze address coverage in the dataset for reverse address extraction."""
    # First analyze name coverage
    name_analysis = analyze_name_coverage(df)

    # Check if we have BCPA processed columns first
    if 'BCPA_Search_Format' in df.columns:
        valid_addresses = df['BCPA_Search_Format'].dropna().astype(str).str.strip()
        valid_addresses = valid_addresses[(valid_addresses != '') & (valid_addresses != 'nan')]

        records_with_address = len(valid_addresses)
        records_without_address = len(df) - records_with_address
        address_coverage = round((records_with_address / len(df)) * 100, 1) if len(df) > 0 else 0

        result = {
            'address_column': 'BCPA_Search_Format (Enhanced)',
            'records_with_address': records_with_address,
            'records_without_address': records_without_address,
            'address_coverage': address_coverage
        }

        # Add name analysis
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
        'Owner Name 2', 'owner_name_2', 'OWNER_NAME_2', 'OwnerName2', 'Owner_Name_2',

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

    # Check for records with actual name data
    records_with_names = 0

    for idx, row in df.iterrows():
        has_name = False
        row_names = []  # Track what names were found for this row
        for col in existing_name_cols:
            value = str(row[col]).strip() if pd.notna(row[col]) else ''
            # Better name validation - check for actual person names
            if value and value.lower() not in ['', 'nan', 'none', 'null', 'n/a'] and len(value) > 2:
                # Check if it looks like a real name (has letters and possibly comma for "LAST, FIRST" format)
                if re.search(r'[A-Za-z]', value) and not value.isdigit():
                    # Additional check for common non-name values
                    non_names = ['n/a', 'none', 'null', 'blank', 'empty', 'unknown', 'na', 'not available']
                    if value.lower() not in non_names:
                        has_name = True
                        row_names.append(f"{col}='{value[:30]}..'" if len(value) > 30 else f"{col}='{value}'")

        if has_name:
            records_with_names += 1
            if idx < 3:  # Log first 3 records for debugging
                logger.info(f"✅ Record {idx+1} HAS names: {', '.join(row_names)}")
        else:
            if idx < 3:  # Log first 3 records for debugging
                logger.info(f"❌ Record {idx+1} NO names detected")

    logger.info(f"📊 Name analysis complete: {records_with_names}/{len(df)} records have names")

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

    logger.info(f"🎯 Final result: {records_with_names} with names, {records_without_names} targets for extraction")
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
    return render_template('enterprise_fixed.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and initial analysis."""
    try:
        logger.info("File upload request received")

        # Check if the post request has the file part
        if 'file' not in request.files:
            logger.error("No file part in request")
            return jsonify({'error': 'No file part in request'}), 400

        file = request.files['file']

        # If user does not select file, browser submits empty part without filename
        if file.filename == '':
            logger.error("No file selected")
            return jsonify({'error': 'No file selected'}), 400

        if file and allowed_file(file.filename):
            logger.info(f"Processing file: {file.filename}")
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{filename}"
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

            # Save the uploaded file
            file.save(filepath)
            logger.info(f"File saved to: {filepath}")

            # Get tab type from form data
            tab_type = request.form.get('tabType', 'phone')  # Default to phone if not specified
            logger.info(f"Processing for tab type: {tab_type}")

            # Process the file with format handler
            try:
                processed_df = csv_format_handler.process_file(filepath)
                logger.info(f"File processed successfully - {len(processed_df)} records")

                # Use enhanced address parser for address tab
                if tab_type == 'address':
                    # Import and use enhanced address parser
                    try:
                        from enhanced_address_parser import process_file_for_bcpa
                        processed_df = process_file_for_bcpa(filepath)
                        logger.info(f"Enhanced address parsing completed - {len(processed_df)} records")
                    except Exception as e:
                        logger.warning(f"Enhanced address parser failed, using basic processing: {e}")

                # Analyze based on tab type
                if tab_type == 'phone':
                    analysis = analyze_phone_coverage(processed_df)
                    analysis_key = 'phone_analysis'
                else:  # address tab
                    analysis = analyze_address_coverage(processed_df)
                    analysis_key = 'address_analysis'

                # Save processed version for later use
                processed_filepath = filepath.replace('.csv', '_processed.csv').replace('.xlsx', '_processed.csv').replace('.xls', '_processed.csv')
                processed_df.to_csv(processed_filepath, index=False, encoding='utf-8')

                response_data = {
                    'message': 'File uploaded and processed successfully',
                    'filename': filename,
                    'filepath': processed_filepath,
                    'total_records': len(processed_df),
                    'columns': list(processed_df.columns),
                    'tab_type': tab_type
                }
                response_data[analysis_key] = analysis

                return jsonify(response_data)

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
        df = pd.read_csv(filepath, encoding='utf-8')

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

        # Save separated files
        base_name = os.path.splitext(os.path.basename(filepath))[0]

        with_file = os.path.join(app.config['RESULTS_FOLDER'], f"{base_name}_{with_label}.csv")
        without_file = os.path.join(app.config['RESULTS_FOLDER'], f"{base_name}_{without_label}.csv")

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
    """Process files based on analysis type (phone, address, bcpa)."""
    try:
        data = request.get_json()
        filepath = data.get('filepath')
        analysis_type = data.get('analysis_type', 'phone')
        max_records = int(data.get('max_records', 30))

        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404

        logger.info(f"Starting {analysis_type} analysis for {max_records} records")

        # Load the data
        df = pd.read_csv(filepath, encoding='utf-8')

        if analysis_type == 'phone':
            # Phone extraction pipeline
            result_file = phone_search_pipeline.process_phone_extraction(filepath, max_records)

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
                'output_file': os.path.basename(result_file)
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

        df = pd.read_csv(filepath, encoding='utf-8')

        # Filter for records without names using the same comprehensive logic as analyze_name_coverage
        name_columns = [
            # Basic name variations
            'Name', 'name', 'NAME',
            'First Name', 'first_name', 'FIRST_NAME', 'FirstName', 'First_Name',
            'Last Name', 'last_name', 'LAST_NAME', 'LastName', 'Last_Name',
            'Full Name', 'full_name', 'FULL_NAME', 'FullName', 'Full_Name',

            # Owner variations
            'Owner Name', 'owner_name', 'OWNER_NAME', 'OwnerName', 'Owner_Name',
            'Owner Name 1', 'owner_name_1', 'OWNER_NAME_1', 'OwnerName1', 'Owner_Name_1',
            'Owner Name 2', 'owner_name_2', 'OWNER_NAME_2', 'OwnerName2', 'Owner_Name_2',

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

        # Filter records without names
        records_without_names = []
        for idx, row in df.iterrows():
            has_name = False
            for col in existing_name_cols:
                value = str(row[col]).strip() if pd.notna(row[col]) else ''
                # Better name validation - check for actual person names
                if value and value.lower() not in ['', 'nan', 'none', 'null'] and len(value) > 2:
                    # Check if it looks like a real name (has letters and possibly comma for "LAST, FIRST" format)
                    if re.search(r'[A-Za-z]', value) and not value.isdigit():
                        has_name = True
                        break
            if not has_name:
                records_without_names.append(idx)

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

        # Create temp file with filtered records for BCPA processing
        temp_input = os.path.join(app.config['UPLOAD_FOLDER'], f"temp_reverse_address_{timestamp}.csv")
        df_limited.to_csv(temp_input, index=False)

        # Use BCPA to process the addresses
        bcpa_result_file = bcpa_owner_search.process_bcpa_lookup(temp_input, max_records)

        if not bcpa_result_file or not os.path.exists(bcpa_result_file):
            logger.error("BCPA processing failed or no results file created")
            return None

        # Step 2: Load BCPA results and separate by phone status
        logger.info(f"📞 Separating BCPA results by phone number status")
        bcpa_df = pd.read_csv(bcpa_result_file, encoding='utf-8')

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

        # Step 3: Create separated output files
        base_name = f"reverse_address_extracted_{timestamp}"

        # Save the main enhanced file (with BCPA owner data for records without names)
        main_output_file = os.path.join(app.config['RESULTS_FOLDER'], f"{base_name}_with_owners.csv")
        bcpa_df.to_csv(main_output_file, index=False, encoding='utf-8')

        # Save separated files
        files_created = []

        if not with_phones.empty:
            with_phones_file = os.path.join(app.config['RESULTS_FOLDER'], f"{base_name}_with_phones.csv")
            with_phones.to_csv(with_phones_file, index=False, encoding='utf-8')
            files_created.append({
                'type': 'with_phones',
                'count': len(with_phones),
                'path': with_phones_file
            })
            logger.info(f"✅ Created with_phones file: {len(with_phones)} records")

        if not without_phones.empty:
            without_phones_file = os.path.join(app.config['RESULTS_FOLDER'], f"{base_name}_without_phones.csv")
            without_phones.to_csv(without_phones_file, index=False, encoding='utf-8')
            files_created.append({
                'type': 'without_phones',
                'count': len(without_phones),
                'path': without_phones_file
            })
            logger.info(f"✅ Created without_phones file: {len(without_phones)} records")

        # Step 4: Create a summary file with all results
        summary_file = os.path.join(app.config['RESULTS_FOLDER'], f"{base_name}_summary.csv")

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

        # Load both files
        original_df = pd.read_csv(original_filepath, encoding='utf-8')
        processed_df = pd.read_csv(processed_filepath, encoding='utf-8')

        logger.info(f"Original file: {len(original_df)} records")
        logger.info(f"Processed file: {len(processed_df)} records")

        # Create merged file with enhanced data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        merged_filename = f"updated_{timestamp}_{os.path.basename(original_filepath)}"
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
            return jsonify({'error': 'Cannot find matching address columns for merge'}), 400

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
            'summary': summary_stats
        })

    except Exception as e:
        logger.error(f"Error in merge_files: {str(e)}")
        return jsonify({'error': f'Merge failed: {str(e)}'}), 500
@app.route('/download/<filename>')
def download_file(filename):
    """Download processed files."""
    try:
        # Look for the file in uploads folder first
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True, download_name=filename)

        # Also check results folder
        file_path = os.path.join(app.config['RESULTS_FOLDER'], filename)
        if os.path.exists(file_path):
            return send_file(file_path, as_attachment=True, download_name=filename)

        return jsonify({'error': 'File not found'}), 404

    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'File too large. Maximum size is 100MB.'}), 413

@app.errorhandler(500)
def internal_error(e):
    logger.error(f"Internal server error: {str(e)}")
    return jsonify({'error': 'Internal server error occurred'}), 500

@app.route('/fetch_data', methods=['POST'])
def fetch_data():
    """Fetch data from uploaded file with pagination"""
    try:
        data = request.get_json()
        filepath = data.get('filepath')
        page = data.get('page', 1)
        per_page = data.get('per_page', 50)

        if not filepath or not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404

        # Load the data
        if filepath.endswith('.csv'):
            df = csv_format_handler.process_file(filepath)
        elif filepath.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(filepath)
        else:
            return jsonify({'error': 'Unsupported file format'}), 400

        # Calculate pagination
        total_records = len(df)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page

        # Get page data
        page_data = df.iloc[start_idx:end_idx]

        # Convert to dict format for JSON
        columns = df.columns.tolist()
        rows = []
        for _, row in page_data.iterrows():
            row_data = {}
            for col in columns:
                value = row[col]
                # Handle NaN values
                if pd.isna(value):
                    row_data[col] = ""
                else:
                    row_data[col] = str(value)
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

        print("\n" + "="*80)
        print("SEPSEPISEP TOOL HUB")
        print("="*80)
        print(f"Server starting on: http://localhost:5000")
        print(f"Upload folder: {app.config['UPLOAD_FOLDER']}")
        print(f"Results folder: {app.config['RESULTS_FOLDER']}")
        print("Features available:")
        print("  - Phone Number Extraction")
        print("  - Reverse Address Extraction (Name-less Records)")
        print("  - BCPA Property Owner Lookup")
        print("  - Multi-format File Support (CSV, Excel)")
        print("="*80)

        app.run(debug=True, port=5000, threaded=True)

    except Exception as e:
        logger.error(f"Failed to start application: {str(e)}")
        print(f"Error starting application: {str(e)}")
