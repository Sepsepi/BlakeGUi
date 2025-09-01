#!/usr/bin/env python3

import os
import sys
import logging
import pandas as pd
import re
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Tuple

class CyberBackgroundChecksSearcher:
    def __init__(self, user_config=None):
        self.user_config = user_config or {}
        self.setup_logging()
        self.records_processed = 0
        self.phones_found = 0
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('cyberbackgroundchecks.log')
            ]
        )
        self.logger = logging.getLogger(__name__)

    def parse_name_from_owner(self, owner_name: str) -> Tuple[str, str]:
        if not owner_name or pd.isna(owner_name):
            return "", ""
            
        name = str(owner_name).strip().upper()
        name = re.sub(r'\b(JR|SR|III|II|IV|ESQ|MD|PHD|DR)\b', '', name)
        name = re.sub(r'\b(MR|MRS|MS|MISS)\b', '', name)
        
        parts = name.split()
        
        if len(parts) >= 2:
            first_name = parts[0]
            last_name = parts[-1]
        elif len(parts) == 1:
            first_name = parts[0]
            last_name = ""
        else:
            first_name = ""
            last_name = ""
            
        return first_name, last_name

    def extract_state_from_address(self, address: str) -> str:
        if not address or pd.isna(address):
            return ""
            
        state_match = re.search(r'\b([A-Z]{2})\b', str(address).upper())
        if state_match:
            return state_match.group(1)
            
        return ""

    def needs_phone_lookup(self, row: pd.Series) -> bool:
        phone_columns = ['Primary_Phone', 'Secondary_Phone', 'DirectName_Phone_Primary']
        
        for col in phone_columns:
            if col in row and pd.notna(row[col]) and str(row[col]).strip():
                return False
                
        return True

    def process_merged_file(self, merged_file_path: str, output_path: str = None) -> Dict:
        try:
            self.logger.info(f"🔍 Starting CyberBackgroundChecks processing for: {merged_file_path}")
            
            df = pd.read_csv(merged_file_path)
            total_records = len(df)
            self.logger.info(f"📊 Loaded {total_records} records from merged file")
            
            records_needing_phones = df.apply(self.needs_phone_lookup, axis=1)
            need_phones_count = records_needing_phones.sum()
            
            self.logger.info(f"📱 Records needing phone lookup: {need_phones_count}")
            
            if need_phones_count == 0:
                self.logger.info("✅ All records already have phone numbers!")
                return {
                    'success': True,
                    'message': 'No additional phone lookup needed',
                    'records_processed': 0,
                    'phones_found': 0,
                    'output_file': merged_file_path
                }
            
            records_to_process = df[records_needing_phones].copy()
            enhanced_records = self.search_cyber_background_checks(records_to_process)
            
            for idx, enhanced_row in enhanced_records.iterrows():
                original_idx = enhanced_row.name
                
                if pd.notna(enhanced_row.get('CBC_Primary_Phone')) and enhanced_row['CBC_Primary_Phone']:
                    df.loc[original_idx, 'Primary_Phone'] = enhanced_row['CBC_Primary_Phone']
                    
                if pd.notna(enhanced_row.get('CBC_Secondary_Phone')) and enhanced_row['CBC_Secondary_Phone']:
                    df.loc[original_idx, 'Secondary_Phone'] = enhanced_row['CBC_Secondary_Phone']
            
            if not output_path:
                base_path = Path(merged_file_path)
                output_path = str(base_path.parent / f"Enhanced_{base_path.name}")
            
            df.to_csv(output_path, index=False)
            
            result = {
                'success': True,
                'message': f'CyberBackgroundChecks processing completed',
                'records_processed': self.records_processed,
                'phones_found': self.phones_found,
                'output_file': output_path,
                'total_records': total_records,
                'records_needing_phones': need_phones_count
            }
            
            self.logger.info(f"✅ CyberBackgroundChecks processing complete!")
            self.logger.info(f"📊 Records processed: {self.records_processed}")
            self.logger.info(f"📱 New phone numbers found: {self.phones_found}")
            self.logger.info(f"💾 Enhanced file saved: {output_path}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"❌ CyberBackgroundChecks processing failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'records_processed': 0,
                'phones_found': 0
            }

    def search_cyber_background_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"🌐 Starting CyberBackgroundChecks automation for {len(df)} records")
        
        df['CBC_Primary_Phone'] = ''
        df['CBC_Secondary_Phone'] = ''
        
        for idx, row in df.iterrows():
            try:
                first_name, last_name = self.parse_name_from_owner(
                    row.get('DirectName_Cleaned', '') or 
                    row.get('Owner_Name', '') or 
                    row.get('Name', '')
                )
                
                state = self.extract_state_from_address(
                    row.get('DirectName_Address', '') or 
                    row.get('Property_Address', '') or 
                    row.get('Address', '')
                )
                
                if not first_name or not last_name:
                    self.logger.warning(f"WARNING: Skipping record {idx}: Could not parse name")
                    continue
                
                self.logger.info(f"🔍 Searching: {first_name} {last_name}, {state}")
                
                phone_results = self.perform_cyber_search(first_name, last_name, state, row)
                
                if phone_results:
                    df.loc[idx, 'CBC_Primary_Phone'] = phone_results.get('primary_phone', '')
                    df.loc[idx, 'CBC_Secondary_Phone'] = phone_results.get('secondary_phone', '')
                    
                    if phone_results.get('primary_phone'):
                        self.phones_found += 1
                    if phone_results.get('secondary_phone'):
                        self.phones_found += 1
                
                self.records_processed += 1
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                self.logger.error(f"❌ Error processing record {idx}: {e}")
                continue
        
        return df

    def perform_cyber_search(self, first_name: str, last_name: str, state: str, original_row: pd.Series) -> Dict:
        try:
            self.logger.info(f"🌐 [MCP] Searching CyberBackgroundChecks: {first_name} {last_name}, {state}")
            
            # For now, return placeholder results until MCP integration is fully implemented
            # This will be replaced with actual MCP Playwright automation
            
            return {
                'primary_phone': '',
                'secondary_phone': '',
                'verified': False
            }
            
        except Exception as e:
            self.logger.error(f"❌ CyberBackgroundChecks search error: {e}")
            return {'primary_phone': '', 'secondary_phone': '', 'verified': False}

def process_cyber_background_checks(merged_file_path: str, user_config: Dict = None) -> Dict:
    searcher = CyberBackgroundChecksSearcher(user_config)
    return searcher.process_merged_file(merged_file_path)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='CyberBackgroundChecks Phone Search')
    parser.add_argument('merged_file', help='Path to merged CSV file')
    parser.add_argument('--output', help='Output file path (optional)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.merged_file):
        print(f"❌ Error: File not found: {args.merged_file}")
        sys.exit(1)
    
    result = process_cyber_background_checks(args.merged_file)
    
    if result['success']:
        print(f"✅ Success: {result['message']}")
        print(f"📊 Records processed: {result['records_processed']}")
        print(f"📱 Phone numbers found: {result['phones_found']}")
    else:
        print(f"❌ Error: {result.get('error', 'Unknown error')}")
        sys.exit(1)
