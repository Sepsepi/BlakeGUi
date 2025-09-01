#!/usr/bin/env python3
"""
File Cleanup Utility for Enterprise Flask Application
Automatically cleans up old files from uploads, results, logs, and temp folders
"""

import os
import time
import logging
import threading
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

# Global variable to track last cleanup time
_last_cleanup_time = None
_cleanup_thread = None

def cleanup_old_files(max_age_days=7):
    """
    Clean up files older than max_age_days from all working directories
    PRESERVES final output files (Cleaned_* and Merged_* files) regardless of age

    Args:
        max_age_days (int): Maximum age of files to keep in days
                           Set to 0 to delete ALL files regardless of age
                           EXCEPT final output files which are always preserved
    """
    current_time = time.time()
    cutoff_time = current_time - (max_age_days * 24 * 60 * 60)

    # Special case: if max_age_days is 0, delete all files EXCEPT final outputs
    delete_all = (max_age_days == 0)

    # Directories to clean
    cleanup_dirs = [
        'uploads',
        'results',
        'logs',
        'temp',
        'output'
    ]

    total_deleted = 0
    total_size_freed = 0

    # Helper function to check if a file should be preserved
    def should_preserve_file(file_path):
        """Check if this is a final output file that should be preserved"""
        filename = file_path.name
        # Preserve final output files: Cleaned_* and Merged_* files
        if filename.startswith('Cleaned_') or filename.startswith('Merged_'):
            return True
        # Also preserve other important final files
        if any(keyword in filename.lower() for keyword in [
            'reverse_address_extracted', 'with_owners', 'bcpa_results',
            'owner_search_results', 'address_enhanced'
        ]):
            return True
        return False

    # First, clean up phone_ready_*.csv files from main directory
    logger.info("🧹 Cleaning up phone_ready_*.csv files from main folder...")
    main_dir = Path('.')
    try:
        for file_path in main_dir.glob('phone_ready_*.csv'):
            if file_path.is_file():
                file_stat = file_path.stat()
                file_age_days = (current_time - file_stat.st_mtime) / (24 * 60 * 60)

                # Check if file should be deleted
                should_delete = delete_all or (file_stat.st_mtime < cutoff_time)

                if should_delete:
                    file_size = file_stat.st_size
                    try:
                        file_path.unlink()
                        total_deleted += 1
                        total_size_freed += file_size
                        if delete_all:
                            logger.info(f"🗑️  Deleted phone_ready file (force): {file_path.name} (age: {file_age_days:.1f} days, size: {file_size:,} bytes)")
                        else:
                            logger.info(f"🗑️  Deleted phone_ready file: {file_path.name} (age: {file_age_days:.1f} days, size: {file_size:,} bytes)")
                    except Exception as e:
                        logger.error(f"❌ Failed to delete phone_ready file {file_path}: {e}")
    except Exception as e:
        logger.error(f"❌ Error cleaning phone_ready files: {e}")

    for dir_name in cleanup_dirs:
        dir_path = Path(dir_name)
        if not dir_path.exists():
            continue

        logger.info(f"🧹 Cleaning up {dir_name} folder...")

        try:
            for file_path in dir_path.rglob('*'):
                if file_path.is_file():
                    file_stat = file_path.stat()
                    file_age_days = (current_time - file_stat.st_mtime) / (24 * 60 * 60)

                    # Check if this is a final output file that should be preserved
                    if should_preserve_file(file_path):
                        logger.info(f"🛡️  Preserving final output file: {file_path} (age: {file_age_days:.1f} days)")
                        continue

                    # Check if file should be deleted
                    should_delete = delete_all or (file_stat.st_mtime < cutoff_time)

                    if should_delete:
                        file_size = file_stat.st_size

                        try:
                            file_path.unlink()
                            total_deleted += 1
                            total_size_freed += file_size
                            if delete_all:
                                logger.info(f"🗑️  Deleted (force): {file_path} (age: {file_age_days:.1f} days, size: {file_size:,} bytes)")
                            else:
                                logger.info(f"🗑️  Deleted: {file_path} (age: {file_age_days:.1f} days, size: {file_size:,} bytes)")
                        except Exception as e:
                            logger.error(f"❌ Failed to delete {file_path}: {e}")

        except Exception as e:
            logger.error(f"❌ Error cleaning {dir_name}: {e}")

    # Log summary
    size_mb = total_size_freed / (1024 * 1024)
    logger.info(f"🎉 Cleanup complete: {total_deleted} files deleted, {size_mb:.2f} MB freed")

    return {
        'files_deleted': total_deleted,
        'size_freed_mb': size_mb,
        'cleanup_date': datetime.now().isoformat()
    }

def get_recent_files(dir_name, max_files=10):
    """
    Get the most recent files from a directory

    Args:
        dir_name (str): Directory name to search
        max_files (int): Maximum number of files to return

    Returns:
        list: List of recent file info dictionaries
    """
    dir_path = Path(dir_name)
    if not dir_path.exists():
        return []

    files = []
    try:
        for file_path in dir_path.rglob('*'):
            if file_path.is_file():
                file_stat = file_path.stat()
                files.append({
                    'name': file_path.name,
                    'path': str(file_path),
                    'size': file_stat.st_size,
                    'modified': datetime.fromtimestamp(file_stat.st_mtime),
                    'age_hours': (time.time() - file_stat.st_mtime) / 3600
                })
    except Exception as e:
        logger.error(f"Error getting recent files from {dir_name}: {e}")

    # Sort by modification time (newest first) and limit
    files.sort(key=lambda x: x['modified'], reverse=True)
    return files[:max_files]

def get_last_processed_files():
    """
    Get the last processed files for both phone and address tabs
    ONLY shows final output files that users actually need

    Returns:
        dict: Dictionary with phone and address file lists
    """
    results = {
        'phone': [],
        'address': []
    }

    # Get recent files from results folder
    recent_files = get_recent_files('results', max_files=30)

    for file_info in recent_files:
        filename = file_info['name']
        filename_lower = filename.lower()

        # ONLY SHOW FINAL OUTPUT FILES - Filter by specific prefixes
        # Phone extraction final files: Cleaned_ and Merged_ files
        if filename.startswith('Cleaned_') or filename.startswith('Merged_'):
            # These are final phone extraction outputs
            results['phone'].append(file_info)

        # Address extraction final files: Files with owner/address processing results
        elif any(keyword in filename_lower for keyword in [
            'reverse_address_extracted', 'with_owners', 'bcpa_results',
            'owner_search_results', 'address_enhanced'
        ]):
            # These are final address extraction outputs
            results['address'].append(file_info)

        # Skip intermediate files like:
        # - phone_extraction_*.csv (intermediate)
        # - phone_formatted_*.csv (intermediate)
        # - *_processed.csv (intermediate)
        # - temp_*.csv (intermediate)

    # Limit to 5 most recent per category
    results['phone'] = results['phone'][:5]
    results['address'] = results['address'][:5]

    logger.info(f"📁 Recent files filter: {len(results['phone'])} phone files, {len(results['address'])} address files")
    logger.info(f"📱 Phone files: {[f['name'] for f in results['phone']]}")
    logger.info(f"🏠 Address files: {[f['name'] for f in results['address']]}")

    return results

def should_run_weekly_cleanup():
    """
    Check if weekly cleanup should run based on last cleanup time

    Returns:
        bool: True if cleanup should run, False otherwise
    """
    global _last_cleanup_time

    # If never run before, should run
    if _last_cleanup_time is None:
        return True

    # Check if 7 days have passed since last cleanup
    time_since_last = time.time() - _last_cleanup_time
    days_since_last = time_since_last / (24 * 60 * 60)

    return days_since_last >= 7

def start_automatic_weekly_cleanup():
    """
    Start automatic weekly cleanup in a background thread
    """
    global _cleanup_thread

    if _cleanup_thread and _cleanup_thread.is_alive():
        logger.info("Automatic cleanup thread already running")
        return

    def weekly_cleanup_worker():
        """Background worker for weekly cleanup"""
        global _last_cleanup_time

        while True:
            try:
                # Sleep for 24 hours before checking again
                time.sleep(24 * 60 * 60)  # 24 hours

                if should_run_weekly_cleanup():
                    logger.info("🗓️ Starting automatic weekly cleanup...")
                    result = cleanup_old_files(max_age_days=7)
                    _last_cleanup_time = time.time()
                    logger.info(f"🎉 Weekly cleanup completed: {result['files_deleted']} files deleted, {result['size_freed_mb']:.2f} MB freed")

            except Exception as e:
                logger.error(f"Error in weekly cleanup worker: {e}")
                # Continue running even if there's an error
                continue

    _cleanup_thread = threading.Thread(target=weekly_cleanup_worker, daemon=True)
    _cleanup_thread.start()
    logger.info("🔄 Automatic weekly cleanup thread started")

def cleanup_temp_folder():
    """
    Clean up all files in the temp folder after processing completion

    Returns:
        dict: Cleanup result information
    """
    temp_dir = Path('temp')
    if not temp_dir.exists():
        logger.info("🗑️ Temp folder does not exist, nothing to clean")
        return {'files_deleted': 0, 'size_freed_mb': 0}

    total_deleted = 0
    total_size_freed = 0

    logger.info("🧹 Cleaning up temp folder after processing completion...")

    try:
        for file_path in temp_dir.rglob('*'):
            if file_path.is_file():
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    total_deleted += 1
                    total_size_freed += file_size
                    logger.info(f"🗑️ Deleted temp file: {file_path.name} ({file_size:,} bytes)")
                except Exception as e:
                    logger.error(f"❌ Failed to delete temp file {file_path}: {e}")
    except Exception as e:
        logger.error(f"❌ Error cleaning temp folder: {e}")

    size_mb = total_size_freed / (1024 * 1024)
    logger.info(f"🎉 Temp cleanup complete: {total_deleted} files deleted, {size_mb:.2f} MB freed")

    return {
        'files_deleted': total_deleted,
        'size_freed_mb': size_mb,
        'cleanup_date': datetime.now().isoformat()
    }

def get_cleanup_status():
    """
    Get the status of automatic cleanup

    Returns:
        dict: Cleanup status information
    """
    global _last_cleanup_time, _cleanup_thread

    status = {
        'automatic_cleanup_enabled': _cleanup_thread is not None and _cleanup_thread.is_alive(),
        'last_cleanup_time': _last_cleanup_time,
        'next_cleanup_due': None,
        'days_until_next': None
    }

    if _last_cleanup_time:
        next_cleanup = _last_cleanup_time + (7 * 24 * 60 * 60)  # 7 days from last cleanup
        status['next_cleanup_due'] = datetime.fromtimestamp(next_cleanup).isoformat()
        status['days_until_next'] = max(0, (next_cleanup - time.time()) / (24 * 60 * 60))

    return status

if __name__ == '__main__':
    # Run cleanup when script is executed directly
    print("🧹 Starting file cleanup...")
    result = cleanup_old_files(max_age_days=7)
    print(f"✅ Cleanup completed: {result}")
