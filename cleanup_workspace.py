#!/usr/bin/env python3
"""
Workspace Cleanup Script - Development Edition
Removes temporary files, logs, and development clutter from the workspace.
For production file lifecycle management, use file_cleanup.py instead.

This script focuses on:
- Development artifacts (logs, temp files, cache)
- Python bytecode files
- System temporary files
- Build artifacts

For server file management (age-based cleanup), use the integrated
file_cleanup.py system via the web interface or /cleanup_files endpoint.
"""

import os
import glob
import shutil
import tempfile
from pathlib import Path
import argparse

# Try to import the production cleanup system
try:
    import file_cleanup
    PRODUCTION_CLEANUP_AVAILABLE = True
except ImportError:
    PRODUCTION_CLEANUP_AVAILABLE = False

def cleanup_workspace(workspace_path=None, dry_run=False, include_production=False):
    """
    Clean up the workspace by removing development files and artifacts

    Args:
        workspace_path: Path to workspace (defaults to current directory)
        dry_run: If True, only show what would be deleted without actually deleting
        include_production: If True, also run production file cleanup (age-based)
    """
    if workspace_path is None:
        workspace_path = os.getcwd()

    workspace_path = Path(workspace_path)

    # Define patterns for DEVELOPMENT cleanup only
    # Note: Production files (uploads, results) are handled by file_cleanup.py
    cleanup_patterns = [
        # Development log files (not in logs/ folder)
        '*.log',
        '*_search_*.log',
        'phone_search_pipeline_*.log',
        'bcpa_search_*.log',
        'enterprise_flask.log',  # Old log file location

        # Temporary development files
        'tmp*',
        'temp*',
        '*_temp.csv',
        '*_tmp.csv',

        # Processing intermediates in root directory
        '*_needs_phones.csv',
        '*_standardized_temp.csv',
        '*_filtered.csv',
        '*_intermediate.csv',
        '*_processing.csv',

        # Python development artifacts
        '__pycache__',
        '*.pyc',
        '*.pyo',

        # System files
        '.DS_Store',
        'Thumbs.db',

        # Editor backup files
        '*.bak',
        '*~',
        '.*.swp',
        '.*.swo',

        # IDE files
        '.vscode/settings.json.bak',
        '*.code-workspace.bak'
    ]

    deleted_files = []
    deleted_dirs = []
    total_size = 0

    print(f"🧹 Development Workspace Cleanup: {workspace_path}")
    if dry_run:
        print("🔍 DRY RUN - No files will be deleted")
    print("-" * 50)

    # Clean up development files based on patterns
    for pattern in cleanup_patterns:
        matches = list(workspace_path.glob(pattern))

        for item in matches:
            # Skip production folders - these are managed by file_cleanup.py
            if any(prod_folder in str(item) for prod_folder in ['uploads/', 'results/', 'logs/', 'temp/', 'output/']):
                continue

            try:
                # Calculate size before deletion
                if item.is_file():
                    size = item.stat().st_size
                    total_size += size

                    if dry_run:
                        print(f"📄 Would delete file: {item.name} ({size:,} bytes)")
                    else:
                        item.unlink()
                        deleted_files.append(item.name)
                        print(f"🗑️  Deleted file: {item.name} ({size:,} bytes)")

                elif item.is_dir():
                    # Calculate directory size
                    dir_size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
                    total_size += dir_size

                    if dry_run:
                        print(f"📁 Would delete directory: {item.name} ({dir_size:,} bytes)")
                    else:
                        shutil.rmtree(item)
                        deleted_dirs.append(item.name)
                        print(f"🗂️  Deleted directory: {item.name} ({dir_size:,} bytes)")

            except Exception as e:
                print(f"❌ Error processing {item}: {e}")

    # Clean system temp files that might be related to our app
    temp_dir = Path(tempfile.gettempdir())
    app_temp_patterns = [
        'tmp*phone_search*.csv',
        'tmp*address_search*.csv',
        'tmp*bcpa*.csv',
        'streamlit*'
    ]

    print("\n🧽 Cleaning system temporary files...")
    for pattern in app_temp_patterns:
        temp_matches = list(temp_dir.glob(pattern))
        for temp_file in temp_matches:
            try:
                if temp_file.is_file():
                    size = temp_file.stat().st_size
                    total_size += size

                    if dry_run:
                        print(f"📄 Would delete temp file: {temp_file.name} ({size:,} bytes)")
                    else:
                        temp_file.unlink()
                        deleted_files.append(f"temp/{temp_file.name}")
                        print(f"🗑️  Deleted temp file: {temp_file.name} ({size:,} bytes)")
            except Exception as e:
                print(f"❌ Error cleaning temp file {temp_file}: {e}")

    # Run production cleanup if requested and available
    production_result = None
    if include_production and PRODUCTION_CLEANUP_AVAILABLE:
        print("\n🏭 Running production file cleanup...")
        try:
            production_result = file_cleanup.cleanup_old_files(max_age_days=7)
            print(f"✅ Production cleanup: {production_result['files_deleted']} files, {production_result['size_freed_mb']:.2f} MB")
        except Exception as e:
            print(f"❌ Production cleanup failed: {e}")
    elif include_production and not PRODUCTION_CLEANUP_AVAILABLE:
        print("\n⚠️  Production cleanup requested but file_cleanup.py not available")

    # Summary
    print("\n" + "=" * 50)
    print("📊 DEVELOPMENT CLEANUP SUMMARY")
    print("=" * 50)

    if dry_run:
        print(f"📄 Development files that would be deleted: {len(deleted_files)}")
        print(f"📁 Development directories that would be deleted: {len(deleted_dirs)}")
        print(f"💾 Development space that would be freed: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    else:
        print(f"✅ Development files deleted: {len(deleted_files)}")
        print(f"✅ Development directories deleted: {len(deleted_dirs)}")
        print(f"✅ Development space freed: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")

        if deleted_files:
            print(f"\n📋 Deleted development files:")
            for file in deleted_files:
                print(f"   • {file}")

        if deleted_dirs:
            print(f"\n📋 Deleted development directories:")
            for dir in deleted_dirs:
                print(f"   • {dir}")

    # Add production cleanup info if it was run
    if production_result:
        print(f"\n🏭 Production cleanup also freed: {production_result['size_freed_mb']:.2f} MB")
        print(f"📊 Total cleanup: {len(deleted_files) + len(deleted_dirs) + production_result['files_deleted']} items")

    return len(deleted_files) + len(deleted_dirs), total_size, production_result

def main():
    parser = argparse.ArgumentParser(description='Clean up workspace development files')
    parser.add_argument('--path', '-p', help='Workspace path (default: current directory)')
    parser.add_argument('--dry-run', '-d', action='store_true', help='Show what would be deleted without deleting')
    parser.add_argument('--quiet', '-q', action='store_true', help='Minimal output')
    parser.add_argument('--production', action='store_true', help='Also run production file cleanup (7+ day old files)')

    args = parser.parse_args()

    if not args.quiet:
        print("🧹 Development Workspace Cleanup Tool")
        print("=" * 40)
        if args.production:
            print("🏭 Production cleanup included")
        print()

    dev_items, dev_space, prod_result = cleanup_workspace(args.path, args.dry_run, args.production)

    if args.quiet:
        total_items = dev_items + (prod_result['files_deleted'] if prod_result else 0)
        total_space = dev_space + (prod_result['size_freed_mb'] * 1024 * 1024 if prod_result else 0)
        print(f"{total_items},{total_space}")
    elif not args.dry_run and dev_items > 0:
        total_freed = dev_space/1024/1024 + (prod_result['size_freed_mb'] if prod_result else 0)
        print(f"\n🎉 Development cleanup complete! Removed {dev_items} items and freed {total_freed:.2f} MB")
        if prod_result:
            print(f"🏭 Production cleanup also freed {prod_result['size_freed_mb']:.2f} MB")
    elif args.dry_run:
        total_freed = dev_space/1024/1024
        print(f"\n👀 Development dry run complete. Would remove {dev_items} items and free {total_freed:.2f} MB")
        if args.production and PRODUCTION_CLEANUP_AVAILABLE:
            print("🏭 Production cleanup would also be performed")
    else:
        print(f"\n✨ Development workspace is already clean!")
        if prod_result and prod_result['files_deleted'] > 0:
            print(f"🏭 But production cleanup freed {prod_result['size_freed_mb']:.2f} MB")

if __name__ == "__main__":
    main()
