#!/usr/bin/env python3
"""
Workspace Cleanup Script
Removes temporary files, logs, and other clutter from the workspace
"""

import os
import glob
import shutil
import tempfile
from pathlib import Path
import argparse

def cleanup_workspace(workspace_path=None, dry_run=False):
    """
    Clean up the workspace by removing temporary files and logs

    Args:
        workspace_path: Path to workspace (defaults to current directory)
        dry_run: If True, only show what would be deleted without actually deleting
    """
    if workspace_path is None:
        workspace_path = os.getcwd()

    workspace_path = Path(workspace_path)

    # Define patterns to clean up
    cleanup_patterns = [
        # Log files
        '*.log',
        '*_search_*.log',
        'phone_search_pipeline_*.log',
        'bcpa_search_*.log',

        # Temporary files
        'tmp*',
        'temp*',
        '*_temp.csv',
        '*_tmp.csv',

        # Processing intermediates
        '*_needs_phones.csv',
        '*_standardized_temp.csv',
        '*_filtered.csv',

        # Python cache
        '__pycache__',
        '*.pyc',
        '*.pyo',

        # System files
        '.DS_Store',
        'Thumbs.db',

        # Backup files
        '*.bak',
        '*~',

        # CSV processing intermediates
        '*_intermediate.csv',
        '*_processing.csv'
    ]

    deleted_files = []
    deleted_dirs = []
    total_size = 0

    print(f"🧹 Cleaning workspace: {workspace_path}")
    if dry_run:
        print("🔍 DRY RUN - No files will be deleted")
    print("-" * 50)

    # Clean up files based on patterns
    for pattern in cleanup_patterns:
        matches = list(workspace_path.glob(pattern))

        for item in matches:
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

    # Summary
    print("\n" + "=" * 50)
    print("📊 CLEANUP SUMMARY")
    print("=" * 50)

    if dry_run:
        print(f"📄 Files that would be deleted: {len(deleted_files)}")
        print(f"📁 Directories that would be deleted: {len(deleted_dirs)}")
        print(f"💾 Total space that would be freed: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")
    else:
        print(f"✅ Files deleted: {len(deleted_files)}")
        print(f"✅ Directories deleted: {len(deleted_dirs)}")
        print(f"✅ Total space freed: {total_size:,} bytes ({total_size/1024/1024:.2f} MB)")

        if deleted_files:
            print(f"\n📋 Deleted files:")
            for file in deleted_files:
                print(f"   • {file}")

        if deleted_dirs:
            print(f"\n📋 Deleted directories:")
            for dir in deleted_dirs:
                print(f"   • {dir}")

    return len(deleted_files) + len(deleted_dirs), total_size

def main():
    parser = argparse.ArgumentParser(description='Clean up workspace files')
    parser.add_argument('--path', '-p', help='Workspace path (default: current directory)')
    parser.add_argument('--dry-run', '-d', action='store_true', help='Show what would be deleted without deleting')
    parser.add_argument('--quiet', '-q', action='store_true', help='Minimal output')

    args = parser.parse_args()

    if not args.quiet:
        print("🧹 Workspace Cleanup Tool")
        print("=" * 30)

    items_deleted, space_freed = cleanup_workspace(args.path, args.dry_run)

    if args.quiet:
        print(f"{items_deleted},{space_freed}")
    elif not args.dry_run and items_deleted > 0:
        print(f"\n🎉 Cleanup complete! Removed {items_deleted} items and freed {space_freed/1024/1024:.2f} MB")
    elif args.dry_run:
        print(f"\n👀 Dry run complete. Would remove {items_deleted} items and free {space_freed/1024/1024:.2f} MB")
    else:
        print(f"\n✨ Workspace is already clean!")

if __name__ == "__main__":
    main()
