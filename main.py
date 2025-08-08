#!/usr/bin/env python3
"""
Phone Search System - Main Interface
Universal CSV processor for phone number searches using ZabaSearch and Radaris
"""

import asyncio
import logging
from pathlib import Path
from phone_search_pipeline import PhoneSearchPipeline

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    """Main interface with multiple usage options"""
    
    print("=" * 60)
    print("🔍 PHONE SEARCH SYSTEM")
    print("Universal CSV Processor for Phone Number Searches")
    print("=" * 60)
    
    # Show available CSV files
    csv_files = list(Path(".").glob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in current directory")
        return
    
    print("\n📁 Available CSV files:")
    for i, file in enumerate(csv_files, 1):
        print(f"  {i}. {file.name}")
    
    # Get user choice
    while True:
        try:
            choice = input(f"\n🔢 Select a file (1-{len(csv_files)}) or 'q' to quit: ").strip()
            
            if choice.lower() == 'q':
                print("👋 Goodbye!")
                return
            
            file_index = int(choice) - 1
            if 0 <= file_index < len(csv_files):
                selected_file = csv_files[file_index]
                break
            else:
                print(f"❌ Please enter a number between 1 and {len(csv_files)}")
                
        except ValueError:
            print("❌ Please enter a valid number or 'q'")
    
    print(f"\n✅ Selected: {selected_file.name}")
    
    # Processing options
    print("\n⚙️ Processing Options:")
    print("  1. Full Pipeline (ZabaSearch → Radaris)")
    print("  2. ZabaSearch Only")
    print("  3. Radaris Only")
    print("  4. Check Current Status")
    
    while True:
        try:
            option = input("\n🔢 Select option (1-4): ").strip()
            
            if option in ['1', '2', '3', '4']:
                break
            else:
                print("❌ Please enter 1, 2, 3, or 4")
                
        except ValueError:
            print("❌ Please enter a valid option")
    
    # Initialize pipeline
    pipeline = PhoneSearchPipeline()
    
    try:
        if option == '4':
            # Check status only
            print(f"\n📊 Checking status of {selected_file.name}...")
            status = pipeline.get_pipeline_status(str(selected_file))
            
            print("\n📊 CURRENT STATUS:")
            print(f"  📋 Total Records: {status['total_records']}")
            print(f"  🔍 ZabaSearch Success: {status['zabasearch_success']}")
            print(f"  🌐 Radaris Success: {status['radaris_success']}")
            print(f"  📞 Total with Phones: {status['total_with_phones']}")
            print(f"  ✅ Completion Rate: {status['completion_rate']}")
            
        else:
            # Processing
            skip_zabasearch = option == '3'  # Skip if Radaris only
            skip_radaris = option == '2'     # Skip if ZabaSearch only
            
            batch_size = 3  # Small batches for stability
            
            output_path = await pipeline.process_complete_pipeline(
                str(selected_file),
                batch_size=batch_size,
                skip_zabasearch=skip_zabasearch,
                skip_radaris=skip_radaris
            )
            
            print(f"\n🎉 Processing completed!")
            print(f"📄 Output file: {output_path}")
            
            # Show final status
            status = pipeline.get_pipeline_status(output_path)
            print("\n📊 FINAL STATUS:")
            print(f"  📋 Total Records: {status['total_records']}")
            print(f"  🔍 ZabaSearch Success: {status['zabasearch_success']}")
            print(f"  🌐 Radaris Success: {status['radaris_success']}")
            print(f"  📞 Total with Phones: {status['total_with_phones']}")
            print(f"  ✅ Completion Rate: {status['completion_rate']}")
            
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
