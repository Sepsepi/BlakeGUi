#!/usr/bin/env python3
"""
Debug the address parsing issue - let's see what's really happening
"""

import sys
import os
sys.path.insert(0, os.getcwd())

from zabasearch_processor import ZabaSearchProcessor

def debug_parsing():
    """Debug what the current processor does with our test addresses"""

    test_addresses = [
        "10310 WATERSIDE CT, PARKLAND, FL, 33076",
        "8661 MIRALAGO WAY, PARKLAND, FL, 33076",
        "8890 WATERSIDE PT, PARKLAND, FL, 33076",
        "9593 TOWN PARC CIR S, PARKLAND, FL, 33076"
    ]

    processor = ZabaSearchProcessor(headless=True)

    print("🧪 DEBUGGING ADDRESS PARSING")
    print("=" * 50)

    for i, address in enumerate(test_addresses, 1):
        print(f"\n{i}. Address: '{address}'")

        # Let's manually call the address parsing logic if we can find it
        # For now, let's just create a test record and see what gets parsed

        # First, let's inspect the processor methods
        print(f"   Processor type: {type(processor).__name__}")
        print(f"   Processor file: {processor.__class__.__module__}")

        # Let's see if we can find the parsing method
        methods = [method for method in dir(processor) if not method.startswith('_')]
        print(f"   Available methods: {methods[:5]}...")  # Just show first 5

        # Let's create a minimal CSV to test
        import pandas as pd
        test_df = pd.DataFrame([{
            'DirectName_Cleaned': 'TEST PERSON',
            'DirectName_Address': address
        }])

        csv_file = f'debug_test_{i}.csv'
        test_df.to_csv(csv_file, index=False)

        print(f"   Created test CSV: {csv_file}")

        # Let's try to parse this manually by checking what the processor extracts
        try:
            # Read it back to see what would be processed
            df = pd.read_csv(csv_file)

            # Simulate what the processor does to extract name and address
            for _, row in df.iterrows():
                name = str(row.get('DirectName_Cleaned', ''))
                addr = str(row.get('DirectName_Address', ''))

                if name and addr and name != 'nan' and addr != 'nan':
                    print(f"   ✓ Name: '{name}'")
                    print(f"   ✓ Address: '{addr}'")

                    # Now the key question: what city would be extracted?
                    # We'll need to look at how the processor actually does this
                    break

        except Exception as e:
            print(f"   ❌ Error: {e}")

        # Clean up
        try:
            os.remove(csv_file)
        except:
            pass

    print(f"\n🔍 PROCESSOR CODE LOCATION:")
    import inspect
    try:
        processor_file = inspect.getfile(ZabaSearchProcessor)
        print(f"   File: {processor_file}")
        print(f"   Size: {os.path.getsize(processor_file)} bytes")
    except Exception as e:
        print(f"   Could not locate file: {e}")

if __name__ == "__main__":
    debug_parsing()
