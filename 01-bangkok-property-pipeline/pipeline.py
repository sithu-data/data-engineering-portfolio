"""
Bangkok Property Market Analytics Pipeline
Run this file to execute the full ETL pipeline manually.
Usage: python pipeline.py
"""
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ingestion.extract import extract_from_csv
from transformation.transform import transform_property_data
from transformation.load import load_to_postgres

def run_pipeline():
    start_time = datetime.now()
    print("=" * 60)
    print("  Bangkok Property Market Analytics Pipeline")
    print(f"  Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # --- EXTRACT ---
        print("\n📥 STEP 1: EXTRACT")
        df_raw = extract_from_csv("data/raw_listings.csv")
        
        # --- TRANSFORM ---
        print("\n⚙️  STEP 2: TRANSFORM")
        df_clean = transform_property_data(df_raw)
        
        # --- LOAD ---
        print("\n📤 STEP 3: LOAD")
        load_to_postgres(df_clean)
        
        # --- DONE ---
        end_time = datetime.now()
        duration = (end_time - start_time).seconds
        print("\n" + "=" * 60)
        print(f"  ✅ Pipeline completed successfully!")
        print(f"  Duration: {duration} seconds")
        print(f"  Rows loaded: {len(df_clean)}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_pipeline()