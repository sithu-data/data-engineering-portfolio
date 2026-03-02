import pandas as pd
import os
from datetime import datetime


def extract_from_csv(filepath: str) -> pd.DataFrame:
    """
    Load raw property listing data from CSV file.
    Returns a pandas DataFrame with an added ingestion timestamp.
    """
    print(f"[EXTRACT] Reading file: {filepath}")
    
    # Check file exists
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"Data file not found at: {filepath}")
    
    # Read CSV
    df = pd.read_csv(filepath)

    # Add metadata column
    df['ingestion_date'] = datetime.now()

    print(f"[EXTRACT] Successfully loaded {len(df)} rows and {len(df.columns)} columns")
    print(f"[EXTRACT] Columns found: {list(df.columns)}")
    
    return df

# This block runs ONLY when you run this file directly (for testing)
if __name__ == "__main__":
    df = extract_from_csv("data/raw_listings.csv")
    print(df.head())
    print(df.dtypes)