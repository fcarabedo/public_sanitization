#!/usr/bin/env python3
"""
Script to fill empty values in ready_to_merge.csv by looking up records 
in the nitin folder CSV files using uid as the lookup key.

The script will:
1. Read ready_to_merge.csv as the target file
2. Read all CSV files in the nitin folder as source files
3. For each record in ready_to_merge.csv, look up the corresponding record in nitin files using uid
4. Fill any empty values in ready_to_merge.csv with values from nitin files
5. Export the result as ready_to_deliver.csv
"""

import pandas as pd
import os
import glob
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_nitin_data(nitin_folder_path):
    """
    Load all CSV files from the nitin folder and create a lookup dictionary
    using uid as the key.
    """
    logger.info("Loading data from nitin folder...")
    
    # Get all CSV files in the nitin folder
    csv_files = glob.glob(os.path.join(nitin_folder_path, "*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files in nitin folder")
    
    # Dictionary to store uid -> record mapping
    uid_lookup = {}
    
    for csv_file in csv_files:
        try:
            logger.info(f"Processing {os.path.basename(csv_file)}...")
            
            # Read CSV file in chunks to handle large files
            chunk_size = 10000
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size, low_memory=False):
                # Process each chunk
                for idx, row in chunk.iterrows():
                    uid = row.get('uid')
                    if pd.notna(uid):  # Only process rows with valid uid
                        # Convert row to dictionary and store in lookup
                        uid_lookup[uid] = row.to_dict()
            
            logger.info(f"Completed processing {os.path.basename(csv_file)}")
            
        except Exception as e:
            logger.error(f"Error processing {csv_file}: {str(e)}")
            continue
    
    logger.info(f"Total records loaded: {len(uid_lookup)}")
    return uid_lookup

def fill_empty_values(target_df, uid_lookup):
    """
    Fill empty values in target DataFrame using the uid lookup dictionary.
    """
    logger.info("Starting to fill empty values...")
    
    filled_count = 0
    total_records = len(target_df)
    
    for idx, row in target_df.iterrows():
        if idx % 10000 == 0:
            logger.info(f"Processing record {idx}/{total_records} ({idx/total_records*100:.1f}%)")
        
        uid = row.get('uid')
        if pd.notna(uid) and uid in uid_lookup:
            source_record = uid_lookup[uid]
            row_modified = False
            
            # Check each column and fill if empty
            for column in target_df.columns:
                # Check if target value is empty (NaN, None, or empty string)
                target_value = row[column]
                if pd.isna(target_value) or target_value == '' or target_value is None:
                    # Check if source has a non-empty value for this column
                    source_value = source_record.get(column)
                    if pd.notna(source_value) and source_value != '' and source_value is not None:
                        target_df.at[idx, column] = source_value
                        row_modified = True
            
            if row_modified:
                filled_count += 1
    
    logger.info(f"Filled empty values in {filled_count} records")
    return target_df

def main():
    """
    Main function to execute the data merging process.
    """
    # Define file paths
    script_dir = Path(__file__).parent
    ready_to_merge_path = script_dir / "delivery/evals_internal_860_2.csv"
    nitin_folder_path = script_dir / "nitin"
    output_path = script_dir / "ready_to_deliver.csv"
    
    # Check if files exist
    if not ready_to_merge_path.exists():
        logger.error(f"Target file not found: {ready_to_merge_path}")
        return
    
    if not nitin_folder_path.exists():
        logger.error(f"Nitin folder not found: {nitin_folder_path}")
        return
    
    try:
        # Load the nitin data into a lookup dictionary
        uid_lookup = load_nitin_data(nitin_folder_path)
        
        if not uid_lookup:
            logger.error("No data loaded from nitin folder. Exiting.")
            return
        
        # Load the target file
        logger.info("Loading ready_to_merge.csv...")
        target_df = pd.read_csv(ready_to_merge_path, low_memory=False)
        logger.info(f"Loaded {len(target_df)} records from ready_to_merge.csv")
        
        # Fill empty values
        filled_df = fill_empty_values(target_df, uid_lookup)
        
        # Save the result
        logger.info(f"Saving results to {output_path}...")
        filled_df.to_csv(output_path, index=False)
        logger.info("Process completed successfully!")
        
        # Print summary statistics
        logger.info("\n=== SUMMARY ===")
        logger.info(f"Input records: {len(target_df)}")
        logger.info(f"Source records in lookup: {len(uid_lookup)}")
        logger.info(f"Output file: {output_path}")
        logger.info("Process completed!")
        
    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")
        raise

if __name__ == "__main__":
    main()
