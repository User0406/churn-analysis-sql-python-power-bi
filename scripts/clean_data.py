"""
Data Cleaning Module
Handles missing values, standardization, duplicates, and data quality issues
"""
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine
from pathlib import Path
from utils import (setup_logger, get_db_path, get_data_path, 
                   format_duration, print_section_header, validate_dataframe)

# Set up logger
logger = setup_logger('cleaning')


def clean_data(input_table='raw_customer_data'):
    """
    Clean raw customer data and save to processed folder
    
    Steps:
    1. Load data from SQLite
    2. Handle missing values
    3. Standardize column names
    4. Remove duplicates
    5. Clean categorical inconsistencies
    6. Handle outliers
    7. Save cleaned data
    
    Args:
        input_table (str): Name of the source table in database
    
    Returns:
        bool: True if cleaning successful, False otherwise
    """
    start_time = time.time()
    
    try:
        print_section_header("DATA CLEANING STARTED")
        logger.info("Starting data cleaning process")
        
        # Load data from database
        db_path = get_db_path()
        engine = create_engine(f'sqlite:///{db_path}')
        
        logger.info(f"Loading data from table: {input_table}")
        df = pd.read_sql_table(input_table, engine)
        
        if not validate_dataframe(df, logger, "Initial Load"):
            return False
        
        initial_rows = len(df)
        logger.info(f"Initial dataset: {initial_rows} rows, {len(df.columns)} columns")
        
        print(f"\n📊 Initial Dataset:")
        print(f"  • Rows: {initial_rows:,}")
        print(f"  • Columns: {len(df.columns)}")
        
        # ===== STEP 1: Standardize column names to snake_case =====
        logger.info("Step 1: Standardizing column names")
        original_columns = df.columns.tolist()
        
        def to_snake_case(name):
            """Convert string to snake_case"""
            import re
            # Insert underscore before capital letters and convert to lowercase
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        
        df.columns = [to_snake_case(col) for col in df.columns]
        logger.info(f"✓ Column names standardized")
        
        # ===== STEP 2: Handle missing values =====
        logger.info("Step 2: Handling missing values")
        missing_before = df.isnull().sum().sum()
        
        print(f"\n🔍 Missing Values Analysis:")
        if missing_before > 0:
            missing_by_col = df.isnull().sum()
            missing_by_col = missing_by_col[missing_by_col > 0]
            for col, count in missing_by_col.items():
                pct = (count / len(df)) * 100
                print(f"  • {col}: {count} ({pct:.1f}%)")
                logger.info(f"  Missing in {col}: {count} ({pct:.1f}%)")
        else:
            print(f"  • No missing values found")
        
        # Handle TotalCharges - convert to numeric and fill with calculated value
        if 'total_charges' in df.columns:
            # Convert to numeric (handles string with spaces)
            df['total_charges'] = pd.to_numeric(df['total_charges'], errors='coerce')
            
            # Fill missing TotalCharges with calculated value
            missing_total = df['total_charges'].isnull()
            if missing_total.any():
                df.loc[missing_total, 'total_charges'] = (
                    df.loc[missing_total, 'monthly_charges'] * df.loc[missing_total, 'tenure']
                )
                logger.info(f"✓ Filled {missing_total.sum()} missing TotalCharges with calculated values")
        
        missing_after = df.isnull().sum().sum()
        logger.info(f"✓ Missing values: {missing_before} → {missing_after}")
        
        # ===== STEP 3: Clean categorical inconsistencies =====
        logger.info("Step 3: Cleaning categorical values")
        
        # Standardize Gender
        if 'gender' in df.columns:
            gender_map = {'M': 'Male', 'F': 'Female'}
            df['gender'] = df['gender'].replace(gender_map)
            logger.info(f"✓ Gender standardized: {df['gender'].unique()}")
        
        # Standardize Yes/No values
        yes_no_columns = ['partner', 'dependents', 'phone_service', 'paperless_billing', 'churn']
        for col in yes_no_columns:
            if col in df.columns:
                df[col] = df[col].str.strip().str.title()
        
        logger.info("✓ Categorical values standardized")
        
        # ===== STEP 4: Remove duplicates =====
        logger.info("Step 4: Removing duplicates")
        duplicates_before = df.duplicated().sum()
        
        if duplicates_before > 0:
            df = df.drop_duplicates()
            logger.info(f"✓ Removed {duplicates_before} duplicate rows")
            print(f"\n🗑️  Removed {duplicates_before} duplicate rows")
        else:
            logger.info("✓ No duplicates found")
        
        # Remove duplicates based on CustomerID
        if 'customer_id' in df.columns:
            customer_id_dupes = df.duplicated(subset=['customer_id'], keep='first').sum()
            if customer_id_dupes > 0:
                df = df.drop_duplicates(subset=['customer_id'], keep='first')
                logger.info(f"✓ Removed {customer_id_dupes} duplicate CustomerIDs")
                print(f"🗑️  Removed {customer_id_dupes} duplicate CustomerIDs")
        
        # ===== STEP 5: Handle outliers =====
        logger.info("Step 5: Handling outliers")
        
        # Check for outliers in numerical columns
        numerical_cols = ['tenure', 'monthly_charges', 'total_charges']
        outliers_info = {}
        
        for col in numerical_cols:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 3 * IQR  # Using 3*IQR for outliers
                upper_bound = Q3 + 3 * IQR
                
                outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                outliers_info[col] = outliers
                
                if outliers > 0:
                    logger.info(f"  {col}: {outliers} outliers detected (not removed)")
        
        if any(outliers_info.values()):
            print(f"\n📊 Outliers detected (retained for analysis):")
            for col, count in outliers_info.items():
                if count > 0:
                    print(f"  • {col}: {count}")
        
        # ===== STEP 6: Data type conversion =====
        logger.info("Step 6: Converting data types")
        
        # Ensure proper data types
        if 'senior_citizen' in df.columns:
            df['senior_citizen'] = df['senior_citizen'].astype(int)
        
        if 'tenure' in df.columns:
            df['tenure'] = df['tenure'].astype(int)
        
        if 'support_calls' in df.columns:
            df['support_calls'] = df['support_calls'].astype(int)
        
        logger.info("✓ Data types converted")
        
        # ===== STEP 7: Final validation =====
        final_rows = len(df)
        rows_removed = initial_rows - final_rows
        
        logger.info(f"Final dataset: {final_rows} rows, {len(df.columns)} columns")
        logger.info(f"Rows removed: {rows_removed}")
        
        # ===== STEP 8: Save cleaned data =====
        logger.info("Step 7: Saving cleaned data")
        
        processed_path = get_data_path('processed')
        output_file = processed_path / 'cleaned_data.csv'
        
        df.to_csv(output_file, index=False)
        logger.info(f"✓ Cleaned data saved to: {output_file}")
        
        # Also save to database
        df.to_sql('cleaned_customer_data', engine, if_exists='replace', index=False)
        logger.info(f"✓ Cleaned data saved to database table: cleaned_customer_data")
        
        # Calculate processing time
        elapsed_time = time.time() - start_time
        logger.info(f"Cleaning completed in {format_duration(elapsed_time)}")
        
        # Print summary
        print(f"\n✅ Cleaning completed successfully!")
        print(f"\n📊 Summary:")
        print(f"  • Initial rows: {initial_rows:,}")
        print(f"  • Final rows: {final_rows:,}")
        print(f"  • Rows removed: {rows_removed:,} ({rows_removed/initial_rows*100:.1f}%)")
        print(f"  • Missing values: {missing_before} → {missing_after}")
        print(f"  • Duplicates removed: {duplicates_before}")
        print(f"  • Output: {output_file.name}")
        print(f"\n⏱️  Processing time: {format_duration(elapsed_time)}")
        print_section_header("DATA CLEANING COMPLETED")
        
        return True
        
    except Exception as e:
        logger.error(f"Cleaning failed: {str(e)}", exc_info=True)
        print(f"\n❌ Error during cleaning: {str(e)}")
        return False


if __name__ == "__main__":
    success = clean_data()
    
    if success:
        # Display sample of cleaned data
        processed_path = get_data_path('processed')
        df = pd.read_csv(processed_path / 'cleaned_data.csv')
        
        print("\n" + "="*70)
        print("  CLEANED DATA PREVIEW")
        print("="*70)
        print(f"\nFirst 5 rows:")
        print(df.head())
        print(f"\nData types:")
        print(df.dtypes)
        print(f"\nBasic statistics:")
        print(df.describe())
