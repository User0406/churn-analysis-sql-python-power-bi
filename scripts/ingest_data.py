"""
Data Ingestion Module
Loads raw CSV data into SQLite database using SQLAlchemy
"""
import pandas as pd
import time
from sqlalchemy import create_engine, text
from pathlib import Path
from utils import setup_logger, get_db_path, get_data_path, format_duration, print_section_header

# Set up logger
logger = setup_logger('ingestion')


def ingest_to_db(csv_filename='telecom_customer_data.csv', table_name='raw_customer_data'):
    """
    Ingest CSV data from raw folder to SQLite database
    
    Args:
        csv_filename (str): Name of the CSV file in data/raw/
        table_name (str): Name of the table to create in database
    
    Returns:
        bool: True if ingestion successful, False otherwise
    """
    start_time = time.time()
    
    try:
        print_section_header("DATA INGESTION STARTED")
        logger.info(f"Starting data ingestion process for {csv_filename}")
        
        # Get file path
        raw_data_path = get_data_path('raw')
        csv_path = raw_data_path / csv_filename
        
        if not csv_path.exists():
            logger.error(f"CSV file not found: {csv_path}")
            return False
        
        logger.info(f"Reading CSV file: {csv_path}")
        
        # Read CSV file
        df = pd.read_csv(csv_path)
        logger.info(f"✓ CSV loaded successfully")
        logger.info(f"  Records: {len(df)}")
        logger.info(f"  Columns: {len(df.columns)}")
        logger.info(f"  Size: {csv_path.stat().st_size / 1024:.2f} KB")
        
        # Display data info
        print(f"\n📊 Dataset Overview:")
        print(f"  • Records: {len(df):,}")
        print(f"  • Columns: {len(df.columns)}")
        print(f"  • Memory Usage: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
        
        # Create database connection
        db_path = get_db_path()
        engine = create_engine(f'sqlite:///{db_path}')
        logger.info(f"✓ Database connection established: {db_path}")
        
        # Check if table already exists
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"
            ))
            table_exists = result.fetchone() is not None
        
        if table_exists:
            logger.warning(f"Table '{table_name}' already exists. Replacing...")
            print(f"\n⚠️  Table '{table_name}' already exists - replacing with new data")
        
        # Write to database
        logger.info(f"Writing data to table: {table_name}")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        # Verify ingestion
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            record_count = result.fetchone()[0]
        
        if record_count == len(df):
            logger.info(f"✓ Data ingestion verified: {record_count} records in database")
            print(f"\n✅ Ingestion successful!")
            print(f"  • Records ingested: {record_count:,}")
            print(f"  • Table name: {table_name}")
            print(f"  • Database: {Path(db_path).name}")
        else:
            logger.error(f"Verification failed: Expected {len(df)}, found {record_count}")
            return False
        
        # Log column information
        logger.info(f"Column details:")
        for col in df.columns:
            logger.info(f"  - {col}: {df[col].dtype}")
        
        # Calculate and log statistics
        elapsed_time = time.time() - start_time
        logger.info(f"Ingestion completed in {format_duration(elapsed_time)}")
        
        print(f"\n⏱️  Processing time: {format_duration(elapsed_time)}")
        print_section_header("DATA INGESTION COMPLETED")
        
        return True
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        print(f"\n❌ Error: CSV file not found")
        return False
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}", exc_info=True)
        print(f"\n❌ Error during ingestion: {str(e)}")
        return False


def get_table_info(table_name='raw_customer_data'):
    """
    Get information about a table in the database
    
    Args:
        table_name (str): Name of the table
    
    Returns:
        dict: Table information including row count, columns, etc.
    """
    try:
        db_path = get_db_path()
        engine = create_engine(f'sqlite:///{db_path}')
        
        with engine.connect() as conn:
            # Get row count
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.fetchone()[0]
            
            # Get column info
            result = conn.execute(text(f"PRAGMA table_info({table_name})"))
            columns = [(row[1], row[2]) for row in result.fetchall()]
        
        return {
            'table_name': table_name,
            'row_count': row_count,
            'columns': columns,
            'database': db_path
        }
        
    except Exception as e:
        logger.error(f"Failed to get table info: {str(e)}")
        return None


def list_all_tables():
    """List all tables in the database"""
    try:
        db_path = get_db_path()
        engine = create_engine(f'sqlite:///{db_path}')
        
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ))
            tables = [row[0] for row in result.fetchall()]
        
        return tables
        
    except Exception as e:
        logger.error(f"Failed to list tables: {str(e)}")
        return []


if __name__ == "__main__":
    # Run ingestion
    success = ingest_to_db()
    
    if success:
        print("\n" + "="*70)
        print("  DATABASE SUMMARY")
        print("="*70)
        
        # List all tables
        tables = list_all_tables()
        print(f"\n📋 Tables in database: {len(tables)}")
        for table in tables:
            print(f"  • {table}")
        
        # Get table info
        info = get_table_info('raw_customer_data')
        if info:
            print(f"\n📊 Table: {info['table_name']}")
            print(f"  • Row count: {info['row_count']:,}")
            print(f"  • Columns: {len(info['columns'])}")
            print(f"\n  Column details:")
            for col_name, col_type in info['columns'][:10]:  # Show first 10
                print(f"    - {col_name}: {col_type}")
            if len(info['columns']) > 10:
                print(f"    ... and {len(info['columns']) - 10} more")
