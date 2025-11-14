"""
Utility functions for Customer Retention Analytics Pipeline
Provides logging, configuration, and helper functions
"""
import logging
import os
from datetime import datetime
from pathlib import Path

def setup_logger(name, log_dir='../data/logs'):
    """
    Set up a logger with file and console handlers
    
    Args:
        name (str): Name of the logger (e.g., 'ingestion', 'cleaning', 'pipeline')
        log_dir (str): Directory to store log files
    
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_path = Path(__file__).parent / log_dir
    log_path.mkdir(parents=True, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    if logger.handlers:
        logger.handlers.clear()
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    simple_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    
    # File handler - detailed logs
    timestamp = datetime.now().strftime('%Y%m%d')
    file_handler = logging.FileHandler(
        log_path / f'{name}_{timestamp}.log',
        mode='a'
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler - simple logs
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def get_db_path():
    """Get the path to the SQLite database"""
    db_path = Path(__file__).parent.parent / 'database' / 'retention.db'
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return str(db_path)


def get_data_path(data_type='raw'):
    """
    Get path to data directories
    
    Args:
        data_type (str): Type of data ('raw', 'processed', 'logs')
    
    Returns:
        Path: Path object to the data directory
    """
    data_path = Path(__file__).parent.parent / 'data' / data_type
    data_path.mkdir(parents=True, exist_ok=True)
    return data_path


def format_duration(seconds):
    """Format duration in seconds to human readable format"""
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.2f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.2f} hours"


def print_section_header(title):
    """Print a formatted section header"""
    width = 70
    print("\n" + "=" * width)
    print(f"  {title}")
    print("=" * width + "\n")


def print_metrics(metrics_dict):
    """
    Pretty print metrics dictionary
    
    Args:
        metrics_dict (dict): Dictionary of metric name to value
    """
    max_key_length = max(len(str(k)) for k in metrics_dict.keys())
    
    for key, value in metrics_dict.items():
        if isinstance(value, float):
            print(f"  {key:<{max_key_length}} : {value:.2f}")
        else:
            print(f"  {key:<{max_key_length}} : {value}")
    print()


def validate_dataframe(df, logger, stage_name):
    """
    Validate dataframe and log basic info
    
    Args:
        df: pandas DataFrame
        logger: Logger instance
        stage_name (str): Name of the processing stage
    
    Returns:
        bool: True if validation passes, False otherwise
    """
    try:
        if df is None or df.empty:
            logger.error(f"{stage_name}: DataFrame is empty or None")
            return False
        
        logger.info(f"{stage_name}: DataFrame shape = {df.shape}")
        logger.info(f"{stage_name}: Columns = {list(df.columns)}")
        
        return True
        
    except Exception as e:
        logger.error(f"{stage_name}: Validation error - {str(e)}")
        return False


def calculate_data_quality_score(df):
    """
    Calculate overall data quality score (0-100)
    
    Args:
        df: pandas DataFrame
    
    Returns:
        float: Quality score between 0 and 100
    """
    scores = []
    
    # Completeness score (percentage of non-null values)
    completeness = (1 - df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
    scores.append(completeness)
    
    # Uniqueness score (percentage of unique values in ID column if exists)
    if 'CustomerID' in df.columns:
        uniqueness = (df['CustomerID'].nunique() / len(df)) * 100
        scores.append(uniqueness)
    
    # Consistency score (no duplicate rows)
    consistency = ((len(df) - df.duplicated().sum()) / len(df)) * 100
    scores.append(consistency)
    
    # Overall score
    quality_score = sum(scores) / len(scores)
    
    return round(quality_score, 2)


if __name__ == "__main__":
    # Test the logger
    logger = setup_logger('test')
    logger.info("Testing logger setup")
    logger.warning("This is a warning")
    logger.error("This is an error")
    
    print(f"\nDatabase path: {get_db_path()}")
    print(f"Raw data path: {get_data_path('raw')}")
    print(f"Processed data path: {get_data_path('processed')}")
    
    print_section_header("Test Section Header")
    
    test_metrics = {
        'Total Records': 7500,
        'Churn Rate': 26.5,
        'Avg Monthly Charges': 64.76,
        'Processing Time': 12.5
    }
    print_metrics(test_metrics)
    
    print(f"Duration format test: {format_duration(125.5)}")
