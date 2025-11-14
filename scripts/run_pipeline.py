"""
Master Pipeline Orchestration Script
Automates the entire Customer Retention Analytics ETL process
"""
import time
import os
import sys
from datetime import datetime
from pathlib import Path
from utils import setup_logger, get_data_path, format_duration, print_section_header

# Import pipeline modules
from ingest_data import ingest_to_db
from clean_data import clean_data
from feature_engineering import create_features
from data_quality_audit import data_quality_report

# Set up logger
logger = setup_logger('pipeline')


def run_full_pipeline():
    """
    Execute the complete ETL pipeline
    
    Pipeline stages:
    1. Data Ingestion (CSV → SQLite)
    2. Data Cleaning (handle missing values, duplicates, etc.)
    3. Feature Engineering (create KPIs and derived features)
    4. Data Quality Audit (generate comprehensive report)
    
    Returns:
        bool: True if pipeline completes successfully, False otherwise
    """
    pipeline_start = time.time()
    
    print("\n" + "="*70)
    print("  CUSTOMER RETENTION ANALYTICS PIPELINE")
    print("  Automated ETL Process")
    print("="*70)
    print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    logger.info("="*50)
    logger.info("PIPELINE EXECUTION STARTED")
    logger.info("="*50)
    
    # Track stage status
    stages_status = {
        'Ingestion': False,
        'Cleaning': False,
        'Feature Engineering': False,
        'Quality Audit': False
    }
    
    try:
        # ===== STAGE 1: DATA INGESTION =====
        logger.info("\n>>> STAGE 1: DATA INGESTION")
        print("\n" + "▶"*35)
        print("STAGE 1/4: DATA INGESTION")
        print("▶"*35)
        
        stage_start = time.time()
        success = ingest_to_db()
        stage_duration = time.time() - stage_start
        
        if not success:
            logger.error("Stage 1 (Ingestion) failed")
            print("\n❌ Pipeline stopped at Stage 1: Data Ingestion")
            return False
        
        stages_status['Ingestion'] = True
        logger.info(f"✓ Stage 1 completed in {format_duration(stage_duration)}")
        print(f"\n✅ Stage 1 completed ({format_duration(stage_duration)})")
        
        # ===== STAGE 2: DATA CLEANING =====
        logger.info("\n>>> STAGE 2: DATA CLEANING")
        print("\n" + "▶"*35)
        print("STAGE 2/4: DATA CLEANING")
        print("▶"*35)
        
        stage_start = time.time()
        success = clean_data()
        stage_duration = time.time() - stage_start
        
        if not success:
            logger.error("Stage 2 (Cleaning) failed")
            print("\n❌ Pipeline stopped at Stage 2: Data Cleaning")
            return False
        
        stages_status['Cleaning'] = True
        logger.info(f"✓ Stage 2 completed in {format_duration(stage_duration)}")
        print(f"\n✅ Stage 2 completed ({format_duration(stage_duration)})")
        
        # ===== STAGE 3: FEATURE ENGINEERING =====
        logger.info("\n>>> STAGE 3: FEATURE ENGINEERING")
        print("\n" + "▶"*35)
        print("STAGE 3/4: FEATURE ENGINEERING")
        print("▶"*35)
        
        stage_start = time.time()
        success = create_features()
        stage_duration = time.time() - stage_start
        
        if not success:
            logger.error("Stage 3 (Feature Engineering) failed")
            print("\n❌ Pipeline stopped at Stage 3: Feature Engineering")
            return False
        
        stages_status['Feature Engineering'] = True
        logger.info(f"✓ Stage 3 completed in {format_duration(stage_duration)}")
        print(f"\n✅ Stage 3 completed ({format_duration(stage_duration)})")
        
        # ===== STAGE 4: DATA QUALITY AUDIT =====
        logger.info("\n>>> STAGE 4: DATA QUALITY AUDIT")
        print("\n" + "▶"*35)
        print("STAGE 4/4: DATA QUALITY AUDIT")
        print("▶"*35)
        
        stage_start = time.time()
        success = data_quality_report()
        stage_duration = time.time() - stage_start
        
        if not success:
            logger.error("Stage 4 (Quality Audit) failed")
            print("\n❌ Pipeline stopped at Stage 4: Quality Audit")
            return False
        
        stages_status['Quality Audit'] = True
        logger.info(f"✓ Stage 4 completed in {format_duration(stage_duration)}")
        print(f"\n✅ Stage 4 completed ({format_duration(stage_duration)})")
        
        # ===== PIPELINE COMPLETION =====
        pipeline_duration = time.time() - pipeline_start
        
        logger.info("="*50)
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        logger.info(f"Total duration: {format_duration(pipeline_duration)}")
        logger.info("="*50)
        
        # Print final summary
        print("\n" + "="*70)
        print("  PIPELINE EXECUTION SUMMARY")
        print("="*70)
        print(f"\n✅ All stages completed successfully!\n")
        
        print("Stage Status:")
        for stage, status in stages_status.items():
            status_icon = "✓" if status else "✗"
            print(f"  {status_icon} {stage}")
        
        print(f"\n⏱️  Total Pipeline Duration: {format_duration(pipeline_duration)}")
        print(f"📅 Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n📂 Output Files:")
        processed_path = get_data_path('processed')
        print(f"  • Cleaned Data: {processed_path}/cleaned_data.csv")
        print(f"  • Final Data: {processed_path}/final_data.csv")
        
        reports_path = Path(__file__).parent.parent / 'reports'
        print(f"  • Audit Report: {reports_path}/insights_summary.md")
        
        db_path = Path(__file__).parent.parent / 'database' / 'retention.db'
        print(f"  • Database: {db_path}")
        
        print("\n" + "="*70)
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        
        # Show which stages completed
        print("\nStage Status:")
        for stage, status in stages_status.items():
            status_icon = "✓" if status else "✗"
            print(f"  {status_icon} {stage}")
        
        return False


def watch_for_new_data(check_interval=600):
    """
    Watch for new data files and run pipeline automatically
    
    Args:
        check_interval (int): Time in seconds between checks (default: 600 = 10 minutes)
    """
    logger.info(f"Starting data watch service (checking every {check_interval}s)")
    print(f"\n🔍 Watching for new data files...")
    print(f"   Check interval: {check_interval} seconds ({check_interval/60:.0f} minutes)")
    print(f"   Press Ctrl+C to stop\n")
    
    raw_data_path = get_data_path('raw')
    
    try:
        while True:
            # Check for CSV files in raw folder
            csv_files = list(raw_data_path.glob('*.csv'))
            
            if csv_files:
                logger.info(f"New data detected: {len(csv_files)} CSV file(s)")
                print(f"\n📥 New data detected: {csv_files[0].name}")
                print(f"   Starting pipeline...\n")
                
                # Run pipeline
                success = run_full_pipeline()
                
                if success:
                    logger.info("Pipeline completed successfully - waiting for new data")
                    print(f"\n✅ Pipeline completed - waiting for new data...")
                else:
                    logger.error("Pipeline failed - will retry on next check")
                    print(f"\n❌ Pipeline failed - will retry on next check...")
            else:
                logger.info("No new data - waiting...")
                print(f"   No new data - waiting... (checked at {datetime.now().strftime('%H:%M:%S')})")
            
            # Wait before next check
            time.sleep(check_interval)
            
    except KeyboardInterrupt:
        logger.info("Watch service stopped by user")
        print("\n\n🛑 Watch service stopped")
        sys.exit(0)


def main():
    """Main entry point for pipeline execution"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Customer Retention Analytics ETL Pipeline'
    )
    parser.add_argument(
        '--mode',
        choices=['run', 'watch'],
        default='run',
        help='Execution mode: "run" for single execution, "watch" for continuous monitoring'
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=600,
        help='Check interval in seconds for watch mode (default: 600)'
    )
    
    args = parser.parse_args()
    
    if args.mode == 'run':
        # Single pipeline execution
        success = run_full_pipeline()
        sys.exit(0 if success else 1)
    else:
        # Continuous monitoring mode
        watch_for_new_data(check_interval=args.interval)


if __name__ == "__main__":
    # For direct execution, run pipeline once
    if len(sys.argv) == 1:
        success = run_full_pipeline()
        sys.exit(0 if success else 1)
    else:
        main()
