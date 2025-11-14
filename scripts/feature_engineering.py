"""
Feature Engineering Module
Creates business-driven KPIs and derived features for analytics
"""
import pandas as pd
import numpy as np
import time
from pathlib import Path
from utils import (setup_logger, get_data_path, format_duration, 
                   print_section_header, validate_dataframe)

# Set up logger
logger = setup_logger('feature_engineering')


def create_features(input_file='cleaned_data.csv'):
    """
    Create business KPIs and derived features
    
    Features created:
    1. tenure_group - Customer tenure categorized
    2. avg_monthly_spend - Monthly charges category
    3. payment_issue_flag - Indicator for payment method risk
    4. retention_score - Composite score for retention likelihood
    5. service_usage_score - Number of services used
    6. customer_value_segment - Customer value classification
    
    Args:
        input_file (str): Name of cleaned CSV file in processed folder
    
    Returns:
        bool: True if feature engineering successful, False otherwise
    """
    start_time = time.time()
    
    try:
        print_section_header("FEATURE ENGINEERING STARTED")
        logger.info("Starting feature engineering process")
        
        # Load cleaned data
        processed_path = get_data_path('processed')
        input_path = processed_path / input_file
        
        logger.info(f"Loading cleaned data from: {input_path}")
        df = pd.read_csv(input_path)
        
        if not validate_dataframe(df, logger, "Input Data"):
            return False
        
        logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")
        print(f"\n📊 Input Dataset: {len(df):,} rows")
        
        initial_columns = len(df.columns)
        
        # ===== FEATURE 1: Tenure Group =====
        logger.info("Creating feature: tenure_group")
        
        def categorize_tenure(tenure):
            """Categorize tenure into groups"""
            if tenure <= 12:
                return '0-12 months'
            elif tenure <= 24:
                return '12-24 months'
            elif tenure <= 36:
                return '24-36 months'
            elif tenure <= 48:
                return '36-48 months'
            else:
                return '48+ months'
        
        df['tenure_group'] = df['tenure'].apply(categorize_tenure)
        logger.info(f"✓ tenure_group created: {df['tenure_group'].nunique()} categories")
        print(f"\n✓ tenure_group:")
        print(df['tenure_group'].value_counts().sort_index())
        
        # ===== FEATURE 2: Average Monthly Spend Category =====
        logger.info("Creating feature: avg_monthly_spend")
        
        def categorize_spend(amount):
            """Categorize monthly spend"""
            if amount < 30:
                return 'Low (<$30)'
            elif amount < 70:
                return 'Medium ($30-$70)'
            elif amount < 100:
                return 'High ($70-$100)'
            else:
                return 'Premium ($100+)'
        
        df['avg_monthly_spend'] = df['monthly_charges'].apply(categorize_spend)
        logger.info(f"✓ avg_monthly_spend created: {df['avg_monthly_spend'].nunique()} categories")
        print(f"\n✓ avg_monthly_spend:")
        print(df['avg_monthly_spend'].value_counts())
        
        # ===== FEATURE 3: Payment Issue Flag =====
        logger.info("Creating feature: payment_issue_flag")
        
        # Electronic check has higher churn risk
        df['payment_issue_flag'] = (df['payment_method'] == 'Electronic check').astype(int)
        issue_count = df['payment_issue_flag'].sum()
        logger.info(f"✓ payment_issue_flag created: {issue_count} customers flagged")
        print(f"\n✓ payment_issue_flag: {issue_count} customers at risk")
        
        # ===== FEATURE 4: Service Usage Score =====
        logger.info("Creating feature: service_usage_score")
        
        # Count number of services customer is using
        service_columns = [
            'phone_service', 'multiple_lines', 'online_security', 
            'online_backup', 'device_protection', 'tech_support',
            'streaming_tv', 'streaming_movies'
        ]
        
        df['service_usage_score'] = 0
        for col in service_columns:
            if col in df.columns:
                df['service_usage_score'] += (df[col] == 'Yes').astype(int)
        
        logger.info(f"✓ service_usage_score created: range {df['service_usage_score'].min()}-{df['service_usage_score'].max()}")
        print(f"\n✓ service_usage_score: {df['service_usage_score'].min()}-{df['service_usage_score'].max()} services")
        
        # ===== FEATURE 5: Contract Value =====
        logger.info("Creating feature: contract_value_score")
        
        # Map contract types to scores
        contract_scores = {
            'Month-to-month': 1,
            'One year': 2,
            'Two year': 3
        }
        df['contract_value_score'] = df['contract'].map(contract_scores)
        logger.info(f"✓ contract_value_score created")
        
        # ===== FEATURE 6: Retention Score (Composite) =====
        logger.info("Creating feature: retention_score")
        
        # Normalize components to 0-100 scale
        # Component 1: Tenure (higher is better)
        tenure_normalized = (df['tenure'] / df['tenure'].max()) * 100
        
        # Component 2: Contract strength (higher is better)
        contract_normalized = ((df['contract_value_score'] - 1) / 2) * 100
        
        # Component 3: Service usage (higher is better)
        service_normalized = (df['service_usage_score'] / df['service_usage_score'].max()) * 100
        
        # Component 4: Payment method (lower risk is better)
        payment_normalized = (1 - df['payment_issue_flag']) * 100
        
        # Component 5: Support calls (fewer is better)
        support_normalized = (1 - (df['support_calls'] / df['support_calls'].max())) * 100
        
        # Weighted composite score
        df['retention_score'] = (
            tenure_normalized * 0.30 +          # 30% weight
            contract_normalized * 0.25 +        # 25% weight
            service_normalized * 0.20 +         # 20% weight
            payment_normalized * 0.15 +         # 15% weight
            support_normalized * 0.10           # 10% weight
        ).round(2)
        
        logger.info(f"✓ retention_score created: range {df['retention_score'].min():.1f}-{df['retention_score'].max():.1f}")
        print(f"\n✓ retention_score: {df['retention_score'].min():.1f} to {df['retention_score'].max():.1f}")
        
        # ===== FEATURE 7: Customer Value Segment =====
        logger.info("Creating feature: customer_value_segment")
        
        def segment_customer(row):
            """Segment customers based on value and retention"""
            if row['retention_score'] >= 70 and row['monthly_charges'] >= 70:
                return 'High Value - High Retention'
            elif row['retention_score'] >= 70 and row['monthly_charges'] < 70:
                return 'Low Value - High Retention'
            elif row['retention_score'] < 70 and row['monthly_charges'] >= 70:
                return 'High Value - At Risk'
            else:
                return 'Low Value - At Risk'
        
        df['customer_value_segment'] = df.apply(segment_customer, axis=1)
        logger.info(f"✓ customer_value_segment created: {df['customer_value_segment'].nunique()} segments")
        print(f"\n✓ customer_value_segment:")
        print(df['customer_value_segment'].value_counts())
        
        # ===== FEATURE 8: Internet Service Value =====
        logger.info("Creating feature: internet_service_value")
        
        internet_value = {
            'No': 0,
            'DSL': 1,
            'Fiber optic': 2
        }
        df['internet_service_value'] = df['internet_service'].map(internet_value)
        logger.info(f"✓ internet_service_value created")
        
        # ===== FEATURE 9: Total Services Count =====
        logger.info("Creating feature: total_services_count")
        
        # Count all active services
        df['total_services_count'] = df['service_usage_score'] + (df['phone_service'] == 'Yes').astype(int)
        logger.info(f"✓ total_services_count created")
        
        # ===== FEATURE 10: Churn Risk Flag =====
        logger.info("Creating feature: churn_risk_flag")
        
        # Flag customers at high risk based on multiple factors
        df['churn_risk_flag'] = (
            (df['retention_score'] < 40) |
            ((df['tenure'] < 6) & (df['contract'] == 'Month-to-month')) |
            (df['support_calls'] > 8)
        ).astype(int)
        
        risk_count = df['churn_risk_flag'].sum()
        logger.info(f"✓ churn_risk_flag created: {risk_count} high-risk customers")
        print(f"\n✓ churn_risk_flag: {risk_count} customers flagged as high risk")
        
        # ===== Summary of new features =====
        final_columns = len(df.columns)
        new_features = final_columns - initial_columns
        
        logger.info(f"Feature engineering completed: {new_features} new features created")
        
        # ===== Save enhanced dataset =====
        logger.info("Saving final dataset")
        
        output_file = processed_path / 'final_data.csv'
        df.to_csv(output_file, index=False)
        logger.info(f"✓ Final dataset saved to: {output_file}")
        
        # Calculate processing time
        elapsed_time = time.time() - start_time
        logger.info(f"Feature engineering completed in {format_duration(elapsed_time)}")
        
        # Print summary
        print(f"\n✅ Feature engineering completed successfully!")
        print(f"\n📊 Summary:")
        print(f"  • Initial columns: {initial_columns}")
        print(f"  • New features: {new_features}")
        print(f"  • Final columns: {final_columns}")
        print(f"  • Output: {output_file.name}")
        print(f"\n⏱️  Processing time: {format_duration(elapsed_time)}")
        
        # Show new features
        print(f"\n📋 New Features Created:")
        new_cols = [
            'tenure_group', 'avg_monthly_spend', 'payment_issue_flag',
            'service_usage_score', 'retention_score', 'customer_value_segment',
            'internet_service_value', 'total_services_count', 'churn_risk_flag'
        ]
        for col in new_cols:
            if col in df.columns:
                print(f"  ✓ {col}")
        
        print_section_header("FEATURE ENGINEERING COMPLETED")
        
        return True
        
    except Exception as e:
        logger.error(f"Feature engineering failed: {str(e)}", exc_info=True)
        print(f"\n❌ Error during feature engineering: {str(e)}")
        return False


if __name__ == "__main__":
    success = create_features()
    
    if success:
        # Display sample of final data
        processed_path = get_data_path('processed')
        df = pd.read_csv(processed_path / 'final_data.csv')
        
        print("\n" + "="*70)
        print("  FINAL DATA PREVIEW")
        print("="*70)
        print(f"\nShape: {df.shape}")
        print(f"\nFirst 3 rows (selected columns):")
        preview_cols = ['customer_id', 'tenure', 'tenure_group', 'monthly_charges', 
                       'avg_monthly_spend', 'retention_score', 'customer_value_segment', 'churn']
        print(df[preview_cols].head(3))
        
        print(f"\n\nRetention Score Distribution:")
        print(df['retention_score'].describe())
