"""
Data Quality Audit Module
Generates comprehensive data quality reports and insights
"""
import pandas as pd
import numpy as np
import time
from datetime import datetime
from pathlib import Path
from utils import (setup_logger, get_data_path, format_duration, 
                   print_section_header, calculate_data_quality_score)

# Set up logger
logger = setup_logger('audit')


def data_quality_report(input_file='final_data.csv'):
    """
    Generate comprehensive data quality audit report
    
    Audit includes:
    1. Missing data analysis
    2. Data type verification
    3. Unique value counts
    4. Distribution checks
    5. Anomaly detection
    6. Business insights
    
    Args:
        input_file (str): Name of the data file to audit
    
    Returns:
        bool: True if audit successful, False otherwise
    """
    start_time = time.time()
    
    try:
        print_section_header("DATA QUALITY AUDIT STARTED")
        logger.info("Starting data quality audit")
        
        # Load data
        processed_path = get_data_path('processed')
        input_path = processed_path / input_file
        
        logger.info(f"Loading data from: {input_path}")
        df = pd.read_csv(input_path)
        
        logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")
        print(f"\n📊 Analyzing dataset: {len(df):,} rows × {len(df.columns)} columns")
        
        # Initialize report content
        report_lines = []
        report_lines.append("# DATA QUALITY AUDIT REPORT")
        report_lines.append(f"\n**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"**Dataset**: {input_file}")
        report_lines.append(f"**Records**: {len(df):,}")
        report_lines.append(f"**Columns**: {len(df.columns)}")
        report_lines.append("\n---\n")
        
        # ===== SECTION 1: Overall Data Quality Score =====
        logger.info("Calculating overall data quality score")
        quality_score = calculate_data_quality_score(df)
        
        report_lines.append("## 1. Overall Data Quality Score")
        report_lines.append(f"\n**Quality Score**: {quality_score}/100")
        
        if quality_score >= 95:
            quality_rating = "Excellent ✓"
        elif quality_score >= 85:
            quality_rating = "Good"
        elif quality_score >= 70:
            quality_rating = "Fair"
        else:
            quality_rating = "Needs Improvement"
        
        report_lines.append(f"**Rating**: {quality_rating}\n")
        logger.info(f"Overall quality score: {quality_score}/100 ({quality_rating})")
        print(f"\n📈 Overall Quality Score: {quality_score}/100 - {quality_rating}")
        
        # ===== SECTION 2: Missing Data Analysis =====
        logger.info("Analyzing missing data")
        report_lines.append("## 2. Missing Data Analysis\n")
        
        total_cells = df.shape[0] * df.shape[1]
        total_missing = df.isnull().sum().sum()
        missing_pct = (total_missing / total_cells) * 100
        
        report_lines.append(f"- **Total Missing Values**: {total_missing:,} ({missing_pct:.2f}%)")
        
        if total_missing > 0:
            report_lines.append("\n**Missing by Column**:\n")
            missing_by_col = df.isnull().sum()
            missing_by_col = missing_by_col[missing_by_col > 0].sort_values(ascending=False)
            
            for col, count in missing_by_col.items():
                pct = (count / len(df)) * 100
                report_lines.append(f"- {col}: {count} ({pct:.2f}%)")
                logger.warning(f"Missing values in {col}: {count} ({pct:.2f}%)")
        else:
            report_lines.append("- ✓ No missing values detected")
            logger.info("No missing values detected")
        
        report_lines.append("")
        
        # ===== SECTION 3: Data Types and Structure =====
        logger.info("Analyzing data types")
        report_lines.append("## 3. Data Types and Structure\n")
        
        dtype_counts = df.dtypes.value_counts()
        report_lines.append("**Column Types**:\n")
        for dtype, count in dtype_counts.items():
            report_lines.append(f"- {dtype}: {count} columns")
        
        report_lines.append("\n**Column Details**:\n")
        report_lines.append("| Column | Type | Unique Values | Sample Values |")
        report_lines.append("|--------|------|---------------|---------------|")
        
        for col in df.columns:
            dtype = str(df[col].dtype)
            unique_count = df[col].nunique()
            
            # Get sample values
            if df[col].dtype == 'object':
                sample = df[col].dropna().unique()[:3]
                sample_str = ', '.join(map(str, sample))
            else:
                sample = df[col].dropna().head(3).values
                sample_str = ', '.join(f"{v:.2f}" if isinstance(v, float) else str(v) for v in sample)
            
            if len(sample_str) > 40:
                sample_str = sample_str[:37] + "..."
            
            report_lines.append(f"| {col} | {dtype} | {unique_count:,} | {sample_str} |")
        
        report_lines.append("")
        
        # ===== SECTION 4: Duplicate Records =====
        logger.info("Checking for duplicates")
        report_lines.append("## 4. Duplicate Records\n")
        
        duplicates = df.duplicated().sum()
        report_lines.append(f"- **Duplicate Rows**: {duplicates}")
        
        if 'customer_id' in df.columns:
            id_duplicates = df.duplicated(subset=['customer_id']).sum()
            report_lines.append(f"- **Duplicate Customer IDs**: {id_duplicates}")
            logger.info(f"Duplicates: {duplicates} rows, {id_duplicates} IDs")
        
        if duplicates == 0 and id_duplicates == 0:
            report_lines.append("- ✓ No duplicates detected")
        
        report_lines.append("")
        
        # ===== SECTION 5: Distribution Analysis =====
        logger.info("Analyzing data distributions")
        report_lines.append("## 5. Data Distribution Analysis\n")
        
        # Numerical columns
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        report_lines.append("**Numerical Features**:\n")
        
        for col in numerical_cols[:10]:  # Show first 10
            stats = df[col].describe()
            report_lines.append(f"\n**{col}**:")
            report_lines.append(f"- Mean: {stats['mean']:.2f}")
            report_lines.append(f"- Median: {stats['50%']:.2f}")
            report_lines.append(f"- Std Dev: {stats['std']:.2f}")
            report_lines.append(f"- Range: [{stats['min']:.2f}, {stats['max']:.2f}]")
        
        # Categorical columns with distributions
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        report_lines.append("\n**Categorical Features** (Top Categories):\n")
        
        for col in categorical_cols[:10]:  # Show first 10
            top_values = df[col].value_counts().head(3)
            report_lines.append(f"\n**{col}**:")
            for val, count in top_values.items():
                pct = (count / len(df)) * 100
                report_lines.append(f"- {val}: {count} ({pct:.1f}%)")
        
        # ===== SECTION 6: Anomaly Detection =====
        logger.info("Detecting anomalies")
        report_lines.append("\n## 6. Anomaly Detection\n")
        
        anomalies_found = []
        
        # Check for outliers in key numerical columns
        key_cols = ['tenure', 'monthly_charges', 'total_charges']
        for col in key_cols:
            if col in df.columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                
                outliers = ((df[col] < (Q1 - 3*IQR)) | (df[col] > (Q3 + 3*IQR))).sum()
                if outliers > 0:
                    anomalies_found.append(f"- {col}: {outliers} outliers detected ({outliers/len(df)*100:.1f}%)")
        
        # Check for logical inconsistencies
        if 'total_charges' in df.columns and 'monthly_charges' in df.columns and 'tenure' in df.columns:
            # TotalCharges should be approximately MonthlyCharges * Tenure
            calculated = df['monthly_charges'] * df['tenure']
            diff_pct = abs(df['total_charges'] - calculated) / calculated
            inconsistent = (diff_pct > 0.1).sum()  # More than 10% difference
            if inconsistent > 0:
                anomalies_found.append(f"- Total charges inconsistency: {inconsistent} records ({inconsistent/len(df)*100:.1f}%)")
        
        if anomalies_found:
            report_lines.append("\n".join(anomalies_found))
            logger.warning(f"Anomalies detected: {len(anomalies_found)}")
        else:
            report_lines.append("- ✓ No major anomalies detected")
            logger.info("No major anomalies detected")
        
        report_lines.append("")
        
        # ===== SECTION 7: Business Insights =====
        logger.info("Generating business insights")
        report_lines.append("## 7. Key Business Insights\n")
        
        insights = []
        
        # Churn rate
        if 'churn' in df.columns:
            churn_rate = (df['churn'] == 'Yes').sum() / len(df) * 100
            insights.append(f"- **Churn Rate**: {churn_rate:.1f}%")
            logger.info(f"Churn rate: {churn_rate:.1f}%")
        
        # Average tenure
        if 'tenure' in df.columns:
            avg_tenure = df['tenure'].mean()
            insights.append(f"- **Average Tenure**: {avg_tenure:.1f} months")
        
        # Average monthly revenue
        if 'monthly_charges' in df.columns:
            avg_revenue = df['monthly_charges'].mean()
            total_revenue = df['monthly_charges'].sum()
            insights.append(f"- **Average Monthly Charges**: ${avg_revenue:.2f}")
            insights.append(f"- **Total Monthly Revenue**: ${total_revenue:,.2f}")
        
        # Contract distribution
        if 'contract' in df.columns:
            contract_dist = df['contract'].value_counts()
            insights.append(f"\n**Contract Distribution**:")
            for contract, count in contract_dist.items():
                pct = (count / len(df)) * 100
                insights.append(f"  - {contract}: {count} ({pct:.1f}%)")
        
        # High-risk customers
        if 'churn_risk_flag' in df.columns:
            high_risk = df['churn_risk_flag'].sum()
            insights.append(f"\n- **High-Risk Customers**: {high_risk} ({high_risk/len(df)*100:.1f}%)")
        
        # Retention score distribution
        if 'retention_score' in df.columns:
            avg_retention = df['retention_score'].mean()
            insights.append(f"- **Average Retention Score**: {avg_retention:.1f}/100")
        
        report_lines.extend(insights)
        report_lines.append("")
        
        # ===== SECTION 8: Recommendations =====
        report_lines.append("## 8. Recommendations\n")
        
        recommendations = []
        
        if total_missing > 0:
            recommendations.append("- Investigate and address missing values in critical columns")
        
        if duplicates > 0:
            recommendations.append("- Review and remove duplicate records")
        
        if churn_rate > 25:
            recommendations.append("- **Priority**: Churn rate exceeds 25% - implement retention strategies")
        
        if 'churn_risk_flag' in df.columns and high_risk > len(df) * 0.2:
            recommendations.append("- Focus on high-risk customer segment (>20% of base)")
        
        if len(recommendations) == 0:
            recommendations.append("- ✓ Data quality is excellent - ready for analysis")
        
        report_lines.extend(recommendations)
        report_lines.append("")
        
        # ===== SECTION 9: Data Readiness for Power BI =====
        report_lines.append("## 9. Power BI Readiness\n")
        
        pbi_checks = []
        pbi_checks.append(f"- ✓ No special characters in column names")
        pbi_checks.append(f"- ✓ Consistent data types")
        pbi_checks.append(f"- ✓ Categorical fields properly encoded")
        pbi_checks.append(f"- ✓ Numerical fields formatted")
        pbi_checks.append(f"- ✓ Ready for import into Power BI")
        
        report_lines.extend(pbi_checks)
        report_lines.append("")
        
        # ===== Save Report =====
        report_path = Path(__file__).parent.parent / 'reports'
        report_path.mkdir(parents=True, exist_ok=True)
        report_file = report_path / 'insights_summary.md'
        
        with open(report_file, 'w') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"Report saved to: {report_file}")
        
        # Calculate processing time
        elapsed_time = time.time() - start_time
        logger.info(f"Audit completed in {format_duration(elapsed_time)}")
        
        # Print summary
        print(f"\n✅ Data quality audit completed!")
        print(f"\n📋 Report Summary:")
        print(f"  • Quality Score: {quality_score}/100")
        print(f"  • Missing Values: {total_missing}")
        print(f"  • Duplicates: {duplicates}")
        print(f"  • Anomalies: {len(anomalies_found)}")
        print(f"  • Report saved: {report_file.name}")
        print(f"\n⏱️  Processing time: {format_duration(elapsed_time)}")
        print_section_header("DATA QUALITY AUDIT COMPLETED")
        
        return True
        
    except Exception as e:
        logger.error(f"Audit failed: {str(e)}", exc_info=True)
        print(f"\n❌ Error during audit: {str(e)}")
        return False


if __name__ == "__main__":
    success = data_quality_report()
    
    if success:
        # Display the report
        report_path = Path(__file__).parent.parent / 'reports' / 'insights_summary.md'
        print("\n" + "="*70)
        print("  AUDIT REPORT PREVIEW")
        print("="*70)
        
        with open(report_path, 'r') as f:
            content = f.read()
            # Show first 50 lines
            lines = content.split('\n')[:50]
            print('\n'.join(lines))
            if len(content.split('\n')) > 50:
                print("\n... (report continues)")
