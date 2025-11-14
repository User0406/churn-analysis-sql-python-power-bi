# Customer Retention Analytics

## 📊 Project Overview

**Customer Retention Analytics** is an enterprise-grade, automated data analytics project focused on understanding customer churn drivers and retention performance in the telecom industry. this project demonstrates professional data engineering practices with modular ETL pipelines, comprehensive logging, and audit-ready outputs.

### Key Features

- ✅ **Automated ETL Pipeline**: End-to-end data processing from raw CSV to analytics-ready outputs
- ✅ **SQLite Integration**: Structured data storage with SQL query capabilities
- ✅ **Data Quality Auditing**: Comprehensive data validation and quality scoring
- ✅ **Business KPI Generation**: Advanced feature engineering with retention metrics
- ✅ **Power BI Ready**: Clean, formatted datasets optimized for visualization
- ✅ **Enterprise Logging**: Detailed audit trails for every pipeline execution
- ✅ **No Machine Learning**: Pure analytics focus on descriptive insights

---

## 🎯 Business Objective

Analyze telecom customer data to identify churn patterns, calculate retention metrics, and generate actionable business insights without machine learning models. The project enables data-driven decision-making for customer retention strategies.

### Key Metrics Analyzed

- **Churn Rate**: Percentage of customers who discontinued service
- **Retention Score**: Composite metric (0-100) indicating retention likelihood
- **Customer Value Segments**: Classification based on revenue and retention
- **Service Usage Patterns**: Number and types of services used
- **Payment Risk Indicators**: Flags for high-risk payment methods
- **Tenure Analysis**: Customer lifecycle stage categorization

---

## 📁 Project Structure

```
Customer_Retention_Analytics/
│
├── data/
│   ├── raw/                    # Unprocessed customer data (CSV files)
│   ├── processed/              # Cleaned and transformed data
│   │   ├── cleaned_data.csv    # Data after cleaning operations
│   │   └── final_data.csv      # Feature-engineered, Power BI-ready dataset
│   └── logs/                   # Pipeline execution logs
│       ├── ingestion_YYYYMMDD.log
│       ├── cleaning_YYYYMMDD.log
│       ├── feature_engineering_YYYYMMDD.log
│       ├── audit_YYYYMMDD.log
│       └── pipeline_YYYYMMDD.log
│
├── scripts/                    # Python modules for ETL pipeline
│   ├── utils.py                # Logging utilities and helper functions
│   ├── ingest_data.py          # Data ingestion (CSV → SQLite)
│   ├── clean_data.py           # Data cleaning and standardization
│   ├── feature_engineering.py  # KPI creation and feature generation
│   ├── data_quality_audit.py   # Data quality reporting
│   ├── run_pipeline.py         # Master orchestration script
│   └── generate_dataset.py     # Sample data generation (for demo)
│
├── reports/
│   └── insights_summary.md     # Comprehensive data quality and business insights
│
├── database/
│   └── retention.db            # SQLite database with processed data
│
└── README.md                   # This file
```

---

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Required libraries: pandas, numpy, sqlalchemy, openpyxl

### Installation

1. **Navigate to the project directory**:
   ```bash
   cd /app/Customer_Retention_Analytics
   ```

2. **Install dependencies** (if not already installed):
   ```bash
   pip install pandas numpy sqlalchemy openpyxl
   ```

3. **Verify data exists**:
   ```bash
   ls data/raw/
   # Should show: telecom_customer_data.csv
   ```

---

## ▶️ Running the Pipeline

### Option 1: Full Pipeline Execution (Recommended)

Execute all four stages automatically:

```bash
cd scripts
python run_pipeline.py
```

**Output**:
- Ingests raw CSV data into SQLite
- Cleans and standardizes data
- Engineers business features and KPIs
- Generates comprehensive audit report
- Creates Power BI-ready CSV files

### Option 2: Run Individual Stages

For debugging or selective execution:

```bash
cd scripts

# Stage 1: Data Ingestion
python ingest_data.py

# Stage 2: Data Cleaning
python clean_data.py

# Stage 3: Feature Engineering
python feature_engineering.py

# Stage 4: Data Quality Audit
python data_quality_audit.py
```

### Option 3: Continuous Monitoring Mode

Automatically run pipeline when new data arrives:

```bash
python run_pipeline.py --mode watch --interval 600
```

- Checks for new CSV files every 10 minutes (600 seconds)
- Automatically processes new data
- Press `Ctrl+C` to stop

---

## 📊 Pipeline Stages

### Stage 1: Data Ingestion

**Module**: `ingest_data.py`

**What it does**:
- Reads raw CSV files from `data/raw/`
- Creates SQLite database connection
- Loads data into `raw_customer_data` table
- Validates record count and logs details

**Key Metrics Logged**:
- Record count
- Column count
- File size
- Processing time

**Output**: `database/retention.db` (table: `raw_customer_data`)

---

### Stage 2: Data Cleaning

**Module**: `clean_data.py`

**What it does**:
1. **Standardize column names**: Convert to snake_case (e.g., `CustomerID` → `customer_id`)
2. **Handle missing values**: Impute or calculate missing data intelligently
3. **Remove duplicates**: Based on entire row and CustomerID
4. **Clean categorical data**: Standardize values (e.g., `M` → `Male`)
5. **Detect outliers**: Identify (but retain) statistical outliers
6. **Convert data types**: Ensure proper numeric/categorical types

**Cleaning Operations**:
- Missing values filled: 15 records (TotalCharges calculated from MonthlyCharges × Tenure)
- Duplicates removed: ~37 records
- Gender values standardized: `M/F` → `Male/Female`
- Yes/No values capitalized consistently

**Output**: 
- `data/processed/cleaned_data.csv`
- Database table: `cleaned_customer_data`

---

### Stage 3: Feature Engineering

**Module**: `feature_engineering.py`

**What it does**:
Creates 9+ business-driven features for advanced analytics:

#### Features Created:

1. **`tenure_group`**: Customer tenure categorization
   - `0-12 months`, `12-24 months`, `24-36 months`, `36-48 months`, `48+ months`

2. **`avg_monthly_spend`**: Monthly charge category
   - `Low (<$30)`, `Medium ($30-$70)`, `High ($70-$100)`, `Premium ($100+)`

3. **`payment_issue_flag`**: Risk indicator (1 = Electronic check, 0 = Other)
   - Electronic check customers have higher churn risk

4. **`service_usage_score`**: Count of active services (0-8)
   - Phone, Multiple Lines, Online Security, Backup, Protection, Tech Support, Streaming TV, Streaming Movies

5. **`retention_score`**: Composite metric (0-100)
   - **Formula**: Weighted combination of:
     - Tenure (30%)
     - Contract strength (25%)
     - Service usage (20%)
     - Payment method (15%)
     - Support calls (10%)

6. **`customer_value_segment`**: Four-quadrant classification
   - `High Value - High Retention`
   - `Low Value - High Retention`
   - `High Value - At Risk`
   - `Low Value - At Risk`

7. **`internet_service_value`**: Numeric encoding (0=No, 1=DSL, 2=Fiber)

8. **`total_services_count`**: Total number of active services

9. **`churn_risk_flag`**: Binary high-risk indicator
   - Flags customers with: retention_score < 40, new month-to-month contracts, or excessive support calls

**Output**: `data/processed/final_data.csv` (33 columns)

---

### Stage 4: Data Quality Audit

**Module**: `data_quality_audit.py`

**What it does**:
Generates a comprehensive Markdown report with:

1. **Overall Quality Score** (0-100)
   - Based on completeness, uniqueness, and consistency

2. **Missing Data Analysis**
   - By column with percentages

3. **Data Types and Structure**
   - Column types, unique values, sample data

4. **Duplicate Detection**
   - Row-level and ID-level duplicates

5. **Distribution Analysis**
   - Statistical summaries for numerical features
   - Frequency distributions for categorical features

6. **Anomaly Detection**
   - Statistical outliers
   - Logical inconsistencies

7. **Business Insights**
   - Churn rate
   - Average tenure and revenue
   - Contract distribution
   - High-risk customer count

8. **Power BI Readiness Check**
   - Column naming compliance
   - Data type consistency
   - Import readiness

**Output**: `reports/insights_summary.md`

---

## 📈 Power BI Integration Guide

### Loading Data into Power BI

1. **Open Power BI Desktop**

2. **Get Data** → **Text/CSV**

3. **Select file**: `data/processed/final_data.csv`

4. **Load** or **Transform** → **Load**

### Recommended Visuals

#### 1. Executive Dashboard
- **Card**: Total Customers
- **Card**: Churn Rate (%)
- **Card**: Average Retention Score
- **Card**: Monthly Revenue

#### 2. Churn Analysis
- **Donut Chart**: Churn by Contract Type
- **Bar Chart**: Churn Rate by Tenure Group
- **Column Chart**: Churn by Payment Method
- **Funnel Chart**: Customer Value Segments

#### 3. Revenue Analysis
- **Scatter Plot**: Monthly Charges vs. Retention Score (colored by Churn)
- **Line Chart**: Average Revenue by Tenure Group
- **Matrix**: Revenue by Region and Internet Service

#### 4. Retention Insights
- **Histogram**: Retention Score Distribution
- **Stacked Bar**: Service Usage Score by Churn Status
- **KPI Card**: High-Risk Customer Count
- **Table**: Top 10 At-Risk Customers (by Retention Score)

#### 5. Service Adoption
- **Clustered Column**: Service Adoption Rates
- **100% Stacked Bar**: Services by Customer Value Segment
- **Tree Map**: Internet Service Distribution

### Key Columns for Analysis

| Column Name | Use Case |
|------------|----------|
| `churn` | Filter/slicer for all visuals |
| `customer_value_segment` | Color-coding and segmentation |
| `retention_score` | Scatter plots, histograms |
| `tenure_group` | X-axis for trend analysis |
| `contract` | Primary grouping dimension |
| `monthly_charges` | Revenue metrics |
| `region` | Geographic analysis |
| `churn_risk_flag` | Risk filtering |

### DAX Measures (Optional)

```dax
Churn Rate = 
    DIVIDE(
        COUNTROWS(FILTER('final_data', 'final_data'[churn] = "Yes")),
        COUNTROWS('final_data')
    )

Average Retention = AVERAGE('final_data'[retention_score])

High Risk Count = COUNTROWS(FILTER('final_data', 'final_data'[churn_risk_flag] = 1))

Total Revenue = SUM('final_data'[monthly_charges])
```

---

## 📝 Logging and Audit Trail

### Log Files Location

All logs are stored in `data/logs/` with timestamp suffix `YYYYMMDD`:

- `ingestion_YYYYMMDD.log`: Data loading details
- `cleaning_YYYYMMDD.log`: Cleaning operations and issues
- `feature_engineering_YYYYMMDD.log`: Feature creation details
- `audit_YYYYMMDD.log`: Quality check results
- `pipeline_YYYYMMDD.log`: Overall pipeline execution

### Log Format

```
YYYY-MM-DD HH:MM:SS - module_name - LEVEL - message
```

**Levels**: INFO, WARNING, ERROR

### Viewing Logs

```bash
# View latest pipeline log
cat data/logs/pipeline_$(date +%Y%m%d).log

# View last 50 lines
tail -50 data/logs/pipeline_$(date +%Y%m%d).log

# Search for errors
grep "ERROR" data/logs/*.log
```

---

## 🗄️ Database Access

### SQLite Database: `database/retention.db`

**Tables**:
1. `raw_customer_data` - Original ingested data (7,537 records)
2. `cleaned_customer_data` - Cleaned data (7,500 records)

### Querying the Database

```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('database/retention.db')

# Example queries
df = pd.read_sql_query("SELECT * FROM cleaned_customer_data LIMIT 10", conn)

# Churn analysis
churn_by_contract = pd.read_sql_query("""
    SELECT contract, 
           SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) as churned,
           COUNT(*) as total,
           ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate
    FROM cleaned_customer_data
    GROUP BY contract
    ORDER BY churn_rate DESC
""", conn)

print(churn_by_contract)

conn.close()
```

### SQL Query Examples

```sql
-- Top 10 customers by total charges
SELECT customer_id, monthly_charges, total_charges, tenure
FROM cleaned_customer_data
ORDER BY total_charges DESC
LIMIT 10;

-- Average monthly charges by internet service
SELECT internet_service, 
       AVG(monthly_charges) as avg_monthly,
       COUNT(*) as customer_count
FROM cleaned_customer_data
GROUP BY internet_service;

-- Churn rate by payment method
SELECT payment_method,
       SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as churn_rate
FROM cleaned_customer_data
GROUP BY payment_method
ORDER BY churn_rate DESC;
```

---

## 🔍 Key Business Insights

*(Generated from latest pipeline run)*

### Overall Metrics
- **Total Customers**: 7,500
- **Churn Rate**: 33.7%
- **Average Tenure**: 24.3 months
- **Average Monthly Charges**: $78.23
- **Average Retention Score**: 45.8/100

### Churn Drivers Identified
1. **Contract Type**: Month-to-month contracts have highest churn
2. **Tenure**: Customers with <6 months tenure are at highest risk
3. **Payment Method**: Electronic check users churn more frequently
4. **Support Calls**: High support call volume correlates with churn
5. **Service Adoption**: Lower service usage increases churn risk

### Customer Segmentation
- **High Value - High Retention**: 1,247 customers (16.6%)
- **Low Value - High Retention**: 2,198 customers (29.3%)
- **High Value - At Risk**: 1,592 customers (21.2%) ⚠️
- **Low Value - At Risk**: 2,463 customers (32.8%) ⚠️

### Recommendations
1. Focus retention efforts on month-to-month contract holders
2. Implement early intervention for customers with <6 months tenure
3. Encourage migration from electronic check to automatic payments
4. Increase service bundling to improve retention scores
5. Prioritize "High Value - At Risk" segment for immediate action

---

## 🛠️ Troubleshooting

### Issue: Pipeline fails at ingestion
**Solution**: Verify CSV file exists in `data/raw/` and is properly formatted

```bash
ls -lh data/raw/
head data/raw/telecom_customer_data.csv
```

### Issue: Missing dependencies
**Solution**: Install required packages

```bash
pip install pandas numpy sqlalchemy openpyxl
```

### Issue: Database locked
**Solution**: Close any SQLite connections and retry

```bash
rm database/retention.db
python scripts/run_pipeline.py
```

### Issue: Logs not generating
**Solution**: Check write permissions on `data/logs/` directory

```bash
chmod -R 755 data/
```

---

## 📚 Technical Specifications

### Python Version
- **Minimum**: Python 3.8
- **Tested**: Python 3.11

### Dependencies
```
pandas>=2.0.0
numpy>=1.24.0
sqlalchemy>=2.0.0
openpyxl>=3.1.0
```

### Data Volume Capacity
- **Current**: 7,500 records
- **Tested up to**: 100,000 records
- **Recommended max**: 500,000 records (for performance)

### Processing Performance
- **Ingestion**: ~0.2 seconds for 7,500 records
- **Cleaning**: ~0.3 seconds
- **Feature Engineering**: ~0.1 seconds
- **Audit**: ~0.1 seconds
- **Total Pipeline**: <1 second for 7,500 records

---

## 🔄 Automation Options

### Option 1: Cron Job (Linux/Mac)

Schedule pipeline to run daily at 2 AM:

```bash
crontab -e
```

Add line:
```
0 2 * * * cd /app/Customer_Retention_Analytics/scripts && python run_pipeline.py
```

### Option 2: Windows Task Scheduler

1. Open Task Scheduler
2. Create Basic Task
3. Trigger: Daily at 2:00 AM
4. Action: Start a program
5. Program: `python`
6. Arguments: `run_pipeline.py`
7. Start in: `C:\path\to\scripts\`

### Option 3: Continuous Monitoring

```bash
# Run in background (Linux)
nohup python run_pipeline.py --mode watch --interval 600 > pipeline.log 2>&1 &

# Check status
ps aux | grep run_pipeline

# Stop
pkill -f run_pipeline.py
```

---

## 📞 Support & Contact

### Project Maintainer
- **Project**: Customer Retention Analytics
- **Type**: Deloitte-Level Data Analytics Project
- **Status**: Production-Ready

### Documentation
- **README**: `README.md`
- **Insights Report**: `reports/insights_summary.md`
- **Logs**: `data/logs/`

---

## 📄 License

This project is a demonstration of enterprise-grade data analytics capabilities.

---

## ✅ Project Checklist

- [x] Automated ETL pipeline
- [x] SQLite database integration
- [x] Data cleaning and standardization
- [x] Business KPI generation
- [x] Retention score calculation
- [x] Data quality auditing
- [x] Comprehensive logging
- [x] Power BI-ready outputs
- [x] Detailed documentation
- [x] SQL query examples
- [x] Automation support
- [x] Error handling
- [x] Performance optimization

---

**Last Updated**: 2025-10-08  
**Version**: 1.0.0  
**Status**: ✅ Production Ready
