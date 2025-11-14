"""
Sample SQL Queries for Customer Retention Analytics
Demonstrates how to query the SQLite database for business insights
"""
import sqlite3
import pandas as pd
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.parent / 'database' / 'retention.db'


def run_query(query, description):
    """Execute a query and display results"""
    print(f"\n{'='*70}")
    print(f"  {description}")
    print('='*70)
    print(f"\nQuery:\n{query}\n")
    
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print("Results:")
    print(df.to_string(index=False))
    print(f"\n({len(df)} rows returned)")
    return df


def main():
    """Run sample queries demonstrating analytics capabilities"""
    
    print("\n" + "="*70)
    print("  CUSTOMER RETENTION ANALYTICS - SQL QUERY EXAMPLES")
    print("="*70)
    
    # Query 1: Overall churn statistics
    query1 = """
    SELECT 
        churn,
        COUNT(*) as customer_count,
        ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM cleaned_customer_data), 2) as percentage
    FROM cleaned_customer_data
    GROUP BY churn
    ORDER BY customer_count DESC;
    """
    run_query(query1, "1. Overall Churn Distribution")
    
    # Query 2: Churn by contract type
    query2 = """
    SELECT 
        contract,
        COUNT(*) as total_customers,
        SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) as churned_customers,
        ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate
    FROM cleaned_customer_data
    GROUP BY contract
    ORDER BY churn_rate DESC;
    """
    run_query(query2, "2. Churn Rate by Contract Type")
    
    # Query 3: Average revenue by churn status
    query3 = """
    SELECT 
        churn,
        COUNT(*) as customers,
        ROUND(AVG(monthly_charges), 2) as avg_monthly_charges,
        ROUND(AVG(total_charges), 2) as avg_total_charges,
        ROUND(AVG(tenure), 1) as avg_tenure_months
    FROM cleaned_customer_data
    GROUP BY churn;
    """
    run_query(query3, "3. Revenue Metrics by Churn Status")
    
    # Query 4: Top 10 highest-paying customers
    query4 = """
    SELECT 
        customer_id,
        monthly_charges,
        total_charges,
        tenure,
        contract,
        churn
    FROM cleaned_customer_data
    ORDER BY monthly_charges DESC
    LIMIT 10;
    """
    run_query(query4, "4. Top 10 Highest-Paying Customers")
    
    # Query 5: Churn by payment method
    query5 = """
    SELECT 
        payment_method,
        COUNT(*) as total,
        SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) as churned,
        ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate
    FROM cleaned_customer_data
    GROUP BY payment_method
    ORDER BY churn_rate DESC;
    """
    run_query(query5, "5. Churn Rate by Payment Method")
    
    # Query 6: Internet service analysis
    query6 = """
    SELECT 
        internet_service,
        COUNT(*) as customers,
        ROUND(AVG(monthly_charges), 2) as avg_revenue,
        ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate
    FROM cleaned_customer_data
    GROUP BY internet_service
    ORDER BY avg_revenue DESC;
    """
    run_query(query6, "6. Internet Service Performance")
    
    # Query 7: Tenure impact on churn
    query7 = """
    SELECT 
        CASE 
            WHEN tenure <= 12 THEN '0-12 months'
            WHEN tenure <= 24 THEN '12-24 months'
            WHEN tenure <= 36 THEN '24-36 months'
            WHEN tenure <= 48 THEN '36-48 months'
            ELSE '48+ months'
        END as tenure_group,
        COUNT(*) as customers,
        ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate
    FROM cleaned_customer_data
    GROUP BY tenure_group
    ORDER BY 
        CASE tenure_group
            WHEN '0-12 months' THEN 1
            WHEN '12-24 months' THEN 2
            WHEN '24-36 months' THEN 3
            WHEN '36-48 months' THEN 4
            ELSE 5
        END;
    """
    run_query(query7, "7. Churn Rate by Tenure Group")
    
    # Query 8: Senior citizen analysis
    query8 = """
    SELECT 
        CASE senior_citizen WHEN 1 THEN 'Senior' ELSE 'Non-Senior' END as customer_type,
        COUNT(*) as total_customers,
        ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate,
        ROUND(AVG(monthly_charges), 2) as avg_monthly_charges
    FROM cleaned_customer_data
    GROUP BY senior_citizen;
    """
    run_query(query8, "8. Senior vs Non-Senior Customer Analysis")
    
    # Query 9: Regional performance
    query9 = """
    SELECT 
        region,
        COUNT(*) as customers,
        ROUND(AVG(monthly_charges), 2) as avg_revenue,
        ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate
    FROM cleaned_customer_data
    GROUP BY region
    ORDER BY churn_rate DESC;
    """
    run_query(query9, "9. Regional Performance Analysis")
    
    # Query 10: Support calls correlation with churn
    query10 = """
    SELECT 
        CASE 
            WHEN support_calls = 0 THEN '0 calls'
            WHEN support_calls <= 2 THEN '1-2 calls'
            WHEN support_calls <= 5 THEN '3-5 calls'
            ELSE '6+ calls'
        END as support_level,
        COUNT(*) as customers,
        ROUND(100.0 * SUM(CASE WHEN churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) as churn_rate
    FROM cleaned_customer_data
    GROUP BY support_level
    ORDER BY churn_rate;
    """
    run_query(query10, "10. Support Calls vs Churn Correlation")
    
    print("\n" + "="*70)
    print("  All queries completed successfully!")
    print("="*70)
    print(f"\nDatabase: {DB_PATH}")
    print(f"Table: cleaned_customer_data")
    print(f"\nTo run custom queries, use:")
    print("  python -c \"import sqlite3; import pandas as pd;")
    print("  conn = sqlite3.connect('database/retention.db');")
    print("  df = pd.read_sql_query('YOUR_QUERY_HERE', conn);")
    print("  print(df)\"")
    print("\n")


if __name__ == "__main__":
    main()
