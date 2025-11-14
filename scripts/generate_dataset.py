"""
Generate realistic telecom customer churn dataset
Simulates a Kaggle-style dataset download
"""
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

def generate_telecom_dataset(n_rows=7500):
    """Generate realistic telecom customer churn data"""
    
    print(f"Generating {n_rows} customer records...")
    
    # Generate CustomerIDs
    customer_ids = [f"CUST{str(i).zfill(6)}" for i in range(1, n_rows + 1)]
    
    # Generate realistic data
    data = {
        'CustomerID': customer_ids,
        'Gender': np.random.choice(['Male', 'Female', 'M', 'F'], size=n_rows, p=[0.48, 0.48, 0.02, 0.02]),
        'SeniorCitizen': np.random.choice([0, 1], size=n_rows, p=[0.84, 0.16]),
        'Partner': np.random.choice(['Yes', 'No'], size=n_rows, p=[0.52, 0.48]),
        'Dependents': np.random.choice(['Yes', 'No'], size=n_rows, p=[0.30, 0.70]),
        'Tenure': np.random.exponential(scale=24, size=n_rows).astype(int).clip(0, 72),
        'PhoneService': np.random.choice(['Yes', 'No'], size=n_rows, p=[0.90, 0.10]),
        'MultipleLines': np.random.choice(['Yes', 'No', 'No phone service'], size=n_rows, p=[0.45, 0.45, 0.10]),
        'InternetService': np.random.choice(['DSL', 'Fiber optic', 'No'], size=n_rows, p=[0.35, 0.50, 0.15]),
        'OnlineSecurity': np.random.choice(['Yes', 'No', 'No internet service'], size=n_rows, p=[0.30, 0.55, 0.15]),
        'OnlineBackup': np.random.choice(['Yes', 'No', 'No internet service'], size=n_rows, p=[0.35, 0.50, 0.15]),
        'DeviceProtection': np.random.choice(['Yes', 'No', 'No internet service'], size=n_rows, p=[0.35, 0.50, 0.15]),
        'TechSupport': np.random.choice(['Yes', 'No', 'No internet service'], size=n_rows, p=[0.30, 0.55, 0.15]),
        'StreamingTV': np.random.choice(['Yes', 'No', 'No internet service'], size=n_rows, p=[0.40, 0.45, 0.15]),
        'StreamingMovies': np.random.choice(['Yes', 'No', 'No internet service'], size=n_rows, p=[0.40, 0.45, 0.15]),
        'Contract': np.random.choice(['Month-to-month', 'One year', 'Two year'], size=n_rows, p=[0.55, 0.23, 0.22]),
        'PaperlessBilling': np.random.choice(['Yes', 'No'], size=n_rows, p=[0.59, 0.41]),
        'PaymentMethod': np.random.choice([
            'Electronic check', 
            'Mailed check', 
            'Bank transfer (automatic)', 
            'Credit card (automatic)'
        ], size=n_rows, p=[0.33, 0.23, 0.22, 0.22]),
        'Region': np.random.choice(['North', 'South', 'East', 'West', 'Central'], size=n_rows),
        'SupportCalls': np.random.poisson(lam=2.5, size=n_rows).clip(0, 15)
    }
    
    df = pd.DataFrame(data)
    
    # Generate MonthlyCharges based on services
    base_charge = 20
    df['MonthlyCharges'] = base_charge
    df.loc[df['InternetService'] == 'DSL', 'MonthlyCharges'] += np.random.uniform(25, 35, size=(df['InternetService'] == 'DSL').sum())
    df.loc[df['InternetService'] == 'Fiber optic', 'MonthlyCharges'] += np.random.uniform(60, 90, size=(df['InternetService'] == 'Fiber optic').sum())
    df.loc[df['OnlineSecurity'] == 'Yes', 'MonthlyCharges'] += 5
    df.loc[df['OnlineBackup'] == 'Yes', 'MonthlyCharges'] += 5
    df.loc[df['DeviceProtection'] == 'Yes', 'MonthlyCharges'] += 5
    df.loc[df['TechSupport'] == 'Yes', 'MonthlyCharges'] += 5
    df.loc[df['StreamingTV'] == 'Yes', 'MonthlyCharges'] += 8
    df.loc[df['StreamingMovies'] == 'Yes', 'MonthlyCharges'] += 8
    df['MonthlyCharges'] = df['MonthlyCharges'].round(2)
    
    # Calculate TotalCharges (with some missing values)
    df['TotalCharges'] = (df['MonthlyCharges'] * df['Tenure']).round(2)
    
    # Introduce some missing values (realistic data quality issues)
    missing_indices = np.random.choice(df.index, size=int(n_rows * 0.002), replace=False)
    df.loc[missing_indices, 'TotalCharges'] = np.nan
    
    # Introduce some data quality issues
    # 1. Some TotalCharges stored as strings with spaces
    string_indices = np.random.choice(df.index, size=int(n_rows * 0.01), replace=False)
    df.loc[string_indices, 'TotalCharges'] = df.loc[string_indices, 'TotalCharges'].astype(str) + ' '
    
    # 2. Some duplicates
    duplicate_rows = df.sample(n=int(n_rows * 0.005))
    df = pd.concat([df, duplicate_rows], ignore_index=True)
    
    # Generate Churn based on realistic factors
    churn_probability = 0.2  # Base churn rate
    df['ChurnProb'] = churn_probability
    
    # Factors that increase churn
    df.loc[df['Contract'] == 'Month-to-month', 'ChurnProb'] += 0.15
    df.loc[df['Tenure'] < 6, 'ChurnProb'] += 0.15
    df.loc[df['PaymentMethod'] == 'Electronic check', 'ChurnProb'] += 0.08
    df.loc[df['SupportCalls'] > 5, 'ChurnProb'] += 0.10
    df.loc[df['InternetService'] == 'Fiber optic', 'ChurnProb'] += 0.05
    df.loc[df['TechSupport'] == 'No', 'ChurnProb'] += 0.05
    df.loc[df['OnlineSecurity'] == 'No', 'ChurnProb'] += 0.05
    
    # Factors that decrease churn
    df.loc[df['Contract'] == 'Two year', 'ChurnProb'] -= 0.20
    df.loc[df['Tenure'] > 36, 'ChurnProb'] -= 0.10
    df.loc[df['Partner'] == 'Yes', 'ChurnProb'] -= 0.05
    
    # Clip probability and generate churn
    df['ChurnProb'] = df['ChurnProb'].clip(0, 0.95)
    df['Churn'] = np.random.binomial(1, df['ChurnProb'])
    df['Churn'] = df['Churn'].map({0: 'No', 1: 'Yes'})
    
    # Drop temporary column
    df = df.drop('ChurnProb', axis=1)
    
    # Shuffle the dataset
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    print(f"✓ Generated {len(df)} records")
    print(f"✓ Churn rate: {(df['Churn'] == 'Yes').sum() / len(df) * 100:.1f}%")
    print(f"✓ Features: {len(df.columns)}")
    
    return df

if __name__ == "__main__":
    # Generate dataset
    df = generate_telecom_dataset(7500)
    
    # Save to raw data folder
    output_path = '../data/raw/telecom_customer_data.csv'
    df.to_csv(output_path, index=False)
    
    print(f"\n✓ Dataset saved to: {output_path}")
    print(f"✓ Shape: {df.shape}")
    print(f"\nFirst few rows:")
    print(df.head())
    print(f"\nColumn types:")
    print(df.dtypes)
