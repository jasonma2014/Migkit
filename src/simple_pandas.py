"""
Pandas-Based Data Migration Example

This module demonstrates how to use the migration framework with Pandas
for data processing without relying on SQLAlchemy or Pydantic.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime, timedelta
from framework import phase, Phase, DataMigrationFramework

# Ensure output directories exist
os.makedirs("output", exist_ok=True)
os.makedirs("output/data", exist_ok=True)
os.makedirs("output/visualizations", exist_ok=True)

# Set seaborn style for better visualizations
sns.set(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12


@phase(Phase.EXTRACT)
def extract_data_to_pandas():
    """
    Extract phase: Create sample data and load it into pandas DataFrames.
    
    Returns:
        dict: Dictionary containing pandas DataFrames
    """
    print("Extracting data to pandas DataFrames...")
    
    # Generate random date range for the last 30 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create sample sales data
    np.random.seed(42)  # For reproducibility
    
    # 1. Daily sales data
    daily_sales_data = pd.DataFrame({
        'date': date_range,
        'revenue': np.random.normal(1000, 200, size=len(date_range)),
        'transactions': np.random.randint(50, 150, size=len(date_range)),
        'advertising_spend': np.random.uniform(100, 300, size=len(date_range))
    })
    
    # 2. Product sales data
    products = ['Laptop', 'Smartphone', 'Tablet', 'Headphones', 'Monitor', 'Keyboard', 'Mouse']
    categories = ['Electronics', 'Electronics', 'Electronics', 'Accessories', 'Accessories', 'Accessories', 'Accessories']
    
    product_sales = []
    for i, date in enumerate(date_range):
        for j, product in enumerate(products):
            # Generate different volumes per product with some randomness
            base_volume = 10 - j  # Higher for first products
            if j < 3:  # Make electronic items more popular
                base_volume += 5
            
            # Add day-of-week effect (higher on weekends)
            weekend_boost = 5 if date.dayofweek >= 5 else 0
            
            # Random variation
            variation = np.random.normal(0, 2)
            
            volume = max(0, int(base_volume + weekend_boost + variation))
            
            # Price with some random variation
            base_price = 100 * (j + 1)  # Different price tiers
            price = round(base_price * np.random.uniform(0.95, 1.05), 2)
            
            product_sales.append({
                'date': date,
                'product': product,
                'category': categories[j],
                'units_sold': volume,
                'unit_price': price,
                'total_sales': volume * price
            })
    
    product_sales_data = pd.DataFrame(product_sales)
    
    # 3. Customer data
    customer_segments = ['New', 'Returning', 'Loyal', 'VIP']
    regions = ['North', 'South', 'East', 'West']
    
    customer_data = []
    for i, date in enumerate(date_range):
        for segment in customer_segments:
            for region in regions:
                # Generate different order counts based on segment and region
                base_orders = {'New': 5, 'Returning': 10, 'Loyal': 15, 'VIP': 20}[segment]
                region_modifier = {'North': 1.2, 'South': 0.9, 'East': 1.0, 'West': 1.1}[region]
                
                # Add time-based trend (growing over time)
                time_trend = i / len(date_range) * 5
                
                # Random variation
                variation = np.random.normal(0, 2)
                
                orders = max(0, int(base_orders * region_modifier + time_trend + variation))
                avg_order_value = round(50 + (orders / 5) + np.random.normal(0, 5), 2)
                
                customer_data.append({
                    'date': date,
                    'segment': segment,
                    'region': region,
                    'orders': orders,
                    'avg_order_value': avg_order_value,
                    'total_value': orders * avg_order_value
                })
    
    customer_segment_data = pd.DataFrame(customer_data)
    
    # Save raw data as CSV files
    daily_sales_data.to_csv("output/data/daily_sales_raw.csv", index=False)
    product_sales_data.to_csv("output/data/product_sales_raw.csv", index=False)
    customer_segment_data.to_csv("output/data/customer_segment_raw.csv", index=False)
    
    # Return all DataFrames in a dictionary
    return {
        'daily_sales': daily_sales_data,
        'product_sales': product_sales_data,
        'customer_segments': customer_segment_data
    }


@phase(Phase.TRANSFORM)
def transform_with_pandas(data_frames):
    """
    Transform phase: Process the data using pandas operations.
    
    Args:
        data_frames (dict): Dictionary containing pandas DataFrames
        
    Returns:
        dict: Dictionary with transformed DataFrames and analysis results
    """
    print("Transforming data with pandas...")
    
    daily_sales = data_frames['daily_sales']
    product_sales = data_frames['product_sales']
    customer_segments = data_frames['customer_segments']
    
    # 1. Daily sales analysis
    # Add derived metrics
    daily_sales['avg_transaction_value'] = daily_sales['revenue'] / daily_sales['transactions']
    daily_sales['roi'] = (daily_sales['revenue'] - daily_sales['advertising_spend']) / daily_sales['advertising_spend']
    daily_sales['day_of_week'] = daily_sales['date'].dt.day_name()
    
    # Create aggregated views
    daily_summary = daily_sales.describe()
    weekly_sales = daily_sales.resample('W', on='date').sum()
    weekly_sales['avg_transaction_value'] = weekly_sales['revenue'] / weekly_sales['transactions']
    weekly_sales['roi'] = (weekly_sales['revenue'] - weekly_sales['advertising_spend']) / weekly_sales['advertising_spend']
    
    day_of_week_analysis = daily_sales.groupby('day_of_week').agg({
        'revenue': 'mean',
        'transactions': 'mean',
        'advertising_spend': 'mean',
        'avg_transaction_value': 'mean',
        'roi': 'mean'
    }).reset_index()
    
    # 2. Product sales analysis
    # Product performance metrics
    product_summary = product_sales.groupby('product').agg({
        'units_sold': 'sum',
        'total_sales': 'sum',
        'unit_price': 'mean'
    }).reset_index()
    
    product_summary['avg_daily_sales'] = product_summary['total_sales'] / 31
    product_summary['market_share'] = product_summary['total_sales'] / product_summary['total_sales'].sum() * 100
    product_summary = product_summary.sort_values('total_sales', ascending=False)
    
    # Category analysis
    category_summary = product_sales.groupby('category').agg({
        'units_sold': 'sum',
        'total_sales': 'sum'
    }).reset_index()
    
    category_summary['avg_price'] = product_sales.groupby('category')['unit_price'].mean().values
    category_summary['market_share'] = category_summary['total_sales'] / category_summary['total_sales'].sum() * 100
    
    # Daily trend by product
    product_daily = product_sales.groupby(['date', 'product']).agg({
        'units_sold': 'sum',
        'total_sales': 'sum'
    }).reset_index()
    
    # 3. Customer segment analysis
    segment_summary = customer_segments.groupby('segment').agg({
        'orders': 'sum',
        'avg_order_value': 'mean',
        'total_value': 'sum'
    }).reset_index()
    
    segment_summary['customer_share'] = segment_summary['total_value'] / segment_summary['total_value'].sum() * 100
    
    region_summary = customer_segments.groupby('region').agg({
        'orders': 'sum',
        'avg_order_value': 'mean',
        'total_value': 'sum'
    }).reset_index()
    
    # Cross-tabulation of segment and region
    segment_region_matrix = pd.pivot_table(
        customer_segments, 
        values='total_value', 
        index='segment', 
        columns='region', 
        aggfunc='sum'
    )
    
    # 4. Time series analysis
    # Add moving averages to daily sales
    daily_sales['revenue_7d_ma'] = daily_sales['revenue'].rolling(window=7).mean()
    daily_sales['transactions_7d_ma'] = daily_sales['transactions'].rolling(window=7).mean()
    
    # Create correlation matrix
    correlation_matrix = daily_sales[['revenue', 'transactions', 'advertising_spend', 'avg_transaction_value', 'roi']].corr()
    
    # Return all transformed data
    transformed_data = {
        'daily_sales': daily_sales,
        'weekly_sales': weekly_sales,
        'day_of_week_analysis': day_of_week_analysis,
        'daily_summary': daily_summary,
        'product_summary': product_summary,
        'category_summary': category_summary,
        'product_daily': product_daily,
        'segment_summary': segment_summary,
        'region_summary': region_summary,
        'segment_region_matrix': segment_region_matrix,
        'correlation_matrix': correlation_matrix
    }
    
    return transformed_data


@phase(Phase.LOAD)
def load_results_and_visualize(transformed_data):
    """
    Load phase: Save processed data and create visualizations with seaborn.
    
    Args:
        transformed_data (dict): Dictionary with transformed data
        
    Returns:
        dict: Summary of the operation
    """
    print("Loading results and creating visualizations with seaborn...")
    
    # Save all transformed DataFrames to CSV
    for name, df in transformed_data.items():
        if isinstance(df, pd.DataFrame):
            df.to_csv(f"output/data/{name}.csv")
    
    # Create a summary report
    total_revenue = transformed_data['daily_sales']['revenue'].sum()
    total_transactions = int(transformed_data['daily_sales']['transactions'].sum())
    avg_transaction = total_revenue / total_transactions
    overall_roi = ((total_revenue - transformed_data['daily_sales']['advertising_spend'].sum()) / 
                   transformed_data['daily_sales']['advertising_spend'].sum() * 100)
    
    # Get top product
    top_product = transformed_data['product_summary'].iloc[0]['product']
    top_product_sales = transformed_data['product_summary'].iloc[0]['total_sales']
    
    # Create a summary report as a DataFrame
    summary_data = {
        'Metric': [
            'Total Revenue',
            'Total Transactions',
            'Average Transaction Value',
            'Overall ROI',
            'Top Product',
            'Top Product Sales'
        ],
        'Value': [
            f"${total_revenue:.2f}",
            f"{total_transactions:,}",
            f"${avg_transaction:.2f}",
            f"{overall_roi:.2f}%",
            top_product,
            f"${top_product_sales:.2f}"
        ]
    }
    
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv("output/data/summary_report.csv", index=False)
    
    # Create visualizations with seaborn
    visualizations = []
    
    # 1. Daily Revenue Trend
    plt.figure(figsize=(12, 6))
    sns.lineplot(x='date', y='revenue', data=transformed_data['daily_sales'], label='Daily Revenue')
    sns.lineplot(x='date', y='revenue_7d_ma', data=transformed_data['daily_sales'], label='7-Day Moving Average')
    plt.title('Daily Revenue Trend', fontsize=16)
    plt.xlabel('Date', fontsize=14)
    plt.ylabel('Revenue ($)', fontsize=14)
    plt.legend(fontsize=12)
    plt.tight_layout()
    plt.savefig('output/visualizations/daily_revenue_trend.png', dpi=300)
    visualizations.append('daily_revenue_trend.png')
    plt.close()
    
    # 2. Day of Week Analysis
    plt.figure(figsize=(10, 6))
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_data = transformed_data['day_of_week_analysis'].set_index('day_of_week').reindex(day_order)
    sns.barplot(x=day_data.index, y='revenue', data=day_data, palette='viridis')
    plt.title('Average Revenue by Day of Week', fontsize=16)
    plt.xlabel('Day of Week', fontsize=14)
    plt.ylabel('Average Revenue ($)', fontsize=14)
    plt.tight_layout()
    plt.savefig('output/visualizations/revenue_by_day.png', dpi=300)
    visualizations.append('revenue_by_day.png')
    plt.close()
    
    # 3. Product Sales Distribution
    plt.figure(figsize=(10, 6))
    sns.barplot(x='product', y='total_sales', data=transformed_data['product_summary'], palette='viridis')
    plt.title('Total Sales by Product', fontsize=16)
    plt.xlabel('Product', fontsize=14)
    plt.ylabel('Total Sales ($)', fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('output/visualizations/product_sales.png', dpi=300)
    visualizations.append('product_sales.png')
    plt.close()
    
    # 4. Category Comparison
    plt.figure(figsize=(8, 5))
    sns.barplot(x='category', y='total_sales', data=transformed_data['category_summary'], palette='viridis')
    plt.title('Sales by Product Category', fontsize=16)
    plt.xlabel('Category', fontsize=14)
    plt.ylabel('Total Sales ($)', fontsize=14)
    plt.tight_layout()
    plt.savefig('output/visualizations/category_sales.png', dpi=300)
    visualizations.append('category_sales.png')
    plt.close()
    
    # 5. Customer Segment Value
    plt.figure(figsize=(8, 5))
    sns.barplot(x='segment', y='total_value', data=transformed_data['segment_summary'], palette='viridis')
    plt.title('Total Value by Customer Segment', fontsize=16)
    plt.xlabel('Customer Segment', fontsize=14)
    plt.ylabel('Total Value ($)', fontsize=14)
    plt.tight_layout()
    plt.savefig('output/visualizations/segment_value.png', dpi=300)
    visualizations.append('segment_value.png')
    plt.close()
    
    # 6. Correlation Heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(transformed_data['correlation_matrix'], annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
    plt.title('Correlation Matrix', fontsize=16)
    plt.tight_layout()
    plt.savefig('output/visualizations/correlation_matrix.png', dpi=300)
    visualizations.append('correlation_matrix.png')
    plt.close()
    
    # 7. Region Comparison
    plt.figure(figsize=(8, 5))
    sns.barplot(x='region', y='total_value', data=transformed_data['region_summary'], palette='viridis')
    plt.title('Total Value by Region', fontsize=16)
    plt.xlabel('Region', fontsize=14)
    plt.ylabel('Total Value ($)', fontsize=14)
    plt.tight_layout()
    plt.savefig('output/visualizations/region_value.png', dpi=300)
    visualizations.append('region_value.png')
    plt.close()
    
    # 8. Segment-Region Heatmap
    plt.figure(figsize=(10, 8))
    sns.heatmap(transformed_data['segment_region_matrix'], annot=True, cmap='YlGnBu', fmt='.0f', linewidths=0.5)
    plt.title('Customer Value by Segment and Region', fontsize=16)
    plt.tight_layout()
    plt.savefig('output/visualizations/segment_region_heatmap.png', dpi=300)
    visualizations.append('segment_region_heatmap.png')
    plt.close()
    
    # Create a summary of the operation
    summary = {
        "timestamp": datetime.now().isoformat(),
        "data_files_created": [f"data/{name}.csv" for name in transformed_data.keys() if isinstance(transformed_data[name], pd.DataFrame)],
        "visualizations_created": visualizations,
        "summary_metrics": {
            "total_revenue": total_revenue,
            "total_transactions": total_transactions,
            "average_transaction_value": avg_transaction,
            "overall_roi": overall_roi,
            "top_product": top_product,
            "top_product_sales": top_product_sales
        }
    }
    
    print(f"Generated {len(summary['data_files_created'])} data files")
    print(f"Created {len(summary['visualizations_created'])} visualizations")
    
    return summary


def run_pandas_migration():
    """
    Run the pandas-based data migration process.
    """
    # Create an instance of the framework
    framework = DataMigrationFramework()
    
    # Register the current module
    import sys
    current_module = sys.modules[__name__]
    framework.register_phases_from_annotations(current_module)
    
    # Execute the migration process
    print("\n===== STARTING PANDAS-BASED DATA MIGRATION =====\n")
    result = framework.run()
    print("\n===== MIGRATION COMPLETE =====\n")
    
    # Print summary
    print("Migration Summary:")
    print(f"- Timestamp: {result['timestamp']}")
    print(f"- Data Files Created: {len(result['data_files_created'])}")
    print(f"- Visualizations Created: {len(result['visualizations_created'])}")
    print("- Key Metrics:")
    print(f"  - Total Revenue: ${result['summary_metrics']['total_revenue']:.2f}")
    print(f"  - Total Transactions: {result['summary_metrics']['total_transactions']:,}")
    print(f"  - Average Transaction Value: ${result['summary_metrics']['average_transaction_value']:.2f}")
    print(f"  - Overall ROI: {result['summary_metrics']['overall_roi']:.2f}%")
    print(f"  - Top Product: {result['summary_metrics']['top_product']} (${result['summary_metrics']['top_product_sales']:.2f})")
    
    return result


# Run the pandas migration if executed directly
if __name__ == "__main__":
    run_pandas_migration() 