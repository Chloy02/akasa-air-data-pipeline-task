"""
KPI Calculator Module
Modular functions to calculate KPIs using pandas DataFrames
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

from src.config import TIMEZONE, LAST_N_DAYS
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def calculate_repeat_customers(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame
) -> pd.DataFrame:
    """
    KPI 1: Identify customers with more than one order
    
    Args:
        customers_df: DataFrame with customer data
        orders_df: DataFrame with order data
        
    Returns:
        DataFrame with repeat customer information
    """
    try:
        logger.info("Calculating repeat customers...")
        
        # Merge customers with orders
        merged_df = orders_df.merge(
            customers_df,
            on='mobile_number',
            how='inner'
        )
        
        # Group by customer and count distinct orders
        repeat_customers = merged_df.groupby(
            ['customer_id', 'customer_name', 'mobile_number', 'region']
        ).agg(
            order_count=('order_id', 'nunique'),
            total_spent=('total_amount', 'sum')
        ).reset_index()
        
        # Filter customers with more than 1 order
        repeat_customers = repeat_customers[repeat_customers['order_count'] > 1]
        
        # Sort by order count and total spent
        repeat_customers = repeat_customers.sort_values(
            by=['order_count', 'total_spent'],
            ascending=False
        ).reset_index(drop=True)
        
        logger.info(f"Found {len(repeat_customers)} repeat customers")
        
        return repeat_customers
        
    except Exception as e:
        logger.error(f"Failed to calculate repeat customers: {str(e)}")
        raise


def calculate_monthly_trends(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame
) -> pd.DataFrame:
    """
    KPI 2: Aggregate orders by month to observe trends
    
    Args:
        orders_df: DataFrame with order data
        order_items_df: DataFrame with order items data
        
    Returns:
        DataFrame with monthly order trends
    """
    try:
        logger.info("Calculating monthly order trends...")
        
        # Create month column
        orders_with_month = orders_df.copy()
        orders_with_month['month'] = orders_with_month['order_date_time'].dt.to_period('M').astype(str)
        
        # Merge with order items to get item counts
        merged_df = orders_with_month.merge(
            order_items_df,
            on='order_id',
            how='left'
        )
        
        # Group by month
        monthly_trends = merged_df.groupby('month').agg(
            order_count=('order_id', 'nunique'),
            total_items=('sku_id', 'count'),
            total_revenue=('total_amount', 'sum'),
            avg_order_value=('total_amount', 'mean'),
            unique_customers=('mobile_number', 'nunique')
        ).reset_index()
        
        # Sort by month descending
        monthly_trends = monthly_trends.sort_values(by='month', ascending=False).reset_index(drop=True)
        
        logger.info(f"Retrieved {len(monthly_trends)} months of order trends")
        
        return monthly_trends
        
    except Exception as e:
        logger.error(f"Failed to calculate monthly trends: {str(e)}")
        raise


def calculate_regional_revenue(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame
) -> pd.DataFrame:
    """
    KPI 3: Sum of total_amount grouped by region
    
    Args:
        customers_df: DataFrame with customer data
        orders_df: DataFrame with order data
        
    Returns:
        DataFrame with regional revenue breakdown
    """
    try:
        logger.info("Calculating regional revenue...")
        
        # Merge customers with orders
        merged_df = orders_df.merge(
            customers_df[['mobile_number', 'customer_id', 'region']],
            on='mobile_number',
            how='inner'
        )
        
        # Group by region
        regional_revenue = merged_df.groupby('region').agg(
            customer_count=('customer_id', 'nunique'),
            order_count=('order_id', 'nunique'),
            total_revenue=('total_amount', 'sum'),
            avg_order_value=('total_amount', 'mean'),
            max_order_value=('total_amount', 'max')
        ).reset_index()
        
        # Sort by total revenue descending
        regional_revenue = regional_revenue.sort_values(
            by='total_revenue',
            ascending=False
        ).reset_index(drop=True)
        
        logger.info(f"Retrieved revenue data for {len(regional_revenue)} regions")
        
        return regional_revenue
        
    except Exception as e:
        logger.error(f"Failed to calculate regional revenue: {str(e)}")
        raise


def calculate_top_customers_last_30_days(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    top_n: int = 10,
    cutoff_date: Optional[datetime] = None
) -> pd.DataFrame:
    """
    KPI 4: Rank customers by total spend in the last 30 days
    
    Args:
        customers_df: DataFrame with customer data
        orders_df: DataFrame with order data
        top_n: Number of top customers to return
        cutoff_date: Optional cutoff date (defaults to current date - 30 days)
        
    Returns:
        DataFrame with top customers by spend
    """
    try:
        logger.info("Calculating top customers for last 30 days...")
        
        # Calculate cutoff date if not provided
        if cutoff_date is None:
            current_time = datetime.now(TIMEZONE)
            cutoff_date = current_time - timedelta(days=LAST_N_DAYS)
        
        # Filter orders from last 30 days
        recent_orders = orders_df[orders_df['order_date_time'] >= cutoff_date].copy()
        
        # Merge with customers
        merged_df = recent_orders.merge(
            customers_df,
            on='mobile_number',
            how='inner'
        )
        
        # Group by customer
        top_customers = merged_df.groupby(
            ['customer_id', 'customer_name', 'mobile_number', 'region']
        ).agg(
            order_count=('order_id', 'nunique'),
            total_spent=('total_amount', 'sum'),
            last_order_date=('order_date_time', 'max'),
            first_order_date=('order_date_time', 'min')
        ).reset_index()
        
        # Sort by total spent and take top N
        top_customers = top_customers.sort_values(
            by='total_spent',
            ascending=False
        ).head(top_n).reset_index(drop=True)
        
        logger.info(
            f"Retrieved top {len(top_customers)} customers for last {LAST_N_DAYS} days "
            f"(from {cutoff_date.strftime('%Y-%m-%d')} onwards)"
        )
        
        return top_customers
        
    except Exception as e:
        logger.error(f"Failed to calculate top customers: {str(e)}")
        raise


def calculate_all_kpis(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame
) -> dict:
    """
    Calculate all KPIs and return as dictionary
    
    Args:
        customers_df: DataFrame with customer data
        orders_df: DataFrame with order data
        order_items_df: DataFrame with order items data
        
    Returns:
        Dictionary containing all KPI DataFrames
    """
    try:
        logger.info("Calculating all KPIs using in-memory approach...")
        
        kpis = {
            'repeat_customers': calculate_repeat_customers(customers_df, orders_df),
            'monthly_trends': calculate_monthly_trends(orders_df, order_items_df),
            'regional_revenue': calculate_regional_revenue(customers_df, orders_df),
            'top_customers_last_30_days': calculate_top_customers_last_30_days(customers_df, orders_df)
        }
        
        logger.info("All KPIs calculated successfully")
        return kpis
        
    except Exception as e:
        logger.error(f"Failed to calculate KPIs: {str(e)}")
        raise


def export_kpis_to_excel(kpis: dict, output_file: str):
    """
    Export all KPIs to Excel file with multiple sheets
    
    Args:
        kpis: Dictionary of KPI DataFrames
        output_file: Path to output Excel file
    """
    try:
        # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
        kpis_copy = {}
        for key, df in kpis.items():
            df_copy = df.copy()
            # Convert datetime columns to timezone-naive
            for col in df_copy.select_dtypes(include=['datetime64[ns, Asia/Kolkata]']).columns:
                df_copy[col] = df_copy[col].dt.tz_localize(None)
            kpis_copy[key] = df_copy
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            kpis_copy['repeat_customers'].to_excel(
                writer, sheet_name='Repeat Customers', index=False
            )
            kpis_copy['monthly_trends'].to_excel(
                writer, sheet_name='Monthly Trends', index=False
            )
            kpis_copy['regional_revenue'].to_excel(
                writer, sheet_name='Regional Revenue', index=False
            )
            kpis_copy['top_customers_last_30_days'].to_excel(
                writer, sheet_name='Top Customers 30D', index=False
            )
        
        logger.info(f"KPIs exported to {output_file}")
        
    except Exception as e:
        logger.error(f"Failed to export KPIs: {str(e)}")
        raise
