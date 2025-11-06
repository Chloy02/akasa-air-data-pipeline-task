"""
KPI Queries Module
SQL queries to calculate business KPIs using database tables
"""

from sqlalchemy import text
from datetime import datetime, timedelta
import pandas as pd

from src.table_based.db_connection import get_db_engine
from src.config import TIMEZONE, LAST_N_DAYS
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def get_repeat_customers() -> pd.DataFrame:
    """
    KPI 1: Identify customers with more than one order
    
    Returns:
        DataFrame with repeat customer information
    """
    try:
        engine = get_db_engine()
        
        query = text("""
            SELECT 
                c.customer_id,
                c.customer_name,
                c.mobile_number,
                c.region,
                COUNT(DISTINCT o.order_id) as order_count,
                SUM(o.total_amount) as total_spent
            FROM customers c
            INNER JOIN orders o ON c.mobile_number = o.mobile_number
            GROUP BY c.customer_id, c.customer_name, c.mobile_number, c.region
            HAVING COUNT(DISTINCT o.order_id) > 1
            ORDER BY order_count DESC, total_spent DESC
        """)
        
        df = pd.read_sql(query, engine)
        logger.info(f"Found {len(df)} repeat customers")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get repeat customers: {str(e)}")
        raise


def get_monthly_order_trends() -> pd.DataFrame:
    """
    KPI 2: Aggregate orders by month to observe trends
    
    Returns:
        DataFrame with monthly order trends
    """
    try:
        engine = get_db_engine()
        
        query = text("""
            SELECT 
                DATE_FORMAT(o.order_date_time, '%Y-%m') as month,
                COUNT(DISTINCT o.order_id) as order_count,
                COUNT(oi.id) as total_items,
                SUM(o.total_amount) as total_revenue,
                AVG(o.total_amount) as avg_order_value,
                COUNT(DISTINCT o.mobile_number) as unique_customers
            FROM orders o
            LEFT JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY DATE_FORMAT(o.order_date_time, '%Y-%m')
            ORDER BY month DESC
        """)
        
        df = pd.read_sql(query, engine)
        logger.info(f"Retrieved {len(df)} months of order trends")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get monthly order trends: {str(e)}")
        raise


def get_regional_revenue() -> pd.DataFrame:
    """
    KPI 3: Sum of total_amount grouped by region
    
    Returns:
        DataFrame with regional revenue breakdown
    """
    try:
        engine = get_db_engine()
        
        query = text("""
            SELECT 
                c.region,
                COUNT(DISTINCT c.customer_id) as customer_count,
                COUNT(DISTINCT o.order_id) as order_count,
                SUM(o.total_amount) as total_revenue,
                AVG(o.total_amount) as avg_order_value,
                MAX(o.total_amount) as max_order_value
            FROM customers c
            INNER JOIN orders o ON c.mobile_number = o.mobile_number
            GROUP BY c.region
            ORDER BY total_revenue DESC
        """)
        
        df = pd.read_sql(query, engine)
        logger.info(f"Retrieved revenue data for {len(df)} regions")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get regional revenue: {str(e)}")
        raise


def get_top_customers_last_30_days(top_n: int = 10) -> pd.DataFrame:
    """
    KPI 4: Rank customers by total spend in the last 30 days
    
    Args:
        top_n: Number of top customers to return
        
    Returns:
        DataFrame with top customers by spend
    """
    try:
        engine = get_db_engine()
        
        # Calculate cutoff date (30 days ago from now in IST)
        current_time = datetime.now(TIMEZONE)
        cutoff_date = current_time - timedelta(days=LAST_N_DAYS)
        
        query = text("""
            SELECT 
                c.customer_id,
                c.customer_name,
                c.mobile_number,
                c.region,
                COUNT(DISTINCT o.order_id) as order_count,
                SUM(o.total_amount) as total_spent,
                MAX(o.order_date_time) as last_order_date,
                MIN(o.order_date_time) as first_order_date
            FROM customers c
            INNER JOIN orders o ON c.mobile_number = o.mobile_number
            WHERE o.order_date_time >= :cutoff_date
            GROUP BY c.customer_id, c.customer_name, c.mobile_number, c.region
            ORDER BY total_spent DESC
            LIMIT :top_n
        """)
        
        df = pd.read_sql(
            query, 
            engine,
            params={
                'cutoff_date': cutoff_date,
                'top_n': top_n
            }
        )
        
        logger.info(
            f"Retrieved top {len(df)} customers for last {LAST_N_DAYS} days "
            f"(from {cutoff_date.strftime('%Y-%m-%d')} onwards)"
        )
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to get top customers: {str(e)}")
        raise


def get_all_kpis() -> dict:
    """
    Calculate all KPIs and return as dictionary
    
    Returns:
        Dictionary containing all KPI DataFrames
    """
    try:
        logger.info("Calculating all KPIs...")
        
        kpis = {
            'repeat_customers': get_repeat_customers(),
            'monthly_trends': get_monthly_order_trends(),
            'regional_revenue': get_regional_revenue(),
            'top_customers_last_30_days': get_top_customers_last_30_days()
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
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            kpis['repeat_customers'].to_excel(
                writer, sheet_name='Repeat Customers', index=False
            )
            kpis['monthly_trends'].to_excel(
                writer, sheet_name='Monthly Trends', index=False
            )
            kpis['regional_revenue'].to_excel(
                writer, sheet_name='Regional Revenue', index=False
            )
            kpis['top_customers_last_30_days'].to_excel(
                writer, sheet_name='Top Customers 30D', index=False
            )
        
        logger.info(f"KPIs exported to {output_file}")
        
    except Exception as e:
        logger.error(f"Failed to export KPIs: {str(e)}")
        raise
