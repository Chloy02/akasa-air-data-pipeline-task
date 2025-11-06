"""
Data Ingestion Module
Loads data from CSV/XML into MySQL database using parameterized queries
"""

from sqlalchemy.dialects.mysql import insert
from sqlalchemy import text
import pandas as pd

from src.utils.data_loader import load_all_data
from src.table_based.db_setup import Customer, Order, OrderItem
from src.table_based.db_connection import get_db_engine
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def ingest_customers(customers_df: pd.DataFrame) -> int:
    """
    Ingest customer data into database using parameterized queries
    
    Args:
        customers_df: DataFrame with customer data
        
    Returns:
        Number of records inserted/updated
    """
    try:
        engine = get_db_engine()
        records_processed = 0
        
        with engine.begin() as conn:
            for _, row in customers_df.iterrows():
                # Using INSERT ... ON DUPLICATE KEY UPDATE for upsert
                stmt = insert(Customer).values(
                    customer_id=row['customer_id'],
                    customer_name=row['customer_name'],
                    mobile_number=row['mobile_number'],
                    region=row['region']
                )
                
                # Update on duplicate key
                stmt = stmt.on_duplicate_key_update(
                    customer_name=stmt.inserted.customer_name,
                    region=stmt.inserted.region
                )
                
                conn.execute(stmt)
                records_processed += 1
        
        logger.info(f"Ingested {records_processed} customer records")
        return records_processed
        
    except Exception as e:
        logger.error(f"Failed to ingest customers: {str(e)}")
        raise


def ingest_orders(orders_df: pd.DataFrame, order_items_df: pd.DataFrame) -> tuple:
    """
    Ingest orders and order items into database using parameterized queries
    
    Args:
        orders_df: DataFrame with order data
        order_items_df: DataFrame with order items data
        
    Returns:
        Tuple of (orders_count, items_count)
    """
    try:
        engine = get_db_engine()
        orders_processed = 0
        items_processed = 0
        
        with engine.begin() as conn:
            # Ingest orders
            for _, row in orders_df.iterrows():
                stmt = insert(Order).values(
                    order_id=row['order_id'],
                    mobile_number=row['mobile_number'],
                    order_date_time=row['order_date_time'],
                    total_amount=row['total_amount']
                )
                
                # Update on duplicate key
                stmt = stmt.on_duplicate_key_update(
                    order_date_time=stmt.inserted.order_date_time,
                    total_amount=stmt.inserted.total_amount
                )
                
                conn.execute(stmt)
                orders_processed += 1
            
            # Delete existing order items for these orders (for clean re-ingestion)
            order_ids = orders_df['order_id'].tolist()
            if order_ids:
                # Using parameterized query for security
                conn.execute(
                    text("DELETE FROM order_items WHERE order_id IN :order_ids"),
                    {"order_ids": tuple(order_ids)}
                )
            
            # Ingest order items
            for _, row in order_items_df.iterrows():
                stmt = insert(OrderItem).values(
                    order_id=row['order_id'],
                    sku_id=row['sku_id'],
                    sku_count=row['sku_count']
                )
                
                conn.execute(stmt)
                items_processed += 1
        
        logger.info(f"Ingested {orders_processed} orders and {items_processed} order items")
        return orders_processed, items_processed
        
    except Exception as e:
        logger.error(f"Failed to ingest orders: {str(e)}")
        raise


def ingest_all_data() -> dict:
    """
    Load and ingest all data from CSV/XML files
    
    Returns:
        Dictionary with ingestion statistics
    """
    try:
        logger.info("Starting data ingestion process...")
        
        # Load data from files
        customers_df, orders_df, order_items_df = load_all_data()
        
        # Ingest customers
        customers_count = ingest_customers(customers_df)
        
        # Ingest orders and items
        orders_count, items_count = ingest_orders(orders_df, order_items_df)
        
        stats = {
            'customers': customers_count,
            'orders': orders_count,
            'order_items': items_count,
            'total_records': customers_count + orders_count + items_count
        }
        
        logger.info(f"Data ingestion completed: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}")
        raise


def clear_all_data():
    """
    Clear all data from tables (for testing purposes)
    """
    try:
        engine = get_db_engine()
        
        with engine.begin() as conn:
            # Delete in order due to foreign key constraints
            conn.execute(text("DELETE FROM order_items"))
            conn.execute(text("DELETE FROM orders"))
            conn.execute(text("DELETE FROM customers"))
        
        logger.info("All data cleared from database")
        
    except Exception as e:
        logger.error(f"Failed to clear data: {str(e)}")
        raise
