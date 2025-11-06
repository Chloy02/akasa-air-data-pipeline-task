"""
Data Loading Module
Handles parsing and loading of CSV and XML data files
"""

import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Tuple, List, Dict
from datetime import datetime
import pytz

from src.config import CUSTOMERS_CSV_PATH, ORDERS_XML_PATH, TIMEZONE
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def load_customers_csv(file_path: Path = CUSTOMERS_CSV_PATH) -> pd.DataFrame:
    """
    Load and parse customer data from CSV file
    
    Args:
        file_path: Path to customers CSV file
        
    Returns:
        DataFrame containing customer data
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        pd.errors.EmptyDataError: If CSV file is empty
    """
    try:
        logger.info(f"Loading customer data from {file_path}")
        
        # Read CSV with explicit dtype for mobile_number to avoid scientific notation
        df = pd.read_csv(
            file_path,
            dtype={'mobile_number': str}
        )
        
        # Basic cleaning
        df = df.dropna(subset=['customer_id', 'mobile_number'])  # Remove rows with missing critical fields
        df['customer_name'] = df['customer_name'].fillna('Unknown')  # Handle missing names
        df['region'] = df['region'].fillna('Unknown')  # Handle missing regions
        
        # Strip whitespace
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()
        
        logger.info(f"Successfully loaded {len(df)} customer records")
        return df
        
    except FileNotFoundError:
        logger.error(f"Customer CSV file not found at {file_path}")
        raise
    except Exception as e:
        logger.error(f"Error loading customer CSV: {str(e)}")
        raise


def load_orders_xml(file_path: Path = ORDERS_XML_PATH) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and parse order data from XML file
    
    Returns normalized data:
    - orders DataFrame: Unique orders with order-level info
    - order_items DataFrame: Individual SKU line items
    
    Args:
        file_path: Path to orders XML file
        
    Returns:
        Tuple of (orders_df, order_items_df)
        
    Raises:
        FileNotFoundError: If XML file doesn't exist
        ET.ParseError: If XML is malformed
    """
    try:
        logger.info(f"Loading order data from {file_path}")
        
        # Parse XML
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Extract all order records
        order_items_list = []
        orders_dict = {}  # To track unique orders
        
        for order_elem in root.findall('order'):
            order_id = order_elem.find('order_id').text.strip()
            mobile_number = order_elem.find('mobile_number').text.strip()
            order_date_time_str = order_elem.find('order_date_time').text.strip()
            sku_id = order_elem.find('sku_id').text.strip()
            sku_count = int(order_elem.find('sku_count').text.strip())
            total_amount = float(order_elem.find('total_amount').text.strip())
            
            # Parse and normalize datetime to timezone-aware (IST)
            order_date_time = datetime.fromisoformat(order_date_time_str)
            if order_date_time.tzinfo is None:
                order_date_time = TIMEZONE.localize(order_date_time)
            
            # Store unique order (order_id is the key)
            if order_id not in orders_dict:
                orders_dict[order_id] = {
                    'order_id': order_id,
                    'mobile_number': mobile_number,
                    'order_date_time': order_date_time,
                    'total_amount': total_amount
                }
            
            # Store order item (SKU line item)
            order_items_list.append({
                'order_id': order_id,
                'sku_id': sku_id,
                'sku_count': sku_count
            })
        
        # Create DataFrames
        orders_df = pd.DataFrame(list(orders_dict.values()))
        order_items_df = pd.DataFrame(order_items_list)
        
        logger.info(f"Successfully loaded {len(orders_df)} orders with {len(order_items_df)} line items")
        return orders_df, order_items_df
        
    except FileNotFoundError:
        logger.error(f"Orders XML file not found at {file_path}")
        raise
    except ET.ParseError as e:
        logger.error(f"XML parsing error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading orders XML: {str(e)}")
        raise


def load_all_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Convenience function to load all data sources
    
    Returns:
        Tuple of (customers_df, orders_df, order_items_df)
    """
    customers_df = load_customers_csv()
    orders_df, order_items_df = load_orders_xml()
    
    return customers_df, orders_df, order_items_df
