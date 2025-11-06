"""
Data Validation Module
Validates data integrity and business rules
"""

import pandas as pd
import re
from typing import List, Dict, Any
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def validate_customer_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate customer data for completeness and correctness
    
    Args:
        df: Customer DataFrame
        
    Returns:
        Dictionary with validation results
    """
    validation_results = {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'stats': {}
    }
    
    try:
        # Check required columns
        required_cols = ['customer_id', 'customer_name', 'mobile_number', 'region']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Missing required columns: {missing_cols}")
            return validation_results
        
        # Check for duplicates
        duplicates = df[df.duplicated(subset=['customer_id'], keep=False)]
        if not duplicates.empty:
            validation_results['warnings'].append(
                f"Found {len(duplicates)} duplicate customer_id records"
            )
        
        # Validate mobile number format (should be 10 digits)
        invalid_mobiles = df[~df['mobile_number'].str.match(r'^\d{10}$', na=False)]
        if not invalid_mobiles.empty:
            validation_results['warnings'].append(
                f"Found {len(invalid_mobiles)} invalid mobile number formats"
            )
        
        # Check for missing values
        null_counts = df[required_cols].isnull().sum()
        if null_counts.any():
            validation_results['warnings'].append(
                f"Missing values detected: {null_counts[null_counts > 0].to_dict()}"
            )
        
        # Statistics
        validation_results['stats'] = {
            'total_records': len(df),
            'unique_customers': df['customer_id'].nunique(),
            'regions': df['region'].unique().tolist()
        }
        
        logger.info(f"Customer data validation completed: {validation_results['stats']}")
        
    except Exception as e:
        validation_results['is_valid'] = False
        validation_results['errors'].append(f"Validation error: {str(e)}")
        logger.error(f"Error validating customer data: {str(e)}")
    
    return validation_results


def validate_order_data(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Validate order and order items data
    
    Args:
        orders_df: Orders DataFrame
        order_items_df: Order items DataFrame
        
    Returns:
        Dictionary with validation results
    """
    validation_results = {
        'is_valid': True,
        'errors': [],
        'warnings': [],
        'stats': {}
    }
    
    try:
        # Check required columns for orders
        required_order_cols = ['order_id', 'mobile_number', 'order_date_time', 'total_amount']
        missing_cols = [col for col in required_order_cols if col not in orders_df.columns]
        if missing_cols:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Missing required order columns: {missing_cols}")
            return validation_results
        
        # Check required columns for order items
        required_item_cols = ['order_id', 'sku_id', 'sku_count']
        missing_cols = [col for col in required_item_cols if col not in order_items_df.columns]
        if missing_cols:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Missing required item columns: {missing_cols}")
            return validation_results
        
        # Validate order_id consistency
        order_ids_in_orders = set(orders_df['order_id'].unique())
        order_ids_in_items = set(order_items_df['order_id'].unique())
        
        if order_ids_in_items - order_ids_in_orders:
            validation_results['warnings'].append(
                "Some order_ids in items are not in orders table"
            )
        
        # Check for negative amounts
        negative_amounts = orders_df[orders_df['total_amount'] < 0]
        if not negative_amounts.empty:
            validation_results['warnings'].append(
                f"Found {len(negative_amounts)} orders with negative amounts"
            )
        
        # Check for invalid SKU counts
        invalid_counts = order_items_df[order_items_df['sku_count'] <= 0]
        if not invalid_counts.empty:
            validation_results['warnings'].append(
                f"Found {len(invalid_counts)} items with invalid SKU counts"
            )
        
        # Statistics
        validation_results['stats'] = {
            'total_orders': len(orders_df),
            'total_line_items': len(order_items_df),
            'unique_customers': orders_df['mobile_number'].nunique(),
            'date_range': f"{orders_df['order_date_time'].min()} to {orders_df['order_date_time'].max()}",
            'total_revenue': float(orders_df['total_amount'].sum())
        }
        
        logger.info(f"Order data validation completed: {validation_results['stats']}")
        
    except Exception as e:
        validation_results['is_valid'] = False
        validation_results['errors'].append(f"Validation error: {str(e)}")
        logger.error(f"Error validating order data: {str(e)}")
    
    return validation_results
