"""
Data Processor Module
Handles in-memory data processing using pandas DataFrames
"""

import pandas as pd
from typing import Tuple

from src.utils.data_loader import load_all_data
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class DataProcessor:
    """
    In-memory data processor using pandas DataFrames
    """
    
    def __init__(self):
        """Initialize the data processor"""
        self.customers_df = None
        self.orders_df = None
        self.order_items_df = None
        self._loaded = False
    
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Load data from CSV/XML files into DataFrames
        
        Returns:
            Tuple of (customers_df, orders_df, order_items_df)
        """
        try:
            logger.info("Loading data into DataFrames...")
            
            # Reuse existing data loaders (already handles cleaning, validation, timezone)
            self.customers_df, self.orders_df, self.order_items_df = load_all_data()
            
            self._loaded = True
            
            logger.info(
                f"Data loaded successfully: "
                f"{len(self.customers_df)} customers, "
                f"{len(self.orders_df)} orders, "
                f"{len(self.order_items_df)} order items"
            )
            
            return self.customers_df, self.orders_df, self.order_items_df
            
        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}")
            raise
    
    def get_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Get loaded DataFrames (load if not already loaded)
        
        Returns:
            Tuple of (customers_df, orders_df, order_items_df)
        """
        if not self._loaded:
            self.load_data()
        
        return self.customers_df, self.orders_df, self.order_items_df
    
    def get_data_summary(self) -> dict:
        """
        Get summary statistics of loaded data
        
        Returns:
            Dictionary with data summary
        """
        if not self._loaded:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        summary = {
            'customers': {
                'count': len(self.customers_df),
                'regions': self.customers_df['region'].unique().tolist(),
                'columns': self.customers_df.columns.tolist()
            },
            'orders': {
                'count': len(self.orders_df),
                'date_range': f"{self.orders_df['order_date_time'].min()} to {self.orders_df['order_date_time'].max()}",
                'total_revenue': float(self.orders_df['total_amount'].sum()),
                'columns': self.orders_df.columns.tolist()
            },
            'order_items': {
                'count': len(self.order_items_df),
                'unique_skus': self.order_items_df['sku_id'].nunique(),
                'columns': self.order_items_df.columns.tolist()
            }
        }
        
        return summary
