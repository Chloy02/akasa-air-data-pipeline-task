"""
Test script to verify data loading and validation
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.data_loader import load_all_data
from src.utils.data_validator import validate_customer_data, validate_order_data
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def test_data_loading():
    """Test data loading functionality"""
    logger.info("=" * 60)
    logger.info("TESTING DATA LOADING AND VALIDATION")
    logger.info("=" * 60)
    
    try:
        # Load all data
        customers_df, orders_df, order_items_df = load_all_data()
        
        # Display data samples
        logger.info("\n--- CUSTOMERS DATA (Sample) ---")
        print(customers_df.head())
        print(f"\nShape: {customers_df.shape}")
        
        logger.info("\n--- ORDERS DATA (Sample) ---")
        print(orders_df.head())
        print(f"\nShape: {orders_df.shape}")
        
        logger.info("\n--- ORDER ITEMS DATA (Sample) ---")
        print(order_items_df.head(10))
        print(f"\nShape: {order_items_df.shape}")
        
        # Validate data
        logger.info("\n" + "=" * 60)
        logger.info("VALIDATING DATA")
        logger.info("=" * 60)
        
        customer_validation = validate_customer_data(customers_df)
        order_validation = validate_order_data(orders_df, order_items_df)
        
        # Display validation results
        logger.info("\n--- CUSTOMER DATA VALIDATION ---")
        print(f"Valid: {customer_validation['is_valid']}")
        print(f"Errors: {customer_validation['errors']}")
        print(f"Warnings: {customer_validation['warnings']}")
        print(f"Stats: {customer_validation['stats']}")
        
        logger.info("\n--- ORDER DATA VALIDATION ---")
        print(f"Valid: {order_validation['is_valid']}")
        print(f"Errors: {order_validation['errors']}")
        print(f"Warnings: {order_validation['warnings']}")
        print(f"Stats: {order_validation['stats']}")
        
        logger.info("\n" + "=" * 60)
        logger.info("DATA LOADING TEST COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_data_loading()
    sys.exit(0 if success else 1)
