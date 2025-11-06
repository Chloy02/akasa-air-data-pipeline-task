"""
Test script for in-memory (Pandas) approach
Tests data loading and KPI calculations using DataFrames
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.in_memory.data_processor import DataProcessor
from src.in_memory.kpi_calculator import calculate_all_kpis, export_kpis_to_excel
from src.config import RESULTS_DIR
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def test_in_memory_approach():
    """
    Complete test of in-memory approach using pandas
    """
    logger.info("=" * 70)
    logger.info("TESTING IN-MEMORY APPROACH (Pandas)")
    logger.info("=" * 70)
    
    try:
        # Step 1: Load data
        logger.info("\n--- Step 1: Loading Data into DataFrames ---")
        processor = DataProcessor()
        customers_df, orders_df, order_items_df = processor.load_data()
        print("✅ Data loaded into DataFrames")
        
        # Step 2: Display data summary
        logger.info("\n--- Step 2: Data Summary ---")
        summary = processor.get_data_summary()
        print("\nData Summary:")
        print(f"  Customers: {summary['customers']['count']} records")
        print(f"  Regions: {summary['customers']['regions']}")
        print(f"  Orders: {summary['orders']['count']} records")
        print(f"  Date Range: {summary['orders']['date_range']}")
        print(f"  Total Revenue: ₹{summary['orders']['total_revenue']:,.2f}")
        print(f"  Order Items: {summary['order_items']['count']} line items")
        print(f"  Unique SKUs: {summary['order_items']['unique_skus']}")
        
        # Step 3: Calculate KPIs
        logger.info("\n--- Step 3: Calculating KPIs ---")
        kpis = calculate_all_kpis(customers_df, orders_df, order_items_df)
        
        print("\n" + "=" * 70)
        print("KPI RESULTS")
        print("=" * 70)
        
        # KPI 1: Repeat Customers
        print("\n📊 KPI 1: REPEAT CUSTOMERS")
        print("-" * 70)
        if not kpis['repeat_customers'].empty:
            print(kpis['repeat_customers'].to_string(index=False))
        else:
            print("No repeat customers found")
        
        # KPI 2: Monthly Order Trends
        print("\n📊 KPI 2: MONTHLY ORDER TRENDS")
        print("-" * 70)
        print(kpis['monthly_trends'].to_string(index=False))
        
        # KPI 3: Regional Revenue
        print("\n📊 KPI 3: REGIONAL REVENUE")
        print("-" * 70)
        print(kpis['regional_revenue'].to_string(index=False))
        
        # KPI 4: Top Customers (Last 30 Days)
        print("\n📊 KPI 4: TOP CUSTOMERS (LAST 30 DAYS)")
        print("-" * 70)
        if not kpis['top_customers_last_30_days'].empty:
            print(kpis['top_customers_last_30_days'].to_string(index=False))
        else:
            print("No customers found in last 30 days")
        
        # Step 4: Export results
        logger.info("\n--- Step 4: Exporting Results ---")
        output_file = RESULTS_DIR / "in_memory_kpis.xlsx"
        export_kpis_to_excel(kpis, str(output_file))
        print(f"✅ Results exported to: {output_file}")
        
        print("\n" + "=" * 70)
        logger.info("IN-MEMORY APPROACH TEST COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_in_memory_approach()
    sys.exit(0 if success else 1)
