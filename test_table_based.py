"""
Test script for table-based (MySQL) approach
Tests database setup, data ingestion, and KPI queries
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.table_based.db_connection import (
    create_database_if_not_exists,
    test_connection,
    close_engine
)
from src.table_based.db_setup import create_tables, get_table_info
from src.table_based.data_ingestion import ingest_all_data
from src.table_based.kpi_queries import get_all_kpis, export_kpis_to_excel
from src.config import RESULTS_DIR
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def test_table_based_approach():
    """
    Complete test of table-based approach
    """
    logger.info("=" * 70)
    logger.info("TESTING TABLE-BASED APPROACH (MySQL)")
    logger.info("=" * 70)
    
    try:
        # Step 1: Test database connection
        logger.info("\n--- Step 1: Testing Database Connection ---")
        create_database_if_not_exists()
        if not test_connection():
            logger.error("Database connection failed!")
            return False
        print("✅ Database connection successful")
        
        # Step 2: Create tables
        logger.info("\n--- Step 2: Creating Database Tables ---")
        create_tables(drop_existing=True)
        print("✅ Tables created successfully")
        
        # Step 3: Ingest data
        logger.info("\n--- Step 3: Ingesting Data ---")
        stats = ingest_all_data()
        print(f"✅ Data ingested: {stats}")
        
        # Step 4: Verify data
        logger.info("\n--- Step 4: Verifying Data ---")
        table_info = get_table_info()
        print("\nTable Information:")
        for table, info in table_info.items():
            print(f"  {table}: {info['row_count']} rows")
        
        # Step 5: Calculate KPIs
        logger.info("\n--- Step 5: Calculating KPIs ---")
        kpis = get_all_kpis()
        
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
        
        # Step 6: Export results
        logger.info("\n--- Step 6: Exporting Results ---")
        output_file = RESULTS_DIR / "table_based_kpis.xlsx"
        export_kpis_to_excel(kpis, str(output_file))
        print(f"✅ Results exported to: {output_file}")
        
        print("\n" + "=" * 70)
        logger.info("TABLE-BASED APPROACH TEST COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        # Cleanup
        close_engine()


if __name__ == "__main__":
    success = test_table_based_approach()
    sys.exit(0 if success else 1)
