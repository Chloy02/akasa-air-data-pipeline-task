"""
Main orchestrator script for Akasa Air Data Engineering Pipeline.
Runs both table-based and in-memory approaches and compares results.
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.config import RESULTS_DIR
from src.utils.logger import setup_logger
from src.utils.data_loader import load_customers_csv, load_orders_xml

# Table-based imports
from src.table_based.db_connection import get_db_engine
from src.table_based.db_setup import create_tables
from src.table_based.data_ingestion import ingest_customers, ingest_orders
from src.table_based.kpi_queries import (
    get_repeat_customers,
    get_monthly_order_trends,
    get_regional_revenue,
    get_top_customers_last_30_days
)

# In-memory imports
from src.in_memory.data_processor import DataProcessor
from src.in_memory.kpi_calculator import (
    calculate_repeat_customers,
    calculate_monthly_trends,
    calculate_regional_revenue,
    calculate_top_customers_last_30_days
)

# Initialize logger
logger = setup_logger('main')


def run_table_based_approach():
    """
    Execute table-based approach using MySQL database.
    
    Returns:
        dict: Dictionary containing KPI DataFrames
    """
    logger.info("=" * 80)
    logger.info("STARTING TABLE-BASED APPROACH (MySQL)")
    logger.info("=" * 80)
    
    try:
        # Load data
        logger.info("Loading data from CSV and XML files...")
        customers_df = load_customers_csv()
        orders_df, order_items_df = load_orders_xml()
        logger.info(f"Loaded {len(customers_df)} customers, {len(orders_df)} orders, {len(order_items_df)} order items")
        
        # Setup database
        logger.info("Setting up database connection and tables...")
        engine = get_db_engine()
        create_tables(engine)
        logger.info("Database tables created/verified successfully")
        
        # Ingest data
        logger.info("Ingesting data into MySQL database...")
        ingest_customers(customers_df)
        ingest_orders(orders_df, order_items_df)
        logger.info("Data ingestion completed successfully")
        
        # Calculate KPIs
        logger.info("Calculating KPIs using SQL queries...")
        kpis = {}
        
        kpis['repeat_customers'] = get_repeat_customers()
        logger.info(f"Repeat Customers: {len(kpis['repeat_customers'])} customers found")
        
        kpis['monthly_trends'] = get_monthly_order_trends()
        logger.info(f"Monthly Trends: {len(kpis['monthly_trends'])} months analyzed")
        
        kpis['regional_revenue'] = get_regional_revenue()
        logger.info(f"Regional Revenue: {len(kpis['regional_revenue'])} regions analyzed")
        
        kpis['top_customers_30d'] = get_top_customers_last_30_days()
        logger.info(f"Top Customers (30d): {len(kpis['top_customers_30d'])} customers listed")
        
        # Export to Excel
        output_file = os.path.join(RESULTS_DIR, 'table_based_kpis.xlsx')
        logger.info(f"Exporting results to {output_file}...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            kpis['repeat_customers'].to_excel(writer, sheet_name='Repeat Customers', index=False)
            kpis['monthly_trends'].to_excel(writer, sheet_name='Monthly Trends', index=False)
            kpis['regional_revenue'].to_excel(writer, sheet_name='Regional Revenue', index=False)
            kpis['top_customers_30d'].to_excel(writer, sheet_name='Top Customers 30D', index=False)
        
        logger.info("Table-based approach completed successfully")
        logger.info("=" * 80)
        
        return kpis
        
    except Exception as e:
        logger.error(f"Error in table-based approach: {str(e)}", exc_info=True)
        raise


def run_in_memory_approach():
    """
    Execute in-memory approach using Pandas DataFrames.
    
    Returns:
        dict: Dictionary containing KPI DataFrames
    """
    logger.info("=" * 80)
    logger.info("STARTING IN-MEMORY APPROACH (Pandas)")
    logger.info("=" * 80)
    
    try:
        # Initialize processor and load data
        logger.info("Initializing data processor and loading data...")
        processor = DataProcessor()
        processor.load_data()
        logger.info(f"Loaded {len(processor.customers_df)} customers, {len(processor.orders_df)} orders, {len(processor.order_items_df)} order items")
        
        # Calculate KPIs
        logger.info("Calculating KPIs using Pandas operations...")
        kpis = {}
        
        kpis['repeat_customers'] = calculate_repeat_customers(
            processor.customers_df, 
            processor.orders_df
        )
        logger.info(f"Repeat Customers: {len(kpis['repeat_customers'])} customers found")
        
        kpis['monthly_trends'] = calculate_monthly_trends(
            processor.orders_df, 
            processor.order_items_df
        )
        logger.info(f"Monthly Trends: {len(kpis['monthly_trends'])} months analyzed")
        
        kpis['regional_revenue'] = calculate_regional_revenue(
            processor.customers_df, 
            processor.orders_df
        )
        logger.info(f"Regional Revenue: {len(kpis['regional_revenue'])} regions analyzed")
        
        kpis['top_customers_30d'] = calculate_top_customers_last_30_days(
            processor.customers_df, 
            processor.orders_df
        )
        logger.info(f"Top Customers (30d): {len(kpis['top_customers_30d'])} customers listed")
        
        # Export to Excel
        output_file = os.path.join(RESULTS_DIR, 'in_memory_kpis.xlsx')
        logger.info(f"Exporting results to {output_file}...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Convert timezone-aware columns to naive for Excel
            for kpi_name, df in kpis.items():
                df_copy = df.copy()
                for col in df_copy.select_dtypes(include=['datetime64[ns, Asia/Kolkata]']).columns:
                    df_copy[col] = df_copy[col].dt.tz_localize(None)
                df_copy.to_excel(writer, sheet_name=kpi_name.replace('_', ' ').title()[:31], index=False)
        
        logger.info("In-memory approach completed successfully")
        logger.info("=" * 80)
        
        return kpis
        
    except Exception as e:
        logger.error(f"Error in in-memory approach: {str(e)}", exc_info=True)
        raise


def compare_results(table_kpis, memory_kpis):
    """
    Compare results from both approaches to verify consistency.
    
    Args:
        table_kpis (dict): KPIs from table-based approach
        memory_kpis (dict): KPIs from in-memory approach
    """
    logger.info("=" * 80)
    logger.info("COMPARING RESULTS FROM BOTH APPROACHES")
    logger.info("=" * 80)
    
    comparison_summary = []
    
    kpi_mapping = {
        'repeat_customers': 'Repeat Customers',
        'monthly_trends': 'Monthly Trends',
        'regional_revenue': 'Regional Revenue',
        'top_customers_30d': 'Top Customers 30D'
    }
    
    all_match = True
    
    for key, name in kpi_mapping.items():
        table_df = table_kpis[key]
        memory_df = memory_kpis[key]
        
        # Compare row counts
        table_count = len(table_df)
        memory_count = len(memory_df)
        
        if table_count == memory_count:
            status = "MATCH"
            logger.info(f"{name}: {table_count} rows (Both approaches agree)")
        else:
            status = "MISMATCH"
            all_match = False
            logger.warning(f"{name}: Table={table_count} rows, Memory={memory_count} rows (MISMATCH)")
        
        comparison_summary.append({
            'KPI': name,
            'Table-Based Rows': table_count,
            'In-Memory Rows': memory_count,
            'Status': status
        })
    
    # Create comparison summary DataFrame
    summary_df = pd.DataFrame(comparison_summary)
    
    # Save comparison to file
    comparison_file = os.path.join(RESULTS_DIR, 'comparison_summary.xlsx')
    summary_df.to_excel(comparison_file, index=False)
    logger.info(f"Comparison summary saved to {comparison_file}")
    
    logger.info("=" * 80)
    if all_match:
        logger.info("RESULT: All KPIs match between both approaches!")
    else:
        logger.warning("RESULT: Some KPIs have mismatches - please review")
    logger.info("=" * 80)
    
    return summary_df


def main():
    """
    Main function to orchestrate the complete pipeline.
    """
    logger.info("=" * 80)
    logger.info("AKASA AIR DATA ENGINEERING PIPELINE")
    logger.info(f"Execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    try:
        # Ensure results directory exists
        os.makedirs(RESULTS_DIR, exist_ok=True)
        
        # Run table-based approach
        table_kpis = run_table_based_approach()
        
        # Run in-memory approach
        memory_kpis = run_in_memory_approach()
        
        # Compare results
        comparison_df = compare_results(table_kpis, memory_kpis)
        
        logger.info("=" * 80)
        logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        logger.info(f"Execution finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 80)
        
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print("\nResults exported to:")
        print(f"  - Table-based: {os.path.join(RESULTS_DIR, 'table_based_kpis.xlsx')}")
        print(f"  - In-memory: {os.path.join(RESULTS_DIR, 'in_memory_kpis.xlsx')}")
        print(f"  - Comparison: {os.path.join(RESULTS_DIR, 'comparison_summary.xlsx')}")
        print("\nComparison Summary:")
        print(comparison_df.to_string(index=False))
        print("=" * 80 + "\n")
        
    except Exception as e:
        logger.error("Pipeline execution failed", exc_info=True)
        print(f"\nERROR: Pipeline execution failed - {str(e)}")
        print("Check logs for detailed error information")
        sys.exit(1)


if __name__ == "__main__":
    main()
