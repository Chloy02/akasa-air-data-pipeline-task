"""
Table-based (MySQL) processing modules
"""

from .db_connection import get_db_engine, get_db_session
from .db_setup import create_tables, drop_tables
from .data_ingestion import ingest_customers, ingest_orders
from .kpi_queries import (
    get_repeat_customers,
    get_monthly_order_trends,
    get_regional_revenue,
    get_top_customers_last_30_days
)

__all__ = [
    'get_db_engine',
    'get_db_session',
    'create_tables',
    'drop_tables',
    'ingest_customers',
    'ingest_orders',
    'get_repeat_customers',
    'get_monthly_order_trends',
    'get_regional_revenue',
    'get_top_customers_last_30_days'
]
