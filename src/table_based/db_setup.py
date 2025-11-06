"""
Database Schema Definition
Defines SQLAlchemy ORM models for customers, orders, and order_items tables
"""

from sqlalchemy import Column, String, Float, Integer, DateTime, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

from src.table_based.db_connection import get_db_engine, create_database_if_not_exists
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Base class for all models
Base = declarative_base()


class Customer(Base):
    """
    Customer table model
    """
    __tablename__ = 'customers'
    
    customer_id = Column(String(50), primary_key=True, nullable=False)
    customer_name = Column(String(255), nullable=False)
    mobile_number = Column(String(15), nullable=False, unique=True, index=True)
    region = Column(String(100), nullable=False, index=True)
    
    # Relationship to orders
    orders = relationship("Order", back_populates="customer", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Customer(id={self.customer_id}, name={self.customer_name})>"


class Order(Base):
    """
    Order table model
    """
    __tablename__ = 'orders'
    
    order_id = Column(String(50), primary_key=True, nullable=False)
    mobile_number = Column(String(15), ForeignKey('customers.mobile_number'), nullable=False)
    order_date_time = Column(DateTime, nullable=False, index=True)
    total_amount = Column(Float, nullable=False)
    
    # Relationships
    customer = relationship("Customer", back_populates="orders")
    order_items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")
    
    # Index for date-based queries (for last 30 days KPI)
    __table_args__ = (
        Index('idx_order_date', 'order_date_time'),
    )
    
    def __repr__(self):
        return f"<Order(id={self.order_id}, amount={self.total_amount})>"


class OrderItem(Base):
    """
    Order items table model (line items with SKUs)
    """
    __tablename__ = 'order_items'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(String(50), ForeignKey('orders.order_id'), nullable=False)
    sku_id = Column(String(50), nullable=False)
    sku_count = Column(Integer, nullable=False)
    
    # Relationship to order
    order = relationship("Order", back_populates="order_items")
    
    # Composite index for faster lookups
    __table_args__ = (
        Index('idx_order_sku', 'order_id', 'sku_id'),
    )
    
    def __repr__(self):
        return f"<OrderItem(order={self.order_id}, sku={self.sku_id}, count={self.sku_count})>"


def create_tables(drop_existing=False):
    """
    Create all database tables
    
    Args:
        drop_existing: If True, drop existing tables before creating
    """
    try:
        # Ensure database exists
        create_database_if_not_exists()
        
        # Get engine
        engine = get_db_engine()
        
        # Drop tables if requested
        if drop_existing:
            logger.warning("Dropping existing tables...")
            Base.metadata.drop_all(engine)
            logger.info("Existing tables dropped")
        
        # Create all tables
        Base.metadata.create_all(engine)
        logger.info("Database tables created successfully")
        
        # Log table information
        tables = Base.metadata.tables.keys()
        logger.info(f"Tables in database: {list(tables)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create tables: {str(e)}")
        raise


def drop_tables():
    """
    Drop all database tables
    """
    try:
        engine = get_db_engine()
        Base.metadata.drop_all(engine)
        logger.info("All tables dropped successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to drop tables: {str(e)}")
        raise


def get_table_info():
    """
    Get information about existing tables
    
    Returns:
        dict: Table information
    """
    try:
        engine = get_db_engine()
        from sqlalchemy import text
        
        with engine.connect() as conn:
            # Get table names
            tables_result = conn.execute(
                text(f"SELECT TABLE_NAME FROM information_schema.TABLES "
                     f"WHERE TABLE_SCHEMA = :db_name"),
                {"db_name": engine.url.database}
            )
            tables = [row[0] for row in tables_result]
            
            # Get row counts
            table_info = {}
            for table in tables:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = count_result.fetchone()[0]
                table_info[table] = {'row_count': count}
            
            return table_info
            
    except Exception as e:
        logger.error(f"Failed to get table info: {str(e)}")
        return {}
