"""
Database Connection Module
Handles MySQL database connections using SQLAlchemy
"""

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from typing import Generator
import pymysql

from src.config import get_database_url, DB_CONFIG
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

# Global engine instance
_engine = None
_SessionLocal = None


def get_db_engine():
    """
    Get or create SQLAlchemy engine with connection pooling
    
    Returns:
        SQLAlchemy Engine instance
    """
    global _engine
    
    if _engine is None:
        try:
            database_url = get_database_url()
            
            # Create engine with connection pooling
            _engine = create_engine(
                database_url,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,  # Verify connections before using
                echo=False  # Set to True for SQL query logging
            )
            
            logger.info(f"Database engine created for {DB_CONFIG['database']}")
            
        except Exception as e:
            logger.error(f"Failed to create database engine: {str(e)}")
            raise
    
    return _engine


def get_session_factory():
    """
    Get or create SessionLocal factory
    
    Returns:
        SessionLocal factory
    """
    global _SessionLocal
    
    if _SessionLocal is None:
        engine = get_db_engine()
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=engine
        )
    
    return _SessionLocal


def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions
    
    Yields:
        Database session
        
    Example:
        with get_db_session() as session:
            session.execute(...)
    """
    SessionLocal = get_session_factory()
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {str(e)}")
        raise
    finally:
        session.close()


def create_database_if_not_exists():
    """
    Create database if it doesn't exist
    """
    try:
        # Connect without specifying database
        connection = pymysql.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']} "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            logger.info(f"Database '{DB_CONFIG['database']}' is ready")
        
        connection.close()
        
    except Exception as e:
        logger.error(f"Failed to create database: {str(e)}")
        raise


def test_connection():
    """
    Test database connection
    
    Returns:
        bool: True if connection successful
    """
    try:
        engine = get_db_engine()
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 as test"))
            logger.info("Database connection test successful")
            return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False


def close_engine():
    """
    Close database engine and cleanup connections
    """
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
        logger.info("Database engine closed")
