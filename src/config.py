"""
Configuration Management Module
Loads environment variables and provides centralized configuration
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import pytz

# Load environment variables from .env file
load_dotenv()

# Base Directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'akasa_air_pipeline')
}

# Application Settings
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
TIMEZONE = pytz.timezone(os.getenv('TIMEZONE', 'Asia/Kolkata'))

# Data File Paths
CUSTOMERS_CSV_PATH = BASE_DIR / os.getenv('CUSTOMERS_CSV_PATH', 'data/task_DE_new_customers.csv')
ORDERS_XML_PATH = BASE_DIR / os.getenv('ORDERS_XML_PATH', 'data/task_DE_new_orders.xml')

# Directories
LOGS_DIR = BASE_DIR / 'logs'
RESULTS_DIR = BASE_DIR / os.getenv('RESULTS_DIR', 'results')

# Ensure directories exist
LOGS_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

# Database Connection String (SQLAlchemy format)
def get_database_url():
    """
    Generate SQLAlchemy database URL with credentials secured from environment
    """
    return (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

# KPI Settings
LAST_N_DAYS = 30  # For "Top Customers by Spend (Last 30 Days)" KPI
