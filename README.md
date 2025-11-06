# Akasa Air Data Engineering Pipeline

## Overview
A comprehensive data pipeline solution for processing customer and order data from multiple sources (CSV and XML) using both database-based and in-memory approaches. This project demonstrates ETL pipeline implementation with dual processing methodologies for business intelligence and analytics.

## Project Structure
```
AkasaAir-DataEngineer-Task1/
├── data/                           # Source data files
│   ├── task_DE_new_customers.csv
│   └── task_DE_new_orders.xml
├── src/                            # Source code
│   ├── config.py                   # Configuration management
│   ├── utils/                      # Utility modules
│   │   ├── logger.py              # Logging setup
│   │   ├── data_loader.py         # CSV/XML parsers
│   │   └── data_validator.py      # Data validation
│   ├── table_based/               # MySQL database approach
│   │   ├── db_connection.py       # Database connection
│   │   ├── db_setup.py            # Schema definition
│   │   ├── data_ingestion.py     # Data loading
│   │   └── kpi_queries.py         # SQL KPI queries
│   └── in_memory/                 # Pandas in-memory approach
│       ├── data_processor.py      # DataFrame operations
│       └── kpi_calculator.py      # KPI calculations
├── logs/                          # Application logs
├── results/                       # KPI output files
├── main.py                        # Main orchestrator script
├── test_table_based.py           # Table-based test script
├── test_in_memory.py             # In-memory test script
├── requirements.txt              # Python dependencies
├── .env.example                  # Environment variables template
└── README.md                     # This file
```

## Features
- Dual processing approaches (MySQL Database + Pandas DataFrames)
- Daily incremental data ingestion capability
- 4 Key Business KPIs calculation
- Secure credential management with environment variables
- Comprehensive error handling and logging
- Timezone-aware date handling (Asia/Kolkata)
- Normalized database schema with proper indexing
- Modular and maintainable code structure

## KPIs Calculated

1. **Repeat Customers**: Customers with more than one order
2. **Monthly Order Trends**: Order aggregation by month with revenue metrics
3. **Regional Revenue**: Total revenue and statistics by customer region
4. **Top Customers (Last 30 Days)**: Top spenders in the last 30 days

## Prerequisites

- Python 3.8 or higher
- MySQL 8.0 or higher
- pip package manager
- Virtual environment (recommended)

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/Chloy02/akasa-air-data-pipeline-task.git
cd akasa-air-data-pipeline-task
```

### 2. Create Virtual Environment
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Set Up MySQL Database

Ensure MySQL is running on your system. Create the database:

```bash
mysql -u root -p
```

```sql
CREATE DATABASE akasa_air_pipeline_task CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

The application will automatically create the required tables on first run.

### 5. Configure Environment Variables

Copy the example environment file and configure with your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your MySQL credentials:
```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=akasa_air_pipeline_task

LOG_LEVEL=INFO
TIMEZONE=Asia/Kolkata

CUSTOMERS_CSV_PATH=data/task_DE_new_customers.csv
ORDERS_XML_PATH=data/task_DE_new_orders.xml
RESULTS_DIR=results
```

## Usage

### Run Complete Pipeline (Both Approaches)

```bash
python main.py
```

This will:
1. Load and validate data from CSV and XML files
2. Execute table-based approach (MySQL)
3. Execute in-memory approach (Pandas)
4. Generate KPI results for both approaches
5. Export results to Excel files in `results/` directory

### Run Individual Approaches

#### Table-Based Approach Only
```bash
python test_table_based.py
```

Output: `results/table_based_kpis.xlsx`

#### In-Memory Approach Only
```bash
python test_in_memory.py
```

Output: `results/in_memory_kpis.xlsx`

## Architecture

### Data Flow
```
CSV (Customers) ──┐
                  ├──> Data Loading & Validation
XML (Orders) ─────┘           |
                               ├──> Table-Based (MySQL)
                               |    - Normalized tables
                               |    - SQL queries
                               |    - Indexed joins
                               |
                               └──> In-Memory (Pandas)
                                    - DataFrames
                                    - Groupby operations
                                    - Merge & filter
                                    
                    Results exported to Excel
```

### Table-Based Approach (MySQL)

**Database Schema:**
- `customers`: customer_id (PK), customer_name, mobile_number (UNIQUE), region
- `orders`: order_id (PK), mobile_number (FK), order_date_time, total_amount
- `order_items`: id (PK), order_id (FK), sku_id, sku_count

**Features:**
- Normalized schema (3NF)
- Indexed foreign keys and date columns
- Parameterized queries using SQLAlchemy ORM
- Connection pooling for performance
- Transaction management with rollback

### In-Memory Approach (Pandas)

**Processing:**
- Load data into DataFrames
- Keep 3 separate DataFrames (customers, orders, order_items)
- Merge as needed per KPI calculation
- Use groupby, merge, and datetime operations

**Features:**
- Modular KPI functions
- Timezone-aware datetime handling
- Memory-efficient operations
- Excel export with multiple sheets

## Security Measures

1. **Credential Management**
   - Database credentials stored in `.env` file (excluded from version control)
   - No hardcoded credentials in source code
   - Environment variable validation on startup

2. **SQL Injection Prevention**
   - Parameterized queries using SQLAlchemy ORM
   - No string concatenation in SQL statements
   - Use of prepared statements

3. **Data Validation**
   - Input validation for CSV and XML files
   - Type checking and consistency validation
   - Error handling with detailed logging

## Data Processing

### Data Cleaning
- Handle missing values in customer names and regions
- Validate mobile number format (10 digits)
- Normalize datetime to timezone-aware (Asia/Kolkata)
- Ensure data type consistency
- Remove duplicate records

### Error Handling
- Comprehensive try-except blocks
- Detailed logging to `logs/` directory
- Graceful failure with informative error messages
- Transaction rollback on database errors

## Testing

Run the data loading test:
```bash
python test_data_loading.py
```

This validates:
- CSV and XML parsing
- Data validation rules
- Timezone conversion
- Missing value handling

## Results

KPI results are exported to Excel files in `results/` directory:
- `table_based_kpis.xlsx`: Results from MySQL approach
- `in_memory_kpis.xlsx`: Results from Pandas approach

Each Excel file contains 4 sheets:
1. Repeat Customers
2. Monthly Trends
3. Regional Revenue
4. Top Customers 30D

## Dependencies

See `requirements.txt` for full list:
- pandas: Data manipulation
- pymysql: MySQL database driver
- sqlalchemy: ORM and database toolkit
- python-dotenv: Environment variable management
- lxml: XML parsing
- pytz: Timezone handling
- openpyxl: Excel file operations

## Logging

Logs are stored in `logs/` directory with daily rotation:
- File: `logs/pipeline_YYYYMMDD.log`
- Console output: INFO level and above
- File output: DEBUG level and above

## Future Enhancements

Given more time, the following improvements would be considered:

1. **Data Pipeline Automation**
   - Implement Apache Airflow for workflow orchestration
   - Schedule daily automated runs
   - Add data quality checks and alerts

2. **Performance Optimization**
   - Implement data partitioning for large datasets
   - Add caching layer (Redis) for frequently accessed data
   - Optimize SQL queries with query execution plans

3. **Advanced Analytics**
   - Add predictive analytics for customer behavior
   - Implement cohort analysis for customer retention
   - Create interactive dashboards with visualization tools

4. **Monitoring and Alerting**
   - Add Prometheus metrics for pipeline monitoring
   - Implement Grafana dashboards for visualization
   - Set up alerts for pipeline failures or data quality issues

5. **Data Quality Framework**
   - Implement Great Expectations for data validation
   - Add data profiling and anomaly detection
   - Create data lineage tracking

6. **Scalability Improvements**
   - Migrate to distributed processing with Apache Spark
   - Implement data lake architecture (S3/HDFS)
   - Add horizontal scaling for database

7. **Security Enhancements**
   - Implement row-level security in database
   - Add data encryption at rest and in transit
   - Integrate with secret management services (AWS Secrets Manager, HashiCorp Vault)

8. **Testing and CI/CD**
   - Comprehensive unit and integration tests
   - GitHub Actions for automated testing
   - Code coverage reporting
   - Automated deployment pipeline

## Contributing

This is a demonstration project for Akasa Air hiring process.

## License

This project is created for evaluation purposes.

## Contact

Created by: Chloy Costa
Repository: https://github.com/Chloy02/akasa-air-data-pipeline-task
