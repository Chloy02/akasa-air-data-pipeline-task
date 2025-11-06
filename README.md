# Akasa Air Data Engineering Pipeline

## Overview
A comprehensive data pipeline solution for processing customer and order data from multiple sources (CSV and XML) using both database-based and in-memory approaches.

## Features
- ✅ Dual processing approaches (MySQL Database + In-Memory Pandas)
- ✅ Daily incremental data ingestion
- ✅ 4 Key Business KPIs calculation
- ✅ Secure credential management
- ✅ Comprehensive error handling and logging
- ✅ Docker support for MySQL
- ✅ Timezone-aware date handling (IST)

## Architecture

```
CSV (Customers) ──┐
                  ├──> Data Ingestion ──> Data Validation
XML (Orders) ─────┘                              │
                                                  ├──> MySQL Database (SQL Queries)
                                                  │
                                                  └──> Pandas DataFrames (In-Memory)
                                                                │
                                                                ├──> KPI Calculations
                                                                └──> Results Export
```

## KPIs Calculated

1. **Repeat Customers**: Customers with more than one order
2. **Monthly Order Trends**: Order aggregation by month
3. **Regional Revenue**: Total revenue by region
4. **Top Customers (Last 30 Days)**: Top spenders in the last 30 days

## Prerequisites

- Python 3.8+
- MySQL 8.0+ (or Docker)
- pip package manager

## Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/akasa-data-pipeline.git
cd akasa-data-pipeline
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Set Up MySQL Database

#### Option A: Using Docker (Recommended)
```bash
docker-compose up -d
```

#### Option B: Local MySQL Installation
```bash
mysql -u root -p
CREATE DATABASE akasa_db;
```

### 5. Configure Environment Variables
```bash
cp config/.env.example config/.env
# Edit config/.env with your database credentials
```

## Configuration

Edit `config/.env`:
```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=akasa_user
DB_PASSWORD=your_secure_password
DB_NAME=akasa_db
```

## Usage

### Run the Complete Pipeline
```bash
python main.py
```

### Run Specific Approach Only

#### Database Approach Only
```bash
python main.py --approach database
```

#### In-Memory Approach Only
```bash
python main.py --approach inmemory
```

### View Results
Results are saved in the `output/` directory:
- `kpi_results_database_YYYYMMDD_HHMMSS.csv`
- `kpi_results_inmemory_YYYYMMDD_HHMMSS.csv`

## Project Structure

```
akasa-data-pipeline/
├── config/
│   ├── .env.example              # Template for environment variables
│   └── db_config.py               # Database configuration
├── data/
│   ├── customers.csv              # Customer data (CSV)
│   └── orders.xml                 # Order data (XML)
├── src/
│   ├── __init__.py
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── csv_parser.py          # CSV parsing logic
│   │   └── xml_parser.py          # XML parsing logic
│   ├── database/
│   │   ├── __init__.py
│   │   ├── db_setup.py            # Database schema creation
│   │   ├── db_loader.py           # Data loading into MySQL
│   │   └── db_queries.py          # SQL queries for KPIs
│   ├── inmemory/
│   │   ├── __init__.py
│   │   └── kpi_calculator.py      # Pandas-based KPI calculations
│   └── utils/
│       ├── __init__.py
│       ├── logger.py              # Logging configuration
│       └── validators.py          # Data validation utilities
├── output/                        # Generated KPI results
├── logs/                          # Application logs
├── docker-compose.yml             # Docker MySQL setup
├── requirements.txt               # Python dependencies
├── main.py                        # Main orchestrator
└── README.md                      # This file
```

## Data Schema

### Customers Table (CSV)
| Field | Type | Description |
|-------|------|-------------|
| customer_id | VARCHAR(20) | Unique customer identifier |
| customer_name | VARCHAR(100) | Customer name |
| mobile_number | VARCHAR(15) | Mobile number (unique) |
| region | VARCHAR(50) | Geographic region |

### Orders Table (XML)
| Field | Type | Description |
|-------|------|-------------|
| order_id | VARCHAR(20) | Order identifier |
| mobile_number | VARCHAR(15) | Customer mobile number |
| order_date_time | DATETIME | Order timestamp |
| sku_id | VARCHAR(20) | SKU identifier |
| sku_count | INT | Quantity ordered |
| total_amount | DECIMAL(10,2) | Order total amount |

**Note**: Multiple SKU items can share the same `order_id`. The `total_amount` represents the total order value (counted once per order_id).

## Security Features

- ✅ Environment variables for sensitive credentials
- ✅ Parameterized SQL queries (SQLAlchemy ORM)
- ✅ No hardcoded credentials in code
- ✅ .gitignore for sensitive files
- ✅ SQL injection prevention

## Error Handling

- Comprehensive try-catch blocks
- Detailed logging to `logs/pipeline.log`
- Graceful failure with informative error messages
- Data validation before processing

## Testing

Run validation tests:
```bash
python -m pytest tests/
```

## Performance Considerations

- Database indexes on mobile_number and order_date_time
- Efficient SQL joins and aggregations
- Pandas groupby operations for in-memory processing
- Batch inserts for database loading

## Troubleshooting

### MySQL Connection Error
```bash
# Check MySQL is running
docker ps  # If using Docker
mysql -u root -p  # If local installation

# Verify credentials in config/.env
```

### Missing Dependencies
```bash
pip install -r requirements.txt --upgrade
```

### Permission Errors
```bash
chmod +x main.py
```

## Future Enhancements

Given more time, the following improvements would be considered:

1. **Data Quality**: Implement comprehensive data quality checks and anomaly detection
2. **Incremental Processing**: Add change data capture (CDC) for efficient daily updates
3. **Scheduling**: Integrate with Apache Airflow for automated daily runs
4. **Monitoring**: Add Prometheus/Grafana for pipeline monitoring
5. **Data Versioning**: Implement data versioning for audit trails
6. **API Layer**: Build REST API for on-demand KPI queries
7. **Visualization**: Create interactive dashboards with Plotly/Streamlit
8. **Unit Tests**: Comprehensive test coverage with pytest
9. **CI/CD**: GitHub Actions for automated testing and deployment
10. **Scalability**: Migrate to distributed processing (PySpark) for large datasets

## License

Proprietary - Akasa Air Interview Task

## Contact

For questions or issues, please contact: [Your Name] - [Your Email]

---

**Note**: This project was developed as part of the Data Engineer interview process for Akasa Air.
