# Migkit Data Migration Tool

## Overview
Migkit is a data migration tool designed to facilitate the transfer of data from a source database to a target database. It uses SQLAlchemy for database connections and Pandas for data manipulation.

## Features
- **Data Extraction**: Fetch data from the source database.
- **Data Transformation**: Transform data using Pandas.
- **Data Loading**: Load transformed data into the target database.

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/migkit.git
   cd migkit
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage Examples

### 1. Basic Configuration
Create a configuration file `application-dev.yml`:
```yaml
source_db_uri: "mysql+pymysql://user:password@localhost/source_db"
target_db_uri: "postgresql+psycopg2://user:password@localhost/target_db"
```

### 2. Simple Data Migration
```python
from data_migration import fetch_data_from_source, transform_data_with_pandas, save_data_to_target
from framework import Phase

# Execute data migration
data = fetch_data_from_source()
transformed_data = transform_data_with_pandas(data)
save_data_to_target(transformed_data)
```

### 3. Custom Transformation Example
```python
from data_migration import fetch_data_from_source, save_data_to_target
from framework import phase, Phase

@phase(Phase.TRANSFORM)
def custom_transform(data):
    # Example: Calculate average of numeric columns
    numeric_columns = data.select_dtypes(include=['int64', 'float64']).columns
    data['average'] = data[numeric_columns].mean(axis=1)
    return data

# Usage
data = fetch_data_from_source()
transformed_data = custom_transform(data)
save_data_to_target(transformed_data)
```

### 4. Running the Migration
```bash
# Using default profile (dev)
python run_migration.py

# Using specific profile
PROFILE=prod python run_migration.py
```

## Configuration Guide
- Use `PROFILE` environment variable to specify configuration file (default: `dev`)
- Supported configuration file format: `application-{profile}.yml`
- Database URI formats:
  - MySQL: `mysql+pymysql://user:password@host:port/database`
  - PostgreSQL: `postgresql+psycopg2://user:password@host:port/database`

## Best Practices
1. Always use environment variables for sensitive information
2. Implement proper error handling in transformation functions
3. Use appropriate batch sizes for large datasets
4. Test migrations with a subset of data first
5. Keep transformation logic modular and reusable