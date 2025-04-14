# Migkit - Data Migration Toolkit

A powerful Python-based toolkit for ETL (Extract, Transform, Load) operations using SQLAlchemy 2.0 and Pandas.

## Features

- Modern SQLAlchemy 2.0 ORM with type hints
- Advanced Pandas data processing capabilities
- Flexible ETL framework with phase annotations
- Support for both SQL and ORM-based operations
- Relationship handling between source and target models

## Project Structure

```
├── src/
│   ├── config.py                # Configuration settings
│   ├── framework.py             # ETL framework with phase system
│   ├── models.py                # SQLAlchemy 2.0 models
│   ├── data_migration.py        # ETL implementation with SQLAlchemy
│   ├── simple.py                # Basic ETL example
│   ├── simple_multi_source.py   # Multi-source ETL example
│   └── simple_pandas.py         # Pandas-based ETL example
├── application-dev.yml          # Development configuration
└── README.md                    # This file
```

## Requirements

- Python 3.10+
- SQLAlchemy 2.0+
- Pandas
- PyYAML
- Matplotlib (for visualization examples)

## Configuration

Edit the `application-dev.yml` file to configure database connections:

```yaml
source_db_uri: "mysql+pymysql://user:password@localhost/source_db"
target_db_uri: "postgresql+psycopg2://user:password@localhost/target_db"
```

You can set the `PROFILE` environment variable to choose different configuration files:

```bash
export PROFILE=production  # Will use application-production.yml
```

## Usage

### Running the main SQLAlchemy-based migration

To run the main migration process that uses SQLAlchemy and Pydantic:

```bash
python -m src.data_migration
```

### Using the example files

Migkit includes several example files that demonstrate the framework without requiring database setup:

#### Basic Example

Run the simple CSV-to-JSON migration example:

```bash
python -m src.simple
```

This example:
- Creates sample product data
- Transforms it (adding price in EUR, availability info, etc.)
- Saves to JSON files
- Provides a simple execution report

#### Multi-Source Example

Run the multi-source data migration example:

```bash
python -m src.simple_multi_source
```

This example:
- Extracts data from multiple sources (CSV, JSON, and XML)
- Joins and transforms the data
- Exports to multiple formats (JSON, CSV, HTML, XML)
- Creates relationship-based reports

#### Pandas Analysis Example

Run the Pandas-based data analysis and visualization example:

```bash
python -m src.simple_pandas
```

This example:
- Generates realistic sample data
- Performs advanced data transformation with Pandas
- Creates visualizations with Matplotlib
- Produces a comprehensive HTML report with charts

Each example demonstrates different aspects of the ETL framework without requiring database connections.

## ETL Process

1. **Extract**: Fetches data from source database using both raw SQL and ORM
2. **Transform**: Processes data with advanced Pandas operations
3. **Load**: Saves transformed data to target database

## Extending

To create new migration processes:

1. Define new models in `models.py`
2. Create functions with `@phase` decorators in your migration file
3. Run the migration with the framework

### Creating Your Own Migration

1. Import the framework:
   ```python
   from framework import phase, Phase, DataMigrationFramework
   ```

2. Define your extraction, transformation, and loading functions with the appropriate decorators:
   ```python
   @phase(Phase.EXTRACT)
   def extract_my_data():
       # Your extraction code here
       return data

   @phase(Phase.TRANSFORM)
   def transform_my_data(data):
       # Your transformation code
       return transformed_data

   @phase(Phase.LOAD)
   def load_my_data(transformed_data):
       # Your loading code
       return result
   ```

3. Create a runner function:
   ```python
   def run_my_migration():
       framework = DataMigrationFramework()
       import sys
       current_module = sys.modules[__name__]
       framework.register_phases_from_annotations(current_module)
       return framework.run()
   ```

4. Run your migration:
   ```python
   if __name__ == "__main__":
       run_my_migration()
   ```

## License

MIT