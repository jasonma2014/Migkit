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
│   ├── config.py          # Configuration settings
│   ├── framework.py       # ETL framework with phase system
│   ├── models.py          # SQLAlchemy 2.0 models
│   └── data_migration.py  # ETL implementation
├── application-dev.yml    # Development configuration
└── README.md              # This file
```

## Requirements

- Python 3.10+
- SQLAlchemy 2.0+
- Pandas
- PyYAML

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

To run a migration process:

```bash
python -m src.data_migration
```

## ETL Process

1. **Extract**: Fetches data from source database using both raw SQL and ORM
2. **Transform**: Processes data with advanced Pandas operations
3. **Load**: Saves transformed data to target database

## Extending

To create new migration processes:

1. Define new models in `models.py`
2. Create functions with `@phase` decorators in your migration file
3. Run the migration with the framework

## License

MIT