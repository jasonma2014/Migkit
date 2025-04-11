from sqlalchemy import create_engine  # Add missing import for create_engine
import pandas as pd  # Add missing import for pandas
from config import Config  # Import Config class from config module
from framework import phase, Phase

# Create global SQLAlchemy engine connections
source_engine = create_engine(Config.SOURCE_DB_URI)
target_engine = create_engine(Config.TARGET_DB_URI)

@phase(Phase.EXTRACT)  # Use enum member instead of string
def fetch_data_from_source():
    # Fetch data from the source database using Pandas' read_sql method
    query = "SELECT * FROM source_table"  # Example query
    data = pd.read_sql(query, source_engine)  # Execute query using Pandas
    return data

@phase(Phase.TRANSFORM)  # Use enum member instead of string
def transform_data_with_pandas(data):
    # Transform data using Pandas
    data['column3_transformed'] = data['column3'] * 2
    transformed_data = data[['column1', 'column2', 'column3_transformed']]
    return transformed_data

@phase(Phase.LOAD)  # Use enum member instead of string
def save_data_to_target(transformed_data):
    # Save the transformed data to the target database
    transformed_data.to_sql('target_table', target_engine, if_exists='replace', index=False)