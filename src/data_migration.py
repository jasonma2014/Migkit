from sqlalchemy import create_engine, select, text
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from config import Config
from framework import phase, Phase
from models import (
    SourceModel, TargetModel, 
    SourceDetailModel, TargetDetailModel,
    init_db, get_session
)
# Import Pydantic validators
from validators import (
    validate_source_data,
    validate_transformed_data,
    SourceRecord,
    SourceDetailRecord,
    SourceDataset
)
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("data_migration")

# Create global SQLAlchemy engine connections
source_engine = create_engine(Config.SOURCE_DB_URI)
target_engine = create_engine(Config.TARGET_DB_URI)

# Initialize databases if needed
def setup_databases():
    """Initialize both source and target databases with required tables"""
    init_db(source_engine)
    init_db(target_engine)

@phase(Phase.EXTRACT)
def fetch_data_from_source():
    """Extract data from source database using both raw SQL and ORM approaches"""
    logger.info("Starting data extraction phase")
    
    # Method 1: Using pandas read_sql with raw SQL query
    query = "SELECT * FROM source_table"
    df_raw = pd.read_sql(query, source_engine)
    
    # Method 2: Using SQLAlchemy 2.0 ORM with select statement
    with get_session(source_engine) as session:
        # Get main source data
        stmt = select(SourceModel)
        result = session.execute(stmt).scalars().all()
        source_data = [item.to_dict() for item in result]
        
        # Get related details data
        detail_stmt = select(SourceDetailModel)
        detail_result = session.execute(detail_stmt).scalars().all()
        details_data = [{
            'id': detail.id,
            'source_id': detail.source_id,
            'detail_data': detail.detail_data
        } for detail in detail_result]
    
    # Convert ORM data to DataFrame
    df_orm = pd.DataFrame(source_data)
    df_details = pd.DataFrame(details_data)
    
    # Merge the datasets (optional)
    if not df_orm.empty and not df_details.empty and 'id' in df_orm and 'source_id' in df_details:
        df_merged = pd.merge(df_orm, df_details, left_on='id', right_on='source_id', how='left')
    else:
        df_merged = df_orm
    
    # Create result dictionary
    data_dict = {
        'df_raw': df_raw,
        'df_orm': df_orm, 
        'df_details': df_details,
        'df_merged': df_merged
    }
    
    # Validate source data using Pydantic
    validation_results = validate_source_data(data_dict)
    
    if not validation_results["valid"]:
        logger.warning("Source data validation failed")
        logger.debug(f"Validation details: {validation_results['validation_details']}")
        
        # Example: Handle validation failures (can be customized)
        if Config.features.get("data_validation", False):
            # Depending on configuration, raise exception or continue with warning
            if Config.enable_detailed_logging:
                for source, result in validation_results["validation_details"].items():
                    if not result.get("valid", False):
                        logger.error(f"Validation failed for {source}: {result.get('errors', [])}")
            
            # Example: Raise exception if strict validation is required
            # raise ValueError(f"Source data validation failed: {validation_results}")
    else:
        logger.info("Source data validation successful")
    
    # Add validation results to the output
    data_dict['validation_results'] = validation_results
    
    return data_dict

def validate_and_clean_row(
    row: pd.Series, 
    invalid_rows: List[int], 
    row_index: int
) -> Tuple[bool, Optional[pd.Series]]:
    """
    Validate a single row and attempt to clean/fix issues.
    
    Args:
        row: The row to validate and clean
        invalid_rows: List to collect indices of rows that can't be fixed
        row_index: Index of the current row
        
    Returns:
        Tuple with (is_valid, cleaned_row)
    """
    # Create a copy to avoid modifying the original
    cleaned_row = row.copy()
    is_valid = True
    
    # Perform common cleaning operations
    if 'column1' in cleaned_row and pd.isna(cleaned_row['column1']):
        # Column1 is required - mark as invalid
        invalid_rows.append(row_index)
        is_valid = False
    
    if 'column2' in cleaned_row and pd.isna(cleaned_row['column2']):
        # Column2 is optional - replace with default
        cleaned_row['column2'] = 'N/A'
    
    if 'column3' in cleaned_row:
        if pd.isna(cleaned_row['column3']):
            # Missing numeric value - set to default
            cleaned_row['column3'] = 0
        elif cleaned_row['column3'] < 0:
            # Negative values not allowed - use absolute value
            cleaned_row['column3'] = abs(cleaned_row['column3'])
        elif cleaned_row['column3'] > 1000:
            # Values over 1000 not allowed - cap at 1000
            cleaned_row['column3'] = 1000
    
    # Try to validate using Pydantic (for more complex validation)
    try:
        if all(k in cleaned_row for k in ['id', 'column1']):
            record_dict = {k: v for k, v in cleaned_row.items() if k in 
                         ['id', 'column1', 'column2', 'column3', 'created_at']}
            SourceRecord(**record_dict)  # Validate with Pydantic
    except Exception as e:
        # Cannot fix this row - mark as invalid
        logger.debug(f"Row {row_index} validation failed: {str(e)}")
        invalid_rows.append(row_index)
        is_valid = False
    
    return is_valid, cleaned_row if is_valid else None

@phase(Phase.TRANSFORM)
def transform_data_with_pandas(data_dict):
    """Advanced data transformation using pandas features with validation"""
    logger.info("Starting data transformation phase")
    
    # Get the validation results from the previous phase
    validation_results = data_dict.get('validation_results', {"valid": False})
    
    # Get DataFrames from input
    df = data_dict['df_merged'] if 'df_merged' in data_dict else data_dict['df_raw']
    
    # Skip processing if DataFrame is empty
    if df.empty:
        logger.warning("Empty DataFrame, skipping transformation")
        return pd.DataFrame()
    
    # Step 1: Clean and validate the data (row-by-row)
    invalid_rows = []
    cleaned_rows = []
    
    for i, row in df.iterrows():
        is_valid, cleaned_row = validate_and_clean_row(row, invalid_rows, i)
        if is_valid and cleaned_row is not None:
            cleaned_rows.append(cleaned_row)
    
    # If we found invalid rows, log them and proceed with valid ones
    if invalid_rows:
        logger.warning(f"Found {len(invalid_rows)} invalid rows that couldn't be fixed")
        logger.debug(f"Invalid row indices: {invalid_rows}")
        
        # Create a new DataFrame with only the valid, cleaned rows
        if cleaned_rows:
            df = pd.DataFrame(cleaned_rows)
        else:
            logger.error("No valid rows to process")
            return pd.DataFrame()
    
    # 1. Basic transformations
    if 'column3' in df.columns:
        df['column3_transformed'] = df['column3'] * 2
    
    # 2. Handle missing values
    df = df.fillna({
        'column2': 'N/A',
        'column3': 0
    })
    
    # 3. Apply custom function to a column
    if 'column1' in df.columns:
        df['column1_upper'] = df['column1'].apply(lambda x: str(x).upper())
    
    # 4. Add timestamp column
    df['migrated_at'] = datetime.now()
    
    # 5. Group by and aggregate (if applicable)
    if 'source_id' in df.columns:
        df_agg = df.groupby('source_id').agg({
            'column3': ['sum', 'mean', 'min', 'max'],
            'detail_data': 'count'
        }).reset_index()
        df_agg.columns = ['source_id', 'column3_sum', 'column3_avg', 'column3_min', 'column3_max', 'detail_count']
        
        # Add aggregation results back to the main DataFrame
        df = pd.merge(df, df_agg, on='source_id', how='left')
    
    # 6. Apply conditional logic
    if 'column3' in df.columns:
        conditions = [
            (df['column3'] < 10),
            (df['column3'] >= 10) & (df['column3'] < 50),
            (df['column3'] >= 50)
        ]
        choices = ['low', 'medium', 'high']
        df['category'] = np.select(conditions, choices, default='unknown')
    
    # Select and reorder columns for target table
    target_columns = [c for c in [
        'id', 'column1', 'column2', 'column3_transformed', 
        'migrated_at', 'column1_upper', 'category'
    ] if c in df.columns]
    
    transformed_data = df[target_columns].copy()
    
    # Validate the transformed data
    transformed_validation = validate_transformed_data(transformed_data)
    
    if not transformed_validation["valid"]:
        logger.warning(f"Transformed data validation found {transformed_validation['records_with_errors']} records with errors")
        
        # Get indices of invalid records
        if transformed_validation['error_indices']:
            logger.debug(f"Invalid record indices: {transformed_validation['error_indices']}")
            logger.debug(f"Error details: {transformed_validation['error_details']}")
            
            # Example: Remove invalid records if configured to do so
            if Config.features.get("data_validation", False):
                # Filter out invalid records
                transformed_data = transformed_data.drop(transformed_validation['error_indices']).reset_index(drop=True)
                logger.info(f"Removed {len(transformed_validation['error_indices'])} invalid records from result")
    else:
        logger.info(f"Successfully validated {transformed_validation['records_validated']} transformed records")
    
    return transformed_data

@phase(Phase.LOAD)
def save_data_to_target(transformed_data):
    """Save data to target database using both pandas and ORM approaches"""
    logger.info("Starting data loading phase")
    
    if transformed_data.empty:
        logger.warning("No data to save")
        return {"success": False, "reason": "No data to save"}
    
    # Method 1: Using pandas to_sql for bulk insert
    try:
        transformed_data.to_sql('target_table', target_engine, 
                              if_exists='replace', index=False)
        logger.info(f"Successfully saved {len(transformed_data)} records via pandas to_sql")
    except Exception as e:
        logger.error(f"Error saving data via pandas: {str(e)}")
        # Continue with ORM approach as fallback
    
    # Method 2: Using SQLAlchemy ORM for more control
    records = transformed_data.to_dict(orient='records')
    
    # Create a list to collect any validation errors during load
    load_errors = []
    
    # Validate each record before loading using Pydantic
    valid_records = []
    for i, record in enumerate(records):
        try:
            # Validate that required fields are present and properly formatted
            # Here we're using direct field access validation since records are already transformed
            if 'id' not in record or not isinstance(record['id'], (int, np.integer)):
                raise ValueError("Missing or invalid ID field")
            
            if 'column1' not in record or not isinstance(record['column1'], str):
                raise ValueError("Missing or invalid column1 field")
            
            # If we pass validation, add to valid records
            valid_records.append(record)
        except Exception as e:
            # Record the error but continue processing valid records
            load_errors.append({"index": i, "error": str(e), "record": record})
    
    if load_errors:
        logger.warning(f"Found {len(load_errors)} invalid records during load phase")
        if Config.enable_detailed_logging:
            logger.debug(f"Load errors: {load_errors}")
    
    try:
        with get_session(target_engine) as session:
            # Clear existing data if needed
            session.execute(text("TRUNCATE TABLE target_detail"))
            session.commit()
            
            # Create and save target model instances
            target_instances = []
            detail_instances = []
            
            for record in valid_records:
                # Create main record
                target = TargetModel.from_source(record, datetime.now())
                target_instances.append(target)
                
                # Create a detail record (as an example)
                if 'category' in record:
                    detail = TargetDetailModel(
                        detail_data=f"Category: {record['category']}"
                    )
                    detail.target = target  # Set relationship
                    detail_instances.append(detail)
            
            # Bulk save
            session.add_all(target_instances)
            session.add_all(detail_instances)
            session.commit()
            
            logger.info(f"Successfully saved {len(target_instances)} records and {len(detail_instances)} details via ORM")
            
    except Exception as e:
        logger.error(f"Error saving data via ORM: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "records_processed": len(valid_records),
            "records_with_errors": len(load_errors)
        }
        
    return {
        "success": True,
        "records_processed": len(valid_records),
        "records_with_errors": len(load_errors),
        "target_instances": len(target_instances),
        "detail_instances": len(detail_instances)
    }

def run_migration():
    """Execute the entire ETL pipeline"""
    logger.info("Starting migration pipeline")
    
    # Initialize framework
    from framework import DataMigrationFramework
    import sys
    
    # Create framework instance
    framework = DataMigrationFramework()
    
    # Register current module
    import src.data_migration as module
    framework.register_phases_from_annotations(module)
    
    # Setup databases if needed (optional)
    setup_databases()
    
    # Run the pipeline
    try:
        result = framework.run()
        logger.info("Migration completed successfully")
        return result
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_migration()