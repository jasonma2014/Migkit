import re
from pydantic import BaseModel, Field, validator, root_validator, field_validator, model_validator
from typing import Optional, List, Dict, Any
from datetime import datetime
import pandas as pd
import numpy as np

# Basic source data models for validation
class SourceRecord(BaseModel):
    """Pydantic model for validating individual source records"""
    id: int = Field(..., gt=0, description="Record ID must be positive")
    column1: str = Field(..., min_length=1, max_length=100, description="Column1 is required")
    column2: Optional[str] = Field(None, max_length=200, description="Column2 is optional")
    column3: Optional[float] = Field(None, description="Column3 should be a numeric value")
    created_at: Optional[datetime] = None
    
    @field_validator('column1')
    def column1_must_not_contain_special_chars(self, v):
        """Validate that column1 doesn't contain special characters"""
        if re.search(r'[!@#$%^&*()_+=\[\]{}|\\:;"\'<>,.?/~`]', v):
            raise ValueError("column1 should not contain special characters")
        return v
    
    @field_validator('column3')
    def column3_must_be_in_range(self, v):
        """Validate that column3 is within acceptable range"""
        if v is not None and (v < 0 or v > 1000):
            raise ValueError("column3 must be between 0 and 1000")
        return v
    
    @model_validator(mode='after')
    def check_created_at_not_future(cls, values):
        """Validate that created_at is not in the future"""
        created_at = values.get('created_at')
        if created_at and created_at > datetime.now():
            raise ValueError("created_at cannot be in the future")
        return values

class SourceDetailRecord(BaseModel):
    """Pydantic model for validating source detail records"""
    id: int = Field(..., gt=0, description="Detail ID must be positive")
    source_id: int = Field(..., gt=0, description="Source ID must be positive")
    detail_data: Optional[str] = Field(None, max_length=500, description="Detail data")
    
    @field_validator('detail_data')
    def detail_data_must_be_valid_format(cls, v):
        """Validate that detail_data has a valid format if present"""
        if v and not v.strip():
            raise ValueError("detail_data cannot be empty string if provided")
        return v

class SourceDataset(BaseModel):
    """Pydantic model for validating an entire source dataset"""
    records: List[SourceRecord]
    detail_records: Optional[List[SourceDetailRecord]] = None
    
    @validator('records')
    def records_must_not_be_empty(cls, v):
        """Validate that dataset contains at least one record"""
        if not v:
            raise ValueError("Dataset must contain at least one record")
        return v
    
    @model_validator(mode='after')
    def check_detail_references(cls, values):
        """Validate that all detail records reference existing source records"""
        records = values.get('records', [])
        detail_records = values.get('detail_records', [])
        
        if records and detail_records:
            source_ids = {record.id for record in records}
            invalid_details = [d for d in detail_records if d.source_id not in source_ids]
            
            if invalid_details:
                raise ValueError(f"Detail records reference non-existent source IDs: {[d.source_id for d in invalid_details]}")
        
        return values

# Validation functions for pandas DataFrames
def validate_dataframe(df: pd.DataFrame, detail_df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    """
    Validate a source DataFrame using Pydantic models
    
    Args:
        df: Main source DataFrame
        detail_df: Optional detail DataFrame
        
    Returns:
        Dictionary containing validation results
    """
    # Skip validation if DataFrame is empty
    if df.empty:
        return {
            "valid": False,
            "errors": ["Empty DataFrame provided"],
            "validated_count": 0,
            "error_count": 0
        }
    
    # Convert DataFrames to dictionaries for Pydantic validation
    try:
        records = df.to_dict(orient='records')
        source_records = [SourceRecord(**record) for record in records]
        
        detail_records = None
        if detail_df is not None and not detail_df.empty:
            detail_dicts = detail_df.to_dict(orient='records')
            detail_records = [SourceDetailRecord(**record) for record in detail_dicts]
        
        # Validate entire dataset
        dataset = SourceDataset(records=source_records, detail_records=detail_records)
        
        return {
            "valid": True,
            "validated_count": len(records),
            "detail_count": len(detail_records) if detail_records else 0,
            "errors": []
        }
        
    except Exception as e:
        # Handle validation errors
        return {
            "valid": False,
            "errors": [str(e)],
            "validated_count": 0,
            "error_count": 1
        }

def validate_source_data(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Validate source data dictionary containing DataFrames
    
    Args:
        data_dict: Dictionary containing DataFrames
        
    Returns:
        Dictionary containing validation results
    """
    validation_results = {}
    
    # Validate raw data
    if 'df_raw' in data_dict and not data_dict['df_raw'].empty:
        validation_results['raw_data'] = validate_dataframe(data_dict['df_raw'])
    
    # Validate ORM data
    if 'df_orm' in data_dict and not data_dict['df_orm'].empty:
        validation_results['orm_data'] = validate_dataframe(
            data_dict['df_orm'], 
            data_dict.get('df_details')
        )
    
    # Validate merged data
    if 'df_merged' in data_dict and not data_dict['df_merged'].empty:
        validation_results['merged_data'] = validate_dataframe(data_dict['df_merged'])
    
    # Calculate overall validation status
    validation_statuses = [result.get('valid', False) for result in validation_results.values()]
    overall_valid = all(validation_statuses) if validation_statuses else False
    
    return {
        "valid": overall_valid,
        "validation_details": validation_results
    }

# Row-level validation for transformed data
def validate_transformed_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a single transformed row
    
    Args:
        row: Dictionary representing a row of transformed data
        
    Returns:
        Dictionary with validation result
    """
    errors = []
    
    # Required fields
    if 'id' not in row or not isinstance(row['id'], (int, np.integer)):
        errors.append("Missing or invalid 'id' field")
    
    if 'column1' not in row or not isinstance(row['column1'], str):
        errors.append("Missing or invalid 'column1' field")
    
    # Transformed fields validation
    if 'column3_transformed' in row:
        try:
            value = float(row['column3_transformed'])
            if value < 0 or value > 2000:
                errors.append("column3_transformed is out of valid range (0-2000)")
        except (ValueError, TypeError):
            errors.append("column3_transformed is not a valid numeric value")
    
    return {
        "valid": len(errors) == 0,
        "errors": errors
    }

# Validate transformed dataset
def validate_transformed_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate transformed DataFrame
    
    Args:
        df: Transformed DataFrame
        
    Returns:
        Dictionary with validation results
    """
    if df.empty:
        return {
            "valid": False,
            "error": "Empty DataFrame",
            "records_validated": 0,
            "records_with_errors": 0
        }
    
    # Convert DataFrame to records for row-by-row validation
    records = df.to_dict(orient='records')
    validation_results = [validate_transformed_row(record) for record in records]
    
    # Count invalid records
    invalid_records = [i for i, result in enumerate(validation_results) if not result["valid"]]
    
    return {
        "valid": len(invalid_records) == 0,
        "records_validated": len(records),
        "records_with_errors": len(invalid_records),
        "error_indices": invalid_records,
        "error_details": [validation_results[i] for i in invalid_records] if invalid_records else []
    } 