import pytest
import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Shared fixtures can be added here
@pytest.fixture(scope="session")
def test_data():
    return {
        "source_data": "test_data",
        "expected_transform": "transformed_test_data",
        "expected_load": "loaded_transformed_test_data"
    } 