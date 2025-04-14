"""
Simple Data Migration Example

This module demonstrates basic usage of the data migration framework
without relying on SQLAlchemy or Pydantic. It shows how to implement
the ETL (Extract, Transform, Load) pattern with simple data structures.
"""
import csv
import json
import os
from datetime import datetime
from framework import phase, Phase, DataMigrationFramework


@phase(Phase.EXTRACT)
def extract_from_csv():
    """
    Extract phase: Read data from a CSV file.
    
    Returns:
        list: List of dictionaries containing the source data
    """
    print("Extracting data from CSV file...")
    
    # Sample data - In a real scenario, this would read from a file
    # For demonstration, we'll create the data in memory
    source_data = [
        {"id": 1, "name": "Product A", "category": "Electronics", "price": 199.99, "in_stock": True},
        {"id": 2, "name": "Product B", "category": "Books", "price": 29.99, "in_stock": True},
        {"id": 3, "name": "Product C", "category": "Electronics", "price": 99.50, "in_stock": False},
        {"id": 4, "name": "Product D", "category": "Clothing", "price": 49.99, "in_stock": True},
        {"id": 5, "name": "Product E", "category": "Books", "price": 14.99, "in_stock": False}
    ]
    
    # Optionally, write to CSV for demonstration
    with open('source_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['id', 'name', 'category', 'price', 'in_stock']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for row in source_data:
            writer.writerow(row)
    
    print(f"Extracted {len(source_data)} records")
    return source_data


@phase(Phase.TRANSFORM)
def transform_data(source_data):
    """
    Transform phase: Process and enrich the extracted data.
    
    Args:
        source_data (list): List of dictionaries from the extract phase
        
    Returns:
        list: Processed data ready for loading
    """
    print("Transforming data...")
    
    transformed_data = []
    
    # Process each record
    for item in source_data:
        # Create a new transformed record
        transformed_item = {
            "product_id": item["id"],
            "product_name": item["name"].upper(),  # Convert name to uppercase
            "category": item["category"],
            "price_usd": item["price"],
            "price_eur": round(item["price"] * 0.91, 2),  # Convert to EUR
            "availability": "In Stock" if item["in_stock"] else "Out of Stock",
            "processed_at": datetime.now().isoformat()
        }
        
        # Add discount information for specific categories
        if item["category"] == "Electronics":
            transformed_item["discount"] = 0.10  # 10% discount
        elif item["category"] == "Books":
            transformed_item["discount"] = 0.05  # 5% discount
        else:
            transformed_item["discount"] = 0.00  # No discount
        
        # Calculate discounted prices
        transformed_item["discounted_price_usd"] = round(
            item["price"] * (1 - transformed_item["discount"]), 2
        )
        
        transformed_data.append(transformed_item)
    
    print(f"Transformed {len(transformed_data)} records")
    return transformed_data


@phase(Phase.LOAD)
def load_to_json(transformed_data):
    """
    Load phase: Save the transformed data to a JSON file.
    
    Args:
        transformed_data (list): Transformed data from the transform phase
        
    Returns:
        dict: Summary of the load operation
    """
    print("Loading data to JSON file...")
    
    # Prepare output for different categories
    categories = {}
    for item in transformed_data:
        category = item["category"]
        if category not in categories:
            categories[category] = []
        categories[category].append(item)
    
    # Save to a single JSON file with all data
    with open('target_data.json', 'w') as json_file:
        json.dump(transformed_data, json_file, indent=2)
    
    # Save separate JSON files for each category
    os.makedirs('target_data', exist_ok=True)
    for category, items in categories.items():
        filename = f"target_data/{category.lower()}.json"
        with open(filename, 'w') as json_file:
            json.dump(items, json_file, indent=2)
    
    # Return a summary of the operation
    summary = {
        "total_records": len(transformed_data),
        "categories": {category: len(items) for category, items in categories.items()},
        "files_created": [f"target_data/{category.lower()}.json" for category in categories.keys()],
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"Loaded {summary['total_records']} records into {len(summary['files_created']) + 1} files")
    return summary


def run_simple_migration():
    """
    Run the simple data migration process.
    This function demonstrates how to use the framework to run a migration.
    """
    # Create an instance of the framework
    framework = DataMigrationFramework()
    
    # Register the current module
    import sys
    current_module = sys.modules[__name__]
    framework.register_phases_from_annotations(current_module)
    
    # Execute the migration process
    print("\n===== STARTING SIMPLE DATA MIGRATION =====\n")
    result = framework.run()
    print("\n===== MIGRATION COMPLETE =====\n")
    
    # Print summary
    print("Migration Summary:")
    print(f"- Total records processed: {result['total_records']}")
    print("- Records by category:")
    for category, count in result['categories'].items():
        print(f"  - {category}: {count}")
    print(f"- Files created: {len(result['files_created']) + 1}")
    print(f"- Timestamp: {result['timestamp']}")
    
    return result


# Run the simple migration if executed directly
if __name__ == "__main__":
    run_simple_migration()