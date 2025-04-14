"""
Multi-Source Data Migration Example

This module demonstrates how to use the migration framework to handle multiple data sources
and target formats without using complex ORMs or validation libraries.
"""
import csv
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime
from framework import phase, Phase, DataMigrationFramework


@phase(Phase.EXTRACT)
def extract_from_multiple_sources():
    """
    Extract phase: Gather data from multiple sources (CSV, JSON, and XML).
    
    Returns:
        dict: Dictionary containing data from different sources
    """
    print("Extracting data from multiple sources...")
    
    # 1. Sample CSV data (in practice, this would read from files)
    customers_data = [
        {"id": 1, "name": "John Smith", "email": "john@example.com", "country": "USA"},
        {"id": 2, "name": "Maria Garcia", "email": "maria@example.com", "country": "Spain"},
        {"id": 3, "name": "Liu Wei", "email": "liu@example.com", "country": "China"}
    ]
    
    # 2. Sample JSON data
    orders_data = [
        {"order_id": 1001, "customer_id": 1, "products": [101, 102], "total": 129.99, "date": "2023-01-15"},
        {"order_id": 1002, "customer_id": 3, "products": [103], "total": 79.99, "date": "2023-01-16"},
        {"order_id": 1003, "customer_id": 1, "products": [101, 104], "total": 159.98, "date": "2023-01-20"},
        {"order_id": 1004, "customer_id": 2, "products": [102, 105], "total": 94.98, "date": "2023-01-25"}
    ]
    
    # 3. Sample XML data (converted to Python objects)
    products_data = [
        {"product_id": 101, "name": "Smartphone", "category": "Electronics", "price": 99.99},
        {"product_id": 102, "name": "Headphones", "category": "Electronics", "price": 29.99},
        {"product_id": 103, "name": "Laptop", "category": "Electronics", "price": 799.99},
        {"product_id": 104, "name": "Tablet", "category": "Electronics", "price": 199.99},
        {"product_id": 105, "name": "External Drive", "category": "Electronics", "price": 64.99}
    ]
    
    # Write sample files for demonstration
    # CSV file
    with open('customers.csv', 'w', newline='') as csvfile:
        fieldnames = ['id', 'name', 'email', 'country']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for customer in customers_data:
            writer.writerow(customer)
    
    # JSON file
    with open('orders.json', 'w') as jsonfile:
        json.dump(orders_data, jsonfile, indent=2)
    
    # XML file
    root = ET.Element("products")
    for product in products_data:
        product_elem = ET.SubElement(root, "product")
        for key, value in product.items():
            elem = ET.SubElement(product_elem, key)
            elem.text = str(value)
    
    tree = ET.ElementTree(root)
    tree.write("products.xml")
    
    # Combine all data into a single dictionary
    extracted_data = {
        "customers": customers_data,
        "orders": orders_data,
        "products": products_data
    }
    
    print(f"Extracted data from 3 sources:")
    print(f"- Customers: {len(customers_data)} records")
    print(f"- Orders: {len(orders_data)} records")
    print(f"- Products: {len(products_data)} records")
    
    return extracted_data


@phase(Phase.TRANSFORM)
def transform_and_join_data(extracted_data):
    """
    Transform phase: Process and join data from multiple sources.
    
    Args:
        extracted_data (dict): Dictionary with data from different sources
        
    Returns:
        dict: Transformed and joined data
    """
    print("Transforming and joining data...")
    
    customers = {c["id"]: c for c in extracted_data["customers"]}
    products = {p["product_id"]: p for p in extracted_data["products"]}
    orders = extracted_data["orders"]
    
    # Create detailed order reports with customer and product information
    detailed_orders = []
    
    for order in orders:
        customer = customers.get(order["customer_id"])
        
        if not customer:
            print(f"Warning: Customer not found for order {order['order_id']}")
            continue
        
        # Create a detailed order with customer information
        detailed_order = {
            "order_id": order["order_id"],
            "order_date": order["date"],
            "total_amount": order["total"],
            "customer_name": customer["name"],
            "customer_email": customer["email"],
            "customer_country": customer["country"],
            "items": []
        }
        
        # Add product details
        for product_id in order["products"]:
            product = products.get(product_id)
            
            if product:
                item = {
                    "product_id": product["product_id"],
                    "product_name": product["name"],
                    "product_category": product["category"],
                    "unit_price": product["price"]
                }
                detailed_order["items"].append(item)
            else:
                print(f"Warning: Product {product_id} not found for order {order['order_id']}")
        
        detailed_orders.append(detailed_order)
    
    # Generate sales by country report
    sales_by_country = {}
    for order in detailed_orders:
        country = order["customer_country"]
        if country not in sales_by_country:
            sales_by_country[country] = {
                "total_sales": 0,
                "order_count": 0,
                "customer_count": set()
            }
        
        sales_by_country[country]["total_sales"] += order["total_amount"]
        sales_by_country[country]["order_count"] += 1
        sales_by_country[country]["customer_count"].add(order["customer_name"])
    
    # Convert sets to counts for JSON serialization
    for country in sales_by_country:
        sales_by_country[country]["customer_count"] = len(sales_by_country[country]["customer_count"])
    
    # Generate product popularity report
    product_popularity = {}
    for order in detailed_orders:
        for item in order["items"]:
            product_id = item["product_id"]
            if product_id not in product_popularity:
                product_popularity[product_id] = {
                    "product_name": item["product_name"],
                    "product_category": item["product_category"],
                    "order_count": 0,
                    "total_revenue": 0
                }
            
            product_popularity[product_id]["order_count"] += 1
            product_popularity[product_id]["total_revenue"] += item["unit_price"]
    
    transformed_data = {
        "detailed_orders": detailed_orders,
        "sales_by_country": sales_by_country,
        "product_popularity": product_popularity
    }
    
    print(f"Transformed data into:")
    print(f"- Detailed Orders: {len(detailed_orders)} records")
    print(f"- Sales by Country: {len(sales_by_country)} records")
    print(f"- Product Popularity: {len(product_popularity)} records")
    
    return transformed_data


@phase(Phase.LOAD)
def load_to_multiple_formats(transformed_data):
    """
    Load phase: Save transformed data in multiple formats (JSON, CSV, HTML).
    
    Args:
        transformed_data (dict): Transformed data from the transform phase
        
    Returns:
        dict: Summary of the load operation
    """
    print("Loading data to multiple formats...")
    
    # Create output directory
    os.makedirs("output", exist_ok=True)
    
    # 1. Save detailed orders as JSON
    with open("output/detailed_orders.json", "w") as jsonfile:
        json.dump(transformed_data["detailed_orders"], jsonfile, indent=2)
    
    # 2. Save sales by country as CSV
    with open("output/sales_by_country.csv", "w", newline="") as csvfile:
        fieldnames = ["country", "total_sales", "order_count", "customer_count"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for country, data in transformed_data["sales_by_country"].items():
            row = {
                "country": country,
                "total_sales": data["total_sales"],
                "order_count": data["order_count"],
                "customer_count": data["customer_count"]
            }
            writer.writerow(row)
    
    # 3. Save product popularity as HTML
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Product Popularity Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            tr:nth-child(even) { background-color: #f9f9f9; }
        </style>
    </head>
    <body>
        <h1>Product Popularity Report</h1>
        <p>Generated on: %s</p>
        <table>
            <tr>
                <th>Product ID</th>
                <th>Product Name</th>
                <th>Category</th>
                <th>Order Count</th>
                <th>Total Revenue</th>
            </tr>
            %s
        </table>
    </body>
    </html>
    """ 
    
    table_rows = ""
    for product_id, data in transformed_data["product_popularity"].items():
        row = f"""
        <tr>
            <td>{product_id}</td>
            <td>{data['product_name']}</td>
            <td>{data['product_category']}</td>
            <td>{data['order_count']}</td>
            <td>${data['total_revenue']:.2f}</td>
        </tr>
        """
        table_rows += row
    
    html_content = html_content % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), table_rows)
    
    with open("output/product_popularity.html", "w") as htmlfile:
        htmlfile.write(html_content)
    
    # 4. Create a summary XML file
    root = ET.Element("migration_summary")
    
    timestamp = ET.SubElement(root, "timestamp")
    timestamp.text = datetime.now().isoformat()
    
    sources = ET.SubElement(root, "sources")
    sources.set("count", "3")
    
    source1 = ET.SubElement(sources, "source")
    source1.set("name", "customers")
    source1.set("format", "CSV")
    
    source2 = ET.SubElement(sources, "source")
    source2.set("name", "orders")
    source2.set("format", "JSON")
    
    source3 = ET.SubElement(sources, "source")
    source3.set("name", "products")
    source3.set("format", "XML")
    
    outputs = ET.SubElement(root, "outputs")
    outputs.set("count", "3")
    
    output1 = ET.SubElement(outputs, "output")
    output1.set("name", "detailed_orders.json")
    output1.set("format", "JSON")
    output1.set("record_count", str(len(transformed_data["detailed_orders"])))
    
    output2 = ET.SubElement(outputs, "output")
    output2.set("name", "sales_by_country.csv")
    output2.set("format", "CSV")
    output2.set("record_count", str(len(transformed_data["sales_by_country"])))
    
    output3 = ET.SubElement(outputs, "output")
    output3.set("name", "product_popularity.html")
    output3.set("format", "HTML")
    output3.set("record_count", str(len(transformed_data["product_popularity"])))
    
    tree = ET.ElementTree(root)
    tree.write("output/summary.xml")
    
    # Create a summary of the operation
    summary = {
        "timestamp": datetime.now().isoformat(),
        "sources": [
            {"name": "customers", "format": "CSV"},
            {"name": "orders", "format": "JSON"},
            {"name": "products", "format": "XML"}
        ],
        "outputs": [
            {"file": "detailed_orders.json", "format": "JSON", "records": len(transformed_data["detailed_orders"])},
            {"file": "sales_by_country.csv", "format": "CSV", "records": len(transformed_data["sales_by_country"])},
            {"file": "product_popularity.html", "format": "HTML", "records": len(transformed_data["product_popularity"])},
            {"file": "summary.xml", "format": "XML", "records": 1}
        ]
    }
    
    print(f"Output files created:")
    for output in summary["outputs"]:
        print(f"- {output['file']} ({output['format']}): {output['records']} records")
    
    return summary


def run_multi_source_migration():
    """
    Run the multi-source data migration process.
    """
    # Create an instance of the framework
    framework = DataMigrationFramework()
    
    # Register the current module
    import sys
    current_module = sys.modules[__name__]
    framework.register_phases_from_annotations(current_module)
    
    # Execute the migration process
    print("\n===== STARTING MULTI-SOURCE DATA MIGRATION =====\n")
    result = framework.run()
    print("\n===== MIGRATION COMPLETE =====\n")
    
    # Print summary
    print("Migration Summary:")
    print(f"- Timestamp: {result['timestamp']}")
    print("- Sources:")
    for source in result["sources"]:
        print(f"  - {source['name']} ({source['format']})")
    print("- Outputs:")
    for output in result["outputs"]:
        print(f"  - {output['file']} ({output['format']}): {output['records']} records")
    
    return result


# Run the multi-source migration if executed directly
if __name__ == "__main__":
    run_multi_source_migration() 