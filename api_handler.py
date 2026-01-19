import pandas as pd

import requests



url = "https://dummyjson.com/products"
headers = {"Authorization": "Bearer YOUR_TOKEN"}

response = requests.get(url, headers=headers)

print(response.json())

response = requests.get('https://dummyjson.com/products')
data = response.json()
# data['products'] contains list of all products
# data['total'] gives total count

print("single product:")
response = requests.get('https://dummyjson.com/products/1')
singleproduct = response.json()
print(singleproduct)
# Returns single product object

print("Specific number of products:")
specificresponse = requests.get('https://dummyjson.com/products?limit=100')
data = specificresponse.json()
products = data.get('products', [])
print(specificresponse.json())



#search products
response = requests.get('https://dummyjson.com/products/search?q=phone')

searchdata = response.json()
print("search products:")   
print(searchdata)  # List of products matching the search query



#fetch all products

print(" products:")
specificresponse = requests.get('https://dummyjson.com/products?limit=100')
data = specificresponse.json()
products = data.get('products', [])
print(specificresponse.json())

#Create Product Mapping 
def create_product_mapping(api_products):
    
    product_mapping = {}

    for product in api_products:
        product_id = product.get("id")

        if product_id is None:
            continue

        product_mapping[product_id] = {
            "title": product.get("title"),
            "category": product.get("category"),
            "brand": product.get("brand"),
            "rating": product.get("rating")
        }
    print("Product mapping created successfully")
    print(product_mapping)
    return product_mapping

#Enrich Sales Data
# import os

def enrich_sales_data(transactions, product_mapping):
    import os
    enriched_transactions = []

    # Ensure output directory exists
    os.makedirs("data", exist_ok=True)
    output_path = "data/enriched_sales_data.txt"

    for txn in transactions:
        enriched_txn = txn.copy()

        try:
            # Extract numeric product ID (P101 → 101)
            product_id = txn.get("ProductID", "")
            numeric_id = int(product_id.replace("P", ""))

            api_data = product_mapping.get(numeric_id)

            if api_data:
                enriched_txn["API_Category"] = api_data.get("category")
                enriched_txn["API_Brand"] = api_data.get("brand")
                enriched_txn["API_Rating"] = api_data.get("rating")
                enriched_txn["API_Match"] = True
            else:
                enriched_txn["API_Category"] = None
                enriched_txn["API_Brand"] = None
                enriched_txn["API_Rating"] = None
                enriched_txn["API_Match"] = False

        except Exception:
            enriched_txn["API_Category"] = None
            enriched_txn["API_Brand"] = None
            enriched_txn["API_Rating"] = None
            enriched_txn["API_Match"] = False

        enriched_transactions.append(enriched_txn)

    # Write enriched data to file (pipe-delimited)
    headers = [
        "TransactionID", "Date", "ProductID", "ProductName",
        "Quantity", "UnitPrice", "CustomerID", "Region",
        "API_Category", "API_Brand", "API_Rating", "API_Match"
    ]

    with open(output_path, "w", encoding="utf-8") as file:
        file.write("|".join(headers) + "\n")
        for txn in enriched_transactions:
            row = [str(txn.get(col, "")) for col in headers]
            file.write("|".join(row) + "\n")

    return enriched_transactions
 



