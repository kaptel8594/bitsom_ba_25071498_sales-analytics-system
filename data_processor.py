import pandas as pd

# Read file with encoding handling
df = pd.read_csv(
    "sales_data.txt",
    delimiter="|",
    encoding="latin1",
    engine="python"
)
# Display the first few rows of the DataFrame
print(df.to_string())


#a.Calculate Total Revenue from transactions
def calculate_total_revenue(transactions):
   
    
    total_revenue = 0.0

    for tx in transactions:
        total_revenue += tx["Quantity"] * tx["UnitPrice"]
        print(f"Transaction ID: {tx['TransactionID']}, Quantity: {tx['Quantity']}, Unit Price: {tx['UnitPrice']}, Subtotal: {tx['Quantity'] * tx['UnitPrice']}")
        print(f"Accumulated Total Revenue: {total_revenue}")
    return total_revenue


#b.Analyzes sales by region
def region_wise_sales(transactions):  
       
    region_data = {}
    grand_total = 0.0

    # First pass: calculate totals and counts
    for tx in transactions:
        region = tx["Region"]
        amount = tx["Quantity"] * tx["UnitPrice"]
        grand_total += amount

        if region not in region_data:
            region_data[region] = {
                "total_sales": 0.0,
                "transaction_count": 0
            }

        region_data[region]["total_sales"] += amount
        region_data[region]["transaction_count"] += 1

    # Second pass: calculate percentage
    for region in region_data:
        percentage = (region_data[region]["total_sales"] / grand_total) * 100
        region_data[region]["percentage"] = round(percentage, 2)

    # Sort by total_sales (descending)
    sorted_regions = dict(
        sorted(
            region_data.items(),
            key=lambda item: item[1]["total_sales"],
            reverse=True
        )
    )

    return sorted_regions

#c.Top Selling Products
def top_selling_products(transactions, top_n=5):
    
    product_sales = {}

    for tx in transactions:
        product = tx["ProductName"]
        amount = tx["Quantity"] * tx["UnitPrice"]

        if product not in product_sales:
            product_sales[product] = 0.0

        product_sales[product] += amount

    # Sort products by sales amount (descending)
    sorted_products = sorted(
        product_sales.items(),
        key=lambda item: item[1],
        reverse=True
    )

    return sorted_products[:top_n]

#d.Customer Purchase Analysis
def customer_purchase_analysis(transactions):    
     
    customer_data = {}

    # Aggregate data per customer
    for tx in transactions:
        cid = tx["CustomerID"]
        product = tx["ProductName"]
        amount = tx["Quantity"] * tx["UnitPrice"]

        if cid not in customer_data:
            customer_data[cid] = {
                "total_spent": 0.0,
                "purchase_count": 0,
                "products_bought": set()
            }

        customer_data[cid]["total_spent"] += amount
        customer_data[cid]["purchase_count"] += 1
        customer_data[cid]["products_bought"].add(product)

    # Calculate averages and format output
    for cid in customer_data:
        total = customer_data[cid]["total_spent"]
        count = customer_data[cid]["purchase_count"]

        customer_data[cid]["avg_order_value"] = round(total / count, 2)
        customer_data[cid]["products_bought"] = list(
            customer_data[cid]["products_bought"]
        )

    # Sort by total_spent (descending)
    sorted_customers = dict(
        sorted(
            customer_data.items(),
            key=lambda item: item[1]["total_spent"],
            reverse=True
        )
    )

    return sorted_customers

#Daily Sales Trends
def daily_sales_trend(transactions):
    """
    Analyzes sales trends by date
    """
    daily_data = {}

    # Aggregate data by date
    for tx in transactions:
        date = tx["Date"]
        amount = tx["Quantity"] * tx["UnitPrice"]
        customer = tx["CustomerID"]

        if date not in daily_data:
            daily_data[date] = {
                "revenue": 0.0,
                "transaction_count": 0,
                "unique_customers": set()
            }

        daily_data[date]["revenue"] += amount
        daily_data[date]["transaction_count"] += 1
        daily_data[date]["unique_customers"].add(customer)

    # Convert unique_customers set to count
    for date in daily_data:
        daily_data[date]["unique_customers"] = len(
            daily_data[date]["unique_customers"]
        )

    # Sort by date (chronological order)
    sorted_daily = dict(
        sorted(daily_data.items(), key=lambda item: item[0])
    )

    return sorted_daily

#Find peak sales day
def find_peak_sales_day(transactions):
    
    daily_revenue = {}

    # Aggregate revenue and transaction count by date
    for tx in transactions:
        date = tx["Date"]
        amount = tx["Quantity"] * tx["UnitPrice"]

        if date not in daily_revenue:
            daily_revenue[date] = {
                "revenue": 0.0,
                "transaction_count": 0
            }

        daily_revenue[date]["revenue"] += amount
        daily_revenue[date]["transaction_count"] += 1

    # Find peak sales day
    peak_date = None
    peak_revenue = 0.0
    peak_tx_count = 0

    for date, data in daily_revenue.items():
        if data["revenue"] > peak_revenue:
            peak_date = date
            peak_revenue = data["revenue"]
            peak_tx_count = data["transaction_count"]

    return peak_date, peak_revenue, peak_tx_count

#Find low performing products
def low_performing_products(transactions, threshold=10):
    """
    Identifies products with low sales
    """
    product_data = {}

    # Aggregate quantity and revenue by ProductName
    for tx in transactions:
        name = tx["ProductName"]
        qty = tx["Quantity"]
        revenue = qty * tx["UnitPrice"]

        if name not in product_data:
            product_data[name] = {
                "quantity": 0,
                "revenue": 0.0
            }

        product_data[name]["quantity"] += qty
        product_data[name]["revenue"] += revenue

    # Filter products with total quantity < threshold
    low_products = [
        (name, data["quantity"], data["revenue"])
        for name, data in product_data.items()
        if data["quantity"] < threshold
    ]

    # Sort by TotalQuantity (ascending)
    low_products.sort(key=lambda x: x[1])

    return low_products

