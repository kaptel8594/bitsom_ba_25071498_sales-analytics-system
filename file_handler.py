import pandas as pd

if __name__ == "__main__":
    print("File handler running")

df = pd.read_csv(
    "sales_data.txt",
    delimiter="|",
    encoding="latin1",
    engine="python"
)

# Display the first few rows of the DataFrame
print(df.to_string())


def read_sales_data(df):
    print("Reading sales data from file...")
    #Reads sales data from file handling encoding issues

    #Returns: list of raw lines (strings)

    #Expected Output Format:
    #['T001|2024-12-01|P101|Laptop|2|45000|C001|North', ...]

    #Requirements:
    #- Use 'with' statement
    #- Handle different encodings (try 'utf-8', 'latin-1', 'cp1252')
    #- Handle FileNotFoundError with appropriate error message
    #- Skip the header row
    #- Remove empty lines
   

    encodings = ['utf-8', 'latin-1', 'cp1252']
    lines = []
    
    for encoding in encodings:
        try:
            with open(df, 'r', encoding=encoding) as file:
                # Read lines, skip header, and remove empty lines
                lines = [line.strip() for i, line in enumerate(file) if i > 0 and line.
                         strip()]
                
                lines = [
                line.strip()
                for line in lines[1:]
                if line.strip()
            ]
            return lines
        except FileNotFoundError:
            print(f"Error: The file '{df}' was not found.")
            return []
        except UnicodeDecodeError:
            continue
    print(f"Error: Unable to read the file '{df}' with the specified encodings.")
    return lines


  
def parse_transactions(raw_lines):
    """
    Parses raw lines into clean list of dictionaries
    """
    parsed_data = []
    expected_fields = 8

    for line in raw_lines:
        parts = line.split("|")

        # Skip rows with incorrect number of fields
        if len(parts) != expected_fields:
            continue

        (
            transaction_id,
            date,
            product_id,
            product_name,
            quantity,
            unit_price,
            customer_id,
            region
        ) = parts

        try:
            # Clean ProductName (remove commas)
            product_name = product_name.replace(",", "")

            # Clean and convert Quantity
            quantity = int(quantity)

            # Clean and convert UnitPrice
            unit_price = float(unit_price.replace(",", ""))

            record = {
                "TransactionID": transaction_id,
                "Date": date,
                "ProductID": product_id,
                "ProductName": product_name,
                "Quantity": quantity,
                "UnitPrice": unit_price,
                "CustomerID": customer_id,
                "Region": region
            }

            parsed_data.append(record)

        except ValueError:
            # Skip rows with conversion errors
            continue

    return parsed_data
  
def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    """
    Validates transactions and applies optional filters
    """
    valid_transactions = []
    invalid_count = 0

    # ---- Validation ----
    for tx in transactions:
        try:
            # Required fields check
            required_fields = [
                "TransactionID", "Date", "ProductID",
                "ProductName", "Quantity", "UnitPrice",
                "CustomerID", "Region"
            ]

            if not all(field in tx for field in required_fields):
                invalid_count += 1
                continue

            # Business rules
            if (
                tx["Quantity"] <= 0 or
                tx["UnitPrice"] <= 0 or
                not tx["TransactionID"].startswith("T") or
                not tx["ProductID"].startswith("P") or
                not tx["CustomerID"].startswith("C")
            ):
                invalid_count += 1
                continue

            valid_transactions.append(tx)

        except Exception:
            invalid_count += 1

    # ---- Display available regions ----
    available_regions = sorted(
        set(tx["Region"] for tx in valid_transactions if tx.get("Region"))
    )
    print("Available regions:", available_regions)

    # ---- Compute transaction amount range ----
    amounts = [
        tx["Quantity"] * tx["UnitPrice"]
        for tx in valid_transactions
    ]

    if amounts:
        print(f"Transaction amount range: min={min(amounts)}, max={max(amounts)}")

    total_input = len(transactions)
    filtered_by_region = 0
    filtered_by_amount = 0

    filtered_transactions = valid_transactions

    # ---- Region Filter ----
    if region:
        before = len(filtered_transactions)
        filtered_transactions = [
            tx for tx in filtered_transactions
            if tx["Region"] == region
        ]
        filtered_by_region = before - len(filtered_transactions)
        print(f"Records after region filter ({region}): {len(filtered_transactions)}")

    # ---- Amount Filters ----
    if min_amount is not None or max_amount is not None:
        before = len(filtered_transactions)
        filtered_transactions = [
            tx for tx in filtered_transactions
            if (
                (min_amount is None or tx["Quantity"] * tx["UnitPrice"] >= min_amount) and
                (max_amount is None or tx["Quantity"] * tx["UnitPrice"] <= max_amount)
            )
        ]
        filtered_by_amount = before - len(filtered_transactions)
        print(f"Records after amount filter: {len(filtered_transactions)}")

    # ---- Summary ----
    filter_summary = {
        "total_input": total_input,
        "invalid": invalid_count,
        "filtered_by_region": filtered_by_region,
        "filtered_by_amount": filtered_by_amount,
        "final_count": len(filtered_transactions)
    }

    return filtered_transactions, invalid_count, filter_summary
