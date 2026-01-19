import pandas as pd
import utils.api_handler as ah
import utils.data_processor as dp
import os
import utils.file_handler as fh

# Read file with encoding handling
df = pd.read_csv(
    "sales_data.txt",
    delimiter="|",
    encoding="latin1",
    engine="python"
)
# Display the first few rows of the DataFrame
print(df.to_string())

def main():
   
    try:
        print("=" * 40)
        print("SALES ANALYTICS SYSTEM")
        print("=" * 40)

        # --------------------------------------------------
        # 1. Read sales data file
        # --------------------------------------------------
        print("\n[1/10] Reading sales data...")
        with open("data/sales_data.txt", "r", encoding="utf-8") as file:
            raw_lines = file.readlines()

        print(f"✓ Successfully read {len(raw_lines) - 1} transactions")

        # --------------------------------------------------
        # 2. Parse and clean transactions
        # --------------------------------------------------
        print("\n[2/10] Parsing and cleaning data...")
        transactions = fh.parse_transactions(raw_lines)
        print(f"✓ Parsed {len(transactions)} records")

        # --------------------------------------------------
        # 3. Display filter options
        # --------------------------------------------------
        print("\n[3/10] Filter Options Available:")

        regions = sorted({t["Region"] for t in transactions if t["Region"]})
        amounts = [t["Quantity"] * t["UnitPrice"] for t in transactions]
        min_amt, max_amt = min(amounts), max(amounts)

        print(f"Regions: {', '.join(regions)}")
        print(f"Amount Range: ₹{min_amt:,.0f} - ₹{max_amt:,.0f}")

        apply_filter = input("\nDo you want to filter data? (y/n): ").strip().lower()

        region_filter = None
        min_amount = None
        max_amount = None

        if apply_filter == "y":
            region_filter = input("Enter region (or press Enter to skip): ").strip()
            region_filter = region_filter if region_filter else None

            min_val = input("Enter minimum amount (or press Enter to skip): ").strip()
            max_val = input("Enter maximum amount (or press Enter to skip): ").strip()

            min_amount = float(min_val) if min_val else None
            max_amount = float(max_val) if max_val else None

        # --------------------------------------------------
        # 4. Validate and filter transactions
        # --------------------------------------------------
        print("\n[4/10] Validating transactions...")
        valid_txns, invalid_count, summary = fh.validate_and_filter(
            transactions,
            region=region_filter,
            min_amount=min_amount,
            max_amount=max_amount
        )

        print(f"✓ Valid: {len(valid_txns)} | Invalid: {invalid_count}")

        # --------------------------------------------------
        # 5. Analysis
        # --------------------------------------------------
        print("\n[5/10] Analyzing sales data...")
        dp.region_wise_sales(valid_txns)
        dp.top_selling_products(valid_txns)
        dp.customer_analysis(valid_txns)
        dp.daily_sales_trend(valid_txns)
        dp.find_peak_sales_day(valid_txns)
        dp.low_performing_products(valid_txns)
        print("✓ Analysis complete")

        # --------------------------------------------------
        # 6. Fetch API products
        # --------------------------------------------------
        print("\n[6/10] Fetching product data from API...")
        api_products = ah.fetch_all_products()
        print(f"✓ Fetched {len(api_products)} products")

        # --------------------------------------------------
        # 7. Enrich sales data
        # --------------------------------------------------
        print("\n[7/10] Enriching sales data...")
        product_mapping = ah.create_product_mapping(api_products)
        enriched_transactions = ah.enrich_sales_data(valid_txns, product_mapping)

        enriched_count = sum(1 for t in enriched_transactions if t.get("API_Match"))
        success_rate = (enriched_count / len(enriched_transactions)) * 100 if enriched_transactions else 0

        print(f"✓ Enriched {enriched_count}/{len(enriched_transactions)} transactions ({success_rate:.1f}%)")

        # --------------------------------------------------
        # 8. Save enriched data
        # --------------------------------------------------
        print("\n[8/10] Saving enriched data...")
        ah.save_enriched_data(enriched_transactions)
        print("✓ Saved to: data/enriched_sales_data.txt")

        # --------------------------------------------------
        # 9. Generate report
        # --------------------------------------------------
        print("\n[9/10] Generating report...")
       # generate_sales_report(valid_txns, enriched_transactions)
        print("✓ Report saved to: output/sales_report.txt")

        # --------------------------------------------------
        # 10. Complete
        # --------------------------------------------------
        print("\n[10/10] Process Complete!")
        print("=" * 40)

    except FileNotFoundError:
        print("❌ Error: Required data file not found. Please check file paths.")
    except ValueError as ve:
        print(f"❌ Data format error: {ve}")
    except Exception as e:
        print(f"❌ Unexpected error occurred: {e}")


if __name__ == "__main__":
    main()

















if "ProductName" in df.columns:
    total_records = len(df)
    print(f"Total records before cleaning: {total_records}")
    # Remove empty rows based on all columns    
    df = df.dropna(how="all")

    # Clean ProductName (remove commas)
    df["ProductName"] = df["ProductName"].astype(str).str.replace(",", "", regex=False)
    cleaned_records = len(df)
    print(f"Total records after cleaning: {cleaned_records}")

    # Clean UnitPrice (remove commas, convert to float)
    df["UnitPrice"] = (
        df["UnitPrice"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

     # Apply INVALID record rules
    invalid_condition = (
        df["CustomerID"].isna() |
        df["Region"].isna() |
        (df["Quantity"] <= 0) |
        (df["UnitPrice"] <= 0) |
        (~df["TransactionID"].astype(str).str.startswith("T"))
    )

    invalid_records = df[invalid_condition]
    valid_df = df[~invalid_condition]

    # Print validation output (EXACT FORMAT)
    print(f"Total records parsed: {total_records}")
    print(f"Invalid records removed: {len(invalid_records)}")
    print(f"Valid records after cleaning: {len(valid_df)}")

    valid_df