# BankCustomerTransactionsETL

📊 Bank Customer Transactions ETL – AWS Glue Project

1.This is an end-to-end ETL project built using AWS Glue to process and enrich bank customer transaction data.

2.Ingests raw CSV files from Amazon S3 containing inconsistent and incomplete transaction records.

3.Cleans the data by handling null values and correcting invalid account types.

4.Maps inconsistent country codes (e.g., "US", "USA", "840") to standardized country names.

5.Standardizes and converts currency formats to a common base (e.g., USD) using exchange rates.

6.Identifies suspicious transactions based on customizable rules (e.g., high-value or cross-border activity).

7.Outputs a qualified transaction history with enriched metadata in Parquet format to S3.

8.Designed for compliance audits, fraud detection, and integration with tools like Athena for analysis.
