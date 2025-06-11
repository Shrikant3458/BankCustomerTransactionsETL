import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, col, upper, trim, coalesce, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 1. Data Ingestion ---
# Define your S3 input and output paths
input_path = 'S3/banktransaction/banktransaction_rawdata.csv'
output_path = 'S3/banktransaction/banktransaction_cleandata.csv'

# Read the raw CSV data from S3
print(f"Reading raw data from: {'S3/banktransaction/banktransaction_rawdata.csv'}")
customer_transactions_df = spark.read.csv(input_path, header=True, inferSchema=True)
print("Raw schema:")
customer_transactions_df.printSchema()

# --- 2. Data Transformation and Cleaning ---
print("Handling nulls and standardizing basic data...")
transformed_df = customer_transactions_df.withColumn("amount",
                                                 when(col("amount").cast("string").isNull(), lit(None))
                                                 .otherwise(col("amount").cast("double"))) \
                                     .withColumn("amount", coalesce(col("amount"), lit(0.0))) \
                                     .withColumn("description",
                                                 when(upper(trim(col("description"))) == "NULL", lit(None))
                                                 .otherwise(col("description")))

 # 2. Map country codes to country names (simplified mapping)
print("Mapping country codes to names...")
transformed_df = transformed_df.withColumn("country_name",
    when(upper(trim(col("country_code"))).isin("US", "USA"), "United States")
    .when(upper(trim(col("country_code"))).isin("GB", "UK"), "United Kingdom")
    .when(upper(trim(col("country_code"))).isin("DEU", "DE", "GERMANY"), "Germany")
    .when(upper(trim(col("country_code"))).isin("IN", "IND", "INDIA"), "India")
    .when(upper(trim(col("country_code"))) == "CA", "Canada")
    .when(upper(trim(col("country_code"))) == "AUS", "Australia")
    .otherwise("Other/Unknown") 
)
 # 3. Standardize account types (simplified mapping)
print("Standardizing account types...")
transformed_df = transformed_df.withColumn("account_type_standardized",
    when(upper(trim(col("account_type"))).isin("SAVING", "SAVINGS"), "Savings")
    .when(upper(trim(col("account_type"))).isin("CHEKING", "CHECKING", "CHKNG"), "Checking")
    .when(upper(trim(col("account_type"))).isin("CURRENT"), "Current")
    .when(upper(trim(col("account_type"))).isin("CREDITCARD"), "Credit Card")
    .when(upper(trim(col("account_type"))).isin("LOAN"), "Loan")
    .otherwise("Other") # For 'UNKNOWN_TYPE' or any other unmapped types
)

# 4. Identify suspicious transactions (simple rule: any transaction > 5000)
print("Identifying suspicious transactions (simple rule)...")
transformed_df = transformed_df.withColumn("is_suspicious",
    when(col("amount") > 5000, lit(True))
    .otherwise(lit(False))
)

# Select and reorder columns for the qualified output
qualified_output_df = transformed_df.select(
    "transaction_id",
    "customer_id",
    "account_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "currency",
    "description",
    "account_type_standardized", 
    "country_name",               
    "is_suspicious”
)
print("Qualified output schema:")
qualified_output_df.printSchema()
print("Sample of qualified output data:")
qualified_output_df.show(5, truncate=False)

# --- 3. Data Loading (to S3) ---
# Write the qualified output to S3 in Parquet format
print(f"Writing qualified output to: {'S3/banktransaction/banktransaction_cleandata.csv'}")
qualified_output_df.write.mode("overwrite").csv('S3/banktransaction/banktransaction_cleandata.csv')
print("ETL job completed successfully!")
job.commit()
