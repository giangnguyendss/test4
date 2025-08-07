spark.catalog.setCurrentCatalog("purgo_databricks")

# Test Data Generation for purgo_playground.bai_sales_agg_obu_customer_datapack
# All imports are required for PySpark DataFrame and data types
from pyspark.sql import Row  
from pyspark.sql.types import (  
    StructType, StructField, StringType, BooleanType, TimestampType, LongType
)
from pyspark.sql.functions import lit  
from datetime import datetime  

# Test data covers: happy path, edge, error, null, special/multibyte chars, boundary, duplicates, zero/null sales, invalid dates

test_data = [
    # --- Happy Path: Valid records for all brands, all channels, valid cdl_effective_date within 36 months ---
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=False, channel_name="online", transaction_timestamp="2023-03-21T10:00:00.000+0000", integrated_units=100, integrated_normalized_units=90, integrated_dollars=10000, cdl_effective_date="2022-01-01"),
    Row(brand_normalized_name="mvasi", normalized_name="bevacizumab", market_normalized_name="oncology", competitor_flag=True, channel_name="retail", transaction_timestamp="2022-12-15T15:30:00.000+0000", integrated_units=200, integrated_normalized_units=180, integrated_dollars=20000, cdl_effective_date="2021-06-15"),
    Row(brand_normalized_name="riabni", normalized_name="rituximab", market_normalized_name="immunology", competitor_flag=False, channel_name="mobile", transaction_timestamp="2021-07-01T08:00:00.000+0000", integrated_units=150, integrated_normalized_units=140, integrated_dollars=15000, cdl_effective_date="2020-07-01"),
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=True, channel_name="other", transaction_timestamp="2023-01-10T12:00:00.000+0000", integrated_units=50, integrated_normalized_units=45, integrated_dollars=5000, cdl_effective_date="2022-12-31"),
    # --- Edge: cdl_effective_date exactly 36 months before reference (2023-01-01) ---
    Row(brand_normalized_name="mvasi", normalized_name="bevacizumab", market_normalized_name="oncology", competitor_flag=False, channel_name="online", transaction_timestamp="2020-01-01T00:00:00.000+0000", integrated_units=80, integrated_normalized_units=75, integrated_dollars=8000, cdl_effective_date="2020-01-01"),
    # --- Error: cdl_effective_date more than 36 months before reference (should be excluded) ---
    Row(brand_normalized_name="riabni", normalized_name="rituximab", market_normalized_name="immunology", competitor_flag=True, channel_name="retail", transaction_timestamp="2019-12-31T23:59:59.000+0000", integrated_units=60, integrated_normalized_units=55, integrated_dollars=6000, cdl_effective_date="2019-12-31"),
    # --- Error: brand not in allowed list (should be excluded) ---
    Row(brand_normalized_name="herceptin", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=False, channel_name="online", transaction_timestamp="2022-05-05T09:00:00.000+0000", integrated_units=120, integrated_normalized_units=110, integrated_dollars=12000, cdl_effective_date="2022-01-01"),
    # --- Error: null cdl_effective_date (should be excluded) ---
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=False, channel_name="retail", transaction_timestamp="2023-04-01T10:00:00.000+0000", integrated_units=70, integrated_normalized_units=65, integrated_dollars=7000, cdl_effective_date=None),
    # --- Error: invalid cdl_effective_date format (should be excluded) ---
    Row(brand_normalized_name="mvasi", normalized_name="bevacizumab", market_normalized_name="oncology", competitor_flag=True, channel_name="mobile", transaction_timestamp="2022-11-11T11:11:11.000+0000", integrated_units=90, integrated_normalized_units=85, integrated_dollars=9000, cdl_effective_date="2022/11/11"),
    Row(brand_normalized_name="riabni", normalized_name="rituximab", market_normalized_name="immunology", competitor_flag=False, channel_name="other", transaction_timestamp="2021-01-01T00:00:00.000+0000", integrated_units=30, integrated_normalized_units=28, integrated_dollars=3000, cdl_effective_date="01-01-2021"),
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=True, channel_name="online", transaction_timestamp="2022-02-02T02:02:02.000+0000", integrated_units=40, integrated_normalized_units=38, integrated_dollars=4000, cdl_effective_date="invalid-date"),
    # --- Edge: duplicate records for same group (should sum) ---
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=False, channel_name="online", transaction_timestamp="2023-03-21T10:00:00.000+0000", integrated_units=100, integrated_normalized_units=90, integrated_dollars=10000, cdl_effective_date="2022-01-01"),
    # --- Edge: zero sales values ---
    Row(brand_normalized_name="mvasi", normalized_name="bevacizumab", market_normalized_name="oncology", competitor_flag=True, channel_name="retail", transaction_timestamp="2022-12-15T15:30:00.000+0000", integrated_units=0, integrated_normalized_units=0, integrated_dollars=0, cdl_effective_date="2021-06-15"),
    # --- Edge: null sales values (should be treated as zero in sum) ---
    Row(brand_normalized_name="riabni", normalized_name="rituximab", market_normalized_name="immunology", competitor_flag=False, channel_name="mobile", transaction_timestamp="2021-07-01T08:00:00.000+0000", integrated_units=None, integrated_normalized_units=None, integrated_dollars=None, cdl_effective_date="2020-07-01"),
    # --- Special: special characters and multi-byte characters in string fields ---
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab™", market_normalized_name="oncológía", competitor_flag=False, channel_name="onlïne", transaction_timestamp="2023-05-05T05:05:05.000+0000", integrated_units=55, integrated_normalized_units=50, integrated_dollars=5500, cdl_effective_date="2022-05-05"),
    Row(brand_normalized_name="mvasi", normalized_name="bévacizumab", market_normalized_name="肿瘤学", competitor_flag=True, channel_name="零售", transaction_timestamp="2022-06-06T06:06:06.000+0000", integrated_units=66, integrated_normalized_units=60, integrated_dollars=6600, cdl_effective_date="2021-06-06"),
    Row(brand_normalized_name="riabni", normalized_name="ритуксимаб", market_normalized_name="иммунология", competitor_flag=False, channel_name="мобильный", transaction_timestamp="2021-07-07T07:07:07.000+0000", integrated_units=77, integrated_normalized_units=70, integrated_dollars=7700, cdl_effective_date="2020-07-07"),
    # --- Edge: competitor_flag null (should be allowed as per schema) ---
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=None, channel_name="online", transaction_timestamp="2023-03-21T10:00:00.000+0000", integrated_units=10, integrated_normalized_units=9, integrated_dollars=1000, cdl_effective_date="2022-01-01"),
    # --- Edge: channel_name null (should be allowed as per schema) ---
    Row(brand_normalized_name="mvasi", normalized_name="bevacizumab", market_normalized_name="oncology", competitor_flag=True, channel_name=None, transaction_timestamp="2022-12-15T15:30:00.000+0000", integrated_units=20, integrated_normalized_units=18, integrated_dollars=2000, cdl_effective_date="2021-06-15"),
    # --- Edge: all fields null except cdl_effective_date (should be allowed as per schema, but filtered out if brand not in list or cdl_effective_date invalid) ---
    Row(brand_normalized_name=None, normalized_name=None, market_normalized_name=None, competitor_flag=None, channel_name=None, transaction_timestamp=None, integrated_units=None, integrated_normalized_units=None, integrated_dollars=None, cdl_effective_date="2022-01-01"),
    # --- Edge: cdl_effective_date in the future (should be included if within 36 months window) ---
    Row(brand_normalized_name="kanjinti", normalized_name="trastuzumab", market_normalized_name="oncology", competitor_flag=False, channel_name="online", transaction_timestamp="2025-01-01T00:00:00.000+0000", integrated_units=5, integrated_normalized_units=4, integrated_dollars=500, cdl_effective_date="2024-12-31"),
    # --- Edge: cdl_effective_date is today (should be included) ---
    Row(brand_normalized_name="mvasi", normalized_name="bevacizumab", market_normalized_name="oncology", competitor_flag=True, channel_name="retail", transaction_timestamp=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0000"), integrated_units=1, integrated_normalized_units=1, integrated_dollars=100, cdl_effective_date=datetime.now().strftime("%Y-%m-%d")),
    # --- Edge: cdl_effective_date is empty string (should be excluded) ---
    Row(brand_normalized_name="riabni", normalized_name="rituximab", market_normalized_name="immunology", competitor_flag=False, channel_name="mobile", transaction_timestamp="2021-07-01T08:00:00.000+0000", integrated_units=10, integrated_normalized_units=10, integrated_dollars=1000, cdl_effective_date=""),
]

# Define schema matching the table
test_schema = StructType([
    StructField("brand_normalized_name", StringType(), True),
    StructField("normalized_name", StringType(), True),
    StructField("market_normalized_name", StringType(), True),
    StructField("competitor_flag", BooleanType(), True),
    StructField("channel_name", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),  # Will cast to TimestampType after DataFrame creation
    StructField("integrated_units", LongType(), True),
    StructField("integrated_normalized_units", LongType(), True),
    StructField("integrated_dollars", LongType(), True),
    StructField("cdl_effective_date", StringType(), True),
])

# Create DataFrame
df = spark.createDataFrame(test_data, schema=test_schema)

# Convert transaction_timestamp from string to timestamp (Databricks format)
from pyspark.sql.functions import col, to_timestamp  

df = df.withColumn(
    "transaction_timestamp",
    to_timestamp(col("transaction_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
)

# Validate and convert data types for sales metrics (ensure LongType, treat null as zero for test purposes)
from pyspark.sql.functions import coalesce  

df = df.withColumn("integrated_units", coalesce(col("integrated_units"), lit(0).cast(LongType())))
df = df.withColumn("integrated_normalized_units", coalesce(col("integrated_normalized_units"), lit(0).cast(LongType())))
df = df.withColumn("integrated_dollars", coalesce(col("integrated_dollars"), lit(0).cast(LongType())))

# Show the generated test data (for validation)
df.show(truncate=False)
