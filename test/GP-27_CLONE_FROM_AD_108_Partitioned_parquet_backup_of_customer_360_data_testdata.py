spark.catalog.setCurrentCatalog("purgo_databricks")

# Test Data Generation for purgo_playground.customer_360_raw
# PySpark code for Databricks

# from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import (  
    StructType, StructField, LongType, StringType, DateType
)
from pyspark.sql import Row  
from datetime import date, timedelta  

# Define schema matching purgo_playground.customer_360_raw
customer_360_raw_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("company", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("industry", StringType(), True),
    StructField("account_manager", StringType(), True),
    StructField("creation_date", DateType(), True),
    StructField("last_interaction_date", DateType(), True),
    StructField("purchase_history", StringType(), True),
    StructField("notes", StringType(), True),
    StructField("zip", StringType(), True)
])

today = date.today()
thirty_days_ago = today - timedelta(days=30)
sixty_days_ago = today - timedelta(days=60)
future_date = today + timedelta(days=10)

# Diverse test data covering happy path, edge, error, null, special/multibyte
test_data = [
    # 1. Happy path: all fields populated, valid US state, recent dates
    Row(1, "Alice Smith", "alice@example.com", "+1-555-1234", "Acme Corp", "Manager", "123 Main St", "San Francisco", "CA", "USA", "Technology", "John Doe", today, today, "Order#1234: $500", "VIP customer", "94105"),
    # 2. Happy path: another state, different data
    Row(2, "Bob Lee", "bob.lee@example.com", "+1-555-5678", "Beta Inc", "Engineer", "456 Market Ave", "New York", "NY", "USA", "Finance", "Jane Roe", today, today, "Order#5678: $1200", "Frequent buyer", "10001"),
    # 3. Edge: creation_date exactly 30 days ago (should be retained after vacuum)
    Row(3, "Carol O'Connor", "carol.o'connor@example.com", "+1-555-8765", "Gamma LLC", "Director", "789 Broadway", "Austin", "TX", "USA", "Healthcare", "Jim Poe", thirty_days_ago, today, "Order#9101: $300", "Edge case", "73301"),
    # 4. Edge: creation_date exactly 31 days ago (should be removed after vacuum)
    Row(4, "David Kim", "david.kim@example.com", "+1-555-4321", "Delta Ltd", "Analyst", "321 1st Ave", "Seattle", "WA", "USA", "Retail", "Sue Lin", today - timedelta(days=31), today, "Order#1122: $700", "Old record", "98101"),
    # 5. Error: negative id, invalid email, invalid phone
    Row(-5, "Eve Adams", "not-an-email", "no-phone", "Epsilon GmbH", "Consultant", "654 2nd St", "Berlin", "BE", "Germany", "Consulting", "Max Mustermann", today, today, "Order#3344: $900", "Invalid contact info", "10115"),
    # 6. Error: null id, null name, null email, null phone
    Row(None, None, None, None, "Zeta S.A.", "CEO", "Av. Siempre Viva 123", "Madrid", "MD", "Spain", "Energy", "Ana García", today, today, "Order#5566: $1500", "Nulls in key fields", "28001"),
    # 7. Edge: empty strings in all string fields
    Row(7, "", "", "", "", "", "", "", "", "", "", "", today, today, "", "", ""),
    # 8. Edge: special characters in all string fields
    Row(8, "François L'œuf", "françois+test@exämple.com", "+33-1-23-45-67-89", "Société Générale", "Développeur", "12, rue de l'Université", "Paris", "Île-de-France", "France", "Banque/Finance", "Élodie Dubois", today, today, "Achat#7890: €2000", "Client spécial: ☃️", "75007"),
    # 9. Edge: multi-byte/unicode in notes, name, address
    Row(9, "山田太郎", "taro.yamada@example.jp", "+81-3-1234-5678", "株式会社山田", "部長", "東京都千代田区1-1-1", "東京", "東京都", "日本", "製造業", "佐藤花子", today, today, "注文#123: ¥100000", "重要顧客🌸", "100-0001"),
    # 10. Edge: long text in notes and purchase_history
    Row(10, "Long Text", "long.text@example.com", "+1-555-9999", "LongText Inc", "Writer", "1000 Long Rd", "Longville", "LT", "USA", "Publishing", "Long Manager", today, today, "Order#" + "X"*1000, "N"*2000, "12345"),
    # 11. Edge: nulls in all nullable fields except id
    Row(11, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # 12. Edge: duplicate id
    Row(1, "Duplicate Alice", "alice2@example.com", "+1-555-0000", "Acme Corp", "Manager", "123 Main St", "San Francisco", "CA", "USA", "Technology", "John Doe", today, today, "Order#9999: $100", "Duplicate id", "94105"),
    # 13. Edge: zero id
    Row(0, "Zero Id", "zero.id@example.com", "+1-555-0001", "Zero Corp", "Zero", "0 Zero St", "Zero City", "ZZ", "Nowhere", "None", "Zero Manager", today, today, "Order#0: $0", "Zero id", "00000"),
    # 14. Edge: null creation_date and last_interaction_date
    Row(14, "Null Dates", "null.dates@example.com", "+1-555-0002", "Null Corp", "Null", "Null St", "Null City", "NU", "Nullland", "Null", "Null Manager", None, None, "Order#null", "Null dates", "NULL"),
    # 15. Edge: future creation_date
    Row(15, "Future Date", "future.date@example.com", "+1-555-0003", "Future Corp", "Futurist", "123 Future Rd", "Tomorrowland", "FD", "Futureland", "Futurism", "Future Manager", future_date, future_date, "Order#future", "Future record", "99999"),
    # 16. Edge: state with special characters
    Row(16, "Special State", "special.state@example.com", "+1-555-0004", "Special Corp", "Specialist", "123 Special St", "Special City", "N/A", "USA", "Special", "Special Manager", today, today, "Order#special", "Special state value", "SPL01"),
    # 17. Edge: state with whitespace
    Row(17, "Whitespace State", "whitespace.state@example.com", "+1-555-0005", "Whitespace Corp", "Whitespace", "123 Whitespace St", "Whitespace City", " CA ", "USA", "Whitespace", "Whitespace Manager", today, today, "Order#ws", "Whitespace in state", "WS001"),
    # 18. Edge: state with mixed case
    Row(18, "Mixed Case State", "mixed.case@example.com", "+1-555-0006", "MixedCase Corp", "Mixed", "123 Mixed St", "Mixed City", "cA", "USA", "Mixed", "Mixed Manager", today, today, "Order#mc", "Mixed case state", "MC001"),
    # 19. Edge: state is null
    Row(19, "Null State", "null.state@example.com", "+1-555-0007", "NullState Corp", "Nuller", "123 Null St", "Null City", None, "USA", "Null", "Null Manager", today, today, "Order#ns", "Null state", "NS001"),
    # 20. Edge: state is empty string
    Row(20, "Empty State", "empty.state@example.com", "+1-555-0008", "EmptyState Corp", "Empty", "123 Empty St", "Empty City", "", "USA", "Empty", "Empty Manager", today, today, "Order#es", "Empty state", "ES001"),
    # 21. Edge: zip with special characters
    Row(21, "Special Zip", "special.zip@example.com", "+1-555-0009", "SpecialZip Corp", "Zipper", "123 Zip St", "Zip City", "ZZ", "USA", "Zip", "Zip Manager", today, today, "Order#zip", "Special zip", "12-345*?"),
    # 22. Edge: zip is null
    Row(22, "Null Zip", "null.zip@example.com", "+1-555-0010", "NullZip Corp", "Nuller", "123 NullZip St", "NullZip City", "NZ", "USA", "NullZip", "NullZip Manager", today, today, "Order#nz", "Null zip", None),
    # 23. Edge: zip is empty string
    Row(23, "Empty Zip", "empty.zip@example.com", "+1-555-0011", "EmptyZip Corp", "Empty", "123 EmptyZip St", "EmptyZip City", "EZ", "USA", "EmptyZip", "EmptyZip Manager", today, today, "Order#ez", "Empty zip", ""),
    # 24. Edge: all string fields with maximum length (simulate with 255 chars)
    Row(24, "N"*255, "E"*255, "P"*255, "C"*255, "J"*255, "A"*255, "CI"*255, "ST"*255, "CO"*255, "IN"*255, "AM"*255, today, today, "PH"*255, "NO"*255, "Z"*255),
    # 25. Edge: all string fields with special unicode
    Row(25, "😀😁😂🤣😃😄😅😆😉😊", "unicode@example.com", "+1-555-0012", "Emoji Corp", "Emojist", "123 Emoji St", "Emoji City", "EM", "USA", "Emoji", "Emoji Manager", today, today, "Order#emoji", "Notes with emoji 😎👍", "EM001"),
    # 26. Edge: all fields null except id
    Row(26, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None),
    # 27. Edge: id is None, all other fields populated
    Row(None, "No Id", "no.id@example.com", "+1-555-0013", "NoId Corp", "NoId", "123 NoId St", "NoId City", "NI", "USA", "NoId", "NoId Manager", today, today, "Order#noid", "No id", "NI001"),
    # 28. Edge: invalid date format (simulate as string, will cause error if inserted)
    # This record is for error scenario, not to be inserted directly, but included for completeness
    # Row(28, "Invalid Date", "invalid.date@example.com", "+1-555-0014", "InvalidDate Corp", "Invalid", "123 Invalid St", "Invalid City", "ID", "USA", "Invalid", "Invalid Manager", "2024-02-30", "2024-02-30", "Order#invalid", "Invalid date", "ID001"),
    # 29. Edge: purchase_history and notes are null
    Row(29, "Null Purchase/Notes", "null.pn@example.com", "+1-555-0015", "NullPN Corp", "NullPN", "123 NullPN St", "NullPN City", "NP", "USA", "NullPN", "NullPN Manager", today, today, None, None, "NP001"),
    # 30. Edge: purchase_history and notes are empty string
    Row(30, "Empty Purchase/Notes", "empty.pn@example.com", "+1-555-0016", "EmptyPN Corp", "EmptyPN", "123 EmptyPN St", "EmptyPN City", "EP", "USA", "EmptyPN", "EmptyPN Manager", today, today, "", "", "EP001"),
]

# Remove the invalid date format record for actual DataFrame creation
test_data_filtered = [row for row in test_data if not (isinstance(row[12], str) and "-" in row[12] and not isinstance(row[12], date))]

# Create DataFrame
df_customer_360_raw = spark.createDataFrame(test_data_filtered, schema=customer_360_raw_schema)

# Show sample data for verification
df_customer_360_raw.show(truncate=False)

# Save as compressed parquet partitioned by state to /Volumes/customer_360_raw_backup
try:
    (
        df_customer_360_raw
        .write
        .mode("overwrite")
        .partitionBy("state")
        .option("compression", "snappy")
        .parquet("/Volumes/customer_360_raw_backup")
    )
    print("Backup parquet files written successfully.")
except Exception as e:
    print(f"Error writing backup parquet files: {e}")

# Test Data Generation for purgo_playground.customer_360_raw_backup_log
from pyspark.sql.types import StructType, StructField, StringType, LongType  

backup_log_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True),
    StructField("operation_type", StringType(), True),
    StructField("record_count", LongType(), True),
    StructField("error_message", StringType(), True)
])

# ISO 8601 timestamp format
now_iso = date.today().isoformat() + "T12:00:00Z"
yesterday_iso = (date.today() - timedelta(days=1)).isoformat() + "T11:00:00Z"

backup_log_data = [
    # Happy path: successful backup
    Row(now_iso, "SUCCESS", "BACKUP", len(test_data_filtered), None),
    # Happy path: successful vacuum
    Row(now_iso, "SUCCESS", "VACUUM", 25, None),
    # Error: invalid volume path
    Row(yesterday_iso, "FAILURE", "BACKUP", 0, "Invalid volume path: /Volumes/nonexistent_backup"),
    # Error: insufficient storage
    Row(yesterday_iso, "FAILURE", "BACKUP", 0, "Insufficient storage space"),
    # Error: table lock
    Row(yesterday_iso, "FAILURE", "VACUUM", 0, "Table is locked"),
    # Error: missing source table (backup)
    Row(yesterday_iso, "FAILURE", "BACKUP", 0, "Source table not found"),
    # Error: missing source table (vacuum)
    Row(yesterday_iso, "FAILURE", "VACUUM", 0, "Source table not found"),
    # Error: permission denied
    Row(yesterday_iso, "FAILURE", "BACKUP", 0, "Permission denied: /Volumes/protected_backup"),
    # Error: invalid date format
    Row(yesterday_iso, "FAILURE", "BACKUP", 0, "Invalid date format"),
]

df_backup_log = spark.createDataFrame(backup_log_data, schema=backup_log_schema)

# Show backup log test data
df_backup_log.show(truncate=False)

# Note: Do not write backup log to table or file as per instructions.
# spark.stop()  # Do not stop SparkSession in Databricks
