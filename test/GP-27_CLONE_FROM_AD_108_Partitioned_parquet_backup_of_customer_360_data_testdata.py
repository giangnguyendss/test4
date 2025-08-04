# Test Data Generation for purgo_playground.customer_360_raw
# PySpark code for Databricks

from pyspark.sql import SparkSession  # SparkSession is already available in Databricks
from pyspark.sql.types import (  
    StructType, StructField, LongType, StringType, DateType
)
from pyspark.sql import Row  
from datetime import date, timedelta  

# Define schema matching purgo_playground.customer_360_raw
customer_360_schema = StructType([
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

today = date(2024, 6, 30)
thirty_days_ago = today - timedelta(days=30)
ninety_days_ago = today - timedelta(days=90)
one_year_ago = today - timedelta(days=365)

test_data = [
    # Happy path: valid, typical record
    Row(1, "Alice Smith", "alice.smith@example.com", "+1-555-1234", "Acme Corp", "Manager", "123 Main St", "San Francisco", "CA", "USA", "Technology", "John Doe", today, today, "Order#1234:2024-06-01", "VIP customer", "94105"),
    # Happy path: another valid record, different state
    Row(2, "Bob Lee", "bob.lee@example.com", "+1-555-5678", "Beta Inc", "Engineer", "456 Market St", "New York", "NY", "USA", "Finance", "Jane Roe", today, today, "Order#5678:2024-06-15", "Frequent buyer", "10001"),
    # Happy path: valid, with multi-byte characters
    Row(3, "李雷", "li.lei@example.cn", "+86-10-12345678", "北京科技", "开发工程师", "中关村大街1号", "北京", "BJ", "China", "科技", "王伟", today, today, "订单#8888:2024-06-20", "重要客户", "100080"),
    # Happy path: valid, with special characters
    Row(4, "O'Connor, Sean", "sean.o'connor@example.ie", "+353-1-2345678", "Dublin Tech", "CTO", "1 St. Patrick's Rd.", "Dublin", "D", "Ireland", "IT", "Mary O'Brien", today, today, "Order#9999:2024-06-25", "Loves ☘️", "D02X285"),
    # Edge: creation_date exactly 30 days ago (should be retained after vacuum)
    Row(5, "Carol Edge", "carol.edge@example.com", "+1-555-0000", "Edge Cases Ltd", "Analyst", "789 Edge St", "Austin", "TX", "USA", "Consulting", "Sam Edge", thirty_days_ago, today, "Order#0001:2024-05-31", "Boundary test", "73301"),
    # Edge: creation_date just before 30 days ago (should be deleted after vacuum)
    Row(6, "Dan Old", "dan.old@example.com", "+1-555-1111", "Old Data Inc", "Retired", "321 Old Rd", "Houston", "TX", "USA", "History", "Old Timer", thirty_days_ago - timedelta(days=1), today, "Order#0002:2024-05-30", "Should be vacuumed", "77001"),
    # Edge: creation_date exactly today
    Row(7, "Eve New", "eve.new@example.com", "+1-555-2222", "New Data LLC", "Intern", "654 New Ave", "Los Angeles", "CA", "USA", "Startups", "New Boss", today, today, "Order#0003:2024-06-30", "Brand new", "90001"),
    # Edge: creation_date 1 year ago
    Row(8, "Frank Year", "frank.year@example.com", "+1-555-3333", "Yearly Co", "Director", "987 Year Blvd", "Chicago", "IL", "USA", "Manufacturing", "Year Lead", one_year_ago, today, "Order#0004:2023-06-30", "Old record", "60601"),
    # Error: invalid email format
    Row(9, "Grace Error", "invalid-email-format", "+1-555-4444", "Error Inc", "Tester", "111 Bug St", "Seattle", "WA", "USA", "QA", "Error Handler", today, today, "Order#0005:2024-06-29", "Invalid email", "98101"),
    # Error: NULL email (required field)
    Row(10, "Hank Null", None, "+1-555-5555", "Nullables", "Consultant", "222 Null Rd", "Portland", "OR", "USA", "Consulting", "Null Boss", today, today, "Order#0006:2024-06-28", "Email is NULL", "97201"),
    # Error: NULL id (required field)
    Row(None, "Ivy NoID", "ivy.noid@example.com", "+1-555-6666", "NoID Corp", "Analyst", "333 NoID Ave", "Boston", "MA", "USA", "Analytics", "NoID Manager", today, today, "Order#0007:2024-06-27", "ID is NULL", "02101"),
    # Error: NULL state (required field)
    Row(12, "Jack NoState", "jack.nostate@example.com", "+1-555-7777", "NoState LLC", "Manager", "444 NoState Blvd", "Miami", None, "USA", "Tourism", "NoState Lead", today, today, "Order#0008:2024-06-26", "State is NULL", "33101"),
    # NULL handling: NULL in non-required fields
    Row(13, None, "kate.nullname@example.com", "+1-555-8888", "Null Name Inc", "Developer", None, "Denver", "CO", "USA", "Software", "Null Name Boss", today, today, "Order#0009:2024-06-25", None, "80201"),
    # Special characters: emoji in notes
    Row(14, "Leo Emoji", "leo.emoji@example.com", "+1-555-9999", "Emoji Corp", "Designer", "555 Emoji St", "Orlando", "FL", "USA", "Design", "Emoji Lead", today, today, "Order#0010:2024-06-24", "Loves 🦄🚀", "32801"),
    # Special characters: multi-byte in company
    Row(15, "Miyuki 山田", "miyuki.yamada@example.jp", "+81-3-1234-5678", "株式会社サンプル", "営業", "東京都千代田区1-1-1", "東京", "TK", "Japan", "商社", "田中", today, today, "注文#123:2024-06-23", "日本語テスト", "100-0001"),
    # Edge: zip with special chars
    Row(16, "Nina Zip", "nina.zip@example.com", "+1-555-1010", "Zip Testers", "QA", "666 Zip Rd", "Phoenix", "AZ", "USA", "Testing", "Zip Boss", today, today, "Order#0011:2024-06-22", "Zip test", "85-001"),
    # Edge: phone with special chars
    Row(17, "Oscar Phone", "oscar.phone@example.com", "(555) 202-2020 ext.123", "Phone Co", "Support", "777 Phone St", "Dallas", "TX", "USA", "Telecom", "Phone Lead", today, today, "Order#0012:2024-06-21", "Phone test", "75201"),
    # Edge: purchase_history with JSON string
    Row(18, "Paula JSON", "paula.json@example.com", "+1-555-3030", "JSON Inc", "Data Scientist", "888 JSON Ave", "San Jose", "CA", "USA", "Data", "JSON Boss", today, today, '{"orders":[{"id":1,"date":"2024-06-20"}]}', "JSON in purchase_history", "95101"),
    # Edge: notes with SQL injection attempt
    Row(19, "Quinn SQL", "quinn.sql@example.com", "+1-555-4040", "SQLi Corp", "DBA", "999 SQL Blvd", "Austin", "TX", "USA", "Security", "SQL Lead", today, today, "Order#0013:2024-06-20", "Robert'); DROP TABLE Students;--", "73301"),
    # Edge: all fields NULL except id, email, state, creation_date
    Row(20, 20, "rachel.nulls@example.com", None, None, None, None, None, "CA", None, None, None, today, None, None, None, None),
    # Edge: all fields NULL except id, email, state, creation_date, last_interaction_date
    Row(21, 21, "sam.nulls@example.com", None, None, None, None, None, "NY", None, None, None, today, today, None, None, None),
    # Edge: all fields NULL except id, email, state, creation_date, last_interaction_date, purchase_history
    Row(22, 22, "tom.nulls@example.com", None, None, None, None, None, "TX", None, None, None, today, today, "Order#0014:2024-06-19", None, None),
    # Edge: all fields NULL except id, email, state, creation_date, last_interaction_date, purchase_history, notes
    Row(23, 23, "uma.nulls@example.com", None, None, None, None, None, "FL", None, None, None, today, today, "Order#0015:2024-06-18", "Only notes", None),
    # Edge: all fields NULL except id, email, state, creation_date, last_interaction_date, purchase_history, notes, zip
    Row(24, 24, "vic.nulls@example.com", None, None, None, None, None, "IL", None, None, None, today, today, "Order#0016:2024-06-17", "Only notes and zip", "60601"),
    # Edge: id at max bigint value
    Row(9223372036854775807, "Max BigInt", "max.bigint@example.com", "+1-555-5050", "BigInt Co", "Lead", "1000 BigInt St", "Houston", "TX", "USA", "Data", "BigInt Boss", today, today, "Order#0017:2024-06-16", "Max id", "77001"),
    # Edge: id at min bigint value
    Row(-9223372036854775808, "Min BigInt", "min.bigint@example.com", "+1-555-6060", "BigInt Co", "Lead", "1001 BigInt St", "Houston", "TX", "USA", "Data", "BigInt Boss", today, today, "Order#0018:2024-06-15", "Min id", "77001"),
    # Edge: name with special unicode
    Row(25, "Zoë 🌟", "zoe.star@example.com", "+1-555-7070", "Unicode Inc", "Artist", "200 Unicode Rd", "San Diego", "CA", "USA", "Arts", "Unicode Lead", today, today, "Order#0019:2024-06-14", "Unicode in name", "92101"),
    # Edge: company with newline/tab
    Row(26, "Yuri Tab", "yuri.tab@example.com", "+1-555-8080", "Tab\nCorp\tLtd", "Tabber", "300 Tab St", "San Jose", "CA", "USA", "Tech", "Tab Lead", today, today, "Order#0020:2024-06-13", "Tab/newline in company", "95101"),
    # Edge: notes with long string (max length test)
    Row(27, "Wendy Long", "wendy.long@example.com", "+1-555-9090", "LongText Inc", "Writer", "400 Long St", "San Francisco", "CA", "USA", "Publishing", "Long Lead", today, today, "Order#0021:2024-06-12", "A"*1000, "94105"),
    # Edge: purchase_history with special chars
    Row(28, "Xander Special", "xander.special@example.com", "+1-555-1112", "Specials", "Specialist", "500 Special St", "Austin", "TX", "USA", "Special", "Special Lead", today, today, "Order#0022:2024-06-11;DROP TABLE", "Special chars in purchase_history", "73301"),
    # Edge: zip with unicode
    Row(29, "Yasmin Unicode", "yasmin.unicode@example.com", "+1-555-1313", "UnicodeZip", "Manager", "600 Unicode St", "Miami", "FL", "USA", "Retail", "Unicode Lead", today, today, "Order#0023:2024-06-10", "Unicode zip", "〒123-4567"),
    # Edge: all fields NULL (should be rejected by validation)
    Row(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
]

# Create DataFrame
df_customer_360 = spark.createDataFrame(test_data, schema=customer_360_schema)

# Show the test data (for validation)
df_customer_360.show(truncate=False)

# Comments for test scenarios:
# 1-4: Happy path, valid data, including multi-byte and special characters
# 5-8: Edge cases for creation_date (boundary for vacuum), old/new records
# 9: Error case, invalid email format
# 10: Error case, NULL email (required)
# 11: Error case, NULL id (required)
# 12: Error case, NULL state (required)
# 13: NULL in non-required fields
# 14-15: Special/multi-byte characters in notes/company
# 16-17: Special chars in zip/phone
# 18: JSON string in purchase_history
# 19: SQL injection attempt in notes
# 20-24: Various levels of NULLs, edge for required fields
# 25-26: Max/min bigint, unicode in name/company
# 27: Long string in notes
# 28: Special chars in purchase_history
# 29: Unicode in zip
# 30: All fields NULL (should be rejected by validation)

# Note: Do not include spark.stop() in Databricks notebooks
