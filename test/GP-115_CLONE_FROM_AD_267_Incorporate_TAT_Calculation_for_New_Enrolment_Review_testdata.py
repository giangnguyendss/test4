spark.catalog.setCurrentCatalog("purgo_databricks")

# PySpark test data generation for pat_case, sr_activity, tat_report
# All necessary imports
from pyspark.sql import Row  
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType  
from pyspark.sql.functions import lit  
from datetime import datetime  

# Helper function to create timestamp in Databricks format
def make_ts(dt_str):
    # Accepts 'YYYY-MM-DD HH:MM:SS' and returns Databricks timestamp string
    if dt_str is None:
        return None
    # Databricks timestamp format: '2024-03-21T00:00:00.000+0000'
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")
    except Exception:
        return None

# ------------------- pat_case test data -------------------
pat_case_schema = StructType([
    StructField("case_id", StringType(), True),
    StructField("service_request_type", StringType(), True),
    StructField("sr_created_date", TimestampType(), True)
])

pat_case_data = [
    # Happy path
    Row(case_id="C123", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-01 10:15:00")),
    # Error: No matching sr_activity
    Row(case_id="C124", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-10 08:00:00")),
    # Error: sr_created_date is NULL
    Row(case_id="C125", service_request_type="Patient Foundation", sr_created_date=None),
    # Error: last_modified_date < sr_created_date
    Row(case_id="C127", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-20 09:00:00")),
    # Edge: last_modified_date = sr_created_date
    Row(case_id="C128", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-22 09:00:00")),
    # Multiple activities, happy path
    Row(case_id="C129", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-01 10:00:00")),
    Row(case_id="C130", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-07 08:00:00")),
    # Special chars, multi-byte
    Row(case_id="C131", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-01 10:00:00")),
    Row(case_id="C132", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-02 10:00:00")),
    # Non-target service_request_type
    Row(case_id="C133", service_request_type="Other Service", sr_created_date=make_ts("2024-06-01 10:00:00")),
    # NULL case_id
    Row(case_id=None, service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-03 10:00:00")),
    # NULL service_request_type
    Row(case_id="C134", service_request_type=None, sr_created_date=make_ts("2024-06-04 10:00:00")),
    # Edge: sr_created_date on weekend
    Row(case_id="C135", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-08 10:00:00")), # Saturday
    # Edge: sr_created_date on Sunday
    Row(case_id="C136", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-09 10:00:00")), # Sunday
    # Edge: sr_created_date and last_modified_date both NULL
    Row(case_id="C137", service_request_type="Patient Foundation", sr_created_date=None),
    # Edge: sr_created_date with special unicode
    Row(case_id="C138", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-11 10:00:00")),
    # Edge: sr_created_date with special chars
    Row(case_id="C139", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-12 10:00:00")),
    # Edge: sr_created_date far in past
    Row(case_id="C140", service_request_type="Patient Foundation", sr_created_date=make_ts("2020-01-01 10:00:00")),
    # Edge: sr_created_date far in future
    Row(case_id="C141", service_request_type="Patient Foundation", sr_created_date=make_ts("2030-01-01 10:00:00")),
    # Edge: sr_created_date with time at midnight
    Row(case_id="C142", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-13 00:00:00")),
    # Edge: sr_created_date with time at 23:59:59
    Row(case_id="C143", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-14 23:59:59")),
]

pat_case_df = spark.createDataFrame(pat_case_data, schema=pat_case_schema)

# ------------------- sr_activity test data -------------------
sr_activity_schema = StructType([
    StructField("activity_id", StringType(), True),
    StructField("case_id", StringType(), True),
    StructField("subject", StringType(), True),
    StructField("last_modified_date", TimestampType(), True),
    StructField("status", StringType(), True)
])

sr_activity_data = [
    # Happy path: C123, two activities, min date is 2024-06-04
    Row(activity_id="A1", case_id="C123", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-05 09:00:00"), status="Completed"),
    Row(activity_id="A2", case_id="C123", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-04 15:30:00"), status="Completed"),
    # Error: C124, no matching activity
    # Error: C125, matching activity, last_modified_date present
    Row(activity_id="A3", case_id="C125", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-12 00:00:00"), status="Completed"),
    # Error: C126, matching activity, last_modified_date NULL
    Row(activity_id="A4", case_id="C126", subject="Perform New Enrollment Review Activity", last_modified_date=None, status="Completed"),
    # Error: C127, last_modified_date < sr_created_date
    Row(activity_id="A5", case_id="C127", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-18 00:00:00"), status="Completed"),
    # Edge: C128, last_modified_date = sr_created_date
    Row(activity_id="A6", case_id="C128", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-22 00:00:00"), status="Completed"),
    # Multiple activities: C129
    Row(activity_id="A7", case_id="C129", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-05 09:00:00"), status="Completed"),
    Row(activity_id="A8", case_id="C129", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-04 15:30:00"), status="Completed"),
    Row(activity_id="A9", case_id="C129", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-06 12:00:00"), status="Completed"),
    # Multiple activities: C130
    Row(activity_id="A10", case_id="C130", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-10 09:00:00"), status="Completed"),
    Row(activity_id="A11", case_id="C130", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-09 15:30:00"), status="Completed"),
    Row(activity_id="A12", case_id="C130", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-08 12:00:00"), status="Completed"),
    # Special chars, multi-byte: C131
    Row(activity_id="A13", case_id="C131", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-03 10:00:00"), status="Completed"),
    Row(activity_id="A14", case_id="C131", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-02 10:00:00"), status="Completed"),
    Row(activity_id="A15", case_id="C131", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-04 10:00:00"), status="Completed"),
    # Special chars, multi-byte: C132
    Row(activity_id="A16", case_id="C132", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-03 10:00:00"), status="Completed"),
    Row(activity_id="A17", case_id="C132", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-02 10:00:00"), status="Completed"),
    Row(activity_id="A18", case_id="C132", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-04 10:00:00"), status="Completed"),
    # Non-target service_request_type: C133, should be ignored
    Row(activity_id="A19", case_id="C133", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-05 10:00:00"), status="Completed"),
    # NULL case_id: should be ignored
    Row(activity_id="A20", case_id=None, subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-06 10:00:00"), status="Completed"),
    # NULL service_request_type: should be ignored
    Row(activity_id="A21", case_id="C134", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-07 10:00:00"), status="Completed"),
    # Edge: sr_created_date on weekend: C135
    Row(activity_id="A22", case_id="C135", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-10 10:00:00"), status="Completed"),
    # Edge: sr_created_date on Sunday: C136
    Row(activity_id="A23", case_id="C136", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-11 10:00:00"), status="Completed"),
    # Edge: sr_created_date and last_modified_date both NULL: C137
    Row(activity_id="A24", case_id="C137", subject="Perform New Enrollment Review Activity", last_modified_date=None, status="Completed"),
    # Edge: sr_created_date with special unicode: C138
    Row(activity_id="A25", case_id="C138", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-12 10:00:00"), status="Completed"),
    # Edge: sr_created_date with special chars: C139
    Row(activity_id="A26", case_id="C139", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-13 10:00:00"), status="Completed"),
    # Edge: sr_created_date far in past: C140
    Row(activity_id="A27", case_id="C140", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2020-01-03 10:00:00"), status="Completed"),
    # Edge: sr_created_date far in future: C141
    Row(activity_id="A28", case_id="C141", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2030-01-03 10:00:00"), status="Completed"),
    # Edge: sr_created_date with time at midnight: C142
    Row(activity_id="A29", case_id="C142", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-13 00:00:00"), status="Completed"),
    # Edge: sr_created_date with time at 23:59:59: C143
    Row(activity_id="A30", case_id="C143", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-14 23:59:59"), status="Completed"),
    # Error: status not Completed
    Row(activity_id="A31", case_id="C144", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-15 10:00:00"), status="In Progress"),
    # Error: subject not matching
    Row(activity_id="A32", case_id="C145", subject="Other Activity", last_modified_date=make_ts("2024-06-16 10:00:00"), status="Completed"),
    # Special chars in subject/status
    Row(activity_id="A33", case_id="C146", subject="Perform New Enrollment Review Activity – 特殊字符", last_modified_date=make_ts("2024-06-17 10:00:00"), status="Completed"),
    Row(activity_id="A34", case_id="C147", subject="Perform New Enrollment Review Activity", last_modified_date=make_ts("2024-06-18 10:00:00"), status="完了"),
]

sr_activity_df = spark.createDataFrame(sr_activity_data, schema=sr_activity_schema)

# ------------------- tat_report test data (for validation) -------------------
tat_report_schema = StructType([
    StructField("case_id", StringType(), True),
    StructField("service_request_type", StringType(), True),
    StructField("sr_created_date", TimestampType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("total_time_elapsed", LongType(), True)
])

tat_report_data = [
    # Happy path: C123, start=2024-06-01, end=2024-06-04, weekdays=2
    Row(case_id="C123", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-01 10:15:00"), end_date=make_ts("2024-06-04 15:30:00"), total_time_elapsed=2),
    # Error: C124, no matching sr_activity
    Row(case_id="C124", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-10 08:00:00"), end_date=None, total_time_elapsed=None),
    # Error: C125, sr_created_date NULL
    Row(case_id="C125", service_request_type="Patient Foundation", sr_created_date=None, end_date=make_ts("2024-06-12 00:00:00"), total_time_elapsed=None),
    # Error: C126, last_modified_date NULL
    Row(case_id="C126", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-15 12:00:00"), end_date=None, total_time_elapsed=None),
    # Error: C127, end_date < start_date
    Row(case_id="C127", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-20 09:00:00"), end_date=make_ts("2024-06-18 00:00:00"), total_time_elapsed=None),
    # Edge: C128, end_date = start_date
    Row(case_id="C128", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-22 09:00:00"), end_date=make_ts("2024-06-22 00:00:00"), total_time_elapsed=0),
    # Multiple activities: C129, min end_date=2024-06-04, weekdays=2
    Row(case_id="C129", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-01 10:00:00"), end_date=make_ts("2024-06-04 15:30:00"), total_time_elapsed=2),
    # Multiple activities: C130, min end_date=2024-06-08, weekdays=1
    Row(case_id="C130", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-07 08:00:00"), end_date=make_ts("2024-06-08 12:00:00"), total_time_elapsed=1),
    # Special chars, multi-byte: C131, min end_date=2024-06-02, weekdays=0
    Row(case_id="C131", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-01 10:00:00"), end_date=make_ts("2024-06-02 10:00:00"), total_time_elapsed=0),
    # Special chars, multi-byte: C132, min end_date=2024-06-02, weekdays=0
    Row(case_id="C132", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-02 10:00:00"), end_date=make_ts("2024-06-02 10:00:00"), total_time_elapsed=0),
    # Non-target service_request_type: C133, should be ignored
    # NULL case_id: should be ignored
    # NULL service_request_type: should be ignored
    # Edge: sr_created_date on weekend: C135, start=2024-06-08 (Sat), end=2024-06-10 (Mon), weekdays=1
    Row(case_id="C135", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-08 10:00:00"), end_date=make_ts("2024-06-10 10:00:00"), total_time_elapsed=1),
    # Edge: sr_created_date on Sunday: C136, start=2024-06-09 (Sun), end=2024-06-11 (Tue), weekdays=2
    Row(case_id="C136", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-09 10:00:00"), end_date=make_ts("2024-06-11 10:00:00"), total_time_elapsed=2),
    # Edge: sr_created_date and last_modified_date both NULL: C137
    Row(case_id="C137", service_request_type="Patient Foundation", sr_created_date=None, end_date=None, total_time_elapsed=None),
    # Edge: sr_created_date with special unicode: C138, start=2024-06-11, end=2024-06-12, weekdays=1
    Row(case_id="C138", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-11 10:00:00"), end_date=make_ts("2024-06-12 10:00:00"), total_time_elapsed=1),
    # Edge: sr_created_date with special chars: C139, start=2024-06-12, end=2024-06-13, weekdays=1
    Row(case_id="C139", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-12 10:00:00"), end_date=make_ts("2024-06-13 10:00:00"), total_time_elapsed=1),
    # Edge: sr_created_date far in past: C140, start=2020-01-01, end=2020-01-03, weekdays=2
    Row(case_id="C140", service_request_type="Patient Foundation", sr_created_date=make_ts("2020-01-01 10:00:00"), end_date=make_ts("2020-01-03 10:00:00"), total_time_elapsed=2),
    # Edge: sr_created_date far in future: C141, start=2030-01-01, end=2030-01-03, weekdays=2
    Row(case_id="C141", service_request_type="Patient Foundation", sr_created_date=make_ts("2030-01-01 10:00:00"), end_date=make_ts("2030-01-03 10:00:00"), total_time_elapsed=2),
    # Edge: sr_created_date with time at midnight: C142, start=2024-06-13 00:00:00, end=2024-06-13 00:00:00, weekdays=0
    Row(case_id="C142", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-13 00:00:00"), end_date=make_ts("2024-06-13 00:00:00"), total_time_elapsed=0),
    # Edge: sr_created_date with time at 23:59:59: C143, start=2024-06-14 23:59:59, end=2024-06-14 23:59:59, weekdays=0
    Row(case_id="C143", service_request_type="Patient Foundation", sr_created_date=make_ts("2024-06-14 23:59:59"), end_date=make_ts("2024-06-14 23:59:59"), total_time_elapsed=0),
    # Error: status not Completed: C144, should be ignored
    # Error: subject not matching: C145, should be ignored
    # Special chars in subject/status: C146, C147, should be ignored unless status is "Completed"
]

tat_report_df = spark.createDataFrame(tat_report_data, schema=tat_report_schema)

# Example: Save test data to Unity Catalog tables (commented out, for reference)
# pat_case_df.write.mode("overwrite").saveAsTable("purgo_databricks.purgo_playground.pat_case")
# sr_activity_df.write.mode("overwrite").saveAsTable("purgo_databricks.purgo_playground.sr_activity")
# tat_report_df.write.mode("overwrite").saveAsTable("purgo_databricks.purgo_playground.tat_report")
