
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from datetime import datetime

# Initialize Spark session with Iceberg and S3 support
spark = SparkSession.builder \
    .appName("Airflow PySpark Job") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "rest") \
    .config("spark.sql.catalog.demo.uri", "http://rest:8181") \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.demo.warehouse", "s3://warehouse/wh/") \
    .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.defaultCatalog", "demo") \
    .getOrCreate()

print("Spark session created")

# Create sample data
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("processed_at", StringType(), False)
])

# Get current timestamp
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

data = [
    (1, "Product A", 10.5, current_time),
    (2, "Product B", 20.1, current_time),
    (3, "Product C", 30.9, current_time),
    (4, "Product D", 40.5, current_time),
    (5, "Product E", 50.2, current_time)
]

print("Creating DataFrame")
df = spark.createDataFrame(data, schema)
df.show()

# Write to Iceberg table
print("Writing to Iceberg table")
table_name = "demo.default.airflow_spark_job"
df.writeTo(table_name).createOrReplace()

# Read back and validate
print("Reading back from Iceberg table")
result_df = spark.read.table(table_name)
count = result_df.count()
print(f"Total records: {count}")

print("Job completed successfully")
spark.stop()
        