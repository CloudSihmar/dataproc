from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize SparkSession with Hive support
spark = SparkSession.builder \
    .appName("EmployeeTableExample") \
    .enableHiveSupport() \
    .getOrCreate()

# Define the schema for the employees table
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("department", StringType(), nullable=False)
])

# Create sample data for employees
data = [
    (1, "John Doe", 30, "Engineering"),
    (2, "Jane Smith", 35, "Marketing"),
    (3, "Sam Brown", 28, "Sales"),
    (4, "Lisa White", 32, "HR")
]

# Create a DataFrame with the sample data
employees_df = spark.createDataFrame(data, schema)

# Write the DataFrame to a Hive table named "employees"
employees_df.write.saveAsTable("employees")

print("Employee information has been stored in the Hive table 'employees'.")
