# Import pyspark modules
from pyspark.context import SparkContext

# Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

# Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

# Parameters
db = "test_db"
table = "test_output"

# Read table from Aws Glue Hive
dymc_frame = glue_context.create_dynamic_frame.from_catalog(database=db, table_name=table).toDf()
# Select all table in table
df = dymc_frame.select('*')
# Transform back to dynamic frame for optimized write
dynamic_frame_write = DynamicFrame.fromDF(df, glue_context, "dynamic_frame_write")
# Write to Dynamodb
datasink5 = glue_context.write_dynamic_frame.from_options(dymc_frame, connection_type="dynamodb",connection_options={"dynamodb.output.tableName": "test"})
