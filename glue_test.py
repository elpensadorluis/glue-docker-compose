import sys
import time

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# read from local bucket
df = spark.read.csv(
    "s3://sample-bucket/sample/", header=True, sep=",", inferSchema=True
)

df_write = df.select("firstName", "email", "phoneNumber")

df_write.show()

# try out some glue libs
glue_context = GlueContext(spark.sparkContext)
spark_session = glue_context.spark_session
logger = glue_context.get_logger()
job = Job(glue_context)
job.init("test_avro")

member_dyn = DynamicFrame.fromDF(df_write, glue_context, "member_dyn")

glue_context.write_dynamic_frame_from_options(
    frame=member_dyn,
    connection_type="s3",
    connection_options={"path": "s3://sample-bucket/avro"},
    format="avro",
    format_options={"version": "1.8"},
)

job.commit()

# read avro via spark
df_avro = spark_session.read.format("avro").load("s3://sample-bucket/avro")

df_avro.show()
