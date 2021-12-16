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

hc = sc._jsc.hadoopConfiguration()
hc.set("fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE")
hc.set("fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
hc.set("fs.s3a.endpoint", "http://172.20.0.2:9000")
hc.set("fs.s3a.connection.ssl.enabled", "false")
hc.set("fs.s3a.path.style.access", "true")

glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.option("header", "true").csv(
    "s3a://awsglue-datasets/Medicare_Hospital_Provider.csv"
)
df.printSchema()
df.show()

medicare_dynamicframe = DynamicFrame.fromDF(df, glueContext, "medicare")

medicare_dynamicframe.printSchema()

medicare_res = medicare_dynamicframe.resolveChoice(specs=[("Provider Id", "cast:long")])
medicare_res.printSchema()

medicare_res.toDF().where("`provider id` is NULL").show()

medicare_dataframe = medicare_res.toDF()
medicare_dataframe = medicare_dataframe.where("`provider id` is NOT NULL")

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

chop_f = udf(lambda x: x[1:], StringType())
medicare_dataframe = (
    medicare_dataframe.withColumn(
        "ACC", chop_f(medicare_dataframe[" Average Covered Charges "])
    )
    .withColumn("ATP", chop_f(medicare_dataframe[" Average Total Payments "]))
    .withColumn("AMP", chop_f(medicare_dataframe["Average Medicare Payments"]))
)
medicare_dataframe.select(["ACC", "ATP", "AMP"]).show()

from awsglue.dynamicframe import DynamicFrame

medicare_tmp_dyf = DynamicFrame.fromDF(medicare_dataframe, glueContext, "nested")
medicare_nest_dyf = medicare_tmp_dyf.apply_mapping(
    [
        ("drg definition", "string", "drg", "string"),
        ("provider id", "long", "provider.id", "long"),
        ("provider name", "string", "provider.name", "string"),
        ("provider city", "string", "provider.city", "string"),
        ("provider state", "string", "provider.state", "string"),
        ("provider zip code", "long", "provider.zip", "long"),
        ("hospital referral region description", "string", "rr", "string"),
        ("ACC", "string", "charges.covered", "double"),
        ("ATP", "string", "charges.total_pay", "double"),
        ("AMP", "string", "charges.medicare_pay", "double"),
    ]
)
medicare_nest_dyf.printSchema()

medicare_nest_dyf.toDF().show()

spark_df = medicare_nest_dyf.toDF()

spark_df.write.format("parquet").save(
    "s3a://awsglue-datasets/output-dir/medicare.parquet"
)
