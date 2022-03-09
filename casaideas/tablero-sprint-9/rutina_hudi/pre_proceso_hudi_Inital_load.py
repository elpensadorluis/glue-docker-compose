import sys
import os
import json

from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as f

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import boto3
from botocore.exceptions import ClientError

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark = SparkSession.builder.config(
    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
).getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

logger.info("Initialization.")

### Inicio de funciones ###
def search_todays_objects(env, stage, prefix, year, month, day):
    # Today's objects
    # FIXED: pensar en caso de uso diario, también pensar en rangos y personalización de día puntual

    # array two-dimensional

    # Get current date
    # today = datetime.today() - timedelta(days=1)
    # today = today.date()

    # Get current date
    today = datetime(int(year), int(month), int(day)).date()
    # today = datetime.today().date()
    print(today)
    # Search bucket
    # session = boto3.session.Session(
    #     aws_access_key_id="foobar", aws_secret_access_key="foobarfoo"
    # )
    # s3 = boto3.resource("s3", endpoint_url="http://s3:9000")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket("prod" + "-" + ACCOUNT_ID + "-" + stage)
    print("Imprimiendo el Bucket")
    print("prefijo: " + prefix)
    print(bucket)
    # Search in bucket objects with prefix and today's last modified date
    for object in bucket.objects.filter(Prefix=prefix):
        # last = object.last_modified
        todays_objects.append(object.key)
        objects_time.append(
            str(object.last_modified.strftime("%Y-%m-%d %H:%M:%S.")) + "000"
        )
        # if last.date() == today:
        #     todays_objects.append(object.key)
    # Return today's objects
    return todays_objects


### Inicializacion de variables ###
todays_objects = []
objects_time = []

### Tomando variables de entorno ###
# Tablas:
# s3://prod-534086549449-data-raw/easy_store/ESTORE/PROMOCIONES/

try:
    event = getResolvedOptions(sys.argv, ["ENV", "YEAR", "MONTH", "DAY", "PREFIX"],)
except:
    event = {
        "ENV": "dev",
        "YEAR": "-",
        "MONTH": "-",
        "DAY": "-",
        "PREFIX": "easy_store/ESTORE/DESCUENTOS/",
    }

env = "prod" if event["ENV"] == "prod" else "dev"
year = event["YEAR"] if event["YEAR"] != "-" else datetime.today().strftime("%Y")
month = event["MONTH"] if event["MONTH"] != "-" else datetime.today().strftime("%m")
day = event["DAY"] if event["DAY"] != "-" else datetime.today().strftime("%d")
prefix = event["PREFIX"]

ACCOUNT_ID = "534086549449"
stage = ["data-raw", "data-raw-2"]

source = "s3://prod" + "-" + ACCOUNT_ID + "-" + stage[0] + "/"
target = (
    "s3://"
    + env
    + "-"
    + ACCOUNT_ID
    + "-"
    + stage[1]
    + "/parquet/"
    + prefix
    + "historico/"
)
print("source: " + source)
print("target: " + target)
### codigo ###

objects = search_todays_objects(env, stage[0], prefix, year, month, day)

# i = 0
# for object in objects:
#     if object.find("LOAD") != -1:
#         print(f"[{object}: {objects_time[i]}],")

#     i += 1

i = 0
for object in objects:
    if object.find("LOAD") != -1:
        print(f"[{object}: {objects_time[i]}],")

        df = (
            spark.read.format("csv")
            .options(header="false")
            .option("delimiter", ",")
            .load(source + object)
        )

        # df.printSchema()
        print("Nombre de la tabla: " + object)

        # cast string as number in column _c0
        # df = df.withColumn("_c0", df["_c0"].cast("int"))

        # df = df.withColumn("_c0", df["_c0"].cast(IntegerType()))
        # drop column _c1 null values
        df = df.dropna(subset=["_c1"])

        df.printSchema()
        df.show(2, truncate=False)

        df = (
            df.withColumn("update_ts_dms", lit(objects_time[i]))
            .withColumn("schema_name", lit("ci_db"))
            .withColumn("table_name", lit("ventas"))
        )

        df.repartition(1).write.mode("append").parquet(target)

    i += 1


print("Ha terminado satisfactoriamente")
