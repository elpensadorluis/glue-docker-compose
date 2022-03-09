import sys
import os
import json

from datetime import datetime, timedelta

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import concat, col, lit, to_timestamp, row_number
from pyspark.sql import functions as f

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

import boto3
from botocore.exceptions import ClientError

from pyspark.sql.window import Window

# w = Window.partitionBy().orderBy("x")
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
except:
    print("Error al recuperar los argumentos")

spark = SparkSession.builder.config(
    "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
).getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
try:
    job.init(args["JOB_NAME"], args)
except:
    job.init("hudi_deltas_job")

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
        objects_time.append(str(object.last_modified.strftime("%Y-%m-%d %H:%M:%S.")))
        # if last.date() == today:
        #     todays_objects.append(object.key)
    # Return today's objects
    return todays_objects


### Inicializacion de variables ###
todays_objects = []
objects_time = []


### Tomando variables de entorno ###

try:
    event = getResolvedOptions(sys.argv, ["ENV", "YEAR", "MONTH", "DAY", "PREFIX"],)
except:
    event = {
        "ENV": "dev",
        "YEAR": "-",
        "MONTH": "-",
        "DAY": "-",
        "PREFIX": "easy_store/ESTORE/VENTAS/",
    }

env = "prod" if event["ENV"] == "prod" else "dev"
year = event["YEAR"] if event["YEAR"] != "-" else datetime.today().strftime("%Y")
month = event["MONTH"] if event["MONTH"] != "-" else datetime.today().strftime("%m")
day = event["DAY"] if event["DAY"] != "-" else datetime.today().strftime("%d")
prefix = event["PREFIX"]

d = datetime.today() - timedelta(days=1)
day = d.strftime("%d")

dias = year + month + day + "-"

ACCOUNT_ID = "534086549449"
stage = ["data-raw", "data-raw-2"]

source = "s3://prod" + "-" + ACCOUNT_ID + "-" + stage[0] + "/"
target = "s3://" + env + "-" + ACCOUNT_ID + "-" + stage[1] + "/parquet/" + prefix
### codigo ###
### Crea una copia fisica en s3 de la tabla full y la carga desde tmp

objects = search_todays_objects(env, stage[0], prefix, year, month, day)
archivo = []
for obj in objects:
    if obj.find(".parquet") != -1:
        archivo.append(obj)

print("Imprimiendo la respuesta de la funcion:")
# print(objects)
print("Imprimiendo objetos:")
# print(todays_objects)
print("Imprimiendo tiempo de objetos:")
# print(objects_time)
print("Cantidad de Objects: " + str(len(objects)))

print("año: " + year + " mes: " + month + " dia: " + day)

print("Inicio del dia: " + dias)

i = 0
for object in objects:
    if object.find(dias) != -1:
        print(f"[{object}: {objects_time[i]}],")

        df = spark.read.format("csv").options(header="false").load(source + object)

        # df.printSchema()

        df = (
            df.withColumn("i", f.row_number().over(Window.orderBy("_c1")))
            .withColumn(
                "update_ts_dms",
                f.concat(lit(objects_time[i]), f.lpad(col("i"), 3, "0")),
            )
            .withColumn("schema_name", lit("ci_db"))
            .withColumn("table_name", lit("ventas"))
        )
        df = df.drop("i")

        # df.printSchema()
        # df.show(5, truncate=False)

        df.repartition(1).write.mode("append").parquet(
            target + "/" + "year=" + year + "/month=" + month + "/day=" + day
        )
    i += 1

print("Ha terminado satisfactoriamente")

