import json
import logging
import boto3
import argparse

import awswrangler as wr
import pandas as pd
import sys
from datetime import datetime
import time
from boto3.dynamodb.conditions import Key

############
import pyspark
import pyspark.sql as sql
from pyspark.sql import functions as f
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col
from pyspark.sql.functions import current_date
from pyspark.sql.types import *
from pyspark.sql.functions import *

############
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Start function main
def save_log(
    stage,
    origin,
    env,
    status,
    error,
    quantity,
    path,
    prefix,
    last_modified,
    enviromment,
):
    now = datetime.now()
    data = {
        "key": origin + "-" + path + "-" + stage + "-" + env,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "stage": stage,  # possible values: raw, staging, analytics or redshift
        "origin": origin,  # possible values: sap. In the future: magento, omnyx, zendesk, easystore, etc
        "environment": env,  # possible values: dev, qa or prod
        "status": status,  # possible values: True or False
        "error": error,  # error: error description, null if all is ok, failed and concatenate error description
        "quantity": quantity,  # number of items processed
        "path": path,  # full S3 path of the object
        "prefix": prefix,  # path of the object from S3 root folder
        "file_last_modified": last_modified,  # object's last modified date
    }
    send_to_dynamo(data, enviromment)
    return True


def send_to_dynamo(data, enviromment):
    print("Enviando a Dynamo de Tracking")
    # dynamo = boto3.resource("dynamodb", endpoint_url="http://dynamodb-local:8000")
    dynamo = boto3.resource("dynamodb")
    ambiente = enviromment
    table = dynamo.Table(f"casaideas_datalake_tracking_process_{ambiente}")
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    table.put_item(Item=data)
    print("Se envio de forma correcta")
    print(f"casaideas_datalake_tracking_process_{ambiente}")
    return True


# Search objects in bucket + prefix with today's date
def search_todays_objects(env, stage, prefix, year, month, day):
    # Today's objects
    # FIXED: pensar en caso de uso diario, también pensar en rangos y personalización de día puntual
    todays_objects = []

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
        last = object.last_modified
        todays_objects.append(object.key)
    # Return today's objects
    return todays_objects


def check_glueTable(TABLE, DATABASE):
    glue_client = boto3.client("glue")
    try:
        glue_client.get_table(DatabaseName=DATABASE, Name=TABLE)
        return True
    except:
        print("La tabla {} no existe en la base de datos {}".format(TABLE, DATABASE))

        return False


# Start variables

logger = logging.getLogger()
logger = logging.getLogger(name="Transversal Job Starting")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

print("Iniciando el job: acceso a datos dynamodb")
# dynamodb = boto3.resource(
#     "dynamodb", region_name="us-east-1", endpoint_url="http://dynamodb-local:8000"
# )
# client = boto3.client("dynamodb", endpoint_url="http://dynamodb-local:8000")
# table = dynamodb.Table("casaideas_datalake_sap_qa")
date = datetime.today().strftime("%Y-%m-%d")
try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
except:
    print("Error: no se pudo obtener los argumentos")
    args = {
        "JOB_NAME": "casaideas-etl-sap-qa",
    }

print(date)
# print(table)


# --- Inicializacion configuraciones de Spark ---

sc = SparkContext()
# # Para uso Local
# hc = sc._jsc.hadoopConfiguration()
# hc.set("fs.s3a.secret.key", "foobarfoo")
# hc.set("fs.s3a.access.key", "foobar")
# hc.set("fs.s3a.endpoint", "http://s3:9000")
# hc.set("fs.s3a.connection.ssl.enabled", "false")
# hc.set("fs.s3a.path.style.access", "true")

glueContext = GlueContext(sc)
spark = glueContext.spark_session
# spark = SparkSession.builder.appName("Test") # revisar si es necesario.
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Preparing Spark Context
conf = pyspark.SparkConf()

# FIXED: ubicar estos parámetros desde step functions
# year = datetime.today().strftime("%Y")
# month = datetime.today().strftime("%m")
# day = datetime.today().strftime("%d")

try:
    event = getResolvedOptions(
        sys.argv,
        [
            "ENV",  # "dev", "prod"
            "STAGE",  # "analytics", "staging", "raw"
            "PREFIX",  # "csv/tables/0co_pc_act_05"
            "ORIGIN",  # "sap"
            "TABLE",  # "0co_pc_act_05"
            "ENVIROMMENT",  # "qa", "prd"
            "YEAR",  # "2021"
            "MONTH",  # "12"
            "DAY",  # "06"
        ],
    )
except:
    event = {
        "ENV": "dev",
        "STAGE": "raw",
        "PREFIX": "/tables/2lis_13_vditm",
        "ORIGIN": "sap",
        "TABLE": "2lis_13_vditm",
        "ENVIROMMENT": "qa",
        "YEAR": "2021",
        "MONTH": "12",
        "DAY": "20",
    }

print("Variables de entorno cargadas correctamente")
print(event)
print(event["TABLE"])

enviromment = event["ENVIROMMENT"]

print("Iniciando el job: acceso a datos dynamodb en la tabla {}".format(event["TABLE"]))
# scan all table dynamodb
# response = client.list_tables()
# response = client.get_item(
#     TableName=f"casaideas_datalake_sap_{enviromment}",
#     Key={"table": {"S": str(event["TABLE"])}},
# )
# print(response)

# test = json.dumps(response)
# ACCOUNT_ID = response["Item"]["account_id"]["S"]
# # FIXED: tanto producción como dev debe funcionar

ACCOUNT_ID = "534086549449"


env = "prod" if event["ENV"] == "prod" else "dev"
if event["STAGE"] == "analytics":
    # Analytics
    stage = "redshift"
elif event["STAGE"] == "staging":
    # Staging
    stage = "data-lake"
else:
    # Raw
    stage = "data-raw"
bucket = env + "-" + ACCOUNT_ID + "-" + stage
origin = event["ORIGIN"] if event["ORIGIN"] != "" else "sap"
year = event["YEAR"] if event["YEAR"] != "" else datetime.today().strftime("%Y")
month = event["MONTH"] if event["MONTH"] != "" else datetime.today().strftime("%m")
day = event["DAY"] if event["DAY"] != "" else datetime.today().strftime("%d")

# FIXED: agregar Fuente en la tabla de dynamo
print("Iniciando el carga desde dynamodb")

# STAGES = response["Item"]["stages"]["S"]
STAGES = "data-raw,data-raw-2,data-lake,analytics,redshift"
STAGES = STAGES.split(",")
print(STAGES)
print("stages: " + STAGES[0])
INPUT_BUCKET = "prod-" + ACCOUNT_ID + "-" + STAGES[0]
OUTPUT_BUCKET = env + "-" + ACCOUNT_ID + "-" + STAGES[1]
# FORMAT = response["Item"]["format"]["S"]
FORMAT = "csv,parquet"
FORMAT = FORMAT.split(",")
print("format: " + FORMAT[0] + " " + FORMAT[1])
PREFIX_IN = FORMAT[0] + event["PREFIX"]
PREFIX_OUT = FORMAT[1] + event["PREFIX"]
prefix = FORMAT[0] + event["PREFIX"]  # "csv/tables/"
# DATABASE = response["Item"]["db"]["S"]
TABLE = event["TABLE"]
# COLUMNS = response["Item"]["columns"]["S"]
# SCHEMA = response["Item"]["schema"]["S"]
SEPARADOR = ";"
QUOTECHAR = '"'

print("seleccionando ultimo objeto en el bucket")

objects = search_todays_objects(env, stage, prefix, year, month, day)

# End routine
print("Imprimiendo la respuesta de la funcion:")
# print(objects)

for object in objects:
    if object.find("2020.csv") != -1:
        # print("object: " + object)
        year = object[49:53]
        month = object[54:56]
        day = object[57:59]
        SOURCE_PATH = "s3://{}/{}/".format(INPUT_BUCKET, object)
        TARGET_PATH = "s3://{}/{}/year={}/month={}/day={}/".format(
            OUTPUT_BUCKET, PREFIX_OUT, year, month, day
        )
        print("SOURCE_PATH: {}".format(SOURCE_PATH))
        print("TARGET_PATH: {}".format(TARGET_PATH))

        df = glueContext.create_dynamic_frame.from_options(
            format_options={"withHeader": True, "separator": SEPARADOR},
            connection_type="s3",
            format="csv",
            connection_options={"paths": [SOURCE_PATH], "recurse": True},
            transformation_ctx="df",
            quoteChar=QUOTECHAR,
        )
        df = df.toDF()

        # df.printSchema()

        df.repartition(1).write.mode("overwrite").parquet(
            "s3://dev-534086549449-data-raw-2/parquet/tables/2lis_13_vditm/year=2020/"
        )

        job.commit()

print("Rutina Completada: Felicidades!!!")

