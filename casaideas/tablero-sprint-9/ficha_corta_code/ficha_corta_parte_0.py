# integration glue job
from os import truncate
import awswrangler as wr

import pandas as pd

from datetime import datetime
from datetime import timedelta, date
import uuid
import pytz
import requests
import json
import sys
import boto3
import base64

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as f
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    BooleanType,
    LongType,
    TimestampType,
)
from pyspark.sql.functions import when, lit, col


import logging

logger = logging.getLogger(name="Prebuc Job")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = (
    SparkSession.builder.appName("purple_prebuc")
    .config(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .enableHiveSupport()
    .getOrCreate()
)
sqlContext = SQLContext(spark)
spark.conf.set("spark.sql.codegen.wholeStage", False)
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.canned.acl", "BucketOwnerFullControl")
# hadoop_conf.set("fs.s3a.secret.key", "foobarfoo")
# hadoop_conf.set("fs.s3a.access.key", "foobar")
# hadoop_conf.set("fs.s3a.endpoint", "http://s3:9000")
hadoop_conf.set("fs.s3a.acl.default", "BucketOwnerFullControl")

### Inicio de funciones ###

### Inicializacion de variables ###

ACCOUNT_ID = "534086549449"
stage = ["data-raw", "data-raw-2"]
prefix = "parquet/ficha_corta/corporativo/madre/"
object = "ficha_corta/corporativo/madre/LOAD00000001.csv"
source = "s3://prod" + "-" + ACCOUNT_ID + "-" + stage[0] + "/"
target = "s3://dev" + "-" + ACCOUNT_ID + "-" + stage[1] + "/" + prefix + "full/"
### codigo ###

df = spark.read.format("csv").options(header="false").load(source + object)

df.repartition(1).write.mode("append").parquet(target)

print("Escritura en parquet finalizada")

print("Ha terminado satisfactoriamente")
