import sys
import logging

import awswrangler as wr
import pandas as pd
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as f
from pyspark.sql.functions import when, lit, col

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


def check_Table(con, schema, table):
    stmt = (
        "SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = '%s' AND table_name = '%s');"
        % (schema, table)
    )
    with con.cursor() as cursor:
        cursor.execute(stmt)
        s = cursor.fetchone()
        s = " ".join(map(str, s))
    if s == "False":
        return False
    else:
        return True


logger = logging.getLogger(name="Job Starting")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# --- Inicializacion configuraciones de Spark ---
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = (
    SparkSession.builder.appName("dim_mix_comercial")
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

# --- Inicializacion de Variables ---
try:
    event = getResolvedOptions(sys.argv, ["ENV", "YEAR", "MONTH", "DAY",],)
except:
    event = {
        "ENV": "dev",
        "YEAR": "-",
        "MONTH": "-",
        "DAY": "-",
    }

env = "prod" if event["ENV"] == "prod" else "dev"
year = datetime.today().strftime("%Y")
month = datetime.today().strftime("%m")
day = datetime.today().strftime("%d")

date = year + "-" + month + "-" + day
id_date = year + month + day


print("Year: " + year + " Month: " + month + " Day: " + day)

account_id = "534086549449"
r_stage = "data-lake"
w_stage = "analytics"
r_formato = "parquet"
w_formato = "parquet"
r_source = "marc_transito"
table = "dim_mix_comercial"
path_tmp = "s3://aws-glue-scripts-534086549449/prueba_s3_to_redshift/tmp00/"
output_s3_path = f"s3://{env}-{account_id}-{w_stage}/{w_formato}/tables/{table}/"

df = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        f"s3://{env}-{account_id}-{r_stage}/{r_formato}/sap/tables/{r_source}/year={year}/month={month}/day={day}/"
    )
)


df = df.withColumn("fecha_db", lit(date)).withColumn("id_tiempo", lit(id_date))
df = df.withColumn(
    "id_mix", f.concat(lit(id_date), lit("-"), df.werks, lit("-"), df.matnr),
)

df.createOrReplaceTempView("marc")
query = f"""
         SELECT
             id_mix, 
             id_tiempo,
             fecha_db,
             werks as centro_id_sap,
             matnr as num_material,
             mmsta as status_material
         FROM marc 
         """
dim_mix = spark.sql(query)

dim_mix = dim_mix.withColumn("fecha_db", f.to_date(dim_mix.fecha_db, "yyyy-MM-dd"))
dim_mix.printSchema()
dim_mix.show(truncate=False)

print("Grabando a disco")
dim_mix.repartition(1).write.format("parquet").options(header="true").mode(
    "append"
).save(output_s3_path)
print("Guardado en Analitycs")
print("Ha terminado satisfactoriamente")
