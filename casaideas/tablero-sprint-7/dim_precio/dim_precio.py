# integration glue job
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


condiciones = ["ZP03", "ZP02", "ZP01"]

col_dim_precios = [
    "num_material",
    "num_reg_condicion",
    "uni_condicion",
    "clase_condicion",
    "inicio_validez_reg_condicion",
    "fin_validez_reg_condicion",
    "ZP03",
    "ZP02",
    "ZP01",
]

col_orden_precios = [
    "num_material",
    "uni_condicion",
    "inicio_validez_reg_condicion",
    "fin_validez_reg_condicion",
]

orden_condicion = ["liquidacion", "ajuste", "regular"]

dict_cols = {
    "uni_condicion": "moneda",
    "ZP03": "regular",
    "ZP02": "ajuste",
    "ZP01": "liquidacion",
    "inicio_validez_reg_condicion": "inicio",
    "fin_validez_reg_condicion": "fin",
}

### CARGA TABLAS KONP / A004 como precios y condicion
# print("Cargando tablas...")
# precios = pd.read_parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/knop/year=2022/month=01/day=18/"
# )
# condicion = pd.read_parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/a004/year=2022/month=01/day=18/"
# )

# df_p = spark.read.option("header", "true").parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/knop/year=2022/month=01/day=18/"
# )
# df_c = spark.read.option("header", "true").parquet(
#     "s3://dev-534086549449-data-lake/parquet/sap/tables/a004/year=2022/month=01/day=18/"
# )

df_p = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        "s3://dev-534086549449-data-lake/parquet/sap/tables/knop/year=2022/month=01/day=18/"
    )
)
df_c = (
    spark.read.format("parquet")
    .options(header="true")
    .load(
        "s3://dev-534086549449-data-lake/parquet/sap/tables/a004/year=2022/month=01/day=18/"
    )
)
print("Convirtiendo a pandas")
df_p.printSchema()
df_c.printSchema()
precios = df_p.select("*").toPandas()
condicion = df_c.select("*").toPandas()


# precios = df_p.toDF()
# condicion = df_c.toDF()

## Ajustes y filtros a precios
precios = precios[precios.clase_condicion.isin(condiciones)]  # Filtro Condiciones
condicion = condicion[condicion.clase_condicion.isin(condiciones)]
precios.loc[:, "importe_porcentaje_cond"] = precios["importe_porcentaje_cond"].apply(
    lambda x: x * 100
)  # Ajuste al precio.

# Pivote de valores de tipos de precio a columnas
precios_pv = (
    precios.pivot_table(
        index=["num_reg_condicion", "uni_condicion"],
        values="importe_porcentaje_cond",
        columns="clase_condicion",
    )
    .reset_index()
    .set_index("num_reg_condicion")
)

# Inner Join
dim_precios = condicion.merge(
    precios_pv, how="inner", left_on="num_reg_condicion", right_on="num_reg_condicion"
)

# Orden de columnas útiles + orden de tabla
dim_precios = dim_precios[col_dim_precios].sort_values(col_orden_precios)

# Renombrar columnas
dim_precios.rename(columns=dict_cols, inplace=True)

#  Asignar clase_condicion como categoria organizada.
dim_precios["clase_condicion"] = pd.Categorical(
    dim_precios.clase_condicion.map(dict_cols), orden_condicion
)

# Null Fillers
dim_precios["regular"] = dim_precios.groupby(["num_material", "moneda"], sort=False)[
    "regular"
].apply(lambda x: x.ffill().bfill())
dim_precios.loc[
    dim_precios.clase_condicion == "liquidacion", "ajuste"
] = dim_precios.groupby(["num_material", "moneda"], sort=False)["ajuste"].apply(
    lambda x: x.ffill()
)

# Calcular Descuentos. Nulos = 0
dim_precios["r_a"] = (round(1 - (dim_precios.ajuste / dim_precios.regular), 4)).fillna(
    0
)
dim_precios["r_l"] = (
    round(1 - (dim_precios.liquidacion / dim_precios.regular), 4)
).fillna(0)
dim_precios["a_l"] = (
    round(1 - (dim_precios.liquidacion / dim_precios.ajuste), 4)
).fillna(0)

print(dim_precios.head(10))
print("Ha terminado satisfactoriamente")
