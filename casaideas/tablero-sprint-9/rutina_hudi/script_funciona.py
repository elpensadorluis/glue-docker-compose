import sys
import json
from numpy import record
import boto3
import logging
import pathlib
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame
import inspect

# Client for glue
glue = boto3.client("glue")

# set logger
logging.getLogger().setLevel(logging.INFO)

# Config table
# dynamodb = boto3.resource('dynamodb')

# Mandatory Parameters
# job_parameters = ['CONFIG_TABLE', 'TABLE_NAME', 'JOB']

job_parameters = ["JOB"]

# Read Glue Parameters
# args = getResolvedOptions(sys.argv, job_parameters)
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# table = dynamodb.Table(args.get("CONFIG_TABLE"))


# config = table.get_item(
#    Key={
#        'table_name': args.get("TABLE_NAME")
#    }
# ).get('Item')

spark = (
    SparkSession.builder.config(
        "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
    )
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .config(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)

logging.info("Initializing job...")

year = "2022"
month = "02"
day = "15"


FULL_FORMAT = "LOAD*.parquet"
DELTA_FORMAT = "*.parquet"
COLUMNS = '{"_c1": "_c0","_c2": "_c1","_c3": "_c2","_c4": "_c3","_c5": "_c4","_c6": "_c5","_c7": "_c6","_c8": "_c7","_c9": "_c8","_c10": "_c9","_c11": "_c10","_c12": "_c11","_c13": "_c12","_c14": "_c13","_c15": "_c14","_c16": "_c15","_c17": "_c16","_c18": "_c17","_c19": "_c18","_c20": "_c19","_c21": "_c20","_c22": "_c21","_c23": "_c22","_c24": "_c23","_c25": "_c24","_c26": "_c25","_c27": "_c26","_c28": "_c27","_c29": "_c28","_c30": "_c29","_c31": "_c30","update_ts_dms": "update_ts_dms","schema_name":"schema_name","table_name":"table_name","pk":"pk"}'

df_new_column = json.loads(COLUMNS)

# s3://dev-534086549449-data-raw-2/parquet/easy_store/ESTORE/VENTAS_DET/year=2022/month=02/day=15/
INPUT_BUCKET = "dev-534086549449-data-raw-2"  # config['input_bucket']
INPUT_PREFIX = (
    "parquet/easy_store/ESTORE/VENTAS_DET/year="
    + year
    + "/month="
    + month
    + "/day="
    + day
)  # config['input_prefix']

OUTPUT_BUCKET = "dev-534086549449-data-raw-2"  # config['output_bucket']
OUTPUT_PREFIX = "hudi/data/ventas_det/default"  # config['output_prefix']


PRECOMBINE_FIELDS = "update_ts_dms"  # config['precombine']

DATABASE = "test_hudi"  # config['target_db']
PK = "pk"  # config['pk']

PARTITION_FIELDS = ""  # config.get("partition_fields")
BUSINESS = ""  # config["business"]


# function for adding the column to dynamicframe
def addCompany(record):
    record["Empresa"] = BUSINESS
    return record


# _, schema, table_name = INPUT_PREFIX.split('/')
table_name = "ventas_det"  # table_name.lower()
out_location = f"{OUTPUT_PREFIX}/{DATABASE}/{table_name}"

# job.init(args['JOB'] + table_name, args)
job.init(args["JOB_NAME"], args)


COMMON_CONFIG = {
    "className": "org.apache.hudi",
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.recordkey.field": PK,
    "hoodie.datasource.write.precombine.field": PRECOMBINE_FIELDS,
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": DATABASE,
    "hoodie.datasource.hive_sync.table": table_name,
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.NonPartitionedExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "path": f"s3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}",
}


"""
COMMON_CONFIG = {
    'className': 'org.apache.hudi',
    "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': PK,
    'hoodie.datasource.write.precombine.field': PRECOMBINE_FIELDS,
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': DATABASE,
    'hoodie.datasource.hive_sync.table': table_name,
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'path': f's3://{OUTPUT_BUCKET}/{OUTPUT_PREFIX}'
}
"""


#'path': f's3://{OUTPUT_BUCKET}/{out_location}'

initLoadConfig = {
    "hoodie.bulkinsert.shuffle.parallelism": 4,
    "hoodie.datasource.write.operation": "bulk_insert",
}

incrementalConfig = {
    "hoodie.upsert.shuffle.parallelism": 20,
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.cleaner.policy": "KEEP_LATEST_COMMITS",
    "hoodie.cleaner.commits.retained": 1,
}

partitioned_options = {
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.datasource.write.partitionpath.field": PARTITION_FIELDS,
    "hoodie.datasource.hive_sync.partition_fields": PARTITION_FIELDS,
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
}

delete_config = {**incrementalConfig, "hoodie.datasource.write.operation": "delete"}

# if not spark._jsparkSession.catalog().tableExists(DATABASE, table_name):
if not spark._jsparkSession.catalog().tableExists(DATABASE, "ventas_det_default"):

    logging.info("Table does not exists, performing init load...")
    s3_path = "s3://{0}/{1}/{2}".format(INPUT_BUCKET, INPUT_PREFIX, FULL_FORMAT)
    logging.info("Reading files...")

    input_df = spark.read.parquet(s3_path)

    input_df = input_df.withColumn("Empresa", lit(BUSINESS))

    combinedConf = {**COMMON_CONFIG, **initLoadConfig}

    if PARTITION_FIELDS:
        combinedConf = {**combinedConf, **partitioned_options}

    logging.info("Writing init load...")

    glueContext.write_dynamic_frame.from_options(
        frame=DynamicFrame.fromDF(input_df, glueContext, "inputDf"),
        connection_type="custom.spark",
        connection_options=combinedConf,
    )

    logging.info("Writing init load files succeeded.")

logging.info("------ INCREMENTAL LOAD ------")


def mapping_types(db, table, df_schema):
    existing_df = glue.get_table_versions(DatabaseName=db, TableName=table)

    mappings = []
    for col in existing_df["TableVersions"][0]["Table"]["StorageDescriptor"]["Columns"]:
        target_name = col["Name"]
        target_type = col["Type"]
        print(target_name.lower())
        for col_origin in df_schema:
            if col_origin.name.lower() == target_name.lower():
                print(col_origin.name.lower())
                print("entró")
                mappings.append(
                    (
                        col_origin.name,
                        col_origin.dataType.typeName(),
                        col_origin.name,
                        target_type,
                    )
                )
    return mappings


upsert_conf = {**COMMON_CONFIG, **incrementalConfig}
delete_conf = {**COMMON_CONFIG, **delete_config}

# if PARTITION_FIELDS:
#    upsert_conf = {**upsert_conf, **partitioned_options}
#    delete_conf = {**delete_conf, **partitioned_options}

s3_path_cdc = "s3://{0}/{1}/".format(INPUT_BUCKET, INPUT_PREFIX)

print(s3_path_cdc)

conn_ops = {"paths": [s3_path_cdc], "exclusions": '["**LOAD*"]'}

logging.info("Reading cdc files...")

inputDyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options=conn_ops,
    format="parquet",
    transformation_ctx=table_name,
)

schema = inputDyf.schema()

if len(schema.fields) > 0:
    logging.info("Should be processed...")

    # logging.info(inputDyf.show())
    # print(inputDyf.show())

    # inputDyf = Map.apply(frame = inputDyf, f = addCompany)
    schema = inputDyf.schema()
    # table_mapping = mapping_types(DATABASE, table_name, schema)
    table_mapping = mapping_types(DATABASE, "ventas_det_default", schema)

    upsert_df = Filter.apply(frame=inputDyf, f=lambda x: x["_c0"] != "D")

    # logging.info(upsert_df.show())

    upsert_df = DropFields.apply(upsert_df, paths=["_c0"])

    dfUpsert = upsert_df.toDF()
    print("renaming columns upsert_df...")
    for col in dfUpsert.columns:
        # print("{}: {}".format(col, df_new_column[col]))
        dfUpsert = dfUpsert.withColumnRenamed(col, df_new_column[col])

    upsert_df = DynamicFrame.fromDF(dfUpsert, glueContext, "dfUpsert")
    # logging.info("upsert normal")
    # logging.info(upsert_df.show())

    delete_df = Filter.apply(frame=inputDyf, f=lambda x: x["_c0"] == "D")

    delete_df = DropFields.apply(delete_df, paths=["_c0"])

    dfDelete = delete_df.toDF()
    print("renaming columns delete_df...")
    for col in dfDelete.columns:
        # print("{}: {}".format(col, df_new_column[col]))
        dfDelete = dfDelete.withColumnRenamed(col, df_new_column[col])

    delete_df = DynamicFrame.fromDF(dfDelete, glueContext, "dfDelete")

    # upsert_df = ApplyMapping.apply(frame=upsert_df, mappings=table_mapping, transformation_ctx=table_name)
    # delete_df = ApplyMapping.apply(frame=delete_df, mappings=table_mapping, transformation_ctx=table_name)

    upsert_df = ApplyMapping.apply(
        frame=upsert_df, mappings=table_mapping, transformation_ctx="ventas_det_default"
    )
    delete_df = ApplyMapping.apply(
        frame=delete_df, mappings=table_mapping, transformation_ctx="ventas_det_default"
    )

    upsert_df.printSchema()
    delete_df.printSchema()

    logging.info("Writing cdc load...")

    glueContext.write_dynamic_frame.from_options(
        frame=upsert_df, connection_type="custom.spark", connection_options=upsert_conf
    )

    glueContext.write_dynamic_frame.from_options(
        frame=delete_df, connection_type="custom.spark", connection_options=delete_conf
    )

logging.info("Finished processing!")
job.commit()
