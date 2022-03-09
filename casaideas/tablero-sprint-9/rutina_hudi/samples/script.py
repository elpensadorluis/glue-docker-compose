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

#Client for glue
glue = boto3.client("glue")

#set logger
logging.getLogger().setLevel(logging.INFO)

#Config table
dynamodb = boto3.resource('dynamodb')

# Mandatory Parameters
job_parameters = ['CONFIG_TABLE', 'TABLE_NAME', 'JOB']

# Read Glue Parameters
args = getResolvedOptions(sys.argv, job_parameters)

table = dynamodb.Table(args.get("CONFIG_TABLE"))


config = table.get_item(
    Key={
        'table_name': args.get("TABLE_NAME")
    }
).get('Item')

spark = SparkSession.builder \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)

logging.info("Initializing job...")

FULL_FORMAT = "LOAD*.parquet"
INPUT_BUCKET = config['input_bucket']
INPUT_PREFIX = config['input_prefix']
OUTPUT_BUCKET = config['output_bucket']
OUTPUT_PREFIX = config['output_prefix']
PRECOMBINE_FIELDS = config['precombine']
DATABASE = config['target_db']
PK = config['pk']
PARTITION_FIELDS = config.get("partition_fields")
BUSINESS = config["business"]

#function for adding the column to dynamicframe
def addCompany(record):
    record["Empresa"] = BUSINESS
    return record

_, schema, table_name = INPUT_PREFIX.split('/')
table_name = table_name.lower()
out_location = f"{OUTPUT_PREFIX}/{DATABASE}/{table_name}"

job.init(args['JOB'] + table_name, args)

COMMON_CONFIG = {
    'className': 'org.apache.hudi',
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': PK,
    'hoodie.datasource.write.precombine.field': PRECOMBINE_FIELDS,
    'hoodie.datasource.hive_sync.enable': 'true',
    'hoodie.datasource.hive_sync.database': DATABASE,
    'hoodie.datasource.hive_sync.table': table_name,
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.NonpartitionedKeyGenerator',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.NonPartitionedExtractor',
    'hoodie.datasource.hive_sync.use_jdbc': 'false',
    'path': f's3://{OUTPUT_BUCKET}/{out_location}'
}

initLoadConfig = {
    'hoodie.bulkinsert.shuffle.parallelism': 4,
    'hoodie.datasource.write.operation': 'bulk_insert'
}

incrementalConfig = {
    'hoodie.upsert.shuffle.parallelism': 20, 
    'hoodie.datasource.write.operation': 'upsert', 
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
    'hoodie.cleaner.commits.retained': 1
}

partitioned_options = {
    'hoodie.datasource.write.hive_style_partitioning': 'true',
    'hoodie.datasource.write.partitionpath.field': PARTITION_FIELDS,
    'hoodie.datasource.hive_sync.partition_fields': PARTITION_FIELDS,
    'hoodie.datasource.write.keygenerator.class': "org.apache.hudi.keygen.ComplexKeyGenerator",
    'hoodie.datasource.hive_sync.partition_extractor_class': "org.apache.hudi.hive.MultiPartKeysValueExtractor"
}

delete_config = {
    **incrementalConfig,
    'hoodie.datasource.write.operation': 'delete'
}

if not spark._jsparkSession.catalog().tableExists(DATABASE, table_name):

    logging.info("Table does not exists, performing init load...")
    s3_path = "s3://{0}/{1}/{2}".format(INPUT_BUCKET, INPUT_PREFIX, FULL_FORMAT)
    logging.info("Reading files...")
    
    input_df = spark.read.parquet(s3_path)
    
    input_df = input_df.withColumn("Empresa", lit(BUSINESS))
    
    combinedConf = {**COMMON_CONFIG, **initLoadConfig}
    
    if PARTITION_FIELDS:
        combinedConf = {**combinedConf, **partitioned_options}
        
    logging.info("Writing init load...")
    
    glueContext.write_dynamic_frame.from_options(frame = DynamicFrame.fromDF(input_df, glueContext, "inputDf"), connection_type = "marketplace.spark", connection_options = combinedConf)

    logging.info("Writing init load files succeeded.")

logging.info("------ INCREMENTAL LOAD ------")

def mapping_types(db, table, df_schema):
    existing_df = glue.get_table_versions(
        DatabaseName = db,
        TableName = table
    )
    
    mappings = []
    for col in existing_df['TableVersions'][0]['Table']['StorageDescriptor']['Columns']:
        target_name = col["Name"]
        target_type = col["Type"]
        print(target_name.lower())
        for col_origin in df_schema:
            if col_origin.name.lower() == target_name.lower():
                print(col_origin.name.lower())
                print("entró")
                mappings.append((col_origin.name, col_origin.dataType.typeName(), col_origin.name, target_type))
    return mappings

upsert_conf = {**COMMON_CONFIG, **incrementalConfig}
delete_conf = {**COMMON_CONFIG, **delete_config}

if PARTITION_FIELDS:
    upsert_conf = {**upsert_conf, **partitioned_options}
    delete_conf = {**delete_conf, **partitioned_options}

s3_path_cdc = "s3://{0}/{1}/".format(INPUT_BUCKET, INPUT_PREFIX)

conn_ops = {
    'paths': [s3_path_cdc],
    'exclusions': "[\"**LOAD*\"]"
}

logging.info("Reading cdc files...")

inputDyf = glueContext.create_dynamic_frame_from_options(
    connection_type = 's3',
    connection_options = conn_ops,
    format = 'parquet',
    transformation_ctx = table_name
)

schema = inputDyf.schema()

if len(schema.fields) > 0:
    logging.info("Should be processed...")
    
    inputDyf = Map.apply(frame = inputDyf, f = addCompany)
    schema = inputDyf.schema()
    table_mapping = mapping_types(DATABASE, table_name, schema)
    
    upsert_df = Filter.apply(
        frame = inputDyf,
        f = lambda x: x["Op"] != 'D'
    )
    
    delete_df = Filter.apply(
        frame = inputDyf,
        f = lambda x: x["Op"] == 'D'
    )
    
    upsert_df = ApplyMapping.apply(frame=upsert_df, mappings=table_mapping, transformation_ctx=table_name)
    delete_df = ApplyMapping.apply(frame=delete_df, mappings=table_mapping, transformation_ctx=table_name)

    logging.info("Writing cdc load...")
    
    glueContext.write_dynamic_frame.from_options(frame = upsert_df, connection_type = "marketplace.spark", connection_options = upsert_conf)
    
    glueContext.write_dynamic_frame.from_options(frame = delete_df, connection_type = "marketplace.spark", connection_options = delete_conf)
    
logging.info("Finished processing!")    
job.commit()
