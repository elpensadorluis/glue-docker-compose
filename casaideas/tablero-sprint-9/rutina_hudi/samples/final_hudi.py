import sys
import json
import boto3
import logging
import pathlib
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from awsglue.dynamicframe import DynamicFrame

logging.getLogger().setLevel(logging.INFO)

def checkPath(bucket, file_path):
    client = boto3.client('s3')
    result = client.list_objects(Bucket=bucket, Prefix=file_path)
    exists=False
    if len(result['Contents']) > 1:
        exists=True
    return exists

#Config table
dynamodb = boto3.resource('dynamodb')

# Mandatory Parameters
job_parameters = ['JOB_NAME', 'CONFIG_TABLE', 'TABLE_NAME']

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

# Get the parameter values
INPUT_BUCKET = config['input_bucket']
OUTPUT_BUCKET = config['output_bucket']
OUTPUT_PREFIX = config['output_prefix']
PRECOMBINE_FIELDS = config['precombine']
INPUT_PREFIX = config['input_prefix']
DATABASE = config['target_db']
PK = config['pk']
PARTITION_FIELDS = config.get("partition_fields")

# Determine remaining fields
schema, table_name = [value.lower() for value in pathlib.PurePath(INPUT_PREFIX).name.split(".")]
out_location = f"{OUTPUT_PREFIX}/{schema}/{table_name}"

job.init(args['JOB_NAME'] + table_name, args)

CSV_OPTIONS = {
    "separator": "|",
    "withHeader": True,
    "escaper": "~",
    "multiLine": True,
    "quoteChar": '"', 
}

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
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS', # Cambiar la policy del cleaner. No interesa retener commits. Ajuro hay que retener
    'hoodie.cleaner.commits.retained': 10
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

def mapping_builder(bucket: str, full_folder: str, precombine_field: str, current_df):
    """
    Generates a mapping for data typing based on the Full Load DFM File.
    :param bucket: Bucket where the table is being replicated
    :param full_folder: Prefix within the bucket where the replication loaded the full_load file
    :param is_cdc: Indicates if the CDC Metadata columns should be considered
    
    :returns: A schema with the correct data types for the table being processed
    """

    s3 = boto3.resource('s3')
    content_object = s3.Object(bucket, f'{full_folder}/LOAD00000001.dfm')
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    
    df_schema = current_df.schema()

    mappings = []

    for col in json_content["dataInfo"]["columns"]:
        target_name = col["name"]
        target_type = col["type"]
        
        for col_origin in df_schema:
            if col_origin.name.lower() == target_name.lower():
                if target_type == 'DATETIME':
                    type = "timestamp"
                elif target_type.startswith('INT'):
                    type = "int"
                elif target_type.startswith('NUMERIC'):
                    type = "double"
                elif target_type.startswith('STRING'):
                    type = "string"
                mappings.append((col_origin.name, col_origin.dataType.typeName(), col_origin.name.replace("/", ""), type))
            
    # Append mandatory final column
    mappings.append((precombine_field, "string", precombine_field, "string",))

    return mappings

if not spark._jsparkSession.catalog().tableExists(DATABASE, table_name):
    logging.info("Table does not exists, performing init load...")

    s3_path = "s3://{0}/{1}/".format(INPUT_BUCKET, INPUT_PREFIX)

    logging.info("Reading files...")

    input_df = glueContext.create_dynamic_frame.from_options(
        connection_type = "s3", 
        connection_options = {
            "paths": [s3_path],
            'exclusions': "[\"**.dfm\"]"
        },
        format = "csv", 
        format_options=CSV_OPTIONS,
        transformation_ctx=table_name
    )

    logging.info("Reading init load files succeeded...")

    table_mapping = mapping_builder(INPUT_BUCKET, INPUT_PREFIX, PRECOMBINE_FIELDS, input_df)

    input_df = ApplyMapping.apply(frame=input_df, mappings=table_mapping, transformation_ctx=f"{table_name}")

    combinedConf = {**COMMON_CONFIG, **initLoadConfig}
    
    if PARTITION_FIELDS:
        combinedConf = {**combinedConf, **partitioned_options}

    logging.info("Writing init load...")

    glueContext.write_dynamic_frame.from_options(frame = input_df, connection_type = "marketplace.spark", connection_options=combinedConf)

    logging.info("Writing init load files succeeded.")

if checkPath(INPUT_BUCKET, f"{INPUT_PREFIX}__ct/"):
    logging.info("------ INCREMENTAL LOAD ------")
    
    upsert_conf = {**COMMON_CONFIG, **incrementalConfig}
    delete_conf = {**COMMON_CONFIG, **delete_config}
    
    if PARTITION_FIELDS:
        upsert_conf = {**upsert_conf, **partitioned_options}
        delete_conf = {**delete_conf, **partitioned_options}
    
    s3_path_cdc = "s3://{0}/{1}__ct/".format(INPUT_BUCKET, INPUT_PREFIX)
    
    conn_ops = {'paths': [s3_path_cdc], 'exclusions': "[\"**.dfm\"]", "recurse":True}
    
    logging.info("Reading files...")
    
    inputDyf = glueContext.create_dynamic_frame_from_options(
        connection_type='s3',
        connection_options=conn_ops,
        format='csv',
        transformation_ctx=table_name,
        format_options=CSV_OPTIONS
    )
    
    logging.info("Reading cdc load files succeeded...")
    
    upsert_df = Filter.apply(frame = inputDyf,
                                f = lambda x: x["header__change_oper"] != 'D')
    
    delete_df = Filter.apply(frame = inputDyf,
                                f = lambda x: x["header__change_oper"] == 'D')
    
    table_mapping = mapping_builder(INPUT_BUCKET, INPUT_PREFIX, PRECOMBINE_FIELDS, inputDyf)
    
    upsert_df = ApplyMapping.apply(frame=upsert_df, mappings=table_mapping, transformation_ctx=f"{table_name}")
    delete_df = ApplyMapping.apply(frame=delete_df, mappings=table_mapping, transformation_ctx=f"{table_name}")
    
    logging.info("Writing cdc load...")
    
    glueContext.write_dynamic_frame.from_options(frame = upsert_df, connection_type = "marketplace.spark", connection_options = upsert_conf)
    
    logging.info("Processing cdc deletes...")
    
    glueContext.write_dynamic_frame.from_options(frame = delete_df, connection_type = "marketplace.spark", connection_options = delete_conf)
    
    logging.info("Writing cdc load succeeded.")

logging.info("Finished!")
job.commit()
