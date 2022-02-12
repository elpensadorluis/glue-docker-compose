from awsglue.utils import getResolvedOptions

# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
import sys
import os
import pg8000
import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    pass


try:
    event = getResolvedOptions(
        sys.argv, ["ENV",],  # "dev", "prod"  # "2021"  # "12"  # "06"
    )
except:
    event = {
        "ENV": "dev",
    }
env = "prod" if event["ENV"] == "prod" else "dev"

# global variables
REGION_NAME = "us-east-1"
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
print("Base_dir: ", BASE_DIR)
# DB Parameters
DB_NAME = ""
USER_NAME = ""
HOST_NAME = ""
PASSWORD = ""
PORT = ""

################################################
#                    AWS API
################################################
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
def get_parametersList(key):
    """
    Return parameters stored in AWS SSM.
    """
    try:

        ssm = boto3.client("ssm", region_name=REGION_NAME)

        response = ssm.get_parameters(Names=[key,], WithDecryption=True)

        return response["Parameters"][0]["Value"].split(",")
    except ClientError as error:
        raise error


class DBConnection:
    def __init__(self, db_name, user_name, host_name, password, port):
        try:
            print("Connecting to database")
            client = boto3.client("redshift", region_name="us-east-1")
            self.connection = pg8000.connect(
                host=host_name,
                user=user_name,
                database=db_name,
                password=password,
                port=port,
            )
            self.cursor = self.connection.cursor()
        except:
            print("Cannot connect to db")

    def exec_sql_file(self, file):

        self.cursor.execute(open(os.path.join(BASE_DIR, file), "r").read())

    def close_cursor(self):
        self.cursor.close()


def todo():
    try:
        print("entre")
        # SSM Parameters
        result = get_parametersList("/ANALYTICS/REDSHIFT/db_parameters")
        print("ssm cargado")
        if result:
            DB_NAME = result[0]
            USER_NAME = result[1]
            HOST_NAME = result[2]
            PASSWORD = result[3]
            PORT = result[4]

            print(
                f"""
                DB_NAME: {DB_NAME}
                USER_NAME: {USER_NAME}
                HOST_NAME: {HOST_NAME}
                PASSWORD: {PASSWORD}
                Port: {PORT}
                """
            )

            db_connection = DBConnection(DB_NAME, USER_NAME, HOST_NAME, PASSWORD, PORT)
            print(db_connection)
            s3 = boto3.resource("s3", region_name=REGION_NAME)
            i = 0
            for script in s3.Bucket(
                "aws-redshift-scripts-534086549449-us-east-1"
            ).objects.filter(Prefix=env):
                key = script.key
                print(key)
                if i == 0:
                    i = i + 1
                    continue
                try:
                    name = key.split("/")
                    name = name[1]
                    s3.Bucket(
                        "aws-redshift-scripts-534086549449-us-east-1"
                    ).download_file(key, os.path.join(BASE_DIR, name))
                    i = i + 1
                    print("ejecutando: ", name)
                    # db_connection.exec_sql_file(os.path.join(BASE_DIR,name))
                except Exception:
                    print("error {0} en script: ".format(Exception), name)
                    pass
            # db_connection.exec_sql_file('01_0material_attr.sql')
            # db_connection.exec_sql_file('02_0material_text.sql')
            # db_connection.exec_sql_file('03_0plant_attr.sql')
            # db_connection.exec_sql_file('04_tvtwt.sql')
            # db_connection.exec_sql_file('05_t179t.sql')
            # db_connection.exec_sql_file('06_zv_grupoex.sql')
            # db_connection.exec_sql_file('07_zv_etario.sql')
            # db_connection.exec_sql_file('08_ficha_corta.sql')
            # db_connection.exec_sql_file('09_cluster.sql')
            # db_connection.exec_sql_file('10_t_material_dim.sql')
            # db_connection.exec_sql_file('11_s833.sql')

            # #************** sales ***********************
            # db_connection.exec_sql_file('12_2lis_13_vditm.sql')
            # db_connection.exec_sql_file('13_mv_ficha_ventas.sql')
            # db_connection.exec_sql_file('14_mv_ficha_corta_ventas.sql')

            # # *********** Stock ***************************************************************
            # db_connection.exec_sql_file('15_load_update_dev_mov_materiales.sql')
            # db_connection.exec_sql_file('16_dev_load_0mat_sales_attr.sql')

            # # *********** Tracking ***************************************************************
            # db_connection.exec_sql_file('17_dev_load_fct_tracking.sql')

            # # *********** Precios***************************************************************
            # db_connection.exec_sql_file('18_dev_load_knop_precio_material.sql')
            # db_connection.exec_sql_file('19_dev_load_a004_condicion_material.sql')

            db_connection.close_cursor()
            db_connection.connection.close()

            return True

        else:
            return False
    except ClientError as error:
        raise error
        return False


if __name__ == "__main__":

    todo()
    print("Ha finalizado el proceso de carga de sql satisfactoriamente")
