import os
import pandas as pd
import requests
from pyspark.sql.functions import lit
from datetime import datetime,timedelta
import time
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error



BASE_DIR = os.path.dirname(os.path.realpath(__file__))

def get_df_unidade(unidade_vinculada) -> pd.DataFrame:
    """Obtém um DataFrame com as informações da unidade vinculada especificada.

    Args:
        unidade_vinculada (int): Número da unidade vinculada.

    Returns:
        pd.DataFrame: DataFrame contendo as informações da unidade vinculada.
    """
    arquivo_csv = os.path.join(BASE_DIR, "tmp", "empresas.csv")
    df = pd.read_csv(arquivo_csv, sep=",") #old
    df_out = df[df["UNIDADEVINCULADA"] == int(unidade_vinculada)] #old
    return df_out



def insert_datalake(df, tabela):

    spark = SparkSession.builder \
            .appName(f"STG to Bronze") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.571") \
            .config("spark.hadoop.fs.s3a.endpoint", 'minio:9000') \
            .config("spark.hadoop.fs.s3a.access.key", 'minio') \
            .config("spark.hadoop.fs.s3a.secret.key", 'minio123') \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate()

    minio_client = Minio(
        'minio:9000',
        'minio',
        secret_key = 'minio123',
        secure = False
    )

    # Camada (bucket) do MinIO para qual os dados extraídos serão enviados
    bucket_name = "bronze"
    # Origem dos dados
    source_system = 'ssi'
    # Data Mart do DW ao qual esse notebook pertence
    business_area = "smais"
    
    df = spark.createDataFrame(df)

    today = datetime.today().strftime("%Y%m%d")
    today_folder = datetime.today().strftime("%Y%m%d")

    print(f"Removendo arquivos do Bucket")
    try:
        objects = minio_client.list_objects('bronze', prefix= f'{source_system}/{business_area}/{tabela}/' , recursive=True)
        for obj in objects:
            minio_client.remove_object('bronze', obj.object_name)
    except S3Error as err:
        print(f"Erro ao remover arquivos: {err}")
    print(f"Enviando {tabela} para o Bucket {bucket_name} do minIO")
    df.write.mode('overwrite').parquet(f"s3a://{bucket_name}/{source_system}/{business_area}/{tabela}/{today_folder}/{tabela}_{today_folder}.parquet")
    spark.stop()
