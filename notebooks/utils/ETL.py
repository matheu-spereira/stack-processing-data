import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
import glob
import os
from croniter import croniter
from datetime import datetime, timedelta
from delta.tables import DeltaTable
from utils.LerYaml import LeitorYAML
from pyspark.sql.utils import AnalysisException
from minio import Minio
from minio.error import S3Error
import re
from utils.LerYaml import LeitorYAML
from utils.UtilsCbo import CBOUploader
from utils.UtilsTcuAcordados import TCUAUploader
from utils.UtilsReceitaFederal import ReceitaFederalProcessor
from utils.UtilsCaged import CagedFileProcessor
from utils.UtilsApiExtractor import ApiExtractor

from pyspark.sql.types import StringType, DateType
from pyspark.sql.functions import col, regexp_replace, coalesce, lit

from utils.ControleStatus import ControleStatus

class AlinharETL:
    
    def iniciar_sessao_spark(self):
        """
        Inicia uma sessão Spark com configurações específicas.
        Returns:
            SparkSession: Sessão Spark configurada.
            
            spark.executor.memory: quantidade de memória disponível para cada executor.
            spark.executor.cores: número de núcleos disponíveis para cada executor.
            spark.driver.memory: quantidade de memória disponível para o driver.
            spark.driver.cores: número de núcleos disponíveis para o driver.
            spark.dynamicAllocation.enabled: habilita a alocação dinâmica.
            spark.dynamicAllocation.minExecutors: número mínimo de executores.
            spark.dynamicAllocation.maxExecutors: número máximo de executores.
        """        
        self.logger.info("Iniciando Sessão Spark.")
        return SparkSession.builder \
            .master(self.master) \
            .appName(f"{self.bucket}_{self.datamart}") \
            .config("spark.jars",
                ",".join([
                    "/mnt/spark-jars/hadoop-aws-3.2.2.jar",
                    "/mnt/spark-jars/delta-spark_2.12-3.2.0.jar",
                    "/mnt/spark-jars/delta-storage-3.2.0.jar",
                    "/mnt/spark-jars/postgresql-42.2.20.jar",
                    "/mnt/spark-jars/mssql-jdbc-12.6.1.jre11.jar",
                    "/mnt/spark-jars/ojdbc11-23.5.0.24.07.jar",
                    "/mnt/spark-jars/spark-excel_2.12-0.13.5.jar",
                    "/mnt/spark-jars/aws-java-sdk-bundle-1.12.180.jar",
                    "/mnt/spark-jars/xmlbeans-3.1.0.jar",
                    "/mnt/spark-jars/poi-4.1.2.jar",
                    "/mnt/spark-jars/poi-ooxml-schemas-4.1.2.jar"
                ])) \
            .config("spark.executor.memory", "4g") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "file:/tmp/spark-events") \
            .config("spark.history.store.path", "file:/tmp/spark-events") \
            .config("spark.executor.cores", "4") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.cores", "2") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.minExecutors", "1") \
            .config("spark.dynamicAllocation.maxExecutors", "2") \
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
            .config("spark.databricks.delta.logRetentionDuration", "interval 1 hours") \
            .config("spark.databricks.delta.deletedFileRetentionDuration", "interval 1 hours") \
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_HOST")) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.com.amazonaws.services.s3.enableV4", "true") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl","org.apache.hadoop.fs.s3a.S3A") \
            .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
            .config("spark.sql.files.maxPartitionBytes", "128MB") \
            .getOrCreate()    

    def conexao_banco_de_dados(self,tipo,endereco,porta,usuario,senha,nome_banco,esquema):
        """
        conexao_banco_de_dados.

        Args:
            tipo : Tipo do banco de dados.
            endereco : Endereco do banco de dados.
            porta : Porta do banco de dados.
            usuario : Nome do usuario do banco de dados.
            senha : Senha do usuario do banco de dados.
            nome_banco : Nome do banco de dados.
            esquema : Schema do banco de dados.            
        """
        try:
            self.logger.info("Aguarde... Configurando conexao banco de dados")
            self.tipo = tipo
            self.schema = esquema
            self.host = endereco
            self.port = porta
            self.database = nome_banco
            self.username = usuario
            self.password = senha
            if self.tipo == 'sqlserver':
                self.driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
                self.jar = f"{self.path_raiz}/jars/mssql-jdbc-12.6.1.jre11.jar"
                self.jdbc_url = f"jdbc:{self.tipo}://{self.host}:{self.port};databaseName={self.database};encrypt=false"
            elif self.tipo == 'postgresql':
                self.driver = 'org.postgresql.Driver'
                self.jar = f"{self.path_raiz}/jars/postgresql-42.2.20.jar"
                self.jdbc_url = f"jdbc:{self.tipo}://{self.host}:{self.port}/{self.database}"
            elif self.tipo == 'oracle':                   
                self.driver = 'oracle.jdbc.driver.OracleDriver'
                self.jar = f"{self.path_raiz}/jars/ojdbc11.jar"
                self.jdbc_url = f"jdbc:{self.tipo}:thin:@{self.host}:{self.port}/{self.database}"
            elif self.tipo == 'mysql':                   
                self.driver = 'com.mysql.cj.jdbc.Driver'
                self.jar = f"{self.path_raiz}/jars/mysql-connector-j-8.4.0.jar"
                self.jdbc_url = f"jdbc:{self.tipo}://{self.host}:{self.port}/{self.database}?user={self.username}&password={self.database}"
            self.properties = {
                "user": f"{self.username}",
                "password": f"{self.password}",
                "driver": f"{self.driver}"
            }
        except Exception as e:
            self.logger.error("Erro ao configurar banco de dados")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise      
    
    