import subprocess
# Instalar rich
subprocess.run(["pip", "install", "rich"])

import time
import os
from rich.console import Console
from pyspark.sql import SparkSession

class ValidadorDW:    
    def __init__(self,tipo,server,username,password,database,port,schema):
        """
        Inicializa a classe com as informações de conexão ao banco de dados.

        Args:
            tipo (str): sqlserver, postgresql oracle.
            server (str): Endereço do servidor do banco de dados.
            username (str): Nome de usuário para acessar o banco de dados.
            password (str): Senha para acessar o banco de dados.
            database (str): Nome do banco de dados.
            port (str): Porta do banco de dados.

        """
        self.tipo = tipo
        self.server = server
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.schema = schema

        if self.tipo == 'sqlserver':
            self.driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
            self.jar = 'mssql-jdbc-12.6.1.jre11.jar'
            self.jdbc_url = f"jdbc:{self.tipo}://{self.server}:{self.port};databaseName={self.database};encrypt=false"
        elif self.tipo == 'postgresql':
            self.driver = 'org.postgresql.Driver'
            self.jar = 'postgresql-42.2.20.jar'
            self.jdbc_url = f"jdbc:{self.tipo}://{self.server}:{self.port}/{self.database}"
        elif self.tipo == 'oracle':                   
            self.driver = 'oracle.jdbc.driver.OracleDriver'
            self.jar = 'ojdbc11.jar'            
            self.jdbc_url = f"jdbc:{self.tipo}:thin:@{self.server}:{self.port}/{self.database}"

        self.__start_spark_session()

    def __start_spark_session(self):
        """
        Inicializa a sessão do Spark
        """

        caminho_arquivo_jars = f"{os.getenv('PATH_RAIZ')}/jars/{self.jar}"
        #diretorio_atual = os.getcwd()
        #caminho_arquivo_jars = diretorio_atual + f"/jars/{self.jar}"                
        self.__spark = SparkSession.builder \
            .appName("Validador DW") \
            .config("spark.driver.extraClassPath",caminho_arquivo_jars)\
            .config("spark.executor.extraClassPath",caminho_arquivo_jars)\
            .config("spark.driver.memory","8g") \
            .config("spark.executor.memory","8g").getOrCreate()
          
    def fetch_data_count(self,tables):
        """
        Extrai dados das tabelas do banco de dados e os converte para o formato Parquet.

        Args:
            cursor (pymssql.Cursor): Cursor para executar consultas SQL no banco de dados.
            tables (list): Lista de tabelas a partir das quais os dados serão extraídos.
            today (str): Data atual no formato 'YYYYMMDD'.

        Returns:
            list: Lista de tuplas contendo o nome da tabela e o caminho do arquivo JSON onde os dados foram salvos.
        """

        # Inicia a instância do console Rich
        console = Console()        
        console.print("[green4] Validação quantidade de registros.[/green4]")
        dfs = {}
        for table in tables:
            sql_query = f"( SELECT * FROM {self.schema}.{table} ) tmp" 
            df = self.__spark.read.format('jdbc') \
                .option('url',self.jdbc_url) \
                .option('dbtable', sql_query ) \
                .option("user",self.username) \
                .option("password",self.password) \
                .option("driver",self.driver) \
                .load()
            dfs[table] = df
            num_records = dfs[table].count()            
            print(f"Tabela:{table} Total:{num_records}")
        self.__spark.stop()
