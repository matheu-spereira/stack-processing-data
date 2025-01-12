import subprocess

import time
import os
from pyspark.sql import SparkSession
#from dotenv import load_dotenv
import pandas as pd
import re


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
        #elif self.tipo == 'oracle':                   
        #    self.driver = 'oracle.jdbc.driver.OracleDriver'
        #    self.jar = 'ojdbc11.jar'            
        #    self.jdbc_url = f"jdbc:{self.tipo}:thin:@{self.server}:{self.port}/{self.database}"

        self.__start_spark_session()

    def __start_spark_session(self):
        """
        Inicializa a sessão do Spark
        """

        caminho_arquivo_jars = f"/home/jovyan/work/jars/{self.jar}"         
        self.__spark = SparkSession.builder \
            .appName("Validador DW") \
            .config("spark.driver.extraClassPath",caminho_arquivo_jars)\
            .config("spark.executor.extraClassPath",caminho_arquivo_jars)\
            .config("spark.driver.memory","8g") \
            .config("spark.executor.memory","8g").getOrCreate()
                

    def fetch_data(self, tables, revisor, versao, dt_revisao, nome_arquivo):
        with pd.ExcelWriter(f'{nome_arquivo}.xlsx', engine='xlsxwriter') as writer:
            # Criar DataFrame para as informações da lista de tabelas
            lista_tabelas_data = {
                'Schema': [],
                'View': [],
                'Revisão': [],
                'Versão': [],
                'Data Revisão': [],
                'Editor': []
            }
            
            # Adicionar os valores para cada tabela na lista de tabelas
            for table in tables:
                lista_tabelas_data['Schema'].append(self.schema)
                lista_tabelas_data['View'].append(f"{self.schema}.{table}")
                lista_tabelas_data['Revisão'].append(revisor)
                lista_tabelas_data['Versão'].append(versao)
                lista_tabelas_data['Data Revisão'].append(dt_revisao)
                lista_tabelas_data['Editor'].append(revisor)

            df_lista_tabelas = pd.DataFrame(lista_tabelas_data)
            
            # Escrever a tabela lista_tabelas na aba "lista_tabelas"
            df_lista_tabelas.to_excel(writer, sheet_name='lista_tabelas', index=False)
            
            # Iterar sobre as tabelas para escrever cada tabela no arquivo Excel
            for table in tables:
                # Criar DataFrame para as informações do cabeçalho
                header_data = {
                    'View': ['View', 'Revisor', 'Versão', 'Data Revisão']
                }
                
                # Adicionar os valores ao cabeçalho
                values = [f"{self.schema}.{table}", revisor, versao, dt_revisao]
                for value in values:
                    header_data.setdefault('Value', []).append(value)
                
                df_header = pd.DataFrame(header_data)

                
                if self.tipo == 'sqlserver' or self.tipo == 'postgresql':
                    query_sqlserver = f"(SELECT COLUMN_NAME as Coluna, DATA_TYPE as Tipo, CASE WHEN IS_NULLABLE = 'YES' THEN 'Sim' WHEN IS_NULLABLE = 'NO' THEN 'Não' ELSE IS_NULLABLE END AS Nulo FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{self.schema}' AND TABLE_NAME = '{table}') tmp"

                # Carregar dados do SQL Server
                df = self.__spark.read.format('jdbc') \
                    .option('url', self.jdbc_url) \
                    .option('dbtable', query_sqlserver) \
                    .option("user", self.username) \
                    .option("password", self.password) \
                    .option("driver", self.driver) \
                    .load()
        
                # Converter DataFrame Spark para DataFrame Pandas
                df_pandas = df.toPandas()
                
                # Adicionar coluna 'N°' enumerando as linhas
                df_pandas.insert(0, 'N°', range(1, len(df_pandas) + 1))

                # Extrair parte do nome da tabela para o nome da aba
                match = re.search(r'(?:flat_|fato_|dim_)(\w+)', table)
                if match:
                    sheet_name = f'{match.group(1)}'
                else:
                    sheet_name = f'{table[-20:]}' 
                
                # Gravar o cabeçalho na planilha XLSX
                df_header.to_excel(writer, sheet_name=sheet_name, startrow=0, index=False, header=False)
                
                # Gravar as informações do SQL abaixo do cabeçalho
                df_pandas.to_excel(writer, sheet_name=sheet_name, startrow=5, startcol=0, index=False)
            
        self.__spark.stop()






