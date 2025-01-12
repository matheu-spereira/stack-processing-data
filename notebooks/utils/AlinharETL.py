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
    
    def __init__(self, bucket: str, datamart: str, master: str = os.getenv("SPARK_MASTER") ):
        """
        Inicializa a classe com as configurações necessárias e configura o logger.
        Args:
        """
        self.path_raiz = os.getenv("PATH_RAIZ")
        self.bucket = bucket
        self.datamart = datamart
        logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(f"{self.bucket}_{self.datamart}")
        self.master = master
        self.spark = self.iniciar_sessao_spark()
        

    def inicializar_variaveis(self,nome_arquivo_yaml):
        """
        Inicializa variveis.
        """
        try:           
            self.leitor_yaml = LeitorYAML(nome_arquivo_yaml)
            self.lakehouse = self.leitor_yaml.busca_chave_sessao_valor('sink','datalakezone')
            self.datalakepath = self.leitor_yaml.busca_chave_sessao_valor('sink','datalakepath')
            self.app_name = self.leitor_yaml.busca_chave_sessao_valor('database','tablename')            
            self.type = self.leitor_yaml.busca_chave_valor("type")
            self.chunksize = self.leitor_yaml.busca_chave_valor("chunksize")
            self.mode = self.leitor_yaml.busca_chave_sessao_valor('sink','mode')            
            self.caminho_tabela_delta = f"s3a://{self.lakehouse}/{self.datalakepath}"
            self.estado = self.leitor_yaml.busca_chave_sessao_valor('database','state')
            self.pagina = self.leitor_yaml.busca_chave_sessao_valor('database','page')
            self.tablename = self.leitor_yaml.busca_chave_sessao_valor('database','tablename')           
            self.delta_tablename = self.leitor_yaml.busca_chave_sessao_valor('delta','tablename')
            #arquivos
            self.tablename_arquivo = self.leitor_yaml.busca_chave_sessao_valor('arquivo','tablename')
            self.directory_arquivo = self.leitor_yaml.busca_chave_sessao_valor('arquivo','directory')
            self.format = self.leitor_yaml.busca_chave_sessao_valor('arquivo','format')
            self.url_arquivo = self.leitor_yaml.busca_chave_sessao_valor('arquivo','url')
            # api
            self.tablename_api = self.leitor_yaml.busca_chave_sessao_valor('api','tablename')
            self.url_api = self.leitor_yaml.busca_chave_sessao_valor('api','url')
            self.endpoint = self.leitor_yaml.busca_chave_sessao_valor('api','endpoint')
            self.tag = self.leitor_yaml.busca_chave_sessao_valor('api','tag')
            self.origin_api = self.leitor_yaml.busca_chave_sessao_valor('api','origin')
            
            diretorio_json = f"{self.path_raiz}/cache/{self.bucket}/{self.datamart}/"
            if self.type == 'database':
                arquivos_json = os.path.join(diretorio_json,self.tablename+'.json')
            else:
                arquivos_json = os.path.join(diretorio_json,self.delta_tablename+'.json')
            self.controle_status = ControleStatus(arquivos_json)        
        except Exception as e:
            self.logger.error("Erro ao inicializar variaveis")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

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
    
    def __verifica_executar_carga_full(self):
        """
        Verifica se a cron de carga full é completa.        
        Args:
        """
        try:  
            self.extracao_completa = False
            cron = self.leitor_yaml.busca_chave_sessao_valor('database','schedule')
            now = datetime.now()
            iter = croniter(cron, now)
            next_run = iter.get_next(datetime)
            next_run = next_run.replace(minute=0, second=0, microsecond=0)
            data_atual = datetime.now()
            data_atual_ajustada = data_atual.replace(minute=0, second=0, microsecond=0)
            data_fornecida_ajustada = next_run.replace(minute=0, second=0, microsecond=0)
            if data_atual_ajustada == data_fornecida_ajustada:
                self.extracao_completa = True
            #garantir que nao filtre registro parciais sem ser carga completa    
            if self.chunksize == 0:    
                self.extracao_completa = True
            _valor_chave = self.leitor_yaml.busca_chave_sessao_valor("database","filter_partial")
            _valor_chave if _valor_chave else ''
            if (_valor_chave if _valor_chave else '') == '':
                self.extracao_completa = True
            self.logger.info(f"Validando extração completa ({self.extracao_completa})")
        except Exception as e:            
            self.logger.error(f"{e}")
            self.extracao_completa = False
            self.logger.info(f"Validando extração completa ({self.extracao_completa})")
            

    def extrair_dados_bronze(self,arquivo=''):
        """
        Rotina utilizada para extrair dados usado na camada bronze.
        Args:
        """
        try:
            self.logger.info("Aguarde... Iniciando extração de dados camada Bronze")
            consultas = []
            if arquivo == "" or arquivo is None:
                consultas = self.carrega_lista_yaml()                
            else:
                consultas = self.carrega_arquivo_yaml(arquivo)
            if len(consultas) > 0:
                for consulta in consultas:
                    self.logger.info(" ")
                    self.inicializar_variaveis(consulta)
                    if self.type == 'database':
                        self.extrair_database()
                    if self.type == 'api':
                        print('Extraindo dados api')  
                        self.extrair_api(consulta)
                    if self.type == 'arquivo':
                        print('Extraindo dados arquivos')
            self.logger.info(f"Extração de dados camada bronze concluida com sucesso")
            self.parar_sessao()
        except Exception as e:
            self.logger.error("Erro ao extrair dados camada bronze")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

#################################### // ####################################

    def extrair_api(self, consulta):
        """
        Extrai informações de uma API e salva dataframe no formato delta
        Args:
            consulta: path yaml com parametros de extração da api
        """
        
        self.inicializar_variaveis(consulta) 
        try:
            if self.type == 'api' and self.origin_api == 'caravel_al':
                caravel_extractor = ApiExtractor(
                caravel_host=self.url_api, 
                caravel_user=os.getenv("CARAVEL_FE_USERNAME"), 
                caravel_password=os.getenv("CARAVEL_FE_PASSWORD"), 
                spark=self.spark, 
                pagination=self.chunksize, 
                tag = self.tag, 
                included_endpoint=self.endpoint 
                )
                df = caravel_extractor.extract_caravel()
                self.salvar_delta(df,'overwrite')
            elif self.type == 'api' and self.origin_api == 'cnae':
                caravel_extractor = ApiExtractor(
                caravel_host=self.url_api, 
                caravel_user='', 
                caravel_password='',
                spark=self.spark,
                pagination=self.chunksize,
                tag = self.tag,
                included_endpoint=self.endpoint
                )
                df = caravel_extractor.fetch_data_url()
                self.salvar_delta(df,'overwrite')
            elif self.type == 'api' and self.origin_api == 'sgt_api':
                caravel_extractor = ApiExtractor(
                caravel_host=self.url_api, 
                caravel_user='', 
                caravel_password='',
                spark=self.spark, 
                pagination='', 
                tag = '', 
                included_endpoint=self.endpoint 
                )
                df = caravel_extractor.extract_sgt('20240101', self.tablename_api, os.getenv("B_TOKEN") )
                self.salvar_delta(df,'overwrite') 
            elif self.type == 'api' and self.origin_api == 'dynamics':
                caravel_extractor = ApiExtractor(
                caravel_host=self.url_api, 
                caravel_user='', 
                caravel_password='',
                spark=self.spark,
                pagination='',
                tag = '',
                included_endpoint=self.endpoint
                )
                df = caravel_extractor.requisicao_dynamics()
                self.salvar_delta(df,'overwrite') 
                            
            self.logger.info(f"Extração concluída com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao extrair_dados_bronze: {e}")
            self.parar_sessao()
            raise

#################################### // ####################################


    def encontrar_ano_mes(self, caminho_prefixo):
        """
        Retorna a pasta de ANO-DATA mais recente do bucket
        Args:
            caminho_prefixo: prefixo do caminho no minIO
        """
        # Lista os objetos no prefixo fornecido
        url = os.getenv("MINIO_HOST").replace("http://", "")            
        minio_client = Minio(url,
            access_key= os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False  # Use True se estiver usando HTTPS
        )   

        objetos = minio_client.list_objects("landing", prefix=caminho_prefixo, recursive=True)
        
        # Expressão regular para capturar o padrão ano-mês
        padrao = r'(\d{4}-\d{2})'
        
        # Verifica os objetos encontrados
        for objeto in objetos:
            match = re.search(padrao, objeto.object_name)
            if match:
                return match.group(1)  # Retorna o primeiro ano-mês encontrado
    
        return None  # Retorna None se nenhum ano-mês for encontrado




#################################### // ####################################

    def criar_view_temporaria_arquivo(self, local, nome, schema=None, aba=None, **config):
        """
        Cria uma view temporária a partir de um arquivo.
        
        Args:
            local (str): Caminho do arquivo.
            nome (str): Nome da view temporária a ser criada.
            schema (str): Schema do DataFrame (aplicável para CSV). Padrão é None.
            aba (str): Nome da aba do arquivo xlsx.
            **config: Parâmetros adicionais para configuração da leitura.
        """
        try:
            consultas = self.carrega_lista_yaml() 
            if consultas:
                for consulta in consultas:
                    self.inicializar_variaveis(consulta)
                    if self.type == 'arquivo' and self.format in ['csv', 'txt']:
                        if self.tablename_arquivo == 'caged_layout':
                            df = self.spark.read.format("com.crealytics.spark.excel").option("dataAddress", f"{aba}!A1").load(local, **config)
                            df = df.dropna(how="all")
                        else:
                            df = self.spark.read.csv(local, schema=schema, **config)
    
                df.createOrReplaceTempView(nome)
                self.logger.info(f"Criando view temporaria.")
                return df
        except Exception as e:
            self.logger.error(f"Erro ao criar view temporaria: {e}")
            self.parar_sessao()
            raise
  
#################################### // ####################################

    def criar_view_temporaria_df(self, df, nome):
        """
        Cria uma view temporária a partir de um dataframe

        Args:
            df: nome dataframe a ser convertido em tabela ou view temporária spak
            nome: Nome da tabela delta a ser criada.
        """
        try:
            #df = self.spark.read.format("delta").load(caminho)
            df.createOrReplaceTempView(nome)
            self.logger.info(f"Criando view temporaria.")
            return df
        except Exception as e:
            self.logger.error(f"Erro ao criar view temporaria.: {e}")
            self.parar_sessao()
            raise   
 

#################################### // ####################################


    def extrair_dados_landing_compliance(self):
        """
        Extrai arquivos de diferentes origens do datamart de compliance
        """
        try:
            consultas = []
            consultas = self.carrega_lista_yaml() 
            self.parar_sessao()
            if len(consultas) > 0:
                for consulta in consultas:
                    #print(consulta)
                    self.inicializar_variaveis(consulta) 
                    if self.type == 'arquivo' and self.tablename_arquivo == 'cbo':
                        print('Iniciando download dos arquivos de CBO') 
                        uploader = CBOUploader(
                            bucket_name = self.lakehouse,
                            prefix = self.datalakepath,
                            secure= False  # Use True se estiver usando HTTPS
                        ) 
                        uploader.download_and_upload(self.url_arquivo)
                    if self.type == 'arquivo' and self.tablename_arquivo == 'tcu_acordaos':
                        print('Iniciando download dos arquivos de TCU Acordaos') 
                        uploader = TCUAUploader(
                            bucket_name = self.lakehouse,
                            prefix = self.datalakepath,
                            secure= False  # Use True se estiver usando HTTPS
                        ) 
                        uploader.download_and_upload(self.url_arquivo)
                    if self.type == 'arquivo' and self.tablename_arquivo == 'receita_federal_empresas':
                        print('Iniciando download dos arquivos de Receita Federal Empresas') 
                        processor = ReceitaFederalProcessor(
                            terms=["empresas"],
                            bucket_name=self.lakehouse,
                            business_area=self.datalakepath,
                            url = self.url_arquivo
                        )
                        processor.process()
                    if self.type == 'arquivo' and self.tablename_arquivo == 'receita_federal_estabelecimentos':
                        print('Iniciando download dos arquivos de Receita Federal Estabelecimentos') 
                        processor = ReceitaFederalProcessor(
                            terms=["estabelecimentos"],
                            bucket_name=self.lakehouse,
                            business_area=self.datalakepath,
                            url = self.url_arquivo
                        )
                        processor.process()
                    if self.type == 'arquivo' and self.tablename_arquivo == 'receita_federal_inicial':
                        print('Iniciando download dos arquivos de Receita Federal Inicial') 
                        processor = ReceitaFederalProcessor(
                            terms=["cnaes",
                            "motivos",
                            "municipios",
                            "paises",
                            ],
                            bucket_name=self.lakehouse,
                            business_area=self.datalakepath,
                            url = self.url_arquivo
                        )
                        processor.process()
                    if self.type == 'arquivo' and self.tablename_arquivo == 'compliance_caged':
                        print('Iniciando download dos arquivos de CAGED') 
                        processor = CagedFileProcessor(
                            ftp_host=self.url_arquivo,
                            bucket_name=self.lakehouse,
                            business_area=self.datalakepath,
                            caged_cwd=self.directory_arquivo
                        )
                        processor.process_files()
            self.logger.info(f"extrair_dados_bronze com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao extrair_dados_bronze: {e}")
            self.parar_sessao()
            raise

####################################################################################################################################################################################
    

    def prepara_sql_database(self,pagina):
        """
        Rotina responsável por preparar o SQL utilizado na extração validando os diversos database.        
        Args:
            pagina: Numero da paginacao utlizado na extracao
        """
        try:
            self.logger.info("Preparando SQL para banco de dados")
            _select_sql = self.leitor_yaml.busca_chave_sessao_valor("database","query") 
            if self.extracao_completa == True:
                _valor_chave = self.leitor_yaml.busca_chave_sessao_valor("database","filter_full")                
            else:
                _valor_chave = self.leitor_yaml.busca_chave_sessao_valor("database","filter_partial")
            _where_sql = ' ' + _valor_chave if _valor_chave else ''
            _ordenacao_sql = ''
            if self.tipo == 'sqlserver':
                if self.chunksize > 0:
                    _ordenacao_sql = ' order by 1 offset '+str(pagina)+' rows fetch next '+str(self.chunksize)+' rows only'
                comandosql = '(' + _select_sql + _where_sql + _ordenacao_sql + ')temp'
            elif self.tipo == 'postgresql':
                if self.chunksize > 0:
                    _ordenacao_sql = ' order by 1 offset '+str(pagina)+' limit '+str(self.chunksize)
                comandosql = '(' + _select_sql + _where_sql + _ordenacao_sql + ')temp'
            elif self.tipo == 'oracle':
                posicao_from = _select_sql.find("from")
                if self.chunksize > 0:
                    nova_sql = _select_sql[:posicao_from] + ',row_number() over (order by 1) as rn ' + _select_sql[posicao_from:]
                    comandosql = '(select * from ('+nova_sql+' '+_where_sql+') where rn > '+str(pagina)+' and rn <= '+str(pagina + self.chunksize)+')tmp'
                else:
                    comandosql = '(' + _select_sql + _where_sql + ')temp'
            elif self.tipo == 'mysql':
                if self.chunksize > 0:
                    _ordenacao_sql = ' order by 1 limit '+str(self.chunksize)+' offset '+str(pagina)
                comandosql = '(' + _select_sql + _where_sql + _ordenacao_sql + ')temp'
            return comandosql
        except Exception as e:
            self.logger.error("Erro ao prepara SQL")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise       
    
    def extrair_database(self):
        """
        Extrai dados de banco de dados. aplicando as validaões e regras
        
        Args:
            
        """
        try:
            self.logger.info("Aguarde... Extraindo dados do banco de dados")
            self.__verifica_executar_carga_full()
            
            contador = 1
            pagina = 1
            total_registros = 0
            
            #Para garantir que a primeira carga seja completa
            if self.extracao_completa == False:
                if self.__tabela_existe() == False:
                    self.extracao_completa = True
            #------------------------------------------------
            extraiu_dados = False
            if self.chunksize > 0:
                if self.controle_status.get_state() == 'execucao':
                    pagina = self.controle_status.get_page()
                
                if self.extracao_completa == True:
                    mode = "append"
                    if self.controle_status.get_state() != 'execucao':
                        _del_where_sql = self.leitor_yaml.busca_chave_sessao_valor("database","delete_full")
                        if _del_where_sql == "" or _del_where_sql is None:
                            self.__deletar_registros()                        
                        else:
                            self.deletar_registros_SQL(_del_where_sql)
                else:
                    mode = self.leitor_yaml.busca_chave_sessao_valor("sink","mode")  
                
                while contador >= 1:     
                    self.logger.info('')
                    self.logger.info(f"{str(contador).zfill(3)}-{self.tablename}")
                    self.controle_status.set_state('execucao')
                    comandoSQL = self.prepara_sql_database(pagina)
                    df = self.carregar_dados_database(comandoSQL)
                    df_count = df.count()
                    if df is not None:                    
                        if df_count <= 0:
                            contador = -1        
                        else:
                            extraiu_dados = True
                            if self.extracao_completa == True:
                                self.salvar_delta(df,mode)
                            else:
                                self.realizar_merge(df)
                            total_registros += df_count
                            pagina += self.chunksize
                            self.controle_status.set_page(pagina)                            
                            if df_count < self.chunksize:
                                contador = -1
                            else:
                                contador += 1                                        
                    else:
                        contador = -1        
                        print("Erro: DataFrame não foi criado corretamente.")
            else:
                mode = self.leitor_yaml.busca_chave_sessao_valor("sink","mode")
                comandoSQL = self.prepara_sql_database(0)
                df = self.carregar_dados_database(comandoSQL)
                df_count = df.count()
                if df is not None:
                    extraiu_dados = True
                    if self.extracao_completa == True:
                        self.salvar_delta(df,mode)
                    else:
                        self.realizar_merge(df)
            self.controle_status.set_state('finalizado')
            self.controle_status.set_page(0)  
            if extraiu_dados:
                self.logger.info("Extração do banco dados com sucesso")
                self.logger.info(f"{self.caminho_tabela_delta}")                
            else:
                self.logger.info("Sem registros para ser extaidos")
        except Exception as e:
            self.logger.error(f"Erro ao extrair dados do banco")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise    
    
    def carrega_lista_yaml(self):
        """
        Verifica a lista de yaml.
        Args:
        
        """
        try:
            diretorio = f"{self.path_raiz}/yaml/{self.bucket}/{self.datamart}/"
            arquivos_yaml = glob.glob(os.path.join(diretorio,'*.yaml'))
            self.logger.info(f"Lista de yaml carregada com sucesso")
            return list(sorted(arquivos_yaml))
        except Exception as e:
            self.logger.error("Erro ao listar arquivos yaml")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise        

    def carrega_arquivo_yaml(self,arquivo):
        """
        Carrega um arquivo yaml especifico.
        Args:
        arquivo: Nome do arquivo a ser carregado
        """
        try:
            diretorio = f"{self.path_raiz}/yaml/{self.bucket}/{self.datamart}/"
            arquivos_yaml = glob.glob(os.path.join(diretorio,arquivo+'.yaml'))
            self.logger.info(f"Arquivo yaml carregada com sucesso")
            self.logger.info(arquivos_yaml)
            self.inicializar_variaveis(arquivos_yaml[0])
            return list(sorted(arquivos_yaml))
        except Exception as e:
            self.logger.error("Erro ao carregar arquivo yaml")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise
    


    def carregar_dados_delta(self, sql_query: str):
        """
        Carrega dados do banco de dados PostgreSQL usando uma query SQL.
        Args:
            sql_query (str): Consulta SQL para carregar os dados.
        Returns:
            DataFrame: Dados carregados em um DataFrame do Spark.
        """
        try:
            if sql_query == "" or sql_query is None:
                sql_query = self.leitor_yaml.busca_chave_sessao_valor("delta","query")            
            self.logger.info("Aguarde... Executando Query (Delta)")
            self.logger.info(sql_query)
            df = self.spark.sql(sql_query)
            df = df.withColumn("dt_atualizacao", current_timestamp())
            self.logger.info("Query (Delta) executada com sucesso")
            return df
        except Exception as e:
            self.logger.error("Erro ao executar Query (Delta)")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise
            
     
    
    def carregar_dados_database(self, sql_query: str):
        """
        Carrega dados do banco de dados PostgreSQL usando uma query SQL.

        Args:
            sql_query (str): Consulta SQL para carregar os dados.

        Returns:
            DataFrame: Dados carregados em um DataFrame do Spark.
        """
        try:  
            self.logger.info("Aguarde... Executando Query (Database)")
            self.logger.info(sql_query)
            df = self.spark.read.format('jdbc') \
                .option('url',self.jdbc_url) \
                .option('dbtable', sql_query ) \
                .option("user",self.username) \
                .option("password",self.password) \
                .option("driver",self.driver) \
                .load()
            df = df.withColumn("dt_atualizacao", current_timestamp())
            self.logger.info("Query (Database) executada com sucesso")
            return df
        except Exception as e:
            self.logger.error("Erro ao executar Query (Database)")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

    def prepara_tabela_delta(self,arquivo,pagina=0):
        """
        Prepara uma tabela delta, retornando um dataframe.
        Args:
            arquivo : Nome do arquivo yaml, onde contem informações necessarias para o carregamento da tabela delta.
        """
        try:
            self.logger.info("Aguarde... Preparando tabela (Delta)")
            self.carrega_arquivo_yaml(arquivo)
            tabelas = self.leitor_yaml.busca_chave_valor('delta_table')
            for tabela in tabelas:
                _path = tabela['path']
                _name = tabela['name']
                self.criar_view_temporaria(_path,_name)
            _select_sql = self.leitor_yaml.busca_chave_sessao_valor("delta","query") + ' '
            if pagina > 0:
                posicao_from = _select_sql.find("from")
                nova_sql = _select_sql[:posicao_from] + ',ROW_NUMBER() OVER (ORDER BY 1) AS rn ' + _select_sql[posicao_from:]
                _select_sql = f" WITH paginacao AS ({nova_sql}) SELECT * FROM paginacao WHERE rn > {pagina} AND rn <= {pagina + self.chunksize}"
            df_dados = self.carregar_dados_delta(_select_sql)
            self.logger.info(f"Tabela (Delta) preparada com sucesso.")
            return df_dados
        except Exception as e:
            self.logger.error("Erro ao preparar tabela (Delta)")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise   

    def atualizar_tabela_delta(self,arquivo):
        """
        Carrega uma tabela delta, retornando um dataframe.

        Args:
            arquivo : Nome do arquivo yaml, onde contem informações necessarias para o carregamento da tabela delta.
        """
        try:
            self.logger.info("Aguarde... Atualizando tabela (Delta)")

            consulta = self.carrega_arquivo_yaml(arquivo) 
            self.inicializar_variaveis(consulta[0])
            extraiu_dados = False
            if self.chunksize > 0:
                contador = 1
                pagina = 1
                total_registros = 0
                self.__deletar_registros()                        
                mode = 'append'  
                while contador >= 1:     
                    self.logger.info('')
                    self.logger.info(f"{str(contador).zfill(3)}-{self.tablename}")
                    
                    df = self.prepara_tabela_delta(arquivo,pagina)
                    
                    df_count = df.count()
                    if df is not None:
                        extraiu_dados = True
                        if df_count <= 0:
                            contador = -1        
                        else:
                            self.salvar_delta(df,mode)
                            total_registros += df_count
                            pagina += self.chunksize
                            if df_count < self.chunksize:
                                contador = -1
                            else:
                                contador += 1                                
                    else:
                        contador = -1
            else:
                extraiu_dados = True
                df_dados = self.prepara_tabela_delta(arquivo)            
                self.salvar_delta(df_dados,'') ############### salvar_delta

            if extraiu_dados:
                self.logger.info("Tabela (Delta) atualizado com sucesso")   
            else:
                self.logger.info("Sem registros para ser atualizados")            
                         
        except Exception as e:
            self.logger.error("Erro ao atualizar tabela (Delta)")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise   

    def carrega_dados_delta(self,arquivo):
        """
        Persiste os dados na camada consume.
        Args:
        nome : Nome da tabela a ser criada na camada consume.
        """
        try:
            consulta = self.carrega_arquivo_yaml(arquivo) 
            self.inicializar_variaveis(consulta[0])
            
            self.logger.info("Aguarde... Carregando dados")            
            df_delta = self.spark.read.format("delta").load(self.caminho_tabela_delta)
            self.logger.info("Dados carregado com sucesso")
            return df_delta
        except Exception as e:
            self.logger.error("Erro ao carregar dados.")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise    

    def persistencia_consume(self,nm_tabela=None):
        """
        Persiste os dados na camada consume.
        Args:
        nome : Nome da tabela a ser criada na camada consume.
        """
        try:
            if nm_tabela == '' or nm_tabela is None:
                nm_tabela = self.leitor_yaml.busca_chave_sessao_valor('delta','tablename')
            self.logger.info("Aguarde... Persistindo os dados camada Consume")            
            df_delta = self.spark.read.format("delta").load(self.caminho_tabela_delta)
            nome_tabela_consume = self.schema + '.'+ nm_tabela
            df_delta.write.jdbc(url=self.jdbc_url, table=nome_tabela_consume, mode="overwrite", properties=self.properties)            
            self.logger.info("Camada Consume persistida com sucesso")
            self.logger.info(f"{nome_tabela_consume}")
        except Exception as e:
            self.logger.error("Erro ao persistir dados na camada Consume.")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise
    
    def criar_view_temporaria(self,caminho,nome):
        """
        Cria uma view temporaria.

        Args:
            caminho (str): Caminho da tabela delta.
            nome (int): Nome da view delta a ser criada.
        """
        try:
            self.logger.info(f"Aguarde... Criando View ({caminho})")
            _path = f"s3a://{caminho}"
            df = self.spark.read.format("delta").load(_path)
            df.createOrReplaceTempView(nome)
            self.logger.info("View criada com sucesso")
            return df
        except Exception as e:
            self.logger.error("Erro ao criar view")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise    

    def mostrar_dados(self, df, linhas: int = 20):
        """
        Exibe os dados de um DataFrame.

        Args:
            df (DataFrame): DataFrame cujos dados serão exibidos.
            linhas (int): Número de linhas a exibir. O padrão é 20.
        """
        try:
            self.logger.info("Aguarde... Carregando dataframe")
            df.show(linhas,truncate=False)
            self.logger.info(f"Mostrando {linhas} linhas de dados.")
        except Exception as e:
            self.logger.error("Erro ao exibir dataframe")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

    # def salvar_delta(self, df, mode):
    #     """
    #     Salva o DataFrame no formato Delta.

    #     Args:
    #         df: DataFrame a ser salvo no formato Delta.
    #         mode: Mode de escrita do DataFrame.            
    #     """
    #     try:            
    #         if mode == "" or mode is None:
    #             mode = self.leitor_yaml.busca_chave_sessao_valor("sink","mode")
    #         self.logger.info(f"Aguarde... Persistindo dados ({mode})")
    #         df.write.format("delta").mode(mode).option("mergeSchema", "true").save(self.caminho_tabela_delta)
    #         self.logger.info("Dados persistidos com sucesso")
    #         self.logger.info(f"{self.caminho_tabela_delta}")
    #         self.executar_optimize()
    #         self.executar_vacuum()
    #     except Exception as e:
    #         self.logger.error(f"Erro ao persistir dados ({mode})")
    #         self.logger.error(f"{e}")
    #         self.parar_sessao()
    #         raise

    #####################
    def salvar_delta(self, df, mode):
        """
        Salva o DataFrame no formato Delta.
    
        Args:
            df: DataFrame a ser salvo no formato Delta.
            mode: Mode de escrita do DataFrame. Se None, usa o valor do YAML.            
        """
        try:
            # Se o modo não for fornecido, busque do YAML
            if mode is None or mode == "":
                mode = self.leitor_yaml.busca_chave_sessao_valor("sink", "mode")
    
            if mode == "upsert" and self.__tabela_existe():
                self.logger.info(f"Aguarde... Persistindo dados ({mode})")
                self.realizar_merge(df)
            else:
                if mode == "upsert":
                    mode = 'overwrite'  # Se for upsert e a tabela não existe, mudar para overwrite
    
                self.logger.info(f"Aguarde... Persistindo dados ({mode})")
                df.write.format("delta").mode(mode).option("mergeSchema", "true").save(self.caminho_tabela_delta)
                self.logger.info("Dados persistidos com sucesso")
                self.logger.info(f"{self.caminho_tabela_delta}")
                self.executar_optimize()
                self.executar_vacuum()
    
        except Exception as e:
            self.logger.error(f"Erro ao persistir dados ({mode})")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise


    ###################

    def realizar_merge(self, df_novo):
        """
        Realiza o merge entre um DataFrame existente e um novo DataFrame.
    
        Args:
            df_novo (DataFrame): Novo DataFrame com os dados a serem mesclados.
        """
        try:
            self.logger.info("Aguarde... Persistindo dados (Merge)")
            # Carregar a tabela Delta existente
            delta_table = DeltaTable.forPath(self.spark, self.caminho_tabela_delta)      

            # Construir a condição de merge com base nas chaves primárias
            colunas = self.leitor_yaml.busca_chave_sessao_valor("sink","key")
            chaves_primarias = colunas.split(",")
            condition = " AND ".join([f"t.{key} = s.{key}" for key in chaves_primarias])

            df_novo.createOrReplaceTempView("df_view_novo_merge")
            
            # Construir as instruções de atualização e inserção
            colunas_df = df_novo.columns
            update_set = ", ".join([f"t.{col} = s.{col}" for col in colunas_df if col not in chaves_primarias])
            update_columns = " OR ".join([f"t.{col} != s.{col}" for col in colunas_df if col not in chaves_primarias and col != "dt_atualizacao"])
            insert_columns = ", ".join(colunas_df)
            insert_values = ", ".join([f"s.{col}" for col in colunas_df])
   
            # Gerar o SQL para o merge
            sql_merge = f"""
            MERGE INTO delta.`{self.caminho_tabela_delta}` t
            USING (SELECT * FROM df_view_novo_merge) s
            ON {condition}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values})
            """

            # Gerar o SQL para o merge (NOVO) - ALTERA SOMENTE COLUNAS MODIFICADAS
            # sql_merge = f"""
            # MERGE INTO delta.`{self.caminho_tabela_delta}` t
            # USING (SELECT * FROM df_view_novo_merge) s
            # ON {condition}
            # WHEN MATCHED AND {update_columns} THEN
            #     UPDATE SET {update_set}
            # WHEN NOT MATCHED THEN
            #     INSERT ({insert_columns})
            #     VALUES ({insert_values})
            # """
            
            df_novo.sparkSession.sql(sql_merge)
            self.logger.info("Dados persistidos (Merge) com sucesso")
            self.logger.info(f"{self.caminho_tabela_delta}")
            self.executar_optimize()
            self.executar_vacuum()
        except Exception as e:
            self.logger.error("Erro ao realizar merge")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

    def remove_registro_duplicado(self):
        """
        Remove registros duplicados de uma tabela Delta e persiste os dados de volta na tabela.
        """
        try:
            self.logger.info("Aguarde... removendo duplicados")
            delta_table = DeltaTable.forPath(self.spark, self.caminho_tabela_delta)       
            df = delta_table.toDF()
    
            colunas = self.leitor_yaml.busca_chave_sessao_valor("sink","key")
            if isinstance(colunas, str):
                colunas = [colunas]            
            # Remover duplicatas mantendo o primeiro registro de cada grupo
            df_deduplicated = df.dropDuplicates(subset=colunas)
            #df_deduplicated = df.repartition(200).dropDuplicates(subset=colunas)
            # Sobrescrever a tabela Delta com os dados sem duplicatas
            df_deduplicated.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(self.caminho_tabela_delta)
            self.logger.info("Registros duplicados removidos")
        except Exception as e:
            self.logger.error("Erro ao Remove registros duplicados de uma tabela Delta")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise
    
    def historico_tabela(self):
        """
        Exibe o histórico de operações da tabela Delta.
        """
        try:
            delta_table = DeltaTable.forPath(self.spark, self.caminho_tabela_delta)
            delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
            self.logger.info("Histórico da tabela Delta exibido.")
        except Exception as e:
            self.logger.error("Erro ao exibir histórico da tabela Delta")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

    def __tabela_existe(self):
        """
        Verifica se a tabela Delta existe no caminho especificado.

        Returns:
            bool: True se a tabela existe, False caso contrário.
        """
        try:            
            DeltaTable.forPath(self.spark, self.caminho_tabela_delta)
            return True
        except AnalysisException:
            return False    

    def __deletar_registros(self):
        """s
        Deleta todos os registros da tabela Delta.
        """
        try:
            self.logger.info("Aguarde... Excluindo registros")
            if self.__tabela_existe():
                delta_table = DeltaTable.forPath(self.spark, self.caminho_tabela_delta)
                delta_table.delete("true")
            self.logger.info("Registros excluidos com sucesso")
        except Exception as e:
            self.delete_path_minio()
            #self.logger.error("Erro ao deletar registros")
            #self.logger.error(f"{e}")
            #self.parar_sessao()
            #raise

    def deletar_registros_SQL(self, condicao: str):
        """
        Deleta registros de uma tabela Delta com base em uma condição SQL.
        Args:
            condicao (str): Condição SQL para filtrar os registros a serem deletados.
                            Exemplo: "id = 10"
        """
        try:
            self.logger.info("Aguarde... Excluindo registros (SQL)")
            self.logger.info(condicao)            
            if self.__tabela_existe():
                self.spark.read.format("delta").load(self.caminho_tabela_delta).createOrReplaceTempView("tabela_temp")
                sql_query = f"DELETE FROM tabela_temp {condicao}"
                self.spark.sql(sql_query)
            self.logger.info("Registros deletados com a condição")
            self.logger.info(f"{condicao}")
        except Exception as e:
            print("Erro ao deletar registros")
            self.logger.error(f"{e}")
            raise
    
    def __restaurar_versao(self, versao: int):
        """
        Restaura a tabela Delta para uma versão específica.

        Args:
            versao (int): Número da versão a ser restaurada.
        """
        try:
            df_versao = self.spark.read.format("delta").option("versionAsOf", versao).load(self.caminho_tabela_delta)
            self.mostrar_dados(df_versao)
            self.logger.info(f"Tabela restaurada para a versão {versao}.")
        except Exception as e:
            self.logger.error("Erro ao restaurar versão da tabela Delta")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

    def executar_vacuum(self):
        """
        Executa o vacuum na tabela Delta para remover dados antigos.
        """
        try:
            self.logger.info("Aguarde... Realizando vacuum")
            self.spark.sql(f" VACUUM '{self.caminho_tabela_delta}' RETAIN 1 HOURS")
            self.logger.info("Vacuum executado com sucesso.")
        except Exception as e:
            self.logger.error("Erro ao executar vacuum")
            self.logger.error(f"{e}")
            #self.parar_sessao()
            raise

    def executar_checkpoint(self):
        """
        Executa o vacuum na tabela Delta para remover dados antigos.
        """
        try:
            self.logger.info("Aguarde... Realizando checkpoint")            
            delta_table = DeltaTable.forPath(self.spark, self.caminho_tabela_delta)
            delta_table.checkpoint()
            self.logger.info("Checkpoint executado com sucesso.")
        except Exception as e:
            self.logger.error("Erro ao executar vacuum")
            self.logger.error(f"{e}")
            #self.parar_sessao()
            raise    

    def executar_optimize(self):
        """
        Executa o optimize na tabela Delta.
        """
        try:
            self.logger.info("Aguarde... Realizando optimize")
            self.spark.sql(f" OPTIMIZE '{self.caminho_tabela_delta}' ")
            self.logger.info("Optimize executado com sucesso.")
        except Exception as e:
            self.logger.error("Erro ao executar optimize")
            self.logger.error(f"{e}")
            #self.parar_sessao()
            raise

    def executar_statistics(self):
        """
        Executa o statistics na tabela Delta.
        """
        try:
            self.logger.info("Aguarde... Realizando statistics")
            self.spark.sql(f" ANALYZE TABLE '{self.caminho_tabela_delta}' COMPUTE STATISTICS ")
            self.logger.info("Statistics executado com sucesso.")
        except Exception as e:
            self.logger.error("Erro ao executar optimize")
            self.logger.error(f"{e}")
            #self.parar_sessao()
            raise

    def executar_repair(self):
        """
        Executa o statistics na tabela Delta.
        """
        try:
            self.logger.info("Aguarde... Realizando repair")
            self.spark.sql(f" FSCK REPAIR TABLE '{self.caminho_tabela_delta}' ")
            self.logger.info("Repair executado com sucesso.")
        except Exception as e:
            self.logger.error("Erro ao executar optimize")
            self.logger.error(f"{e}")
            #self.parar_sessao()
            raise    
    
    def limpar_cache(self):
        """
        Limpa o cache do catálogo do Spark.
        """
        try:
            self.spark.catalog.clearCache()
            self.logger.info("Cache do catálogo Spark limpo.")
        except Exception as e:
            self.logger.error("Erro ao limpar cache")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

    def parar_sessao(self):
        """
        Finaliza a sessão Spark de forma segura.
        """
        try:
            self.spark.stop()
            self.logger.info("Sessão Spark finalizada.")
        except Exception as e:
            self.logger.error("Erro ao finalizar sessão Spark")

    def configurar_parametro_spark(self, chave: str, valor: str):
        """
        Configura um parâmetro adicional na sessão Spark.

        Args:
            chave (str): A chave do parâmetro de configuração.
            valor (str): O valor do parâmetro de configuração.
        """
        try:
            self.spark.conf.set(chave, valor)
            self.logger.info(f"Parâmetro configurado: {chave} = {valor}")
        except Exception as e:
            self.logger.error("Erro ao configurar o parâmetro {chave}")
            self.logger.error(f"{e}")
            self.parar_sessao()
            raise

    def delete_path_minio(self):
        try:
            url = os.getenv("MINIO_HOST").replace("http://", "")            
            minio_client = Minio(url,
                access_key= os.getenv("MINIO_ACCESS_KEY"),
                secret_key= os.getenv("MINIO_SECRET_KEY"),
                secure=False  # Use True se estiver usando HTTPS
            )
            minio_client.remove_object(self.lakehouse,self.datalakepath)
        except S3Error as e:
            self.logger.info(f"error delete_path_minio: {e}")

    def replace_string_chars(self,df, chars_to_replace, replacement=''):
        """
        Função para tratar colunas de string
        """
        try:
            for column in df.columns:
                if isinstance(df.schema[column].dataType, StringType):
                    for char in chars_to_replace:
                        df = df.withColumn(column, regexp_replace(col(column), char, replacement))
            return df
        except Exception as e:
            self.logger.info(f"error replace_string_chars: {e}")      
    
    def replace_null_dates(self,df, default_date='1970-01-01'):
        """
        Função para tratar colunas de data
        """
        try:
            for column in df.columns:
                if isinstance(df.schema[column].dataType, DateType):
                    df = df.withColumn(column, coalesce(col(column), lit(default_date).cast(DateType())))
            return df                    
        except Exception as e:
            self.logger.info(f"error replace_string_chars: {e}")
    
    def data_quality_df(self,df, chars_to_replace=['\x00'], string_replacement='', default_date='1970-01-01'):
        """
        Função principal que aplica ambos os tratamentos
        Lista de caracteres a serem substituídos chars_list = ['\x00', '\n', '\r', '\t'] Nulo, quebra de linha,enter,tab 
        """
        try:
            df = self.replace_string_chars(df, chars_to_replace, string_replacement)
            df = self.replace_null_dates(df, default_date)
            return df
        except Exception as e:
            self.logger.info(f"error data_quality_df: {e}")     
