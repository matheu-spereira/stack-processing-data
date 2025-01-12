import os
import requests
from pyspark.sql.functions import lit
from datetime import datetime,timedelta
import time
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error

from pyspark.sql.types import StructType, StructField, StringType

#Cria sessão Spark
spark = SparkSession.builder \
        .appName(f"STG to Bronze") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.571") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_HOST')) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate()



B_TOKEN = "Bearer 48152c5b-3d82-3cba-a836-931698063f44"


# Processo para tratar possíveis dicionários presentes dentro das colunas, caso exista @nill ele retorna o valor correspondente de @nill.
def process_row(row):
    if 'numeroAtendimento' in row and isinstance(row['numeroAtendimento'], dict) and '@nil' in row['numeroAtendimento']:
        row['numeroAtendimento'] = row['numeroAtendimento']['@nil']
    if 'numeroDeFuncionarios' in row and isinstance(row['numeroDeFuncionarios'], dict) and '@nil' in row['numeroDeFuncionarios']:
        row['numeroDeFuncionarios'] = row['numeroDeFuncionarios']['@nil']
    if 'isFonteFomento' in row and isinstance(row['isFonteFomento'], dict) and '@nil' in row['isFonteFomento']:
        row['isFonteFomento'] = row['isFonteFomento']['@nil']
    if 'incricaoEstadual' in row and isinstance(row['incricaoEstadual'], dict) and '@nil' in row['incricaoEstadual']:
        row['incricaoEstadual'] = row['incricaoEstadual']['@nil']
    if 'idPorteCliente' in row and isinstance(row['idPorteCliente'], dict) and '@nil' in row['idPorteCliente']:
        row['idPorteCliente'] = row['idPorteCliente']['@nil']
    if 'porteCliente' in row and isinstance(row['porteCliente'], dict) and '@nil' in row['porteCliente']:
        row['porteCliente'] = row['porteCliente']['@nil']
    if 'nomeColaborador' in row and isinstance(row['nomeColaborador'], dict) and '@nil' in row['nomeColaborador']:
        row['nomeColaborador'] = row['nomeColaborador']['@nil']
    if 'nomeFontePagadora' in row and isinstance(row['nomeFontePagadora'], dict) and '@nil' in row['nomeFontePagadora']:
        row['nomeFontePagadora'] = row['nomeFontePagadora']['@nil']
    if 'valorReceitaRealizada' in row and isinstance(row['valorReceitaRealizada'], dict) and '@nil' in row['valorReceitaRealizada']:
        row['valorReceitaRealizada'] = row['valorReceitaRealizada']['@nil']
    return row


def define_df(data, table):
    try:    
        if(table == "sti_relatorios_sgt"):
            
            schema_sti_relatorios_sgt = StructType([
            StructField("id", StringType(), nullable=True),
            StructField("idAtendimento", StringType(), nullable=True),
            StructField("numeroAtendimento", StringType(), nullable=True),
            StructField("tituloAtendimento", StringType(), nullable=True),
            StructField("dia", StringType(), nullable=True),
            StructField("mes", StringType(), nullable=True),
            StructField("ano", StringType(), nullable=True),
            StructField("descricaoProdutoRegional", StringType(), nullable=True),
            StructField("CRProdutoRegional", StringType(), nullable=True),
            StructField("descricaoProdutoNacional", StringType(), nullable=True),
            StructField("descricaoProdutoCategoria", StringType(), nullable=True),
            StructField("descricaoProdutoLinha", StringType(), nullable=True),
            StructField("nomeColaborador", StringType(), nullable=True),
            StructField("emailColaborador", StringType(), nullable=True),
            StructField("cpfColaborador", StringType(), nullable=True),
            StructField("cpfCnpjCliente", StringType(), nullable=True),
            StructField("nomeCliente", StringType(), nullable=True),
            StructField("idPorteCliente", StringType(), nullable=True),
            StructField("porteCliente", StringType(), nullable=True),
            StructField("isEmRede", StringType(), nullable=True),
            StructField("isCompartilhado", StringType(), nullable=True),
            StructField("relatoriosRealizados", StringType(), nullable=True),
            StructField("ensaiosRealizados", StringType(), nullable=True),
            StructField("unidadeOperacional", StringType(), nullable=True)
            ])

            data = data['Producoes']['Producao']
            processed_data = map(process_row, data)
            df_result = spark.createDataFrame(processed_data, schema=schema_sti_relatorios_sgt)
            
        elif(table == "sti_producaohh_sgt"):

            schema_sti_producaohh_sgt = StructType([
            StructField("id", StringType(), nullable=True),
            StructField("idAtendimento", StringType(), nullable=True),
            StructField("numeroAtendimento", StringType(), nullable=True),
            StructField("tituloAtendimento", StringType(), nullable=True),
            StructField("dia", StringType(), nullable=True),
            StructField("mes", StringType(), nullable=True),
            StructField("ano", StringType(), nullable=True),
            StructField("descricaoProdutoRegional", StringType(), nullable=True),
            StructField("CRProdutoRegional", StringType(), nullable=True),
            StructField("descricaoProdutoNacional", StringType(), nullable=True),
            StructField("descricaoProdutoCategoria", StringType(), nullable=True),
            StructField("descricaoProdutoLinha", StringType(), nullable=True),
            StructField("nomeColaborador", StringType(), nullable=True),
            StructField("emailColaborador", StringType(), nullable=True),
            StructField("cpfColaborador", StringType(), nullable=True),
            StructField("cpfCnpjCliente", StringType(), nullable=True),
            StructField("nomeCliente", StringType(), nullable=True),
            StructField("idPorteCliente", StringType(), nullable=True),
            StructField("porteCliente", StringType(), nullable=True),
            StructField("isEmRede", StringType(), nullable=True),
            StructField("isCompartilhado", StringType(), nullable=True),
            StructField("producaoRealizada", StringType(), nullable=True),
            StructField("unidadeOperacional", StringType(), nullable=True)
            ])
            
            data = data['Producoes']['Producao']
            processed_data = map(process_row, data)
            df_result = spark.createDataFrame(processed_data, schema=schema_sti_producaohh_sgt)
        
        elif(table == "sti_receitas_sgt"):

            schema_sti_receitas_sgt = StructType([
            StructField("id", StringType(), nullable=True),
            StructField("idAtendimento", StringType(), nullable=True),
            StructField("numeroAtendimento", StringType(), nullable=True),
            StructField("tituloAtendimento", StringType(), nullable=True),
            StructField("dia", StringType(), nullable=True),
            StructField("mes", StringType(), nullable=True),
            StructField("ano", StringType(), nullable=True),
            StructField("descricaoProdutoRegional", StringType(), nullable=True),
            StructField("CRProdutoRegional", StringType(), nullable=True),
            StructField("descricaoProdutoNacional", StringType(), nullable=True),
            StructField("descricaoProdutoCategoria", StringType(), nullable=True),
            StructField("descricaoProdutoLinha", StringType(), nullable=True),
            StructField("cpfCnpjCliente", StringType(), nullable=True),
            StructField("nomeCliente", StringType(), nullable=True),
            StructField("idPorteCliente", StringType(), nullable=True),
            StructField("porteCliente", StringType(), nullable=True),
            StructField("cnpjFontePagadora", StringType(), nullable=True),
            StructField("nomeFontePagadora", StringType(), nullable=True),
            StructField("isEmRede", StringType(), nullable=True),
            StructField("isCompartilhado", StringType(), nullable=True),
            StructField("valorReceitaRealizada", StringType(), nullable=True),
            StructField("unidadeOperacional", StringType(), nullable=True)
            ])

            data = data['Receitas']['Receita']
            processed_data = map(process_row, data)
            df_result = spark.createDataFrame(processed_data, schema=schema_sti_receitas_sgt)
        
        elif(table == "sti_atendimentos_sgt"): 
            
            schema_sti_atendimentos_sgt = StructType([
            StructField("id", StringType(), True),
            StructField("idAtendimento", StringType(), True),
            StructField("numeroAtendimento", StringType(), True),
            StructField("tituloAtendimento", StringType(), True),
            StructField("descricaoStatusAtendimento", StringType(), True),
            StructField("dia", StringType(), True),
            StructField("mes", StringType(), True),
            StructField("ano", StringType(), True),
            StructField("cpfCnpjCliente", StringType(), True),
            StructField("nomeCliente", StringType(), True),
            StructField("idPorteCliente", StringType(), True),
            StructField("porteCliente", StringType(), True),
            StructField("numeroDeFuncionarios", StringType(), True),
            StructField("descricaoProdutoRegional", StringType(), True),
            StructField("CRProdutoRegional", StringType(), True),
            StructField("codigoDNProdutoNacional", StringType(), True),
            StructField("descricaoProdutoNacional", StringType(), True),
            StructField("codigoDNProdutoCategoria", StringType(), True),
            StructField("descricaoProdutoCategoria", StringType(), True),
            StructField("codigoDNProdutoLinha", StringType(), True),
            StructField("descricaoProdutoLinha", StringType(), True),
            StructField("codigoOBAUnidadeOperacional", StringType(), True),
            StructField("descricaoUnidadeOperacional", StringType(), True),
            StructField("numeroDeProducaoEstimada", StringType(), True),
            StructField("numeroDeRelatorioEstimado", StringType(), True),
            StructField("numeroDeReceitaEstimada", StringType(), True),
            StructField("vlrFinanceiro", StringType(), True),
            StructField("vlrEconomico", StringType(), True),
            StructField("isCompartilhado", StringType(), True),
            StructField("isEmRede", StringType(), True),
            StructField("isEscopoIndefinido", StringType(), True),
            StructField("isValorHora", StringType(), True)
            ])

            data = data['Atendimentos']['Atendimento']
            processed_data = map(process_row, data)
            df_result = spark.createDataFrame(processed_data, schema=schema_sti_atendimentos_sgt)
            
        elif(table == "sti_clientes_sgt"):
            
            schema_sti_clientes_sgt = StructType([
            StructField("id", StringType(), nullable=True),
            StructField("cpfCnpj", StringType(), nullable=True),
            StructField("cnae", StringType(), nullable=True),
            StructField("incricaoEstadual", StringType(), nullable=True),
            StructField("isAtivo", StringType(), nullable=True),
            StructField("isFonteFomento", StringType(), nullable=True),
            StructField("isUnidadeSenai", StringType(), nullable=True),
            StructField("nome", StringType(), nullable=True),
            StructField("razaoSocial", StringType(), nullable=True),
            StructField("numeroDeFuncionarios", StringType(), nullable=True),
            StructField("tipoPessoa", StringType(), nullable=True),
            StructField("idPorteCliente", StringType(), nullable=True),
            StructField("porteCliente", StringType(), nullable=True),
            StructField("tipoEndereco", StringType(), nullable=True),
            StructField("cep", StringType(), nullable=True),
            StructField("logradouro", StringType(), nullable=True),
            StructField("numero", StringType(), nullable=True),
            StructField("bairro", StringType(), nullable=True),
            StructField("complemento", StringType(), nullable=True),
            StructField("descricaoMunicipio", StringType(), nullable=True),
            StructField("UF", StringType(), nullable=True),
            StructField("nomeContato", StringType(), nullable=True),
            StructField("emailContato", StringType(), nullable=True),
            StructField("telefoneContato", StringType(), nullable=True)
            ])

            data = data['Clientes']['Cliente']
            processed_data = map(process_row, data)
            df_result = spark.createDataFrame(processed_data, schema=schema_sti_clientes_sgt)
        
        return df_result 
    except Exception as e:
        print(f"Falha na carga da tabela {table}: {e}")
    finally:
        pass

## Função para criar o DataFrame 
def create_df(url, date, table):
    querystring = {"dataInicio": date}
    payload = ""
    headers = {
        "accept": "application/json",
        "authorization": B_TOKEN
    }

    # Número máximo de tentativas
    max_attempts = 5

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring, verify=False)
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = define_df(data, table)
                    if not df.isEmpty():
                        current_date = datetime.now().strftime('%Y-%m-%d')
                        df = df.withColumn('date_partition', lit(datetime.strptime(current_date, '%Y-%m-%d')))
                        print(f"DataFrame criado para {table} na data {current_date}")
                        return df
                    else:
                        print(f"Nenhum dado retornado ou DataFrame vazio para a data {date}. Ignorando.")
                else:
                    print(f"Nenhum dado retornado para a data {date}. Ignorando.")
            elif response.status_code in [500, 502, 505, 504]:
                if attempt < max_attempts:
                    print(f"Erro {response.status_code}. Tentando novamente em 10 segundos...")
                    time.sleep(10)
                    continue
                else:
                    print(f"Erro {response.status_code}. Número máximo de tentativas excedido.")
                    return None
            else:
                print(f"Falha na conexão com a API, código do erro: {response.status_code}")
                return None
        except Exception as e:
            print(f"Erro ao tentar acessar a API: {e}")
            return None

def insert_datalake(df, tabela):

    minio_client = Minio(
        os.getenv('MINIO_HOST'),
        access_key = os.getenv('MINIO_ACCESS_KEY'),
        secret_key = os.getenv('MINIO_SECRET_KEY'),
        secure = False
    )

    # Camada (bucket) do MinIO para qual os dados extraídos serão enviados
    bucket_name = "bronze"
    # Origem dos dados
    source_system = 'sti'
    # Data Mart do DW ao qual esse notebook pertence
    business_area = "apisgt"

    #if df == None:
    #    print("Não existe arquivo para ser processado na data de hoje")
    #    pass
    #else:  

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

