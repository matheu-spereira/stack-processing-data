import os
import pandas as pd
import requests
from pyspark.sql.functions import lit
from datetime import datetime,timedelta
import time
from pyspark.sql import SparkSession
from minio import Minio
from minio.error import S3Error

B_TOKEN = "Bearer 48152c5b-3d82-3cba-a836-931698063f44"

def define_df(data, table):
    try:    
        if(table == "sti_relatorios_sgt" or table == "sti_producaohh_sgt"):
            df = pd.DataFrame(data)
            df_result = pd.DataFrame(df['Producoes']['Producao'])

            #Ajustes de campos mal formatados no retorno da API
            df_result['numeroAtendimento'] = df_result['numeroAtendimento'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['idPorteCliente'] = df_result['idPorteCliente'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['porteCliente'] = df_result['porteCliente'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['nomeColaborador'] = df_result['nomeColaborador'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
        elif(table == "sti_receitas_sgt"):
            df = pd.DataFrame(data)
            df_result = pd.DataFrame(df['Receitas']['Receita'])

            #Ajustes de campos mal formatados no retorno da API
            df_result['nomeFontePagadora'] = df_result['nomeFontePagadora'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['numeroAtendimento'] = df_result['numeroAtendimento'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['valorReceitaRealizada'] = df_result['valorReceitaRealizada'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
        elif(table == "sti_atendimentos_sgt"):
            df = pd.DataFrame(data)
            df_result = pd.DataFrame(df['Atendimentos']['Atendimento'])

            #Ajustes de campos mal formatados no retorno da API
            df_result['numeroAtendimento'] = df_result['numeroAtendimento'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['numeroDeFuncionarios'] = df_result['numeroDeFuncionarios'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
        elif(table == "sti_clientes_sgt"):
            df = pd.DataFrame(data)
            df_result = pd.DataFrame(df['Clientes']['Cliente'])

            #Ajustes de campos mal formatados no retorno da API 
            df_result['isFonteFomento'] = df_result['isFonteFomento'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['numeroDeFuncionarios'] = df_result['numeroDeFuncionarios'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
            df_result['incricaoEstadual'] = df_result['incricaoEstadual'].apply(lambda x: x['@nil'] if isinstance(x, dict) and '@nil' in x else x)
        
        return df_result 
    except Exception as e:
        print(f"Falha na carga da tabela {table}: {e}")
    finally:
        pass

## Função para criar o DataFrame passando full ou datainicio e datafim
def create_df(url, date, table, op=True):
    #busca filtrando somente a data de inicio
    if op == True:
        querystring = {"dataInicio": date}
    #busca filtrando  a data de inicio e datafim
    else:
        querystring = {"dataInicio": date, "dataFim": date}

    payload = ""
    headers = {
        "accept": "application/json",
        "authorization": B_TOKEN
    }

    # Número máximo de tentativas
    max_attempts = 5

    # Função criada para caso o parametro em OP seja true ele crie a coluna date_partion com a data atual, posteriormente com o particionamento o usuário passa um parametro diferente e o script 
    # irá criar a coluna date_partition com a data informada na exececução, dessa forma teremos os dados particionados pelo parâmetro da requisição.
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring, verify=False)
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = define_df(data, table)
                    if df is not None and not df.empty and op == True:
                        current_date = datetime.now().strftime('%Y-%m-%d')
                        df['date_partition'] = datetime.strptime(current_date, '%Y-%m-%d')
                        print(f"DataFrame criado para {table} na data {current_date}")
                        return df
                    elif df is not None and not df.empty:
                        df['date_partition'] = datetime.strptime(date, '%Y%m%d')
                        print(f"DataFrame criado para {table} na data {date}")
                        return df
                    else:
                        print(f"Nenhum dado retornado ou DataFrame vazio para a data {date}. Ignorando.")
                        return pd.DataFrame(columns=['date_partition'])
                else:
                    print(f"Nenhum dado retornado para a data {date}. Retornando mensagem de DataFrame vazio.")
                    # Retorne um DataFrame vazio com as mesmas colunas que o DataFrame esperado
                    return pd.DataFrame(columns=['date_partition'])
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

    spark = SparkSession.builder \
            .appName(f"STG to Bronze") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.571") \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv('MINIO_HOST')) \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('MINIO_ACCESS_KEY')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('MINIO_SECRET_KEY')) \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").getOrCreate()
    
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

