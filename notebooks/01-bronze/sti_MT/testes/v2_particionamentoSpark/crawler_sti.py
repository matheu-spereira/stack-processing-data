import os
import pandas as pd
import requests
from pyspark.sql.functions import lit
from datetime import datetime,timedelta
import time
from pyspark.sql import SparkSession
from minio import Minio


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
        # Finaliza a sessão Spark após a conclusão da função
        #spark.stop()
        pass

## Função para criar o DataFrame passando full ou datainicio e datafim
def create_df(url, date, table, op):
    
    if op == 'full':
        querystring = {"dataInicio": date}
    else:
        querystring = {"dataInicio": date, "dataFim": date}

    payload = ""
    headers = {
        "accept": "application/json",
        "authorization": B_TOKEN
    }

    # Número máximo de tentativas
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.request("GET", url, data=payload, headers=headers, params=querystring, verify=False)
            if response.status_code == 200:
                data = response.json()
                if data:
                    df = define_df(data, table)
                    if df is not None and not df.empty and op == 'full':
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
            elif response.status_code in [500, 502, 505,504]:
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
    today = datetime.today().strftime("%Y%m%d")
    # Camada (bucket) do MinIO para qual os dados extraídos serão enviados
    bucket_name = "bronze"
    # Data Mart do DW ao qual esse notebook pertence
    business_area = "apisgt"

    df = spark.createDataFrame(df)

    today = datetime.today().strftime("%Y%m%d")
    today_folder = datetime.today().strftime("%Y%m%d")

    df.write.mode('overwrite').parquet(f"s3a://{bucket_name}/sti/{business_area}/{tabela}/{today_folder}/{tabela}_{today_folder}.parquet")
    spark.stop()
