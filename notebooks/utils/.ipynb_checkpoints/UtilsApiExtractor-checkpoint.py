import os
from datetime import datetime, timedelta
from typing import List, Optional, Tuple
import pandas as pd
import minio
from tqdm import tqdm
import requests
from minio.error import S3Error
import time
import json



"""
Classe: ApiExtractor

Descrição:
    Classe responsável por extrair dados de APIs.

Autor:
    Autor: Matheus Oliveira Mendes Pereira
    E-mail: matheus.mendes@sc.senai.br
    Data de Criação: 23/09/2024

Histórico de Alterações:
    - Data da Alteração: 
      Autor da Alteração: 
      Descrição da Alteração:   

Observações:    
    
"""


class ApiExtractor:
    def __init__(self, caravel_host: str, caravel_user: str, caravel_password: str, tag: str, spark, included_endpoint: Optional[str] = None, pagination: int = 1000):
        self.host = caravel_host
        self.caravel_user = caravel_user
        self.caravel_password = caravel_password
        self.bucket_path = "bronze"
        self.tag = tag
        self.source_system = "caravel"
        self.today = datetime.today().strftime("%Y%m%d")
        self.included_endpoint = included_endpoint
        self.pagination = pagination
        self.spark = spark  # Recebe a SparkSession como parâmetro

    def authenticate(self) -> str:
        auth_url = f"{self.host}/user/login"
        auth_data = {
            "username": self.caravel_user,
            "password": self.caravel_password
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(auth_url, json=auth_data, headers=headers, verify=True) #caravel

        if response.status_code != 200:
            raise Exception(f"Autenticação na API falhou! {response.text}")

        json_response = response.json()
        access_token = json_response.get('access_token')

        if not access_token:
            raise Exception("Falha ao obter o token de autenticação!")

        return access_token


    def get_endpoints(self) -> List[str]:
        url = f"{self.host}/openapi.json"
        response = requests.get(url, verify=True)

        if response.status_code != 200:
            raise Exception(f"Falha ao requisitar a lista de endpoints de openapi.json! {response.text}")

        json_response = response.json()
        paths = json_response.get('paths')

        if not paths:
            raise Exception("Falha ao obter a lista de endpoints!")

        default_endpoints = ('/role', '/user', '/groups', '/meta')
        endpoints = [endpoint for endpoint in paths.keys() if not endpoint.startswith(default_endpoints) and endpoint.startswith(f"/{self.tag}")]

        if self.included_endpoint:
            if self.included_endpoint in endpoints:
                return [self.included_endpoint]
            else:
                raise Exception(f"Endpoint incluído '{self.included_endpoint}' não encontrado na lista de endpoints disponíveis!")

        return endpoints

    def fetch_data_from_endpoint(self, endpoint: str) -> Tuple[bool, pd.DataFrame]:
        dataframe = pd.DataFrame()
        success = True

        params = {
            "limit": self.pagination,
            "offset": 0,
            "count": "true"
        }
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        url_data = f"{self.host}{endpoint}?" + "&".join([f"{key}={value}" for key, value in params.items()])

        print(f"\nEndpoint: {endpoint}")
        with tqdm(total=self.pagination, desc="Extração") as pbar:
            while url_data:
                pbar.update(self.pagination)
                response = requests.post(url_data, headers=headers, verify=True) #caravel

                if response.status_code != 200:
                    print(f"Falha ao requisitar endpoint! {response.text}")
                    success = False
                    break

                json_response = response.json()

                # Atualiza o total de páginas após a primeira requisição
                count = json_response.get('count')
                if count:
                    pbar.total = count

                url_data = json_response.get("next", "")
                new_df = pd.DataFrame(json_response.get('results', []))
                new_df = new_df.astype(str)
                dataframe = pd.concat([dataframe, new_df], ignore_index=True)
        return success, dataframe

        
    def extract_caravel(self) -> pd.DataFrame:
        self.access_token = self.authenticate()
        success, dataframe = self.fetch_data_from_endpoint(endpoint=self.included_endpoint)
        
        if success:
            print('Extração do endpoint finalizada!')
            dataframe = self.spark.createDataFrame(dataframe)
        return dataframe


    def fetch_data_url(self) -> pd.DataFrame:
        response = requests.get(self.host)
        response.raise_for_status() 

        data = response.json()
        # Crie o DataFrame Spark sem definir o schema
        dataframe = self.spark.read.json(self.spark.sparkContext.parallelize(data))
        return dataframe


    def extract_sgt(self, date, table, b_token):
        querystring = {"dataInicio": date}
        payload = ""
        headers = {
            "accept": "application/json",
            "authorization": b_token
        }

        url = f"{self.host}{self.included_endpoint}"
    
        # Número máximo de tentativas
        max_attempts = 5

        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.request("GET", url, data=payload, headers=headers, params=querystring, verify=True)
                if response.status_code == 200:
                    data = response.json()
                    main_key = next(iter(data))  # Pega a primeira chave do dicionário
                    sub_key = next(iter(data[main_key]))  # Pega a primeira chave do dicionário aninhado
                    data = data[main_key][sub_key] 
                    if data:
                        df = self.spark.read.json(self.spark.sparkContext.parallelize(data))
                        print(f"DataFrame criado.")
                        return df
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





    def requisicao_dynamics(self):
        all_data = []
        try:
            tenant_id = ''
            client_id = ''
            client_secret = ''
            resource = ''
            prefer = 'odata.include-annotations="OData.Community.Display.V1.FormattedValue"'
            
            # Endpoint para obter o token de acesso
            token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
            
            # Dados necessários para a solicitação do token
            token_data = {
                'grant_type': 'client_credentials',
                'client_id': client_id,
                'client_secret': client_secret,
                'resource': resource
            }
            
            # Solicitar o token de acesso
            token_response = requests.post(token_url, data=token_data)
            token_response_data = token_response.json()
            
            # Verificar se o token foi obtido com sucesso
            if 'access_token' in token_response_data:
                access_token = token_response_data['access_token']
                api_url = f"{self.host}{self.included_endpoint}"
                headers = {
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json',
                    "Prefer": prefer
                }
                
                # Loop para paginar através dos dados
                total_records = 0  # Contador de registros
                while api_url:
                    response = requests.get(api_url, headers=headers)
                    
                    # Verificar se a solicitação foi bem-sucedida
                    if response.status_code == 200:
                        response_json = response.json()
                        data = response_json['value']
                        all_data.extend(data)
                        
                        # Atualiza o contador e imprime o progresso
                        total_records += len(data)
                        print(f'Total de registros acumulados: {total_records}')

                        # Verifica se há mais páginas
                        api_url = response_json.get('@odata.nextLink')
                    else:
                        print(f"Falha na solicitação: {response.status_code} - {response.text}")
                        return None
            
            # Converter todos os dados acumulados em um DataFrame do Pandas
            df = pd.json_normalize(all_data)
            df = df.astype(str)
            df.columns = [col.replace('@OData.Community.Display.V1.FormattedValue', '_name') for col in df.columns]
            df = self.spark.createDataFrame(df)
            
            return df
        
        except Exception as e:
            print('Erro ao realizar a requisição:', e)
            return None
