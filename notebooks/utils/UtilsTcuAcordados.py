import requests
import tempfile
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error

"""
Classe: TCUAUploader

Descrição:
    Classe responsável por extrair dados de acórdãos do site do TCU

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

year_month = datetime.today().strftime('%Y-%m')

class TCUAUploader:
    def __init__(self, bucket_name, prefix, secure=False):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.secure = secure
        
        # Inicializa o cliente MinIO
        self.client = Minio(
            'minio:9000',
            access_key = os.getenv("MINIO_ACCESS_KEY"),
            secret_key = os.getenv("MINIO_SECRET_KEY"),
            secure=self.secure
        )
        
        # Cria o bucket se não existir
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def delete_old_data(self):
        # Identifica o prefixo para pesquisa
        prefix = f"{self.prefix}/"
        
        try:
            # Tenta listar todos os objetos com o prefixo especificado
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            
            old_objects = []
            # Identifica objetos antigos
            for obj in objects:
                # Extrai o prefixo da pasta
                obj_prefix = os.path.dirname(obj.object_name)
                obj_year_month = obj_prefix.split('/')[-1]
                
                # Verifica se a pasta é antiga
                if obj_year_month != year_month:
                    old_objects.append(obj.object_name)
            
            # Remove objetos antigos, se houver algum
            if old_objects:
                for obj_name in old_objects:
                    try:
                        self.client.remove_object(self.bucket_name, obj_name)
                        print(f"Objeto removido: {obj_name}")
                    except S3Error as e:
                        print(f"Erro ao remover objeto: {e}")
        
        except S3Error as e:
            print(f"Erro ao listar objetos: {e}")

    def get_existing_files(self):
        prefix = f"{self.prefix}/{year_month}/"
        existing_files = set()
        
        try:
            objects = self.client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            for obj in objects:
                existing_files.add(os.path.basename(obj.object_name))
        except S3Error as e:
            print(f"Erro ao listar objetos: {e}")
        
        return existing_files

    def download_and_upload(self, base_url):
        # Cria um diretório temporário
        with tempfile.TemporaryDirectory() as temp_dir:
            ano_atual = datetime.now().year
            
            # Remove dados antigos
            self.delete_old_data()
            
            # Obtém os arquivos existentes no MinIO
            existing_files = self.get_existing_files()
            
            # Define os anos a serem baixados
            anos_para_baixar = set(range(1992, ano_atual + 1))
            arquivos_para_baixar = {f"acordao-completo-{ano}.csv" for ano in anos_para_baixar}
            
            # Arquivos faltantes
            arquivos_para_baixar -= existing_files
            
            # Inclui o arquivo do ano atual, mesmo que já esteja presente
            arquivos_para_baixar.add(f"acordao-completo-{ano_atual}.csv")
            
            # Loop para gerar URLs e baixar os arquivos
            for arquivo in sorted(arquivos_para_baixar):
                csv_url = f"{base_url}{arquivo}"
                csv_filename = os.path.join(temp_dir, arquivo)
                
                try:
                    # Faz a requisição para baixar o arquivo CSV
                    response = requests.get(csv_url)
                    response.raise_for_status()  # Verifica se a requisição foi bem-sucedida
                    
                    # Salva o arquivo CSV no diretório temporário
                    with open(csv_filename, 'wb') as file:
                        file.write(response.content)
                    
                    print(f"Arquivo {csv_filename} baixado com sucesso.")
                
                except requests.exceptions.HTTPError as http_err:
                    print(f"Ocorreu um erro HTTP para {arquivo}: {http_err}")
                except Exception as err:
                    print(f"Ocorreu um erro para {arquivo}: {err}")
                
                # Faz o upload dos arquivos CSV para o bucket MinIO
                if os.path.isfile(csv_filename):
                    object_name = f"{self.prefix}/{year_month}/{os.path.basename(csv_filename)}"
                    try:
                        with open(csv_filename, 'rb') as file_data:
                            self.client.put_object(self.bucket_name, object_name, file_data, os.path.getsize(csv_filename))
                        print(f"Upload bem-sucedido: {object_name}")
                    except S3Error as e:
                        print(f"Erro ao fazer upload: {e}")
