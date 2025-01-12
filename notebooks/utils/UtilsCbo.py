import requests
import tempfile
import zipfile
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error


"""
Classe: CBOUploader

Descrição:
    Classe responsável por extrair dados de CBO do site do ministério do trabalho e emprego

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

class CBOUploader:
    def __init__(self, bucket_name, prefix, secure=False):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.secure = secure
        
        # Inicializa o cliente MinIO
        self.client = Minio(
            'minio:9000',
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
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

    def download_and_upload(self, url):
        # Cria um diretório temporário
        with tempfile.TemporaryDirectory() as temp_dir:
            # Baixa o arquivo
            response = requests.get(url)
            response.raise_for_status()
            
            zip_path = os.path.join(temp_dir, "estrutura-cbo.zip")
            
            # Salva o arquivo zip temporariamente
            with open(zip_path, "wb") as zip_file:
                zip_file.write(response.content)
            
            # Extrai o conteúdo do arquivo zip
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            
            # Deleta dados antigos
            self.delete_old_data()
            
            # Faz o upload dos arquivos extraídos para o bucket MinIO
            for file_name in os.listdir(temp_dir):
                file_path = os.path.join(temp_dir, file_name)
                
                if os.path.isfile(file_path):
                    object_name = f"{self.prefix}/{year_month}/{file_name}"  # Define o caminho no bucket
                    try:
                        with open(file_path, 'rb') as file_data:
                            self.client.put_object(self.bucket_name, object_name, file_data, os.path.getsize(file_path))
                        print(f"Upload bem-sucedido: {object_name}")
                    except S3Error as e:
                        print(f"Erro ao fazer upload: {e}")
