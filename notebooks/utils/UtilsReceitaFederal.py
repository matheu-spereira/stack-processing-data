from typing import List
from datetime import datetime
from minio import Minio
import os
import time
import requests
import zipfile
import tempfile
import re
import shutil
from bs4 import BeautifulSoup
import gc


class ReceitaFederalProcessor:
    def __init__(self, terms: List[str], bucket_name: str, business_area: str, url: str):
        self.terms = terms
        self.bucket_name = bucket_name
        self.business_area = business_area
        self.folder_path = f"{self.business_area}"
        self.zip_directory = tempfile.TemporaryDirectory()
        self.url = url
        self.minio_client = Minio(
            'minio:9000',
            access_key= os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )

    def get_list_directories(self, url: str) -> List[str]:
        try:
            response = requests.get(url=url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            print(f"Erro ao buscar {url}: {err}")
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        directories = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('/') and re.match(r'^\d{4}-\d{2}/$', href):
                directory = href.strip('/')
                directories.append(directory)

        return directories

    def get_list_files(self, url: str, search_term: str = None) -> List[str]:
        try:
            response = requests.get(url=url)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 404:
                print(f"404 Não Encontrado: {url}")
                return []
            else:
                raise

        pattern = r'(?<=<a href=")\w+\.zip(?=">)'
        files = re.findall(pattern=pattern, string=response.text)

        if search_term:
            search_term_lower = search_term.lower()
            files = [file for file in files if search_term_lower in file.lower()]
        return files

    def get_file(self, url: str, filename: str, path: str = None) -> None:
        response = requests.get(url + filename)
        response.raise_for_status()

        os.makedirs(name=path, exist_ok=True)

        file_path = f"{path}/{filename}" if path else filename
        
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"Baixado {filename} para {path}")

    def extract_zip(self, zip_path: str, extract_to: str) -> None:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
            print(f"Extraído {zip_path} para {extract_to}")

    def rename_and_upload_files(self, directory: str, bucket_name: str, folder_path: str, original_zip_name: str) -> None:
        # Remove a extensão .zip para gerar o novo nome do arquivo .csv
        base_name = os.path.splitext(original_zip_name)[0]
    
        for root, _, files in os.walk(directory):
            for file in files:
                # Renomear arquivos para garantir que tenham a extensão .csv
                if file.lower().endswith(".zip"):
                    continue  # Ignorar arquivos zipados
    
                new_file_name = f"{base_name}.csv"  # Renomear para .csv
                old_file_path = os.path.join(root, file)
                new_file_path = os.path.join(root, new_file_name)
    
                os.rename(old_file_path, new_file_path)
                print(f"Renomeado {old_file_path} para {new_file_path}")
    
                # Preparar e enviar o arquivo para o MinIO
                file_path = new_file_path
                print(f"Preparando para upload: {file_path}")
                
                try:
                    self.minio_client.fput_object(bucket_name, f"{folder_path}/{new_file_name}", file_path)
                    print(f"Uploaded {new_file_name} para o bucket MinIO {bucket_name}")
                except Exception as e:
                    print(f"Falha ao fazer upload de {new_file_name} para MinIO: {e}")


    def delete_old_files(self) -> None:
        latest_directory = self.get_latest_directory()
        try:
            all_objects = self.minio_client.list_objects(self.bucket_name, prefix=self.folder_path + '/', recursive=True)
            for obj in all_objects:
                if obj.object_name.startswith(f"{self.folder_path}/{latest_directory}/"):
                    continue  # Mantém arquivos do mês atual
                
                try:
                    self.minio_client.remove_object(self.bucket_name, obj.object_name)
                    print(f"Arquivo antigo {obj.object_name} deletado do bucket MinIO {self.bucket_name}")
                except Exception as e:
                    print(f"Falha ao deletar {obj.object_name} do MinIO: {e}")
        except Exception as e:
            print(f"Falha ao listar objetos no MinIO para deleção: {e}")


    def get_latest_directory(self) -> str:
        directories = self.get_list_directories(self.url)
        if directories:
            latest_directory = max(directories)
            return latest_directory
        return None

    def check_term_exists(self, term: str) -> bool:
        latest_directory = self.get_latest_directory()
        prefix = f"{self.folder_path}/{latest_directory}/{term}/"
        try:
            objects = self.minio_client.list_objects(self.bucket_name, prefix=prefix, recursive=True)
            return any(objects)  # Retorna True se houver pelo menos um objeto
        except Exception as e:
            print(f"Falha ao verificar existência do diretório no MinIO para o termo '{term}': {e}")
            return False

    def check_files_exist(self, term: str, expected_files: List[str]) -> List[str]:
        """
        Verifica quais arquivos esperados para o termo existem no MinIO.
        Retorna uma lista de arquivos que estão faltando.
        """
        latest_directory = self.get_latest_directory()
        prefix = f"{self.folder_path}/{latest_directory}/{term}/"
        missing_files = []
        try:
            existing_files = {obj.object_name.split('/')[-1] for obj in self.minio_client.list_objects(self.bucket_name, prefix=prefix, recursive=True)}
            missing_files = [file for file in expected_files if file not in existing_files]
        except Exception as e:
            print(f"Falha ao verificar a existência de arquivos no MinIO para o termo '{term}': {e}")
        
        return missing_files
    
    def process(self):
        # Verificar e baixar arquivos do diretório mais recente se necessário
        latest_directory = self.get_latest_directory()
        if latest_directory:
            print(f"Último diretório: {latest_directory}")
        else:
            print("Nenhum diretório encontrado.")
            return
    
        for term in self.terms:
            print(f"Processando termo: {term}")
    
            # Verifica quais arquivos disponíveis para o termo atual no diretório mais recente
            files = self.get_list_files(url=f"{self.url}{latest_directory}/", search_term=term)
    
            if not files:
                print(f"Nenhum arquivo encontrado para o termo '{term}' na URL {self.url}{latest_directory}/. Ignorando deleção de arquivos antigos.")
                continue
    
            # Cria a lista de arquivos CSV esperados
            expected_files = [f"{os.path.splitext(file)[0]}.csv" for file in files]
    
            # Verifica quais arquivos estão faltando
            missing_files = self.check_files_exist(term, expected_files)
    
            if not missing_files:
                print(f"Todos os arquivos para o termo '{term}' já existem no MinIO. Ignorando download e processamento.")
                continue
            
            # Deletar arquivos dos meses anteriores se o termo não existir
            self.delete_old_files()
    
            for filename in files:
                # Verifica se o arquivo correspondente já está no MinIO
                csv_filename = f"{os.path.splitext(filename)[0]}.csv"
                if csv_filename not in missing_files:  # Verifica se está faltando
                    print(f"{csv_filename} já existe no MinIO. Ignorando download do arquivo {filename}.")
                    continue
                
                inicio_file = time.time()
                
                # Caminho do arquivo ZIP
                zip_file_path = f"{self.zip_directory.name}/{term}/{filename}"
                
                # Baixar o arquivo ZIP
                self.get_file(url=f"{self.url}{latest_directory}/", filename=filename, path=f"{self.zip_directory.name}/{term}")
                
                # Diretório de extração para o arquivo ZIP atual
                extract_dir = tempfile.mkdtemp()
                
                # Extrair o arquivo ZIP
                self.extract_zip(zip_file_path, extract_dir)
                
                # Renomear e fazer upload dos arquivos
                self.rename_and_upload_files(extract_dir, self.bucket_name, f"{self.folder_path}/{latest_directory}/{term}", filename)
                
                shutil.rmtree(extract_dir)
                
                tempo_perc = time.strftime("%H:%M:%S", time.gmtime(time.time() - inicio_file))
                print(f' - "{filename}" - {tempo_perc}')
    
        # Limpar diretórios temporários
        self.zip_directory.cleanup()
        gc.collect()




