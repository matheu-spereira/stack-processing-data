from typing import List, Tuple
import os
import tempfile
import py7zr
from ftplib import FTP
import socket
from minio import Minio
from minio.error import S3Error


"""
Classe: CagedFileProcessor

Descrição:
    Classe responsável por extrair dados do repositório FTP CAGED.

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

class CagedFileProcessor:
    def __init__(self, ftp_host: str,
                 bucket_name: str, business_area: str, caged_cwd: str, timeout: int = 3600):
        self.ftp_host = ftp_host
        self.bucket_name = bucket_name
        self.business_area = business_area
        self.caged_cwd = caged_cwd
        self.timeout = timeout

        # Inicializa o cliente MinIO
        self.minio_client = Minio(
            'minio:9000',
            access_key= os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False
        )

        # Cria o bucket se não existir
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        if not self.minio_client.bucket_exists(self.bucket_name):
            self.minio_client.make_bucket(self.bucket_name)

    def list_files_in_directory(self, ftp: FTP, directory: str) -> List[str]:
        ftp.cwd(directory)
        return ftp.nlst()

    def find_mov_files(self, ftp: FTP, directory: str) -> List[str]:
        return [
            file
            for file in self.list_files_in_directory(ftp, directory)
            if file.lower().startswith('cagedmov') and file.lower().endswith('.7z')
        ]

    def download_ftp_file(self, ftp: FTP, file_path: str, temp_dir: str) -> str:
        file_path_out = os.path.join(temp_dir, os.path.basename(file_path))
        print(f"Baixando {file_path}...")
        with open(file_path_out, "wb") as fp:
            ftp.retrbinary(f"RETR {file_path}", fp.write)
        return file_path_out

    def upload_to_minio(self, file_path: str, object_name: str) -> None:
        try:
            with open(file_path, 'rb') as file_data:
                self.minio_client.put_object(self.bucket_name, object_name, file_data, os.path.getsize(file_path))
            print(f"Upload realizado com sucesso: {object_name}")
        except S3Error as e:
            print(f"Erro no upload: {e}")

    def extract_and_upload_files(self, file_path: str) -> None:
        try:
            extract_dir = os.path.dirname(file_path)

            # Extrai o arquivo .7z
            with py7zr.SevenZipFile(file_path, mode='r') as archive:
                archive.extractall(path=extract_dir)

            # Obtém a lista de arquivos extraídos e faz o upload
            self._upload_extracted_files(extract_dir)

        except Exception as e:
            print(f"Erro na extração: {e}")

    def _upload_extracted_files(self, extract_dir: str):
        for root, _, files in os.walk(extract_dir):
            for file in files:
                if not file.endswith('.7z'):
                    extracted_file_path = os.path.join(root, file)
                    object_name = f"{self.business_area}/{os.path.basename(extracted_file_path)}"
                    self.upload_to_minio(extracted_file_path, object_name)
                    print(f"Arquivo extraído enviado: {object_name}")
                    os.remove(extracted_file_path)  # Remove arquivo após o upload

    def list_existing_objects(self) -> set:
        existing_objects = set()
        try:
            objects = self.minio_client.list_objects(self.bucket_name, recursive=True)
            for obj in objects:
                existing_objects.add(obj.object_name)
        except S3Error as e:
            print(f"Erro ao listar objetos: {e}")
        return existing_objects

    def retrieve_caged_files_from_ftp(self) -> List[Tuple[str, str, str]]:
        socket.setdefaulttimeout(self.timeout)
        files = []

        try:
            with FTP(self.ftp_host, encoding="iso-8859-1") as ftp:
                ftp.login()
                years = [
                    year for year in self.list_files_in_directory(ftp, self.caged_cwd)
                    if year.isdigit() and int(year) >= 2023  # 2023
                ]

                for year in years:
                    year_directory = f"{self.caged_cwd}/{year}"
                    print(f"----- {year} -----")

                    for month in self.list_files_in_directory(ftp, year_directory):
                        month_directory = f"{year_directory}/{month}"
                        ftp.cwd(month_directory)
                        mov_files = self.find_mov_files(ftp, month_directory)

                        if mov_files:
                            filename = mov_files[0]
                            print(year, month, filename)
                            files.append((year, month, filename))

        except socket.timeout:
            print("A operação expirou após a duração especificada.")
        return files

    def process_files(self) -> None:
        existing_objects = self.list_existing_objects()

        if self.caged_cwd == '/pdet/microdados/NOVO CAGED/Layout Não-identificado Novo Caged Movimentação.xlsx':
            with tempfile.TemporaryDirectory() as temp_dir:
                with FTP(self.ftp_host, encoding="iso-8859-1") as ftp:
                    ftp.login()
                    downloaded_file_path = self.download_ftp_file(ftp, self.caged_cwd, temp_dir)
                    object_name = f"{self.business_area}/{os.path.basename(downloaded_file_path)}"
                    # Verifica se o objeto correspondente já existe no MinIO
                    if object_name not in existing_objects:
                        self.upload_to_minio(downloaded_file_path, object_name)
                        print(f"Arquivo processado: {self.caged_cwd}")
                    else:
                        print(f"Arquivo já existe no MinIO: {object_name}. Ignorando upload.")
            return

        # Caso padrão para processar arquivos encontrados
        files = self.retrieve_caged_files_from_ftp()
        if not files:
            print("Nenhum arquivo para processar.")
            return

        with tempfile.TemporaryDirectory() as temp_dir:
            for file in files:
                year, month, filename = file
                object_name = f"{self.business_area}/CAGEDMOV{month}.txt"

                # Verifica se o objeto correspondente já existe no MinIO
                if object_name in existing_objects:
                    print(f"O arquivo já existe no MinIO: {object_name}. Ignorando o download.")
                    continue

                # Mostra qual arquivo está prestes a ser baixado
                print(f"Preparando para download: {filename}")
                with FTP(self.ftp_host, encoding="iso-8859-1") as ftp:
                    ftp.login()
                    file_path = os.path.join(self.caged_cwd, year, month, filename)
                    downloaded_file_path = self.download_ftp_file(ftp, file_path, temp_dir)

                    # Extrai e faz o upload dos arquivos extraídos
                    self.extract_and_upload_files(downloaded_file_path)
                    print(f"Arquivo processado: {filename}")
