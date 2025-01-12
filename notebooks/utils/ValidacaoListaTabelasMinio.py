from minio import Minio
from minio.error import S3Error
import os

class MinioMarkdownGenerator:
    """
    Classe para gerar um arquivo Markdown com subdiretórios de buckets no MinIO.

    A classe utiliza a biblioteca MinIO para interagir com um servidor S3 e
    lista subdiretórios em buckets especificados, gerando um relatório em Markdown
    que pode ser salvo em um arquivo.
    
    """
    def __init__(self):
        """
        Inicializa o cliente MinIO.
    
        Obtém as variáveis de ambiente para o host do MinIO, chave de acesso e chave secreta.
        """
        self.url = os.getenv("MINIO_HOST").replace("http://", "")
        self.url_api = 'minio:9001'
        self.client = Minio(self.url, 'minio', 'minio123', secure=False)

    def list_subdirectories(self, bucket_name, directory):
        """
        Lista subdiretórios de um bucket específico.
    
        Args:
            bucket_name (str): Nome do bucket.
            directory (str): Prefixo do diretório para listar subdiretórios.
    
        Returns:
            list: Lista de subdiretórios encontrados, ou uma lista vazia se o bucket não existir ou ocorrer um erro.
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                print(f"O bucket '{bucket_name}' não existe.")
                return []

            objects = self.client.list_objects(bucket_name, prefix=directory, recursive=False)
            subdirectories = set()

            for obj in objects:
                path = obj.object_name
                if path.startswith(directory):
                    subdir = path[len(directory):].split('/')[0]
                    if subdir:
                        subdirectories.add(subdir)

            return list(subdirectories)

        except S3Error as e:
            print(f"Ocorreu um erro: {e}")
            return []

    def generate_markdown(self, bucket_name, subdirectories, directory):
        """
        Gera uma string em formato Markdown com informações dos subdiretórios em um bucket específico.
    
        Args:
            bucket_name (str): Nome do bucket.
            subdirectories (list): Lista de subdiretórios a serem incluídos no Markdown.
            directory (str): Prefixo do diretório que contém os subdiretórios.
    
        Returns:
            str: Uma string formatada em Markdown contendo uma linha para cada subdiretório, com colunas para o bucket,
                  diretório, nome do subdiretório e um link correspondente.
        """
        base_url = f"http://10.145.4.59/minio:9001/browser/{bucket_name}/{directory}"
        markdown_entries = []

        for subdir in subdirectories:
            link = f"{base_url}{subdir}/"
            markdown_entries.append(f"| {bucket_name} | {directory} | {subdir} | [{link}]({link}) |")
        
        return "\n".join(markdown_entries)

    def save_markdown(self, bucket_directory_pairs, nome_arquivo):
        """
        Salva um arquivo Markdown com a lista de subdiretórios para múltiplos buckets.
    
        Args:
            bucket_directory_pairs (list): Lista de tuplas, onde cada tupla contém o nome do bucket e o diretório a ser analisado.
    
        O método itera sobre os pares de bucket e diretório, obtendo a lista de subdiretórios para cada um.
        Em seguida, gera uma tabela Markdown para cada bucket e adiciona ao arquivo de saída.
        O arquivo gerado é salvo como "output.md".
        """
        all_markdown = ""

        buckets = {}

        for bucket_name, directory in bucket_directory_pairs:
            if bucket_name not in buckets:
                buckets[bucket_name] = []

            subdirs = self.list_subdirectories(bucket_name, directory)
            entries = self.generate_markdown(bucket_name, subdirs, directory)
            buckets[bucket_name].append(entries)

        for bucket_name, entries in buckets.items():
            all_markdown += f"## Tabela para o bucket: {bucket_name}\n"
            all_markdown += "| Bucket | Diretório | Tabela | Link |\n|---|---|---|---|\n"
            all_markdown += "\n".join(entries) + "\n\n"

        # Salva o arquivo no diretório atual
        current_directory = os.getcwd()
        file_path = os.path.join(current_directory, f"{nome_arquivo}.md")

        with open(file_path, "w") as f:
            f.write(all_markdown)

        print("Tabela Markdown gerada e salva")





