# stack-processing-data
Stack open source para processamento de dados utilizando Jupyter Notebook, Cluster Spark, MinIO, PostgreSQL, Metabase e Briefer, aplicada em ambiente Docker.

# Instruções de Instalação e Execução

## Pré-requisitos
- **Docker** instalado.  
  Caso não tenha o Docker, siga a documentação oficial para [instalar o Docker](https://docs.docker.com/get-docker/).

- Caso utilize o Windows. **WSL (Windows Subsystem for Linux)** instalado no Windows.  
Se ainda não tiver o WSL instalado, siga as instruções da [documentação oficial da Microsoft](https://docs.microsoft.com/pt-br/windows/wsl/install).

## Passo a Passo de Instalação

1. **Clone o repositório**:
   Abra o terminal (no WSL ou no prompt de comando do Windows) e execute o seguinte comando para clonar o repositório:
   ```bash
   git clone https://github.com/matheu-spereira/stack-processing-data.git

2. **Acesse o repositório clonado**:
   Acesse o diretório do projeto: Navegue até a pasta do repositório clonado:
   ```bash
   cd {caminho/do/diretorio}

2. **Execute o Docker Compose: No diretório do projeto, onde o arquivo docker-compose.yml está localizado, execute o seguinte comando para construir e iniciar os containers em segundo plano:**:
   ```bash
   docker-compose up --build -d
   
# Endereço dos serviços
- **History Server**: [http://localhost:18080/](http://localhost:18080/)
- **Spark UI**: [http://localhost:8081/](http://localhost:8081/)
- **Jupyter**: [http://localhost:8888/](http://localhost:8888/)
- **Minio**: [http://localhost:9001/](http://localhost:9001/)
- **Metabase**: [http://localhost:3000/](http://localhost:3000/)
- **Briefer**: [http://localhost:4000/](http://localhost:4000/)

# Versões utilizadas no ambiente
- **Spark**: 3.5.2
- **Delta**: 3.2.0
- **Python**: 3.11.9

# Estrutura
![image](https://github.com/user-attachments/assets/b81b34d9-c3f0-4976-a255-48f73da03ce3)








