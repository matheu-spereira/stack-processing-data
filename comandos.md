# docker-compose build --up -d
# docker-compose up -d
# chmod -R 777 ./notebooks/ - Para editar os notebooks no jupyter
# sudo chmod -R 777 /var/lib/docker/volumes/stack-processing-data_spark-events/_data - Para que as aplicação spark seja salva na pasta tmp (usada pelo history)

# endereço sqlserver: localhost ou 127.0.0.1 | user:sa 

# para restaurar o database Adventure_Works_2019:
# 1 - docke ps - para buscar o id do sql server
# 2- docker cp AdventureWorks2019.bak {containerID}:/var/opt/mssql/data/
# 3- docker cp AdventureWorksDW2019.bak {containerID}:/var/opt/mssql/data/
# 4 - No SSMS restaure as duas databases




# ENDEREÇOS:
JUPYTER: http://localhost:8888/
MINIO: http://localhost:9001/
SPARK-HISTORY: http://localhost:18080/
SPARK UI: http://localhost:8081/
PARADEDB(POSTGRESQL): 
    HOST:localhost
    PORTA: 5432
    BANCO DE DADOS: mydatabase

create schema external_table


CREATE FOREIGN DATA WRAPPER delta_wrapper
HANDLER delta_fdw_handler
VALIDATOR delta_fdw_validator;



CREATE SERVER delta_server_minio
FOREIGN DATA WRAPPER delta_wrapper;


CREATE USER MAPPING FOR public
SERVER delta_server_minio
OPTIONS (
  type 'S3',
  key_id '5VejFifl699D0WywV4PQ',
  secret 'K1fhu083hzW43kyI4NfFoaHKXWe2Atfeb5jZZWwq',
  region 'us-east-1',
  endpoint 'minio:9000',
  url_style 'path',
  use_ssl 'false'
);


CREATE FOREIGN TABLE fat_servico_dados_i(
	descricao TEXT,
	grupo varchar(100),
    id TEXT
)
SERVER delta_server_minio
OPTIONS (files 's3://bronze/servicodadosibge/cnaeclasses');


select * from fat_servico_dados_i 