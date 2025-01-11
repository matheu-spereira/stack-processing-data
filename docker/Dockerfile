# Use a imagem base do Spark
FROM bitnami/spark:3.5.2-debian-12-r0

# Diretório onde os JARs serão copiados
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

# Copiar os JARs do diretório local para o diretório de JARs do Spark
COPY jars/s3-2.26.20.jar ${SPARK_JARS_DIR}/
COPY jars/aws-java-sdk-1.12.761.jar ${SPARK_JARS_DIR}/
COPY jars/hadoop-aws-3.4.0.jar ${SPARK_JARS_DIR}/
COPY jars/aws-java-sdk-bundle-1.12.761.jar ${SPARK_JARS_DIR}/
COPY jars/postgresql-42.6.0.jar ${SPARK_JARS_DIR}/
COPY jars/ojdbc11.jar ${SPARK_JARS_DIR}/


# Copiar o arquivo spark-defaults.conf para o diretório de configuração do Spark
COPY spark-defaults.conf /opt/bitnami/spark/conf/

RUN pip install delta-spark==3.2.0
RUN pip install psycopg2-binary==2.9.9
# Expor as portas necessárias (se houver alguma específica para o Spark, já que isso é coberto no docker-compose)
EXPOSE 8080 7077

# Comando para iniciar o Spark Master ou Worker
CMD ["sh", "-c", "/opt/bitnami/scripts/spark/entrypoint.sh ${SPARK_MODE}"]