# Use a imagem base do Spark
FROM bitnami/spark:3.5.2-debian-12-r0

# Diretório onde os JARs serão copiados
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

# Copiar os JARs do diretório local para o diretório de JARs do Spark
# COPY jars/hadoop-aws-3.2.2.jar ${SPARK_JARS_DIR}/
# COPY jars/delta-spark_2.12-3.2.0.jar ${SPARK_JARS_DIR}/
# COPY jars/delta-storage-3.2.0.jar ${SPARK_JARS_DIR}/
# COPY jars/postgresql-42.2.20.jar ${SPARK_JARS_DIR}/
# COPY jars/mssql-jdbc-12.6.1.jre11.jar ${SPARK_JARS_DIR}/
# COPY jars/ojdbc11-23.5.0.24.07.jar ${SPARK_JARS_DIR}/
# COPY jars/spark-excel_2.12-0.13.5.jar ${SPARK_JARS_DIR}/
# COPY jars/aws-java-sdk-bundle-1.12.180.jar ${SPARK_JARS_DIR}/
# COPY jars/xmlbeans-3.1.0.jar ${SPARK_JARS_DIR}/
# COPY jars/poi-4.1.2.jar ${SPARK_JARS_DIR}/
# COPY jars/poi-ooxml-schemas-4.1.2.jar ${SPARK_JARS_DIR}/

COPY jars/ /mnt/spark-jars/

# Copiar o arquivo spark-defaults.conf para o diretório de configuração do Spark
# COPY spark-defaults.conf /opt/bitnami/spark/conf/

RUN pip install delta-spark==3.2.0
RUN pip install psycopg2-binary==2.9.9
# Expor as portas necessárias (se houver alguma específica para o Spark, já que isso é coberto no docker-compose)
EXPOSE 8080 7077

# Comando para iniciar o Spark Master ou Worker
CMD ["sh", "-c", "/opt/bitnami/scripts/spark/entrypoint.sh ${SPARK_MODE}"]