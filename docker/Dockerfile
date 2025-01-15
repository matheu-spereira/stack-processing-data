# Use a imagem base do Spark
FROM bitnami/spark:3.5.2-debian-12-r0

USER root

ENV SPARK_JARS_DIR=/mnt/spark-jars

# Criar diretório de JARs
RUN mkdir -p ${SPARK_JARS_DIR}

# Instalar curl (necessário para baixar os JARs)
RUN apt-get update && apt-get install -y curl

# Arquivos Jars necessários para funcionalidades do SPARK
RUN curl -L -o ${SPARK_JARS_DIR}/hadoop-aws-3.2.2.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.2/hadoop-aws-3.2.2.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-spark_2.12-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-storage-3.2.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/postgresql-42.2.20.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.20/postgresql-42.2.20.jar && \
    curl -L -o ${SPARK_JARS_DIR}/mssql-jdbc-12.6.1.jre11.jar https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.6.1.jre11/mssql-jdbc-12.6.1.jre11.jar && \
    curl -L -o ${SPARK_JARS_DIR}/ojdbc11-23.5.0.24.07.jar https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc11/23.5.0.24.07/ojdbc11-23.5.0.24.07.jar && \
    curl -L -o ${SPARK_JARS_DIR}/spark-excel_2.12-0.13.5.jar https://repo1.maven.org/maven2/com/crealytics/spark-excel_2.12/0.13.5/spark-excel_2.12-0.13.5.jar && \
    curl -L -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.180.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.180/aws-java-sdk-bundle-1.12.180.jar && \
    curl -L -o ${SPARK_JARS_DIR}/xmlbeans-3.1.0.jar https://repo1.maven.org/maven2/org/apache/xmlbeans/xmlbeans/3.1.0/xmlbeans-3.1.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/poi-4.1.2.jar https://repo1.maven.org/maven2/org/apache/poi/poi/4.1.2/poi-4.1.2.jar && \
    curl -L -o ${SPARK_JARS_DIR}/poi-ooxml-schemas-4.1.2.jar https://repo1.maven.org/maven2/org/apache/poi/poi-ooxml-schemas/4.1.2/poi-ooxml-schemas-4.1.2.jar
    

# Expor as portas necessárias (se houver alguma específica para o Spark, já que isso é coberto no docker-compose)
EXPOSE 8080 7077

# Comando para iniciar o Spark Master ou Worker
CMD ["sh", "-c", "/opt/bitnami/scripts/spark/entrypoint.sh ${SPARK_MODE}"]