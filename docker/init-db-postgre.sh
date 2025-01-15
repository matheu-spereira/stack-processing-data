#Script para criar um novo database para usdo do Metabase

#!/bin/bash
# Espera o PostgreSQL iniciar e criar o banco de dados inicial
until psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q'; do
  echo "Aguardando PostgreSQL iniciar..."
  sleep 5
done

# Criação de bancos adicionais
echo "Criando bancos de dados adicionais..."
psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "CREATE DATABASE \"$POSTGRES_DB_METABASE\";"
