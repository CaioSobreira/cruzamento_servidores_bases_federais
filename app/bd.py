from decouple import config

import sqlalchemy
import psycopg2

db_host=config('DB_HOST')
db_port=config('DB_PORT')
db_user=config('DB_USER')
db_password=config('DB_PASSWORD')
db_name=config('DB_NAME')

#A CONEXÃO DIRETA VIA PSYCOPG2 SERÁ UTILIZADA PARA CARREGAR OS CSVS BRUTOS VIA COMANDO "COPY" DO POSTGRES
def get_conn_psycopg2():
    return psycopg2.connect(f'host={db_host} dbname={db_name} port={db_port} user={db_user} password={db_password}')

#A CONEXÃO VIA SQLALCHEMY/PANDAS SERÁ UTILIZADA PARA OS CRUZAMENTOS, POIS FACILITA A EXPORTAÇÃO DOS RESULTADOS
def get_engine_sqlalchemy():
    return sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')