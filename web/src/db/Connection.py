from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


class Connection:
    def __init__(self):
        load_dotenv()

        self.DB_HOST = os.getenv('DB_HOST')
        self.DB_PORT = os.getenv('DB_PORT')
        self.DB_USER = os.getenv('DB_USER')
        self.DB_PASS = os.getenv('DB_PASSWORD')
        self.DB_NAME = os.getenv('DB_NAME')


    def create_conexao_bd(self):
        '''Cria a conexão para galito no projeto'''
        # postgresql+psycopg://user:password@host:port/dbname[?key=value&key=value...]
        engine = create_engine(f'postgresql+psycopg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}')
        return engine

