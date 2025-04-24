from src.db.Connection import Connection
from sqlalchemy import text
import pandas as pd

# from src.Connection import Connection
# from src.data.DataDespacho import DataDespacho






class BasesResumo:
    def __init__(self):
        self.conn = Connection()
        # Conexões
        self.db_engine = self.conn.create_conexao_bd() 


    def data_atualizacao_bases(self):
        query = '''
        WITH
        novo_bolsa_familia AS (
            SELECT
                1 AS ordem,
                MAX(substr(mes_competencia, 1, 4) || '-' || substr(mes_competencia, 5, 2) || '-01')::DATE AS data_competencia
                , 'Novo Bolsa Família' AS base_dados
            FROM benef_federais.novo_bolsa_familia
        )
        , bpc AS (
            SELECT
                2 AS ordem,
                MAX(substr(mes_competencia, 1, 4) || '-' || substr(mes_competencia, 5, 2) || '-01')::DATE AS data_competencia,
                'BPC' AS base_dados
            FROM benef_federais.bpc
        )
        , seguro_defeso AS (
            SELECT
                3 AS ordem,
                MAX(substr(mes_referencia, 1, 4) || '-' || substr(mes_referencia, 5, 2) || '-01')::DATE AS mes_referencia,
                'Seguro Defeso' AS base_dados
            FROM benef_federais.seguro_defeso
        )
        , uniao AS (
            SELECT * FROM novo_bolsa_familia
            UNION
            SELECT * FROM bpc
            UNION
            SELECT * FROM seguro_defeso
        )
        SELECT * FROM uniao ORDER BY ordem;
        '''

        df = pd.read_sql(query, self.db_engine)
        # return df.to_json(orient='records')
        return df.to_dict(orient='records')


    def insert_servidores(self, dataframe):
        query = text( """INSERT INTO servidores.servidores_cruzamento (nome, cpf, pis_pasep, vinculos, remuneracao_bruta) VALUES(:nome, :cpf, :pis_pasep, :vinculos, :remuneracao_bruta)""" ) 

        dataframe = dataframe.to_records(index=False).tolist()

        with self.db_engine.connect() as conn:
            # LIMPA A BASE 
            conn.execute( text( 'DELETE FROM servidores.servidores_cruzamento' ))
            # ADICIONA OS SERVIDORES DA PLANILHA
            conn.execute( query, [  dict( nome=nome, cpf=cpf, pis_pasep=pis_pasep, vinculos=vinculos, remuneracao_bruta=remuneracao_bruta) for  nome, cpf, pis_pasep, vinculos, remuneracao_bruta in dataframe]  )
            conn.commit()

            print("DADOS DOS SERVIDORES INSERIDOS")

