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
                    MAX(substr(mes_competencia, 1, 4) || '-' || substr(mes_competencia, 5, 2) || '-01')::DATE AS data_competencia, 
                    'Novo Bolsa Família' AS base_dados,
                    COUNT(*) AS qtd_registros        
                FROM benef_federais.novo_bolsa_familia
            )
            , bpc AS (
                SELECT
                    2 AS ordem,
                    MAX(substr(mes_competencia, 1, 4) || '-' || substr(mes_competencia, 5, 2) || '-01')::DATE AS data_competencia,
                    'BPC' AS base_dados,
                    COUNT(*) AS qtd_registros        
                FROM benef_federais.bpc
            )
            , seguro_defeso AS (
                SELECT
                    3 AS ordem,
                    MAX(substr(mes_referencia, 1, 4) || '-' || substr(mes_referencia, 5, 2) || '-01')::DATE AS data_competencia,
                    'Seguro Defeso' AS base_dados,
                    COUNT(*) AS qtd_registros
                FROM benef_federais.seguro_defeso
            )
            , servidores AS (
                SELECT
                    4 AS ordem,
                    max(data_insert)::DATE AS data_competencia,
                    'SERVIDORES' AS base_dados,
                    COUNT(*) AS qtd_registros
                FROM servidores.servidores_cruzamento        
            )
            , uniao AS (
                SELECT * FROM novo_bolsa_familia
                UNION
                SELECT * FROM bpc
                UNION
                SELECT * FROM seguro_defeso
                UNION
                SELECT * FROM servidores
            )
            SELECT
                ordem, 
                to_char(data_competencia, 'DD/MM/YYYY' ) AS data_competencia,
                UPPER(base_dados) as base_dados,
                qtd_registros
            FROM uniao ORDER BY ordem;
        '''

        df = pd.read_sql(query, self.db_engine)

        return df.to_dict(orient='records')



    # ================================================================================================
    # REALIZAR CARGA DOS SERVIDORES QUE SERAM ALVO DO CRUZAMENTO
    # ================================================================================================
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



    # ================================================================================================
    # RETORNA O RESUMO DOS ACHADOS DO CRUZAMENTO
    # ================================================================================================
    def base_resultados_resumo(self):
        query = '''
                SELECT 
                    to_char(MAX(data_insert), 'DD/MM/YYYY HH24:MI:SS') as data_cruzamento, 
                    'Novo Bolsa Família' as base, 
                    COUNT(*) AS qtd
                FROM resultados.novo_bolsa_familia
                UNION
                SELECT 
                    to_char(MAX(data_insert), 'DD/MM/YYYY HH24:MI:SS') as data_cruzamento, 
                    'Seguro Defeso' AS base, 
                    COUNT(*) AS qtd 
                FROM resultados.seguro_defeso
                UNION
                SELECT 
                    to_char(MAX(data_insert), 'DD/MM/YYYY HH24:MI:SS') as data_cruzamento, 
                    'BPC' AS base, 
                    COUNT(*) AS qtd
                FROM resultados.bpc;
        '''
        
        df = pd.read_sql(query, self.db_engine)

        return df.to_dict(orient='records')
