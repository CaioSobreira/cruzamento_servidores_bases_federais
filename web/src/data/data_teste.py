from src.db.Connection import Connection
import pandas as pd

# from src.Connection import Connection
# from src.data.DataDespacho import DataDespacho






class DataCadUnico:
    def __init__(self):
        # self.o_dataDespacho = DataDespacho()
        self.conn = Connection()
        # Conexões
        self.db_engine = self.conn.create_conexao_bd() 




    def m_cadunico_dados(self):
        '''Retorna um DataFrame com [codigo_familiar, cpf, nome], só as pessoas do beneficiarios.csv. '''
        query = '''
        SELECT * FROM benef_federais.seguro_defeso LIMIT 2;
        '''


        df = pd.read_sql(query, self.db_engine)


        # return df_csv
        return df








    # # RETORNA A ÚLTIMA DATA DE ATUALIZAÇÃO
    # def m_data_ultima_atualizacao(self):
    #     query = 'SELECT MAX( cadun_p_referencia_cadastro_unico::DATE ) as atualizacao FROM pcr_cadunico.mv_cadunico'
    #     df = pd.read_sql(query, self.p_galitoEngine)
    #     y = df.loc[0, 'atualizacao']
    #     mes_ano = f"{ str(y.month).rjust(2, '0') }/{y.year}"
    #     return mes_ano
