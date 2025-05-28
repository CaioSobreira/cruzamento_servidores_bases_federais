from bd import get_engine_sqlalchemy
from sqlalchemy import text
from cruzamento import cruzamentos_bolsa_familia, cruzamentos_bpc, cruzamentos_seguro_defeso
import pandas as pd



class BolsaFamilia():
    def __init__(self):
        # Tabela do Banco
        self._nome_schema = 'resultados'
        self._nome_tabela = 'novo_bolsa_familia'



    def apagar_registros(self):
        schema_tabela = f'{self._nome_schema}.{self._nome_tabela}'
        query = f'TRUNCATE TABLE {schema_tabela};'

        with get_engine_sqlalchemy().connect() as conn:
            conn.execute(text(query))
            conn.commit() 


    def carga_tabela(self):
        df = cruzamentos_bolsa_familia()

        df.to_sql(
            name      = self._nome_tabela, 
            schema    = self._nome_schema, 
            con       = get_engine_sqlalchemy(),
            if_exists = 'append',
            index     = False 
        )




class BPC():
    def __init__(self):
        # Tabela do Banco
        self._nome_schema = 'resultados'
        self._nome_tabela = 'bpc'


    def apagar_registros(self):
        schema_tabela = f'{self._nome_schema}.{self._nome_tabela}'
        query = f'TRUNCATE TABLE {schema_tabela};'

        with get_engine_sqlalchemy().connect() as conn:
            conn.execute(text(query))
            conn.commit() 


    def carga_tabela(self):
        df = cruzamentos_bpc()

        df.to_sql(
            name      = self._nome_tabela, 
            schema    = self._nome_schema, 
            con       = get_engine_sqlalchemy(),
            if_exists = 'append',
            index     = False 
        )



class SeguroDefeso():
    def __init__(self):
        # Tabela do Banco
        self._nome_schema = 'resultados'
        self._nome_tabela = 'seguro_defeso'


    def apagar_registros(self):
        schema_tabela = f'{self._nome_schema}.{self._nome_tabela}'
        query = f'TRUNCATE TABLE {schema_tabela};'

        with get_engine_sqlalchemy().connect() as conn:
            conn.execute(text(query))
            conn.commit() 


    def carga_tabela(self):
        df = cruzamentos_seguro_defeso()

        df.to_sql(
            name      = self._nome_tabela, 
            schema    = self._nome_schema, 
            con       = get_engine_sqlalchemy(),
            if_exists = 'append',
            index     = False 
        )





# class TabelaMonitoramentoSeiGGJUR():
#     def __init__(self):
#         self._gdrive_util = GoogleDriveUtil()
#         self._conn = Connection()
#         # registro de Atualização
#         self._alias_base = 'CGM GGJUR - MONITORA SEI'
#         self._origem_base = 'Planilha GDriver GGJUR'
#         self._dias_atualiza_base = 1
#         # Tabela do Banco
#         self._nome_schema = 'cgm_dados_ggjur'
#         self._nome_tabela = 'monitoramento_sei_ggjur'
#         # Dados de identificação da planilha GDriver
#         self._planilha_key = '1jQ3a-tg7n_38qoNeL7bo7wqgXpOZs4PdMkBOI5PsB_c'
#         self._aba_nome = 'SEI GGJUR 2025'
#         self._coluna_linha_inicial ='A1'
#         self._coluna_linha_final   ='X10000'



#     def executar(self):
#         self._carga_banco_dados()



#     def apagar_registros(self):
#         schema_tabela = f'{self._nome_schema}.{self._nome_tabela}'
#         query = f'TRUNCATE TABLE {schema_tabela};'

#         with self._conn.create_engine().connect() as conn:
#             conn.execute(text(query))
#             conn.commit() 



#     def _carga_banco_dados(self):
#         df = self._extrair_dados_gdriver()
#         print('# => CARREGAR BANCO DE DADOS')
#         # Carrega o banco de dados
#         df.to_sql(
#             name      = self._nome_tabela, 
#             schema    = self._nome_schema, 
#             con       = self._conn.create_engine(),
#             if_exists = 'append',
#             index     = False
#         )



#     def _extrair_dados_gdriver(self):
#         '''Retorna um Dataframe pandas com os dados da planilha'''

#         print(f'# EXTRAINDO DADOS DA PLANILHA {self.__class__.__name__}')
#         dados = self._gdrive_util.extrair_planilha(
#             planilha_key         = self._planilha_key,
#             aba_nome             = self._aba_nome,
#             coluna_linha_inicial = self._coluna_linha_inicial, 
#             coluna_linha_final   = self._coluna_linha_final
#         )

#         print('# => Cria uma dataframe com os dados da planilha')
#         df = pd.DataFrame(data=dados)

#         print('# => Promove a primeira linha a título, mas a primeira linha ainda existe')
#         # Promove a primeira linha a título, mas a primeira linha ainda existe
#         df.columns = df.iloc[0]

#         print('# => Retira a primeira linha que é o título das colunas e reinicia o índice.')
#         df = df.iloc[1:].reset_index(drop=True)
        
#         print('# => Substitui string vázio por NaN')
#         df['PRAZO INTERNO (DIAS)'] = df['PRAZO INTERNO (DIAS)'].replace( '', np.nan )

#         print('# => Trata valores de datas de DD/MM/YYYY para yyyy-mm-dd')
#         df['ENTRADA GGJUR'] = pd.to_datetime( df['ENTRADA GGJUR'], format='%d/%m/%Y', errors='coerce' )
#         df['CONCLUSÃO GGJUR'] = pd.to_datetime( df['CONCLUSÃO GGJUR'], format='%d/%m/%Y', errors='coerce' )
#         df['PRAZO FINAL'] = pd.to_datetime( df['PRAZO FINAL'], format='%d/%m/%Y', errors='coerce' )
       
#         dtypes = {
#             'TIPO ENTRADA': 'string',
#             'Nº DO SEI': 'string',
#             'NATUREZA DA DEMANDA': 'string',
#             'TIPO PROCESSO': 'string',
#             'DESCRIÇÃO': 'string',
#             'SOLICITANTE': 'string',
#             'ENTRADA GGJUR': 'datetime64[ns]',
#             'CONCLUSÃO GGJUR': 'datetime64[ns]',
#             'PRAZO FINAL': 'datetime64[ns]',
#             'PRAZO INTERNO (DIAS)': 'Int64',
#             'MÊS': 'string',
#             'ANO': 'Int64',
#             'GERÊNCIA': 'string',
#             'PROFISSIONAL': 'string',
#             'STATUS': 'string',
#             'DOC 1': 'string',
#             'DOC LINK1': 'string',
#             'DOC 2': 'string',
#             'DOC LINK2': 'string',
#             'DOC 3': 'string',
#             'DOC LINK3': 'string',
#             'ÚLTIMO MOVIMENTO': 'string',
#             'PROVIDÊNCIAS': 'string',
#             'OBSERVAÇÕES': 'string'
#         }

#         print('# => Tipa as colunas')
#         df.astype( dtype=dtypes )

#         print('# => Renomeia as colunas')
#         df.columns = [
#             'tipo_entrada',
#             'numero_sei',
#             'natureza_demanda',
#             'tipo_processo',
#             'descricao',
#             'solicitante',
#             'entrada_ggjur',
#             'conclusao_ggjur',
#             'prazo_final',
#             'prazo_interno_dias',
#             'mes',
#             'ano',
#             'gerencia',
#             'profissional',
#             'status',
#             'doc_1',
#             'doc_1_link',
#             'doc_2',
#             'doc_2_link',
#             'doc_3',
#             'doc_3_link',
#             'ultimo_movimento',
#             'providencias',
#             'observacoes'
#         ]

#         print('# => Adiciona a data e hora de atualização da planilha google')
#         datahora_modif_gdrive = self._gdrive_util.get_datahora_atualiza_arquivo(arquivo_key= self._planilha_key)
#         df['datahora_modif_gdrive'] = datahora_modif_gdrive

#         print('# => Trata valores de datas de DD/MM/YYYY para yyyy-mm-dd')
#         df['entrada_ggjur'] = pd.to_datetime( df['entrada_ggjur'], format='%d/%m/%Y', errors='coerce' )
#         df['conclusao_ggjur'] = pd.to_datetime( df['conclusao_ggjur'], format='%d/%m/%Y', errors='coerce' )
#         df['prazo_final'] = pd.to_datetime( df['prazo_final'], format='%d/%m/%Y', errors='coerce' )
        
#         print('# => Tratar os números inteiros')
#         df['prazo_interno_dias'] = pd.to_numeric( df['prazo_interno_dias'],  errors='coerce')
#         df['ano'] = pd.to_numeric( df['ano'],  errors='coerce')

#         self._atualizar_registro_atualizacao(datahora_modif_gdrive=datahora_modif_gdrive)

#         return df




#     def _atualizar_registro_atualizacao(self, datahora_modif_gdrive):
#         '''Atualiza a Data da carga'''
#         # Lógica tem que ser atualizada, se houver mais de uma planilha para esse schema vai sobrescrever, podendo ocultar erros.
#         # TRUNCATE
#         query = f"TRUNCATE TABLE {self._nome_schema}.tb_controle_atualiza;"
#         with self._conn.create_engine().connect() as conn:
#             conn.execute(text(query))
#             conn.commit() 

#         # INSERT
#         dthora = datetime.datetime.now()
#         dados = { 
#             'ano_mes_ref': datahora_modif_gdrive, 
#             'data_hora_carga': dthora 
#         }
        
#         df = pd.DataFrame(dados, index=[0])
#         df.to_sql(
#             schema= self._nome_schema, 
#             name='tb_controle_atualiza', 
#             con= self._conn.create_engine(), 
#             if_exists="append", 
#             index=False
#         )
        