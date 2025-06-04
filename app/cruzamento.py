import pandas as pd
from sqlalchemy import text
from bd import get_engine_sqlalchemy
from app_log.AppLog import AppLog

log = AppLog(name="cruzamento.py").get_logger()


def _cruzamento_bolsa_familia(engine):
    query = """
        WITH 
        servidores AS (
        SELECT 
            nome, 
            cpf, 
            pis_pasep, 
            vinculos, 
            remuneracao_bruta
        FROM 
            servidores.servidores_cruzamento
        ),
        bolsa_familia AS (
        SELECT 
            mes_competencia, 
            mes_referencia, 
            uf, 
            codigo_municipio_siafi, 
            nome_municipio, 
            cpf_favorecido, 
            nis_favorecido, 
            nome_favorecido, 
            valor_parcela
        FROM 
            benef_federais.novo_bolsa_familia
        )
        SELECT
            *
        FROM
            servidores
        INNER JOIN
            bolsa_familia
        ON
            servidores.pis_pasep = bolsa_familia.nis_favorecido
        ORDER BY
            servidores.nome
    """

    return pd.read_sql(query, engine)



def _cruzamento_bpc(engine):
    query = """
        SELECT 
        FROM benef_federais.bpc;

        WITH 
        servidores AS (
        SELECT 
            nome, 
            cpf, 
            pis_pasep, 
            vinculos, 
            remuneracao_bruta
        FROM 
            servidores.servidores_cruzamento
        ),
        bpc AS (
        SELECT 
            mes_competencia, 
            mes_referencia, 
            uf, 
            codigo_municipio_siafi, 
            nome_municipio, 
            nis_beneficiario, 
            cpf_beneficiario, 
            nome_beneficiario, 
            nis_representante_legal, 
            cpf_representante_legal, 
            nome_representante_legal, 
            numero_beneficio, 
            beneficio_concedido_judicialmente, 
            valor_parcela
        FROM 
            benef_federais.bpc
        )
        SELECT
            *
        FROM
            servidores
        INNER JOIN
            bpc
        ON
            servidores.pis_pasep = bpc.nis_beneficiario
        OR
            servidores.pis_pasep = bpc.nis_representante_legal
        ORDER BY
            servidores.nome
    """

    return pd.read_sql(query, engine)

def _cruzamento_seguro_defeso(engine):
    query = """
        WITH 
        servidores AS (
        SELECT 
            nome, 
            cpf, 
            pis_pasep, 
            vinculos, 
            remuneracao_bruta
        FROM 
            servidores.servidores_cruzamento
        ),
        seguro_defeso AS (
        SELECT 
            mes_referencia, 
            uf, 
            codigo_municipio_siafi, 
            nome_municipio, 
            cpf_favorecido, 
            nis_favorecido, 
            rgp_favorecido, 
            nome_favorecido, 
            valor_parcela
        FROM 
            benef_federais.seguro_defeso
        )
        SELECT
            *
        FROM
            servidores
        INNER JOIN
            seguro_defeso
        ON
            servidores.pis_pasep = seguro_defeso.nis_favorecido
        ORDER BY
            servidores.nome
    """

    return pd.read_sql(query, engine)


# =================================================================================================
# REALIZA TODOS OS CRUZAMENTO DE SERVIDORES COM OS DADOS
# =================================================================================================
def executa_cruzamentos():
    try:
        log.info("# executa_cruzamentos - CRUZAMENTO DE DADOS INICIADO")
        engine = get_engine_sqlalchemy()

        log.info("#==> EXECUTANDO CRUZAMENTO - SERVIDORES X BOLSA FAMILIA")
        df_bolsa_familia = _cruzamento_bolsa_familia(engine=engine)

        log.info("#==> EXECUTANDO CRUZAMENTO - SERVIDORES X BPC")
        df_bpc = _cruzamento_bpc(engine=engine)
        
        log.info("#==> EXECUTANDO CRUZAMENTO - SERVIDORES X SEGURO DEFESO")
        df_seguro_defeso = _cruzamento_seguro_defeso(engine=engine)
        
        log.info("#==> EXPORTANDO RESULTADOS - PLANILHA XLSX")
        with pd.ExcelWriter("resultados/resultados_cruzamentos.xlsx") as writer:

            df_bolsa_familia.to_excel(
                writer, 
                sheet_name="Bolsa Familia", 
                index=False
            )
            
            df_bpc.to_excel(
                writer, 
                sheet_name="BPC", 
                index=False
            )

            df_seguro_defeso.to_excel(
                writer, 
                sheet_name="Seguro Defeso", 
                index=False
            )
        
        log.info("#==> SALVAR NO BANCO DE DADOS - BOLSA FAMÍLIA")
        _salvar_resultados_bolsa_familia(df_bolsa_familia)
        
        log.info("#==> SALVAR NO BANCO DE DADOS - BPC")
        _salvar_resultados_bpc(df_bpc)
       
        log.info("#==> SALVAR NO BANCO DE DADOS - SEGURO DEFESO")
        _salvar_resultados_seguro_defeso(df_seguro_defeso)

        log.info("#==> CRUZAMENTO DE DADOS FINALIZADO")
        return True
    except Exception as erro:
        log.error(f"#==> executa_cruzamentos() - {erro}")
        return False



# =================================================================================================
# REALIZA O CRUZAMENTO DE SERVIDORES COM OS DADOS DE BOLSA FAMÍLIA
# =================================================================================================
def cruzamentos_bolsa_familia():
    engine = get_engine_sqlalchemy()
    
    df_bolsa_familia = _cruzamento_bolsa_familia(engine=engine)

    return df_bolsa_familia



# =================================================================================================
# REALIZA O CRUZAMENTO DE SERVIDORES COM OS DADOS DO BPC
# =================================================================================================
def cruzamentos_bpc():
    # print("######## CRUZAMENTO DE DADOS INICIADO")
    engine = get_engine_sqlalchemy()

    # print("###### EXECUTANDO CRUZAMENTO - SERVIDORES X BPC")
    df_bpc = _cruzamento_bpc(engine=engine)

    # print("######## CRUZAMENTO DE DADOS FINALIZADO")

    return df_bpc



# =================================================================================================
# REALIZA O CRUZAMENTO DE SERVIDORES COM OS DADOS DO SEGURO DEFESO
# =================================================================================================
def cruzamentos_seguro_defeso():
    # print("######## CRUZAMENTO DE DADOS INICIADO")
    engine = get_engine_sqlalchemy()

    # print("###### EXECUTANDO CRUZAMENTO - SERVIDORES X SEGURO DEFESO")
    df_seguro_defeso = _cruzamento_seguro_defeso(engine=engine)

    # print("######## CRUZAMENTO DE DADOS FINALIZADO")

    return df_seguro_defeso



# =================================================================================================
# SALVAR RESULTADOS DO CRUZAMENTO NO BANCO DE DADOS - BOLSA FAMILIA
# =================================================================================================
def _salvar_resultados_bolsa_familia(df):
    schema = 'resultados'
    tabela = 'novo_bolsa_familia'
    
    _trucar_tabela(schema=schema, tabela=tabela)

    df.to_sql( 
        name=tabela, 
        schema= schema, 
        chunksize=1000, 
        con= get_engine_sqlalchemy(), 
        if_exists='append', 
        index=False
    )



# =================================================================================================
# SALVAR RESULTADOS DO CRUZAMENTO NO BANCO DE DADOS - BPC
# =================================================================================================
def _salvar_resultados_bpc(df):
    schema = 'resultados'
    tabela = 'bpc'
    
    _trucar_tabela(schema=schema, tabela=tabela)

    df.to_sql(
        name=tabela, 
        schema= schema, 
        chunksize=1000, 
        con= get_engine_sqlalchemy(), 
        if_exists='append', 
        index=False 
    )



# =================================================================================================
# SALVAR RESULTADOS DO CRUZAMENTO NO BANCO DE DADOS - SEGURO DEFESO
# =================================================================================================
def _salvar_resultados_seguro_defeso(df):
    schema = 'resultados'
    tabela = 'seguro_defeso'
    
    _trucar_tabela(schema=schema, tabela=tabela)

    df.to_sql( 
        name=tabela, 
        schema= schema, 
        chunksize=1000, 
        con= get_engine_sqlalchemy(), 
        if_exists='append', 
        index=False
    )
    



# =================================================================================================
# FUNÇÃO PARA APAGAR OS REGISTROS DA TABELA
# =================================================================================================
def _trucar_tabela(schema, tabela ):
    engine = get_engine_sqlalchemy()
    query = f'TRUNCATE TABLE {schema}.{tabela};'

    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit() 

