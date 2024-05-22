import pandas as pd
from bd import get_engine_sqlalchemy

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



def executa_cruzamentos():
    engine = get_engine_sqlalchemy()

    df_bolsa_familia = _cruzamento_bolsa_familia(engine=engine)
    df_bpc = _cruzamento_bpc(engine=engine)
    df_seguro_defeso = _cruzamento_seguro_defeso(engine=engine)
    

    with pd.ExcelWriter("resultados/resultados_cruzamentos.xlsx") as writer:

        df_bolsa_familia.to_excel(writer, sheet_name="Bolsa Familia", index=False)
        df_bpc.to_excel(writer, sheet_name="BPC", index=False)
        df_seguro_defeso.to_excel(writer, sheet_name="Seguro Defeso", index=False)