from decouple import config
import requests
from bs4 import BeautifulSoup
from lxml import etree 
import datetime
import pandas as pd
from zipfile import ZipFile
import os
from bd import get_conn_psycopg2, get_engine_sqlalchemy

_url_origem_dados = 'https://portaldatransparencia.gov.br/origem-dos-dados'

_strings_busca_xpath_bases = {
    'novo_bolsa_familia': 'Novo Bolsa Família',
    'bpc': 'Instituto Nacional do Seguro Social - BPC',
    'seguro_defeso': 'Seguro Defeso'
}

_bases_federais = {
    'novo_bolsa_familia': {
        'url': 'https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia',
        'sufixo_arquivo': 'NovoBolsaFamilia',
        'nome_tabela': 'novo_bolsa_familia',
        'nome_coluna_data_atualiza': 'mes_competencia',
        'lista_colunas_indices': [
            'nis_favorecido'
        ]
    },
    'bpc': {
        'url': 'https://portaldatransparencia.gov.br/download-de-dados/bpc',
        'sufixo_arquivo': 'BPC',
        'nome_tabela': 'bpc',
        'nome_coluna_data_atualiza': 'mes_competencia',
        'lista_colunas_indices': [
            'nis_beneficiario',
            'nis_representante_legal'
        ]
    },
    'seguro_defeso': {
        'url': 'https://portaldatransparencia.gov.br/download-de-dados/seguro-defeso',
        'sufixo_arquivo': 'SeguroDefeso',
        'nome_tabela': 'seguro_defeso',
        'nome_coluna_data_atualiza': 'mes_referencia',
        'lista_colunas_indices': [
            'nis_favorecido'
        ]
    }
}

def _get_data_atualiza_bases(url_origem_dados, strings_busca_xpath_bases):
    
    headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0'
    }
    
    origem_dados = requests.get(url_origem_dados, headers=headers)
    
    soup = BeautifulSoup(origem_dados.content, "html.parser") 
    dom = etree.HTML(str(soup))

    data_atualiza_bases = {}

    for base, str_xpath in strings_busca_xpath_bases.items():
        data_atualiza_str = dom.xpath(f'//td[contains(text(), "{str_xpath}")]/following-sibling::*')[2].text.strip()
        data_atualiza_bases[base] = datetime.datetime.strptime(data_atualiza_str, '%m/%Y').date()
    
    return data_atualiza_bases

def _verifica_tabela_atualizada(engine, data_atualiza, nome_tabela, nome_coluna):

    df = pd.read_sql(f"SELECT TO_DATE({nome_coluna}, 'YYYYMM') AS {nome_coluna} FROM benef_federais.{nome_tabela} LIMIT 1", engine)

    df[nome_coluna] = pd.to_datetime(df[nome_coluna]).dt.date

    if(df.empty):
        return False
    
    if(df[nome_coluna].iloc[0] < data_atualiza):
        return False
    
    return True

def _download_csv(url, data_atualiza, sufixo_arquivo):
    url_download = url + f'/{data_atualiza.strftime("%Y%m")}'

    resp = requests.get(url_download)

    arq_salvar=f'download/{data_atualiza.strftime("%Y%m")}_{sufixo_arquivo}.zip'

    with open(arq_salvar, "wb") as f:
        f.write(resp.content)

    arq_csv=f'{data_atualiza.strftime("%Y%m")}_{sufixo_arquivo}.csv'

    with ZipFile(arq_salvar) as zf:
        zf.extract(arq_csv, path='download')

    os.remove(arq_salvar)


def _carrega_bd(conn, data_atualiza, sufixo_arquivo, nome_tabela, lista_colunas_indices):
    arq_csv=f'download/{data_atualiza.strftime("%Y%m")}_{sufixo_arquivo}.csv'

    copy_sql = f"""
            COPY benef_federais.{nome_tabela} FROM stdin
            CSV
            HEADER
            DELIMITER as ';'
            """

    with conn.cursor() as cur:

        for coluna_indice in lista_colunas_indices:
            cur.execute(f'DROP INDEX IF EXISTS benef_federais.{nome_tabela}_{coluna_indice}_idx;')
            conn.commit()

        cur.execute(f'TRUNCATE TABLE benef_federais.{nome_tabela};')
        conn.commit()

        with open(arq_csv, 'r', encoding='ISO 8859-1') as f:
            cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()

        for coluna_indice in lista_colunas_indices:
            cur.execute(f'CREATE INDEX {nome_tabela}_{coluna_indice}_idx ON benef_federais.{nome_tabela} ({coluna_indice});')
            conn.commit()
        
        os.remove(arq_csv)

def etl_bases_federais():
    conn = get_conn_psycopg2()
    engine = get_engine_sqlalchemy()
    
    data_atualiza_bases = _get_data_atualiza_bases(url_origem_dados=_url_origem_dados, strings_busca_xpath_bases=_strings_busca_xpath_bases)

    for nome_base in _bases_federais:
        base_federal = _bases_federais[nome_base]
        data_atualiza = data_atualiza_bases[nome_base]
        tabela_atualizada = _verifica_tabela_atualizada(engine=engine, data_atualiza=data_atualiza, nome_tabela=base_federal['nome_tabela'], nome_coluna=base_federal['nome_coluna_data_atualiza'])
        
        if(tabela_atualizada is False):
            _download_csv(url=base_federal['url'], data_atualiza=data_atualiza, sufixo_arquivo=base_federal['sufixo_arquivo'])
            _carrega_bd(conn=conn, data_atualiza=data_atualiza, sufixo_arquivo=base_federal['sufixo_arquivo'], nome_tabela=base_federal['nome_tabela'], lista_colunas_indices=base_federal['lista_colunas_indices'])

    conn.close()
