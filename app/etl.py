import requests
from bs4 import BeautifulSoup
from lxml import etree 
import datetime
from zipfile import ZipFile
import os
from bd import get_conn_psycopg2

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

def _verifica_tabela_atualizada(conn, data_atualiza, nome_schema, nome_tabela, nome_coluna):

    query = f"SELECT TO_DATE({nome_coluna}, 'YYYYMM') AS {nome_coluna} FROM {nome_schema}.{nome_tabela} LIMIT 1"
    
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()

        if(cur.rowcount < 1):
            return False

        if(result[0] < data_atualiza):
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

    _deleta_arquivo(path_arquivo=arq_salvar)

def _carrega_bd(conn, nome_schema, nome_tabela, lista_colunas_indices, path_arq_csv, encoding, delimitador):
    
    copy_sql = f"""
            COPY {nome_schema}.{nome_tabela} FROM stdin
            CSV
            HEADER
            DELIMITER as '{delimitador}'
            """

    with conn.cursor() as cur:

        for coluna_indice in lista_colunas_indices:
            cur.execute(f'DROP INDEX IF EXISTS {nome_schema}.{nome_tabela}_{coluna_indice}_idx;')
            conn.commit()

        cur.execute(f'TRUNCATE TABLE {nome_schema}.{nome_tabela};')
        conn.commit()

        with open(path_arq_csv, 'r', encoding=encoding) as f:
            cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()

        for coluna_indice in lista_colunas_indices:
            cur.execute(f'CREATE INDEX {nome_tabela}_{coluna_indice}_idx ON {nome_schema}.{nome_tabela} ({coluna_indice});')
            conn.commit()

def _deleta_arquivo(path_arquivo):
    os.remove(path_arquivo)

def etl_bases_federais():
    conn = get_conn_psycopg2()
    
    data_atualiza_bases = _get_data_atualiza_bases(url_origem_dados=_url_origem_dados, strings_busca_xpath_bases=_strings_busca_xpath_bases)
    nome_schema='benef_federais'

    for nome_base in _bases_federais:
        base_federal = _bases_federais[nome_base]
        data_atualiza = data_atualiza_bases[nome_base]
        tabela_atualizada = _verifica_tabela_atualizada(conn=conn, data_atualiza=data_atualiza, nome_schema=nome_schema, nome_tabela=base_federal['nome_tabela'], nome_coluna=base_federal['nome_coluna_data_atualiza'])

        if(tabela_atualizada is False):
            _download_csv(url=base_federal['url'], data_atualiza=data_atualiza, sufixo_arquivo=base_federal['sufixo_arquivo'])
            path_arq_csv=f'download/{data_atualiza.strftime("%Y%m")}_{base_federal["sufixo_arquivo"]}.csv'
            _carrega_bd(conn=conn, nome_schema=nome_schema, nome_tabela=base_federal['nome_tabela'], lista_colunas_indices=base_federal['lista_colunas_indices'], path_arq_csv=path_arq_csv, encoding='ISO 8859-1', delimitador=';')
            _deleta_arquivo(path_arquivo=path_arq_csv)

    conn.close()

def etl_base_servidores():
    conn = get_conn_psycopg2()

    path_arq_csv=f'servidores/servidores_cruzamento.csv'
    nome_schema='servidores'
    nome_tabela='servidores_cruzamento'
    lista_colunas_indices=['pis_pasep']
    _carrega_bd(conn=conn, nome_schema=nome_schema, nome_tabela=nome_tabela, lista_colunas_indices=lista_colunas_indices, path_arq_csv=path_arq_csv, encoding='UTF-8', delimitador=';')
    
    conn.close()