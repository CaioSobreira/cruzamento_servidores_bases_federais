import requests
from bs4 import BeautifulSoup
from lxml import etree 
import datetime
from zipfile import ZipFile
import os
from bd import get_conn_psycopg2
from decouple import config
import csv
import re

_url_origem_dados = 'https://portaldatransparencia.gov.br/origem-dos-dados'

_strings_busca_xpath_bases = {
    'novo_bolsa_familia': 'Novo Bolsa Família',
    'bpc': 'Instituto Nacional do Seguro Social - BPC',
    'seguro_defeso': 'Seguro Defeso'
}

_bases_federais = {
    
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
    },
    'novo_bolsa_familia': {
        'url': 'https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia',
        'sufixo_arquivo': 'NovoBolsaFamilia',
        'nome_tabela': 'novo_bolsa_familia',
        'nome_coluna_data_atualiza': 'mes_competencia',
        'lista_colunas_indices': [
            'nis_favorecido'
        ]
    },

}



def etl_bases_federais():
    print("######## ETL BASES FEDERAIS INICIADO")
    conn = get_conn_psycopg2()
    
    print("###### VERIFICANDO SE BASES ESTÃO ATUALIZADAS")
    data_atualiza_bases = _get_data_atualiza_bases(url_origem_dados=_url_origem_dados, strings_busca_xpath_bases=_strings_busca_xpath_bases)
    nome_schema='benef_federais'

    for nome_base in _bases_federais:
        base_federal = _bases_federais[nome_base]

        print(f"###### BASE ---------- {nome_base.upper()} ----------")

        data_atualiza = data_atualiza_bases[nome_base]
       
        tabela_atualizada = _verifica_tabela_atualizada(conn=conn, data_atualiza=data_atualiza, nome_schema=nome_schema, nome_tabela=base_federal['nome_tabela'], nome_coluna=base_federal['nome_coluna_data_atualiza'])

        if(tabela_atualizada is False):
            _download_csv(url=base_federal['url'], data_atualiza=data_atualiza, sufixo_arquivo=base_federal['sufixo_arquivo'])
            path_arq_csv=f'download/{data_atualiza.strftime("%Y%m")}_{base_federal["sufixo_arquivo"]}.csv'
           
            _carrega_bd(conn=conn, nome_schema=nome_schema, nome_tabela=base_federal['nome_tabela'], lista_colunas_indices=base_federal['lista_colunas_indices'], path_arq_csv=path_arq_csv, encoding='ISO 8859-1', delimitador=';')
           
            # _deleta_arquivo(path_arquivo=path_arq_csv)

    conn.close()
    print(f"######## ETL BASES FEDERAIS FINALIZADO")


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

    print(f"###### DATA ATUALIZACAO SITE GOV BR - {data_atualiza.strftime('%m/%Y')}")


    query = f"SELECT TO_DATE({nome_coluna}, 'YYYYMM') AS {nome_coluna} FROM {nome_schema}.{nome_tabela} LIMIT 1"
    
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchone()

        if(cur.rowcount < 1):
            print(f"###### TABELA BD SEM DADOS - CARREGAR")
            return False
        
        data_atualiza_bd = result[0]

        print(f"###### DATA ATUALIZACAO BD - {data_atualiza_bd.strftime('%m/%Y')}")
        
        if(data_atualiza_bd < data_atualiza):
            print(f"###### TABELA BD DESATUALIZADA - CARREGAR")
            return False
        
        print(f"###### TABELA BD JA ESTA ATUALIZADA")

    return True



def _download_csv(url, data_atualiza, sufixo_arquivo):

    url_download = url + f'/{data_atualiza.strftime("%Y%m")}'
    arq_csv=f'{data_atualiza.strftime("%Y%m")}_{sufixo_arquivo}.csv'
    arq_salvar=f'download/{data_atualiza.strftime("%Y%m")}_{sufixo_arquivo}.zip'

    print(url_download)

    try:
        print(f"###### BAIXANDO ARQUIVO")
        with requests.get(url=url_download, stream=True, timeout=300) as resp:
            with open(arq_salvar, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)  
        
        # Extrair o zip do arquivo
        print(f"###### DESCOMPACTANDO ARQUIVO {arq_salvar}")
        with ZipFile(arq_salvar) as zf:
            zf.extract(arq_csv, path='download')
        print(f"###### ARQUIVO DESCOMPACTADO {arq_salvar}")

        # Apaga o arquivo zip
        _deleta_arquivo(path_arquivo=arq_salvar)

    except Exception as erro:
        print(f'Houve um ERRO ao Baixar ou Descompactar o arquivo - Rotina e ajuste contra o erro: {erro}')
        # Caso erro, quando o arquivo no site mostra que o base está liberada, mas ainda não foi liberada para download.
        ## Apagar o arquivo baixado com erro
        _deleta_arquivo(path_arquivo=arq_salvar)
        ## Alterar a data para um mês anterior
        ### extrai a url sem a data do arquivo
        url_alterada = url_download[:-6]
        ### extrai da url a data do arquivo
        ano_mes_url = url_download[-6:]
        ano_mes_url = _ajuste_mes_download_erro_para_mes_anterior(ano_mes_url)
        ### Reconstroi a url para baixar o arquivo
        url_download = f'{url_alterada}{ano_mes_url}'
        ## Alterar o nome do arquivo salvo
        arq_salvar=f'download/{ano_mes_url}_{sufixo_arquivo}.zip'
        ## 
        arq_csv=f'{ano_mes_url}_{sufixo_arquivo}.csv'

        print(f"###### BAIXANDO ARQUIVO {url_download}")
        # Baixar arquivo de um mês anterior
        with requests.get(url=url_download, stream=True, timeout=300) as resp:
            with open(arq_salvar, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"###### DOWNLOAD CONCLUIDO {url_download}")
        # Extrair esse arquivo

        print(f"###### DESCOMPACTANDO ARQUIVO {arq_salvar}")
        with ZipFile(arq_salvar) as zf:
            zf.extract(arq_csv, path='download')

        # Apaga o arquivo zip
        _deleta_arquivo(path_arquivo=arq_salvar)
        # continuar o fluxo 






def _carrega_bd(conn, nome_schema, nome_tabela, lista_colunas_indices, path_arq_csv, encoding, delimitador):
    
    copy_sql = f"""
            COPY {nome_schema}.{nome_tabela} FROM stdin
            CSV
            HEADER
            DELIMITER as '{delimitador}'
            """
    
    print(f"###### CARREGANDO ARQUIVO PARA TABELA BD")

    with conn.cursor() as cur:

        print(f"###### DELETANDO INDICES")
        for coluna_indice in lista_colunas_indices:
            cur.execute(f'DROP INDEX IF EXISTS {nome_schema}.{nome_tabela}_{coluna_indice}_idx;')
            conn.commit()

        print(f"###### LIMPANDO TABELA")
        cur.execute(f'TRUNCATE TABLE {nome_schema}.{nome_tabela};')
        conn.commit()

        print(f"###### CARREGANDO DADOS")

# dsd
        
        try: 
            with open(path_arq_csv, 'r', encoding=encoding) as f:
                cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()

            print(f"###### RECRIANDO INDICES")
            for coluna_indice in lista_colunas_indices:
                cur.execute(f'CREATE INDEX {nome_tabela}_{coluna_indice}_idx ON {nome_schema}.{nome_tabela} ({coluna_indice});')
                conn.commit()
            print(f"###### CARGA DA TABELA CONCLUIDA")

            # Apaga o arquivo após a carga no banco de dados
            _deleta_arquivo(path_arquivo=path_arq_csv)

        except Exception as erro:
            print("erro no nome do arquivo csv - realizando ajustes!")
            # Modificar a data para o mês anterior
            # path_arq_csv=f'download/{data_atualiza.strftime("%Y%m")}_{base_federal["sufixo_arquivo"]}.csv'
            dt_arq_csv = _extrair_seis_numeros_para_carga_csv(path_arq_csv)
            dt_arq_csv = _ajuste_mes_download_erro_para_mes_anterior(dt_arq_csv)
            path_arq_csv = _substituir_seis_numeros(path_arq_csv, dt_arq_csv)

            with open(path_arq_csv, 'r', encoding=encoding) as f:
                cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()

            print(f"###### RECRIANDO INDICES")
            for coluna_indice in lista_colunas_indices:
                cur.execute(f'CREATE INDEX {nome_tabela}_{coluna_indice}_idx ON {nome_schema}.{nome_tabela} ({coluna_indice});')
                conn.commit()
            print(f"###### CARGA DA TABELA CONCLUIDA")

            # Apaga o arquivo após a carga no banco de dados
            _deleta_arquivo(path_arquivo=path_arq_csv)





        # with open(path_arq_csv, 'r', encoding=encoding) as f:
        #     cur.copy_expert(sql=copy_sql, file=f)
        # conn.commit()

        # print(f"###### RECRIANDO INDICES")
        # for coluna_indice in lista_colunas_indices:
        #     cur.execute(f'CREATE INDEX {nome_tabela}_{coluna_indice}_idx ON {nome_schema}.{nome_tabela} ({coluna_indice});')
        #     conn.commit()
        
        # print(f"###### CARGA DA TABELA CONCLUIDA")



def _deleta_arquivo(path_arquivo):
    os.remove(path_arquivo)



def etl_base_servidores():

    print("######## ETL BASE SERVIDORES INICIADO")

    print("###### BASE ---------- SERVIDORES CSV ----------")
    conn = get_conn_psycopg2()

    modo_teste = config('MODO_TESTE', cast=bool)

    path_arq_csv=f'servidores/servidores_cruzamento.csv'

    if(modo_teste):
        print("###### MODO TESTE ATIVADO -- SERA GERADO UM ARQUIVO CSV COM DADOS FICTICIOS PARA SIMULACAO")
        _gera_csv_teste(conn, path_arq_csv)

    nome_schema='servidores'
    nome_tabela='servidores_cruzamento'
    lista_colunas_indices=['pis_pasep']
    _carrega_bd(conn=conn, nome_schema=nome_schema, nome_tabela=nome_tabela, lista_colunas_indices=lista_colunas_indices, path_arq_csv=path_arq_csv, encoding='UTF-8', delimitador=';')
    
    conn.close()
    print("######## ETL BASE SERVIDORES FINALIZADO")



def _gera_csv_teste(conn, path_arq_csv):

    lista_nis = []

    query_bolsa_familia = "SELECT nis_favorecido FROM benef_federais.novo_bolsa_familia ORDER BY RANDOM() LIMIT 10"
    
    with conn.cursor() as cur:
        cur.execute(query_bolsa_familia)
        results = cur.fetchall()
        lista_nis += results

    query_bpc = "SELECT nis_beneficiario FROM benef_federais.bpc  WHERE nis_beneficiario <> '' ORDER BY RANDOM() LIMIT 10"
    
    with conn.cursor() as cur:
        cur.execute(query_bpc)
        results = cur.fetchall()
        lista_nis += results

    query_seguro_defeso = "SELECT nis_favorecido FROM benef_federais.seguro_defeso ORDER BY RANDOM() LIMIT 10"
    
    with conn.cursor() as cur:
        cur.execute(query_seguro_defeso)
        results = cur.fetchall()
        lista_nis += results

    with open(path_arq_csv, "w", newline='', encoding='utf-8' ) as f:
        writer = csv.writer(f, delimiter =';')

        writer.writerow(['nome', 'cpf', 'pis_pasep', 'vinculos', 'remuneracao_bruta'])

        [writer.writerow(['DADOS FICTICIOS APENAS PARA FINS DE SIMULAÇÃO', '99999999999', row[0], 'VINCULO TESTE', '999.999.999,99']) for row in lista_nis]



def _ajuste_mes_download_erro_para_mes_anterior(ano_mes_str):
    # Converte "202502" para um objeto datetime
    data = datetime.datetime.strptime(ano_mes_str, "%Y%m")
    
    # Subtrai um mês
    if data.month == 1:
        novo_ano = data.year - 1
        novo_mes = 12
    else:
        novo_ano = data.year
        novo_mes = data.month - 1
    # Retorna no mesmo formato "YYYYMM"
    return f"{novo_ano}{novo_mes:02d}"




def _extrair_seis_numeros_para_carga_csv(texto):
    """
    Extrai os seis números que estão entre "/" e "_" em uma string.
    Retorna o número encontrado como string, ou None se não encontrar.
    """
    resultado = re.search(r"/(\d{6})_", texto)
    return resultado.group(1)


def _substituir_seis_numeros(texto, novo_valor):
    """
    Substitui os seis números que estão entre "/" e "_" em uma string pelo novo_valor.
    novo_valor deve ser uma string de 6 dígitos.
    """
    return re.sub(r"/(\d{6})_", f"/{novo_valor}_", texto)
