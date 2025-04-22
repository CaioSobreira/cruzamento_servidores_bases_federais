from etl import etl_bases_federais, etl_base_servidores
from cruzamento import executa_cruzamentos


print("########## EXECUÇÃO INICIADA")

etl_bases_federais()
etl_base_servidores()
executa_cruzamentos()


print("########## EXECUÇÃO FINALIZADA")