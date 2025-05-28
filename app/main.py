from etl import etl_bases_federais, etl_base_servidores
from cruzamento import executa_cruzamentos
import subprocess
from tabelas_resultados import BolsaFamilia, BPC, SeguroDefeso
print("########## EXECUÇÃO INICIADA")



etl_bases_federais()

# except Exception as e:
#     print(e)

etl_base_servidores()
executa_cruzamentos()

res_bolsa_familia = BolsaFamilia()
res_bolsa_familia.apagar_registros()
res_bolsa_familia.carga_tabela()

res_bpc = BPC()
res_bpc.apagar_registros()
res_bpc.carga_tabela()

res_seg_def = SeguroDefeso()
res_seg_def.apagar_registros()
res_seg_def.carga_tabela()



print("########## EXECUÇÃO FINALIZADA")

subprocess.run(["chmod", "-R", "777", "resultados/" ])







