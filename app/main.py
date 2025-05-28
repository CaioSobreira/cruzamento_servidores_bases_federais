from etl import etl_bases_federais, etl_base_servidores
from cruzamento import executa_cruzamentos
import subprocess

print("########## EXECUÇÃO INICIADA")



etl_bases_federais()

# except Exception as e:
#     print(e)

etl_base_servidores()
executa_cruzamentos()


print("########## EXECUÇÃO FINALIZADA")

subprocess.run(["chmod", "-R", "777", "resultados/" ])







