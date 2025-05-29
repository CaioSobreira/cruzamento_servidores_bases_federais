from fastapi import FastAPI, Request, HTTPException
from etl import etl_bases_federais, etl_base_servidores
from cruzamento import executa_cruzamentos
import subprocess
from tabelas_resultados import BolsaFamilia, BPC, SeguroDefeso
print("########## EXECUÇÃO INICIADA")




app = FastAPI()



@app.post("/api/cruzamento")
async def rodar_cruzamentos(request: Request):
    corpo = await request.json()
    nome = corpo.get("chave", "").strip()

    if not nome:
        raise HTTPException(
            status_code=400,
            detail="O campo 'nome' é obrigatório e não pode estar vazio."
        )

    

    return {"mensagem": f"Bem-vindo(a), {nome}!"}




@app.get("/")
def read_root():
    return {"mensagem": "Olá, mundo!"}

# @app.get("/items/{item_id}")
# def read_item(item_id: int, q: str = None):
#     return {"item_id": item_id, "q": q}






# etl_bases_federais()

# # except Exception as e:
# #     print(e)

# etl_base_servidores()
# executa_cruzamentos()

# res_bolsa_familia = BolsaFamilia()
# res_bolsa_familia.apagar_registros()
# res_bolsa_familia.carga_tabela()

# res_bpc = BPC()
# res_bpc.apagar_registros()
# res_bpc.carga_tabela()

# res_seg_def = SeguroDefeso()
# res_seg_def.apagar_registros()
# res_seg_def.carga_tabela()



# print("########## EXECUÇÃO FINALIZADA")

# subprocess.run(["chmod", "-R", "777", "resultados/" ])







