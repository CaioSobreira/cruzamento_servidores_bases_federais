from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from etl import etl_bases_federais, etl_base_servidores, atualizar_bases_federais
from cruzamento import executa_cruzamentos
from app_log.AppLog import AppLog
import subprocess
from tabelas_resultados import BolsaFamilia, BPC, SeguroDefeso


log = AppLog(name="main.py").get_logger()
log.info("########## EXECUÇÃO INICIADA ##########")




app = FastAPI()


@app.get("/")
def read_root():
    log.info('Rota: "/" Foi acessada!')
    return {"mensagem": "API Online!"}



@app.get("/update")
def update_base():
    resultado = atualizar_bases_federais()
    if resultado == True:
        log.info("#==> Base atualizada!")
        return JSONResponse(content={"mensagem": "Base atualizada!"}, status_code=200)
        
    else:
        log.error("#==> Ocorreu um erro ao atualizada Base!")
        return JSONResponse(content={"mensagem": "Ocorreu um erro ao atualizada Base!"}, status_code=422)
            

@app.get("/cruzamento")
def realizar_cruzamento():
    log.info("# realizar_cruzamento() - Realizar Cruzamentos!")
    
    res = executa_cruzamentos()

    if res == True:
        txt_suc = "#==> Cruzamento realizado!"
        log.info(txt_suc)
        return JSONResponse(content={"mensagem": txt_suc}, status_code=200)
        
    else:
        txt_erro = "#==> Ocorreu um erro ao realizar cruzamento de dados!"
        log.error(txt_erro)
        return JSONResponse(content={"mensagem": txt_erro}, status_code=422)
            


# @app.post("/api/cruzamento")
# async def rodar_cruzamentos(request: Request):
#     corpo = await request.json()
#     nome = corpo.get("chave", "").strip()

#     if not nome:
#         raise HTTPException(
#             status_code=400,
#             detail="O campo 'nome' é obrigatório e não pode estar vazio."
#         )

    

#     return {"mensagem": f"Bem-vindo(a), {nome}!"}





# @app.get("/items/{item_id}")
# def read_item(item_id: int, q: str = None):
#     return {"item_id": item_id, "q": q}






# import time
# while True:
#     time.sleep(5)



# etl_bases_federais()


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







