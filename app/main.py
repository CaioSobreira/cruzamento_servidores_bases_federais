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
            

