from flask import Flask, render_template, request, url_for, redirect, send_from_directory, jsonify
import requests
import os
from src.data.bases import BasesResumo
import pandas as pd
import docker


# client_docker = docker.DockerClient(base_url="unix://var/run/docker.sock")


#################################################################
#                       FALTA FAZER
#################################################################

# [OK] Página de sucesso de atualização de Servidores 
# Rodar o Conteiner de Atualização da Bases de Benefícios Sociais e Cruzamento das Bases com servidores
# Criar um dashboard Básico



#################################################################
#                       CONFIG FLASK
#################################################################

UPLOAD_FOLDER =  'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx'}
MODELOS_PATH = 'modelos'
RESULTADOS_PATH = 'resultados'

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


#################################################################
#                            ROTAS
#################################################################

@app.route("/")
def index():
    bases_conn = BasesResumo() 
    dados = bases_conn.data_atualizacao_bases()

    return render_template('index.html', dados=dados )



@app.route('/download/<filename>')
def download(filename):
    return send_from_directory(MODELOS_PATH, filename, as_attachment=True)



@app.route('/resultados/<filename>')
def planilha_resultados(filename):
    return send_from_directory( RESULTADOS_PATH, filename, as_attachment=True)





@app.route('/page_servidores')
def page_servidores():
    return render_template('page_servidores.html')




@app.route('/upload_servidores', methods=['POST'])
def upload_servidores():
    if 'arquivo' not in request.files:
        return "Nenhum arquivo enviado"
    
    file = request.files['arquivo']

    if file.filename == '':
        return "Nome do arquivo vazio"

    if file and allowed_file(file.filename):

        _apagar_uploads_antigos()

        ext = file.filename.rsplit('.', 1)[1].lower()
        caminho = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(caminho)

        try:
            if ext == 'csv':
                df = pd.read_csv(caminho)
            if ext == 'xlsx':
                df = pd.read_excel(caminho)
            if ext == 'xls':
                df = pd.read_excel(caminho)
        except Exception as e:
            return f"Erro ao processar arquivo: {e}"

        # Exibe as 5 primeiras linhas no console
        print(df.head())

        database = BasesResumo()
        database.insert_servidores(dataframe=df)

        mensagem = f"Arquivo {file.filename} enviado e processado com sucesso!"
        # _pagina_resultado_processo(is_sucesso=True, mensagem=mensagem)
        return render_template('resultado_processo.html', mensagem=mensagem, status=True)
        # return f"Arquivo {file.filename} enviado e processado com sucesso!"
    else:
        mensagem = "Tipo de arquivo não permitido. Use .csv, .xls ou .xlsx."
        # _pagina_resultado_processo(is_sucesso=False, mensagem=mensagem)
        return render_template('resultado_processo.html', mensagem=mensagem, status=False)
        # return "Tipo de arquivo não permitido. Use .csv, .xls ou .xlsx."




@app.route('/api/cargas')
def api_cargas():
    bases_conn = BasesResumo() 
    dados = bases_conn.data_atualizacao_bases()

    # df['data_competencia'] = pd.to_datetime(df['data_competencia']).dt.strftime('%m-%Y')

    return jsonify(dados)



@app.route('/api/resultadoscruzamentos')
def api_resultados():
    bases_conn = BasesResumo() 
    dados = bases_conn.base_resultados_resumo()

    return jsonify(dados)


# ================================================================================================
#                              SOLICITA A REALIZAÇÃO DOS CRUZAMENTOS
# ================================================================================================
@app.route('/realizar_cruzamentos', methods=['GET'])
def realizar_cruzamentos():
     # Dados que serão enviados no POST
    payload = {
        "mensagem": "Chamado via GET em /acionar"
    }

    try:
        resposta = requests.post("http://localhost:8081/", json=payload)
        return jsonify({
            "status_post": resposta.status_code,
            "resposta_post": resposta.json()
        }), resposta.status_code
    except requests.exceptions.RequestException as e:
        return jsonify({
            "erro": "Erro ao enviar POST para localhost:8081",
            "detalhes": str(e)
        }), 500



# @app.route('/api/docker')
# def api_docker():
#     client_docker.containers.run()

#     bases_conn = BasesResumo() 
#     dados = bases_conn.data_atualizacao_bases()

#     # df['data_competencia'] = pd.to_datetime(df['data_competencia']).dt.strftime('%m-%Y')

#     return jsonify(dados)






@app.route("/teste")
def route_main():
    print(UPLOAD_FOLDER)
    return '<p>Olá</p>'






#################################################################
#                            FUNÇÔES
#################################################################
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def _apagar_uploads_antigos():
    # Limpar pasta antes de salvar o novo arquivo (exceto .gitkeep)
    for nome_arquivo in os.listdir(app.config['UPLOAD_FOLDER']):
        if nome_arquivo != '.gitkeep':
            caminho_arquivo = os.path.join(app.config['UPLOAD_FOLDER'], nome_arquivo)
            if os.path.isfile(caminho_arquivo):
                os.remove(caminho_arquivo)



def _pagina_resultado_processo(is_sucesso:bool, mensagem:str):
    if is_sucesso:
        return render_template('resultado_processo.html', mensagem=mensagem, status=True)        
    else:
        return render_template('resultado_processo.html', mensagem=mensagem, status=False)
        




if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    app.run(debug=True)