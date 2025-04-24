from flask import Flask, render_template, request, url_for, redirect, send_from_directory
import os
from src.data.bases import BasesResumo
import pandas as pd

#################################################################
#                       FALTA FAZER
#################################################################

# Página de sucesso de atualização de Servidores
# Rodar o Conteiner de Atualização da Bases de Benefícios Sociais e Cruzamento das Bases com servidores
# Criar um dashboard Básico



#################################################################
#                       CONFIG FLASK
#################################################################

UPLOAD_FOLDER =  'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx'}
MODELOS_PATH = 'modelos'

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

        return f"Arquivo {file.filename} enviado e processado com sucesso!"
    else:
        return "Tipo de arquivo não permitido. Use .csv, .xls ou .xlsx."







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




if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    app.run( debug=True)