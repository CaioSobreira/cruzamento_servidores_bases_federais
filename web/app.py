from flask import Flask, render_template
import os
from src.data.data_teste import DataCadUnico




# create the app
app = Flask(__name__)






@app.route("/")
def hello_world():
    dt = DataCadUnico()
    dados = dt.m_cadunico_dados()

    print(dados)



    dados = [
        {"nome": "João", "idade": 25, "cidade": "Recife"},
        {"nome": "Maria", "idade": 30, "cidade": "Olinda"},
        {"nome": "Pedro", "idade": 22, "cidade": "Jaboatão"}
    ]


    return render_template('index.html', nome='Rafael Freitas', dados=dados )

# {versao}
#     return f'''
#         <p>
#      <br>   {os.getenv("DB_NAME")} 
#      <br>   {os.getenv("DB_USER")} 
#      <br>   {os.getenv("DB_PASSWORD")}  
#      <br>   {os.getenv("DB_HOST")}
#      <br>    {os.getenv("DB_PORT")}
     
# </p>
#     '''


if __name__ == '__main__':
    app.run( debug= True)