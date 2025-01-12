import json
import os

class ControleStatus:    
    def __init__(self, caminho_arquivo):
        self.caminho_arquivo = caminho_arquivo
        diretorio = os.path.dirname(caminho_arquivo)
        if diretorio and not os.path.exists(diretorio):
            os.makedirs(diretorio)        
        if not os.path.exists(caminho_arquivo):
            self.dados = {"state": "inicial", "page": 0}
            self.escrever_arquivo_json()
        else:
            self.dados = self.ler_arquivo_json()
    
    def ler_arquivo_json(self):
        try:
            with open(self.caminho_arquivo, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {"state": "inicial", "page": 0}
        except json.JSONDecodeError:
            print(f"Erro ao decodificar o arquivo {self.caminho_arquivo}.")
            return {"state": "inicial", "page": 0}

    def escrever_arquivo_json(self):
        try:
            with open(self.caminho_arquivo, 'w') as f:
                json.dump(self.dados, f, indent=4)            
        except Exception as e:
            print(f"Erro ao escrever no arquivo {self.caminho_arquivo}: {e}")

    def get_state(self):
        return self.dados['state']

    def set_state(self, novo_state):
        self.dados['state'] = novo_state
        self.escrever_arquivo_json()

    def get_page(self):
        return self.dados['page']

    def set_page(self, nova_page):
        self.dados['page'] = nova_page
        self.escrever_arquivo_json()