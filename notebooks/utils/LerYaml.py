import yaml
from ruamel.yaml import YAML

"""
Classe: ConfiguracaoYAML

Descrição:
    Classe responsável por ler o arquivo yaml.

Autor:
    Autor: Luiz Antonio Roussenq
    E-mail: luiz.roussenq@sc.senai.br
    Data de Criação: 01/11/2023

Histórico de Alterações:
    - Data da Alteração: 
      Autor da Alteração: 
      Descrição da Alteração:   

Observações:    
    
"""

class LeitorYAML:
    def __init__(self, arquivos_yaml):
        self.arquivos_yaml = arquivos_yaml
        self.configuracoes = self.carregar_configuracoes()
        
    def alterar_valor_yaml(self,secao, chave, novo_valor):
        yaml = YAML()
        with open(self.arquivos_yaml, 'r') as arquivo:
            dados = yaml.load(arquivo)
        if secao in dados:
            for item in dados[secao]:
                if chave in item:
                    item[chave] = novo_valor
        with open(self.arquivos_yaml, 'w') as arquivo:
            yaml.dump(dados, arquivo)                

    def carregar_configuracoes(self):
        configuracoes = {}
        try:
            with open(self.arquivos_yaml, 'r') as arquivo:
                configuracoes[self.arquivos_yaml] = yaml.safe_load(arquivo)
        except FileNotFoundError:
            print(f"O arquivo {self.arquivos_yaml} não foi encontrado.")
        return configuracoes

    def busca_chave_valor(self,chave):
        try:
            yaml_data = self.configuracoes[self.arquivos_yaml]
            return yaml_data[chave]
        except Exception as e:
            print(f"Ocorreu um erro: {str(e)}")
            return None  # Chave não encontrada ou erro   

    def busca_chave_sessao_valor(self,sessao,chave):
        try:
            yaml_data = self.configuracoes[self.arquivos_yaml]
            for item in yaml_data[sessao]:
                if chave in item:
                    return item[chave]
        except Exception as e:
            print(f"Ocorreu um erro: {str(e)}")
            return str(e)          

    def busca_lista_chave_sessao_valor(self,sessao,chave):
        try:
            yaml_data = self.configuracoes[self.arquivos_yaml]
            lista = []
            for item in yaml_data[sessao]:
                if chave in item:
                    lista.append(item[chave])
            return lista   
        except Exception as e:
            print(f"Ocorreu um erro: {str(e)}")
            return str(e)              