import io
from typing import Union
import re
import pandas as pd
import requests


class ExportaDados:
    """
    Classe que representa a exportação de dados de um serviço web.

    Args:
        args (dict): Um dicionário contendo os parâmetros para a exportação de dados.

    Atributos:
        url (str): URL base do serviço web para exportação de dados.
        parametros (dict): Dicionário contendo os parâmetros para a exportação de dados.
    """

    def __init__(self, args: dict):
        self.url = "https://ws1.soc.com.br/WebSoc/exportadados?parametro="
        self.parametros = args

    def get_url(self) -> str:
        """
        Retorna a representação em string da URL completa para a exportação de dados.

        Returns:
            str: URL completa para a exportação de dados.
        """
        return self.url + str(self.parametros)

    def get_csv(self) -> Union[pd.DataFrame, str]:
        """
        Obtém os dados exportados em formato CSV e retorna um objeto DataFrame do Pandas.

        Returns:
            Union[pd.DataFrame, str]: Um objeto DataFrame do Pandas contendo os dados exportados,
            ou uma string de erro em caso de falha na obtenção dos dados.
        """
        try:
            response = requests.get(self.get_url(),timeout=300)
            c = response.content
            if "Não encontrado exporta dados" in response.text  or "<html>" in response.text:
                return pd.DataFrame()

            # Substitua as aspas simples dentro das aspas duplas por duas aspas simples
            c = c.decode("ISO-8859-1")
            c = re.sub(r'(?<!")"(?!")', "''", c)
            
            df = pd.read_csv(
            io.StringIO(c),
            sep=";",
            dtype="unicode",
            )
            return df
        except requests.exceptions.HTTPError as e:
            print("Error: " + str(e))
        except Exception as e:
                print("Erro: ", e)

    def update_args(self, args: dict):
        """
        Atualiza os parâmetros de exportação de dados com os valores fornecidos.

        Args:
            args (dict): Dicionário contendo os novos valores de parâmetros.
        """
        self.parametros.update(args)
        

    def get_df(self, args: dict) -> pd.DataFrame:
        """
        Obtém os dados exportados em formato DataFrame do Pandas, e adiciona colunas com os valores dos argumentos fornecidos.

        Args:
            args (dict): Dicionário contendo os valores dos argumentos a serem adicionados como colunas no DataFrame.

        Returns:
            pd.DataFrame: Um objeto DataFrame do Pandas contendo os dados exportados e as colunas adicionadas com os valores dos argumentos.
        """
        df = self.get_csv()
        if not df.empty: 
            for key in args.keys():
                df[key] = args[key]   
        return df 