import sys
sys.path.append('/home/jovyan/work')

from util.SSIExportaDados import ExportaDados
from datetime import datetime

class Exporta:

    def __init__(self, unidade: str, codigo: str, chave: str):
        """Inicializa uma nova instância da classe Exporta.

        Args:
            unidade (str): Unidade/empresa para a qual os dados serão exportados.
            codigo (str): Código da empresa.
            chave (str): Chave de acesso para exportação dos dados.
        """
        self.unidade = unidade
        self.codigo = codigo
        self.chave = chave

    def asos_no_periodo(self, dt_inicio = '01/10/2021', dt_fim=datetime.today()) -> ExportaDados:
        """Exporta Atestados de Saúde Ocupacional (ASOs) no período especificado.

        Returns:
            ExportaDados: Instância da classe ExportaDados com as configurações para exportação dos ASOs.
        """
        exporta = ExportaDados(
            {
                "empresa": self.unidade,
                "codigo": self.codigo,
                "chave": self.chave,
                "tipoSaida": "csv",
                "dataInicio": dt_inicio,
                "dataFim": dt_fim,
                "tipoBusca": "",
            }
        )

        return exporta
    
    def cadastro_de_epi(self) -> ExportaDados:
        """Exporta o cadastro de Equipamentos de Proteção Individual (EPIs).

        Returns:
            ExportaDados: Instância da classe ExportaDados com as configurações para exportação do cadastro de EPIs.
        """
        exporta = ExportaDados(
            {
                "empresa": self.unidade,
                "codigo": self.codigo,
                "chave": self.chave,
                "tipoSaida": "csv",
                "empresaTrabalho": "782431",
                "campoBusca": "",
                "tipoBusca": "2",
                "situacao": "T",
            }
        )

        return exporta
    
    def cadastro_empresas(self) -> ExportaDados:
        """Exporta o cadastro de empresas.

        Returns:
            ExportaDados: Instância da classe ExportaDados com as configurações para exportação do cadastro de empresas.
        """
        exporta = ExportaDados(
            {
                "empresa": self.unidade,
                "codigo": self.codigo,
                "chave": self.chave,
                "tipoSaida": "csv",
            }
        )

        return exporta
    
    def cadastro_funcionarios(self, dt_inicio="01/12/2022", dt_fim = datetime.today()) -> ExportaDados:
        """Exporta o cadastro de funcionários.

        Returns:
            ExportaDados: Instância da classe ExportaDados com as configurações para exportação do cadastro de funcionários.
        """
        exporta = ExportaDados(
            {
                "empresa": self.unidade,
                "codigo": self.codigo,
                "chave": self.chave,
                "empresaTrabalho": "782431",
                "tipoSaida": "csv",
                "parametroData": "1",
                "dataInicio": dt_inicio,
                "dataFim": dt_fim,
            }
        )

        return exporta
    
    def cadastro_ghe(self) -> ExportaDados:
        """Exporta o cadastro de Grupos Homogêneos de Exposição (GHEs).

        Returns:
            ExportaDados: Instância da classe ExportaDados com as configurações para exportação do cadastro de GHEs.
        """
        exporta = ExportaDados(
            {
                "empresa": self.unidade,
                "codigo": self.codigo,
                "chave": self.chave,
                "tipoSaida": "csv",
                "gheInicial": "0",
                "gheFinal": "999999",
                "filtraInativos": "1",
                "somenteRiscosAtivos":""
            }
        )

        return exporta
    
    def eventos_esocial(self) -> ExportaDados:
        """Exporta os eventos do eSocial.

        Returns:
            ExportaDados: Instância da classe ExportaDados com as configurações para exportação dos eventos do eSocial.
        """
        exporta = ExportaDados(
            {
                "empresa": self.unidade,
                "codigo": self.codigo,
                "chave": self.chave,
                "tipoSaida": "csv",
                "empresaTrabalho": "782431",
                "dataInicio": "01/01/2022",
                "dataFim": "31/12/2022",
                "status": 99,
                "layout": "0",
                "unidade": "0",
                "ambiente": "1",
            }
        )

        return exporta

    def exames_empresa(self,dt_inicio="01/01/2021", dt_fim = datetime.today()) -> ExportaDados:
        """Exporta os exames da empresa.

        Returns:
            ExportaDados: Instância da classe ExportaDados com as configurações para exportação dos exames da empresa.
        """
        exporta = ExportaDados(
            {
                "empresa": self.unidade,
                "codigo": self.codigo,
                "chave": self.chave,
                "tipoSaida": "csv",
                "empresaTrabalho": "941774",
                "dataInicio": dt_inicio,
                "dataFim": dt_fim,
            }
        )

        return exporta