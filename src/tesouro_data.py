"""
GetTDData Python - Uma versão Python simplificada do pacote R GetTDData

Este módulo baixa e processa dados históricos do Tesouro Direto brasileiro.
Inspirado no pacote R GetTDData de Marcelo Perlin.

Funcionalidades principais:
- Baixar dados históricos de títulos do Tesouro Direto
- Processar arquivos Excel com múltiplas abas
- Retornar dados limpos em formato pandas DataFrame

Exemplo de uso:
    import tesouro_data as td
    
    # Baixar dados do LTN para os anos 2020-2022
    df = td.get_treasury_data(['LTN'], first_year=2020, last_year=2022)
    print(df.head())
"""

import os
import re
import tempfile
import warnings
from datetime import datetime, date
from pathlib import Path
from typing import List, Optional, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class TesouroDireto:
    """
    Classe principal para baixar e processar dados do Tesouro Direto.
    """
    
    # Tipos de ativos disponíveis
    AVAILABLE_ASSETS = ["LFT", "LTN", "NTN-C", "NTN-B", "NTN-B_Principal", "NTN-F"]
    
    # URL base para download dos arquivos
    BASE_URL = "https://cdn.tesouro.gov.br/sistemas-internos/apex/producao/sistemas/sistd"
    
    # Primeiro ano com dados disponíveis
    FIRST_YEAR = 2005
    
    def __init__(self, cache_dir: Optional[str] = None):
        """
        Inicializa a classe TesouroDireto.
        
        Args:
            cache_dir: Diretório para cache dos arquivos baixados. 
                      Se None, usa diretório temporário.
        """
        if cache_dir is None:
            self.cache_dir = Path(tempfile.gettempdir()) / "td-files"
        else:
            self.cache_dir = Path(cache_dir)
        
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Configurar sessão HTTP com retry
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def get_available_assets(self) -> List[str]:
        """Retorna lista de ativos disponíveis."""
        return self.AVAILABLE_ASSETS.copy()
    
    def _normalize_asset_code(self, asset_code: str) -> str:
        """
        Normaliza o código do ativo para o formato usado nas URLs.
        
        Args:
            asset_code: Código do ativo (ex: 'NTN-B Principal')
            
        Returns:
            Código normalizado (ex: 'NTN-B_Principal')
        """
        return asset_code.replace(" ", "_").replace("-", "_")
    
    def _denormalize_asset_code(self, asset_code: str) -> str:
        """
        Desnormaliza o código do ativo para o formato de exibição.
        
        Args:
            asset_code: Código normalizado (ex: 'NTN_B_Principal')
            
        Returns:
            Código desnormalizado (ex: 'NTN-B Principal')
        """
        # Corrige nomes específicos baseado no código R
        asset_code = asset_code.replace("NTN_B_Principal", "NTN-B Principal")
        asset_code = asset_code.replace("NTN_F", "NTN-F")
        asset_code = asset_code.replace("NTN_B", "NTN-B") 
        asset_code = asset_code.replace("NTN_C", "NTN-C")
        asset_code = asset_code.replace("_", " ")
        return asset_code
    
    def _get_maturity_date(self, asset_code: str) -> Optional[date]:
        """
        Extrai data de vencimento do código do ativo.
        
        Args:
            asset_code: Código do ativo (ex: 'LTN 010123')
            
        Returns:
            Data de vencimento ou None se não encontrada
        """
        # Extrair últimos 6 caracteres (formato ddmmyy)
        match = re.search(r'(\d{6})$', asset_code.replace(' ', ''))
        if not match:
            return None
        
        date_str = match.group(1)
        try:
            # Converter ddmmyy para date
            day = int(date_str[:2])
            month = int(date_str[2:4])
            year = int(date_str[4:6])
            
            # Assumir século 20xx se yy <= 50, senão 19xx
            if year <= 50:
                year += 2000
            else:
                year += 1900
            
            return date(year, month, day)
        except ValueError:
            return None
    
    def _download_file(self, asset_code: str, year: int) -> Optional[Path]:
        """
        Baixa arquivo Excel para um ativo e ano específicos.
        
        Args:
            asset_code: Código do ativo
            year: Ano dos dados
            
        Returns:
            Caminho para o arquivo baixado ou None se falhou
        """
        normalized_code = self._normalize_asset_code(asset_code)
        asset_folder = self.cache_dir / normalized_code
        asset_folder.mkdir(parents=True, exist_ok=True)
        
        filename = f"{normalized_code}_{year}.xls"
        local_file = asset_folder / filename
        
        # Se arquivo já existe e não é do ano atual, pular download
        current_year = datetime.now().year
        if local_file.exists() and year != current_year:
            print(f"  Arquivo encontrado em cache: {filename}")
            return local_file
        
        # Construir URL
        url = f"{self.BASE_URL}/{year}/{filename}"
        
        try:
            print(f"  Baixando: {filename}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            with open(local_file, 'wb') as f:
                f.write(response.content)
            
            print(f"  ✓ {filename} baixado com sucesso")
            return local_file
            
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Erro ao baixar {filename}: {e}")
            return None
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e processa dados do DataFrame, capturando bid e ask.
        
        Args:
            df: DataFrame bruto lido do Excel
            
        Returns:
            DataFrame limpo
        """
        if df.empty:
            return df
        # Selecionar as 5 primeiras colunas para capturar bid e ask
        df = df.iloc[:, [0, 1, 2, 3, 4]].copy()
        # Renomear todas as colunas
        df.columns = ['ref_date', 'yield_bid', 'yield_ask', 'price_bid', 'price_ask']
        # Converter todas as colunas numéricas
        for col in ['yield_bid', 'yield_ask', 'price_bid', 'price_ask']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        # Processar datas com formato específico para evitar warnings
        df['ref_date'] = pd.to_datetime(df['ref_date'], format='%d/%m/%Y', errors='coerce')
        # Remover linhas com dados faltantes em qualquer uma das colunas
        df = df.dropna()
        # Remover linhas com preços de compra zerados
        df = df[df['price_bid'] != 0]
        return df
    
    def _read_excel_file(self, file_path: Path) -> pd.DataFrame:
        """
        Lê arquivo Excel e processa todas as abas.
        
        Args:
            file_path: Caminho para o arquivo Excel
            
        Returns:
            DataFrame consolidado com dados de todas as abas
        """
        print(f"  Lendo arquivo: {file_path.name}")
        
        try:
            # Ler todas as abas do arquivo
            excel_file = pd.ExcelFile(file_path)
            all_data = []
            
            for sheet_name in excel_file.sheet_names:
                print(f"    Processando aba: {sheet_name}")
                
                # Ler aba (pular primeira linha como no código R)
                df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=1)
                
                if not df.empty:
                    # Limpar dados
                    df = self._clean_data(df)
                    
                    if not df.empty:
                        # Adicionar código do ativo
                        df['asset_code'] = sheet_name
                        all_data.append(df)
            
            if all_data:
                result = pd.concat(all_data, ignore_index=True)
                print(f"  ✓ Processadas {len(all_data)} abas com {len(result)} registros")
                return result
            else:
                print(f"  ⚠ Nenhum dado encontrado no arquivo")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"  ✗ Erro ao ler arquivo {file_path}: {e}")
            return pd.DataFrame()
    
    def get_treasury_data(self, 
                         asset_codes: Optional[List[str]] = None,
                         first_year: int = 2020,
                         last_year: Optional[int] = None) -> pd.DataFrame:
        """
        Baixa e processa dados do Tesouro Direto.
        
        Args:
            asset_codes: Lista de códigos de ativos. Se None, baixa todos.
            first_year: Primeiro ano dos dados (mínimo 2005)
            last_year: Último ano dos dados. Se None, usa ano atual.
            
        Returns:
            DataFrame com dados consolidados
            
        Example:
            >>> td = TesouroDireto()
            >>> df = td.get_treasury_data(['LTN'], 2020, 2022)
            >>> print(df.head())
        """
        # Validar parâmetros
        if first_year < self.FIRST_YEAR:
            warnings.warn(f'Primeiro ano disponível é {self.FIRST_YEAR}. Ajustando first_year.')
            first_year = self.FIRST_YEAR
        
        if last_year is None:
            last_year = datetime.now().year
        
        if asset_codes is None:
            asset_codes = self.AVAILABLE_ASSETS.copy()
        
        # Validar códigos de ativos
        invalid_codes = [code for code in asset_codes if code not in self.AVAILABLE_ASSETS]
        if invalid_codes:
            raise ValueError(f"Códigos de ativos inválidos: {invalid_codes}. "
                           f"Códigos válidos: {self.AVAILABLE_ASSETS}")
        
        print(f"Baixando dados do Tesouro Direto")
        print(f"Ativos: {asset_codes}")
        print(f"Período: {first_year}-{last_year}")
        print()
        
        # Baixar arquivos
        all_files = []
        years = list(range(first_year, last_year + 1))
        
        print("=== Baixando arquivos ===")
        for asset_code in asset_codes:
            print(f"Ativo: {asset_code}")
            for year in years:
                file_path = self._download_file(asset_code, year)
                if file_path:
                    all_files.append(file_path)
            print()
        
        if not all_files:
            print("Nenhum arquivo foi baixado com sucesso.")
            return pd.DataFrame()
        
        print(f"=== Processando {len(all_files)} arquivos ===")
        
        # Processar arquivos
        all_data = []
        for file_path in all_files:
            df = self._read_excel_file(file_path)
            if not df.empty:
                all_data.append(df)
        
        if not all_data:
            print("Nenhum dado foi extraído dos arquivos.")
            return pd.DataFrame()
        
        # Consolidar dados
        result = pd.concat(all_data, ignore_index=True)
        
        # Processar nomes dos ativos
        result['asset_code'] = result['asset_code'].apply(self._denormalize_asset_code)
        
        # Adicionar data de vencimento
        result['maturity_date'] = result['asset_code'].apply(self._get_maturity_date)
        
        # Ordenar por data
        result = result.sort_values(['asset_code', 'ref_date']).reset_index(drop=True)
        
        print(f"=== Dados consolidados ===")
        print(f"Total de registros: {len(result)}")
        print(f"Período: {result['ref_date'].min().date()} até {result['ref_date'].max().date()}")
        print(f"Ativos únicos: {result['asset_code'].nunique()}")
        
        return result


def get_treasury_data(asset_codes: Optional[List[str]] = None,
                     first_year: int = 2020,
                     last_year: Optional[int] = None,
                     cache_dir: Optional[str] = None) -> pd.DataFrame:
    """
    Função de conveniência para baixar dados do Tesouro Direto.
    
    Args:
        asset_codes: Lista de códigos de ativos. Se None, baixa todos.
        first_year: Primeiro ano dos dados (mínimo 2005)
        last_year: Último ano dos dados. Se None, usa ano atual.
        cache_dir: Diretório para cache. Se None, usa diretório temporário.
        
    Returns:
        DataFrame com dados consolidados
        
    Example:
        >>> import tesouro_data as td
        >>> df = td.get_treasury_data(['LTN'], 2020, 2022)
        >>> print(df.head())
    """
    tesouro = TesouroDireto(cache_dir=cache_dir)
    return tesouro.get_treasury_data(asset_codes, first_year, last_year)


def get_available_assets() -> List[str]:
    """
    Retorna lista de ativos disponíveis.
    
    Returns:
        Lista com códigos dos ativos disponíveis
    """
    return TesouroDireto.AVAILABLE_ASSETS.copy()


if __name__ == "__main__":
    # Exemplo de uso
    print("=== Exemplo de uso do TesouroDireto ===")
    
    # Baixar dados do LTN para 2020-2022
    df = get_treasury_data(['LTN'], first_year=2020, last_year=2022)
    
    if not df.empty:
        print("\n=== Primeiras linhas dos dados ===")
        print(df.head())
        
        print("\n=== Informações sobre os dados ===")
        print(df.info())
        
        print("\n=== Estatísticas descritivas ===")
        print(df.describe())
    else:
        print("Nenhum dado foi obtido.")
