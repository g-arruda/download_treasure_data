"""
TesouroDireto Python - Biblioteca para dados do Tesouro Direto

Uma versão Python do pacote R GetTDData para baixar e processar 
dados históricos do Tesouro Direto brasileiro.

Autor: Baseado no trabalho de Marcelo Perlin (GetTDData)
GitHub: https://github.com/msperlin/GetTDData
"""

from .tesouro_data import (
    TesouroDireto,
    get_treasury_data,
    get_available_assets
)

__version__ = "1.0.0"
__author__ = "Baseado no GetTDData de Marcelo Perlin"
__email__ = "github.com/msperlin/GetTDData"

__all__ = [
    "TesouroDireto",
    "get_treasury_data", 
    "get_available_assets"
]
