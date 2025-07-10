# Tesouro Data - Python

Este é uma tradução simplificada para Python do pacote [GetTDData](https://github.com/msperlin/GetTDData/) em R.

O pacote original permite baixar dados de preços e yields de títulos do Tesouro Direto brasileiro diretamente do site oficial.

## Instalação

```bash
pip install -e .
```

## Uso

```python
from tesouro_data import TesouroData

# Baixar dados de LTN entre 2020 e 2022
td = TesouroData()
df = td.get_data('LTN', 2020, 2022)
```

## Créditos

Baseado no trabalho original de Marcelo Perlin: https://github.com/msperlin/GetTDData/