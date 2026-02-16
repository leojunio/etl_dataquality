import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
import numpy as np
import pandera as pa
import pytest

from bronze._contracts.acesso_basico_contract import AcessoBasicoContract as abc

def test_acesso_basico_valid():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
   })
    abc.validate(df_test)

def test_acesso_basico_invalid():
    df_test = pd.DataFrame({
      "LOCALIDADE": ["Localidade A", "Localidade B"],
      "COD_IBGE": [12345, 67890],
      "TEMA": ["Tema A", "Tema B"],
      "INDICADOR": ["Indicador A", "Indicador B"],
      "ANO": [1900, 2022],
      "MEDIA_RELATIVA": [0.5, 0.7],
      "MEDIA_ABSOLUTA": [100, 150],
      "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
      "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
      "VALOR_RELATIVO": [0.3, 0.4],
      "VALOR_ABSOLUTO": [-150, -60],
      "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_localidade():
    df_test = pd.DataFrame({
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)


def test_acesso_basico_missing_column_cod_ibge():
    df_test = pd.DataFrame({
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_tema():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_indicador():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_ano():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_media_relativa():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_media_absoluta():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)  

def test_acesso_basico_missing_column_desagregador():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_classificacao():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_valor_relativo():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_ABSOLUTO": [50, 60],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_valor_absoluto():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "FONTE": ["Fonte A", "Fonte B"]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)

def test_acesso_basico_missing_column_fonte():
    df_test = pd.DataFrame({
        "LOCALIDADE": ["Localidade A", "Localidade B"],
        "COD_IBGE": [12345, 67890],
        "TEMA": ["Tema A", "Tema B"],
        "INDICADOR": ["Indicador A", "Indicador B"],
        "ANO": [2021, 2022],
        "MEDIA_RELATIVA": [0.5, 0.7],
        "MEDIA_ABSOLUTA": [100, 150],
        "DESAGREGADOR": ["Até 1/2 salário mínimo", "Branca"],
        "CLASSIFICACAO": ["Cor ou Raça da criança", "Renda domiciliar per capita - I"],
        "VALOR_RELATIVO": [0.3, 0.4],
        "VALOR_ABSOLUTO": [50, 60]
    })
    with pytest.raises(pa.errors.SchemaError):
        abc.validate(df_test)