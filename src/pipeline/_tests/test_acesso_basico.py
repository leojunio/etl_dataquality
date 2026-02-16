import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import pandas as pd
import numpy as np
import pandera as pa
import pytest

from pipeline._contracts.acesso_basico import AcessoBasico

def test_acesso_basico_valid():
    data = {
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
    }
    df = pd.DataFrame(data)
    validated_df = AcessoBasico.validate(df)
    assert isinstance(validated_df, pd.DataFrame)

def test_acesso_basico_invalid():
  data = {
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
  }
  df = pd.DataFrame(data)
  with pytest.raises(pa.errors.SchemaError):
      AcessoBasico.validate(df)

def test_acesso_basico_missing_column_localidade():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_cod_ibge():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_tema():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_indicador():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_ano():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_media_relativa():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_media_absoluta():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)    

def test_acesso_basico_missing_column_desagregador():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_classificacao():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_valor_relativo():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_valor_absoluto():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)

def test_acesso_basico_missing_column_fonte():
    data = {
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
    }
    df = pd.DataFrame(data)
    with pytest.raises(pa.errors.SchemaError):
        AcessoBasico.validate(df)
