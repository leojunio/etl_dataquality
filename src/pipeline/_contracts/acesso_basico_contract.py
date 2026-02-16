import pandera.pandas as pa
import pandas as pd
from pandera.typing import Series
from typing import Optional

class AcessoBasicoContract(pa.DataFrameModel):
    LOCALIDADE: Series[str]
    COD_IBGE: Series[int] 
    TEMA: Series[str]  
    INDICADOR: Series[str]    
    ANO: Series[int] = pa.Field(gt=2000)
    MEDIA_RELATIVA: Series[float] = pa.Field(ge=0)
    MEDIA_ABSOLUTA: Series[float] = pa.Field(ge=0)
    DESAGREGADOR:  Series[str]
    CLASSIFICACAO: Series[str]
    VALOR_RELATIVO: Series[float] = pa.Field(ge=0)
    VALOR_ABSOLUTO: Series[float] = pa.Field(ge=0)
    FONTE: Series[str]
    NM_SOURCE_FILE: Optional[str]
    DT_CARGA: Optional[pd.Timestamp]
    ID_EXECUCAO: Optional[int]
    TS_CARGA: Optional[pd.Timestamp]

    class Config:
        strict = True
        coerce = True

    @pa.check(
            "DESAGREGADOR",
            name="desagregador_check",
            error="O campo 'DESAGREGADOR' deve conter apenas que classificam o indicador")
    def desagregador_check(cls, desagregador: Series[str]) -> Series[bool]:
        valid_values = [
            "Até 1/2 salário mínimo",
            "Até 1/4 de salário mínimo",
            "Branca",
            "Negra"
        ]
        return desagregador.isin(valid_values)
    
    @pa.check(
        "CLASSIFICACAO",
        name="classificacao_check",
        error="O campo 'CLASSIFICACAO' deve conter apenas que classificam o indicador")
    def classificacao_check(cls, classificacao: Series[str]) -> Series[bool]:
        valid_values = [
            "Cor ou Raça da criança",
            "Renda domiciliar per capita - I"
        ]
        return classificacao.isin(valid_values)