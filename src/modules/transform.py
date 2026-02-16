import pandas as pd
import os
from pathlib import Path
CWD=Path(os.path.realpath(__file__)).parent.parent

def to_float_ptbr(s: pd.Series) -> pd.Series:
    # converte "1.234,56" -> "1234.56"
    # também trata casos "84;51" -> "84.51"
    s = s.astype("string").str.strip()

    # remove espaços e símbolos comuns
    s = s.str.replace(" ", "", regex=False)

    # troca separadores decimais possíveis para "."
    s = s.str.replace(",", ".", regex=False).str.replace(";", ".", regex=False)

    # remove separador de milhar: se sobrar mais de um ".", mantém só o último como decimal
    # exemplo: "1.234.56" -> "1234.56"
    s = s.str.replace(r"(?<=\d)\.(?=\d{3}(\D|$))", "", regex=True)

    return pd.to_numeric(s, errors="coerce")
