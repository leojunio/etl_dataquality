from __future__ import annotations

import os
import csv, json
from pathlib import Path
from typing import Dict, Optional, Union, Any, List, Tuple
import unicodedata  # para normalizar cabeçalhos (remover acentos etc.)

import pandas as pd
import yaml


class DataFileReader:
    """
    Leitor unificado para CSV/TXT, Excel (xlsx/xls), JSON e YAML com suporte a:
      - UTF-8 (ou encoding do schema)
      - **Schema JSON OBRIGATÓRIO** (via schema_filename)
      - Aplicação de tipos e nomes de colunas do schema (ordem + cast seguro)
      - Excel: cabeçalho na 1ª linha, dados a partir da 2ª (has_header=True, start_row=0)
      - CSV/TXT: cabeçalho na 1ª linha (header=0); delimiter do schema (ou detecção)
      - read_folder: concatena vários arquivos e adiciona coluna NM_SOURCE_FILE (ou nome custom)
    """

    def __init__(
        self,
        logger,
        *,
        default_encoding: str = "utf-8",
        schema_root: Optional[Union[str, Path]] = None,
    ):
        # logger obrigatório
        if logger is None:
            raise ValueError("logger é obrigatório para DataFileReader")
        # valida interface mínima de logger
        for m in ("info", "warning", "error"):
            if not hasattr(logger, m):
                raise TypeError(f"logger não possui método '{m}' necessário")
        self.logger = logger
        self.default_encoding = default_encoding
        self.schema_root = Path(schema_root) if schema_root else Path("schema")

    # --- helper de log (SEM fallback) ---
    def _log(self, level: str, msg: str) -> None:
        if not self.logger:
            raise RuntimeError("Logger ausente. DataFileReader exige logger.")
        if level == "info":
            self.logger.info(msg)
        elif level in ("warn", "warning"):
            self.logger.warning(msg)
        elif level in ("err", "error"):
            self.logger.error(msg)
        else:
            self.logger.info(msg)

    # --- selecionar arquivo dentro de uma pasta ---
    def pick_file_from_folder(
        self,
        folder: Union[str, Path],
        *,
        pattern: str = "*",
        recursive: bool = False,
        prefer: str = "latest"  # "latest" ou "largest"
    ) -> Path:
        p = Path(folder)
        if not p.exists() or not p.is_dir():
            raise NotADirectoryError(f"Pasta inválida: {p}")
        candidates = list(p.rglob(pattern) if recursive else p.glob(pattern))
        files = [f for f in candidates if f.is_file()]
        if not files:
            self._log("error", f"Nenhum arquivo encontrado em: {p} (pattern='{pattern}')")
            raise FileNotFoundError(f"Nenhum arquivo encontrado em: {p} (pattern='{pattern}')")
        if len(files) == 1:
            self._log("info", f"Arquivo encontrado: {files[0]}")
            return files[0]
        if prefer == "largest":
            chosen = max(files, key=lambda f: f.stat().st_size); crit = "maior tamanho"
        else:
            chosen = max(files, key=lambda f: f.stat().st_mtime); crit = "mais recente"
        self._log("warning", f"Foram encontrados {len(files)} arquivos em '{p}'. Selecionando o {crit}: {chosen.name}")
        return chosen

    _TYPE_MAP: Dict[str, Union[str, type]] = {
        "string": "string",
        "int": "Int64", "int64": "Int64", "integer": "Int64",
        "float": "Float64", "float64": "Float64", "number": "Float64",
        "bool": "boolean", "boolean": "boolean",
        "datetime": "datetime64[ns]", "datetime64": "datetime64[ns]",
        "date": "datetime64[ns]",
        "category": "category",
    }

    def read(
        self,
        path: Union[str, Path],
        *,
        schema_filename: Union[str, Path],          # <-- OBRIGATÓRIO
        delimiter: Optional[str] = None,
        sheet_name: Union[int, str, None] = 0,
        json_lines: Optional[bool] = None,
        encoding: Optional[str] = None,
        validate_columns: bool = True               # valida cabeçalhos vs schema
    ) -> pd.DataFrame:
        """
        Lê um arquivo CSV/TXT, XLS/XLSX, JSON ou YAML **sempre** aplicando o schema JSON fornecido.
        - NUNCA infere tipos automaticamente.
        - Lê tudo como string / sem NaN automáticos, e só então aplica cast conforme o schema.
        - Cabeçalho é considerado na 1ª linha; dados começam na 2ª.
        """
        if not schema_filename:
            raise ValueError("schema_filename é obrigatório em DataFileReader.read()")

        path = Path(path)
        ext = self._ext(path)

        # 1) Carrega schema (OBRIGATÓRIO)
        loaded_schema: Dict[str, Any] = self._load_schema_file(schema_filename)

        # 2) Extrai mapping (col -> tipo) e a ordem de colunas
        schema_map = self._extract_schema_mapping(loaded_schema) or {}
        column_names = self._extract_column_names(loaded_schema)
        if not schema_map:
            raise ValueError(f"O schema '{schema_filename}' não definiu colunas/tipos válidos.")

        # 3) Configs de leitura (prioridade: schema > args > defaults)
        delimiter = delimiter if delimiter is not None else loaded_schema.get("delimiter")
        excel_has_header = loaded_schema.get("excel_has_header", True)   # header na 1ª
        excel_start_row = loaded_schema.get("excel_start_row", 0)        # começa na 1ª
        excel_fill_empty_with = loaded_schema.get("excel_fill_empty_with", "")
        excel_read_all_as_str = loaded_schema.get("excel_read_all_as_str", True)
        excel_index_col = loaded_schema.get("excel_index_col", 0)
        enc = encoding or loaded_schema.get("encoding") or self.default_encoding

        # 4) Ler por tipo (sempre como string/no-NA para não perder dados antes do cast)
        if ext in {".csv", ".txt"}:
            df = self._read_csv_or_txt(path, delimiter=delimiter, encoding=enc)
        elif ext in {".xlsx", ".xls"}:
            df = self._read_excel(
                path,
                sheet_name=sheet_name,
                has_header=excel_has_header,
                start_row=excel_start_row,
                fill_empty_with=excel_fill_empty_with,
                read_all_as_str=excel_read_all_as_str,
                column_names=column_names,
                index_col=excel_index_col,
            )
        elif ext == ".json":
            df = self._read_json(path, json_lines=json_lines, encoding=enc)
        elif ext in {".yml", ".yaml"}:
            df = self._read_yaml(path, encoding=enc)
        else:
            raise ValueError(f"Extensão não suportada: {ext}")

        # Renomeia cabeçalhos do arquivo para os nomes do schema (normalização + aliases)
        expected_order = column_names or list(schema_map.keys())
        renamer = self._build_header_renamer(df.columns.tolist(), expected_order, loaded_schema)
        if renamer:
            df = df.rename(columns=renamer)

        # 5) (Opcional) Validar cabeçalhos vs schema
        if validate_columns:
            self._validate_columns(df.columns, expected=expected_order)

        # 6) Aplicar tipos/ordem do schema (NUNCA inferir)
        df = self._apply_schema(df, schema_map, dayfirst=True)
        return df

    # ===================== SCHEMA HELPERS =====================

    def _load_schema_file(self, schema_filename: Union[str, Path]) -> Dict[str, Any]:
        p = Path(schema_filename)
        # Se relativo e não existir, tenta resolver via schema_root
        if not p.exists() and not p.is_absolute():
            candidate = self.schema_root / p
            if candidate.exists():
                p = candidate
        if not p.exists():
            raise FileNotFoundError(f"Schema não encontrado: {p}")
        with open(p, "r", encoding=self.default_encoding) as f:
            return json.load(f)

    def load_generic_schema(self, schema_filename: Union[str, Path]) -> Tuple[Dict[str, str], List[str], Optional[str], str]:
        blob = self._load_schema_file(schema_filename)
        mapping = self._extract_schema_mapping(blob) or {}
        ordered = self._extract_column_names(blob) or list(mapping.keys())
        delimiter = blob.get("delimiter")
        encoding = blob.get("encoding", self.default_encoding)
        return mapping, ordered, delimiter, encoding

    def _autoload_schema_for(self, data_path: Path, ext: str) -> Optional[Dict[str, Any]]:
        # Mantido por compatibilidade, mas NÃO é usado quando o schema é obrigatório.
        tipo = self._kind_from_ext(ext)
        if not tipo:
            return None
        base = data_path.stem
        for cand in [
            self.schema_root / tipo / f"{base}.json",
            self.schema_root / tipo / f"{base}.schema.json",
            self.schema_root / tipo / "default.json",
        ]:
            if cand.exists():
                with open(cand, "r", encoding=self.default_encoding) as f:
                    return json.load(f)
        return None

    def _extract_schema_mapping(self, blob: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """
        Aceita:
          - "columns": [{name,type}, ...]
          - "fields":  [{name,type}, ...]  (formato genérico)
          - fallback: {col: type} (ignorando chaves de config)
        """
        if not blob:
            return None

        if isinstance(blob.get("columns"), list):
            out: Dict[str, str] = {}
            for coldef in blob["columns"]:
                name = coldef.get("name")
                typ = coldef.get("type")
                if name:
                    out[name] = (typ or "string")
            if out:
                return out

        if isinstance(blob.get("fields"), list):
            out: Dict[str, str] = {}
            for coldef in blob["fields"]:
                name = coldef.get("name")
                typ = coldef.get("type")
                if name:
                    out[name] = (typ or "string")
            if out:
                return out

        known_cfg = {
            "delimiter", "encoding",
            "excel_has_header", "excel_start_row",
            "excel_fill_empty_with", "excel_read_all_as_str", "excel_index_col",
            "sheet_name"
        }
        out = {k: v for k, v in blob.items() if k not in known_cfg and isinstance(v, str)}
        return out or None

    def _extract_column_names(self, blob: Dict[str, Any]) -> Optional[List[str]]:
        if not blob:
            return None
        key = "columns" if isinstance(blob.get("columns"), list) else ("fields" if isinstance(blob.get("fields"), list) else None)
        if key is None:
            return None
        names = [c.get("name") for c in blob[key] if c.get("name")]
        return names or None

    # ===================== NORMALIZAÇÃO / RENOMEAÇÃO DE COLUNAS =====================

    @staticmethod
    def _normalize_header(name: Any) -> str:
        s = str(name or "").strip()
        # remove acentos
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
        # troca separadores por underscore e upper-case
        out = []
        for ch in s:
            if ch.isalnum():
                out.append(ch.upper())
            else:
                out.append("_")
        s = "".join(out)
        # colapsa múltiplos underscores
        while "__" in s:
            s = s.replace("__", "_")
        return s.strip("_")

    def _build_header_renamer(
        self,
        got_cols: List[Any],
        expected_cols: List[str],
        loaded_schema: Dict[str, Any],
    ) -> Dict[str, str]:
        """
        Tenta mapear os nomes 'como vêm no arquivo' para os nomes do schema.
        Regras:
          - normalização por _normalize_header
          - suporte opcional a 'source'/'source_name'/'aliases' no schema (se existir)
        """
        # nomes esperados normalizados
        exp_norm_map = {self._normalize_header(c): c for c in expected_cols}

        # aliases vindos do schema (opcionais)
        alias_to_target: Dict[str, str] = {}
        for key in ("columns", "fields"):
            if isinstance(loaded_schema.get(key), list):
                for coldef in loaded_schema[key]:
                    target = coldef.get("name")
                    if not target:
                        continue
                    # campos alternativos
                    for akey in ("source", "source_name"):
                        src = coldef.get(akey)
                        if src:
                            alias_to_target[self._normalize_header(src)] = target
                    aliases = coldef.get("aliases") or []
                    if not isinstance(aliases, list):
                        aliases = [aliases]
                    for alias in aliases:
                        alias_to_target[self._normalize_header(str(alias))] = target

        renamer: Dict[str, str] = {}
        for g in got_cols:
            g_norm = self._normalize_header(g)
            # 1) bate direto com esperado normalizado
            if g_norm in exp_norm_map:
                renamer[g] = exp_norm_map[g_norm]
                continue
            # 2) bate com alias do schema
            if g_norm in alias_to_target:
                renamer[g] = alias_to_target[g_norm]
                continue
            # 3) sem mapeamento -> deixa como está; validação acusará se faltar
        return renamer

    # ===================== READERS =====================

    def _read_csv_or_txt(self, path: Path, *, delimiter: Optional[str], encoding: str) -> pd.DataFrame:
        if delimiter is None:
            delimiter = self._detect_delimiter(path, encoding=encoding)
        df = pd.read_csv(
            path,
            sep=delimiter,
            encoding=encoding,
            dtype=str,
            engine="python",
            keep_default_na=False,
            header=0,                 # cabeçalho na 1ª linha
        )
        df.columns = [str(c).lstrip("\ufeff").strip() for c in df.columns]
        return df

    def _read_excel(
        self,
        path: Path,
        *,
        sheet_name: Union[int, str, None],
        has_header: bool,
        start_row: int,
        fill_empty_with: Optional[str],
        read_all_as_str: bool,
        column_names: Optional[List[str]] = None,
        index_col: Optional[Union[int, str, List[int]]] = 0,
    ) -> pd.DataFrame:
        # Com has_header=True e start_row=0:
        # - header=0 (cabeçalho na 1ª linha)
        # - dados iniciam na 2ª linha
        read_kwargs: Dict[str, Any] = {
            "sheet_name": sheet_name,
            "keep_default_na": False,
            "na_values": [],
            "index_col": index_col,
        }
        if has_header:
            read_kwargs["header"] = start_row
            read_kwargs["skiprows"] = range(0, start_row) if start_row > 0 else None
        else:
            read_kwargs["header"] = None
            read_kwargs["skiprows"] = range(0, start_row) if start_row > 0 else None

        if read_all_as_str:
            read_kwargs["dtype"] = str

        df = pd.read_excel(path, **{k: v for k, v in read_kwargs.items() if v is not None})

        if column_names:
            n = df.shape[1]
            if len(column_names) != n:
                names = column_names[:n] if len(column_names) >= n else (
                    column_names + [f"col_{i+1}" for i in range(n - len(column_names))]
                )
                df.columns = names
            else:
                df.columns = column_names
        elif not has_header:
            df.columns = [f"col_{i+1}" for i in range(df.shape[1])]

        if fill_empty_with is not None:
            df = df.fillna(fill_empty_with)

        return df

    def _read_json(self, path: Path, json_lines: Optional[bool], encoding: str) -> pd.DataFrame:
        if json_lines is None:
            json_lines = self._is_ndjson(path, encoding=encoding)
        if json_lines:
            return pd.read_json(path, lines=True, encoding=encoding, dtype=str)
        with open(path, "r", encoding=encoding) as f:
            data = json.load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            try:
                return pd.json_normalize(data)
            except Exception:
                return pd.DataFrame([data])
        raise ValueError("Formato JSON não reconhecido (nem lista nem objeto/dict).")

    def _read_yaml(self, path: Path, encoding: str) -> pd.DataFrame:
        if yaml is None:
            raise RuntimeError("Leitura de YAML requer 'pyyaml'")
        with open(path, "r", encoding=encoding) as f:
            data = yaml.safe_load(f)
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            try:
                return pd.json_normalize(data)
            except Exception:
                return pd.DataFrame([data])
        raise ValueError("YAML lido não é lista nem objeto/dict.")

    # ===================== SUPORTE =====================

    def _validate_columns(self, got: List[str], expected: List[str]) -> None:
        if not expected:
            return
        got_set = set(map(str, got))
        exp_set = set(map(str, expected))
        missing = [c for c in expected if c not in got_set]
        extra = [c for c in got if c not in exp_set]
        if missing or extra:
            msg = []
            if missing:
                msg.append(f"faltando no arquivo: {missing}")
            if extra:
                msg.append(f"colunas inesperadas no arquivo: {extra}")
            raise ValueError("Cabeçalhos divergentes do schema: " + "; ".join(msg))

    def _apply_schema(self, df: pd.DataFrame, schema: Dict[str, str], *, dayfirst: bool = True) -> pd.DataFrame:
        out = df.copy()
        # cria colunas ausentes e ordena exatamente como no schema
        for col in schema.keys():
            if col not in out.columns:
                out[col] = pd.NA
        out = out[list(schema.keys())]

        # casting seguro por tipo
        for col, type_txt in schema.items():
            tkey = str(type_txt).strip().lower()
            s = out[col]
            if tkey in {"datetime", "datetime64"}:
                out[col] = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst, infer_datetime_format=True)
            elif tkey == "date":
                dt = pd.to_datetime(s, errors="coerce", dayfirst=dayfirst, infer_datetime_format=True)
                out[col] = dt.dt.date
            elif tkey in {"int", "int64", "integer"}:
                # trata números com ponto de milhar e vírgula decimal
                s_num = s.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
                out[col] = pd.to_numeric(s_num, errors="coerce").astype("Int64")
            elif tkey in {"float", "float64", "number"}:
                s_num = s.astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
                out[col] = pd.to_numeric(s_num, errors="coerce").astype("Float64")
            elif tkey in {"bool", "boolean"}:
                out[col] = (
                    s.astype("string").str.strip().str.lower().map({
                        "true": True, "t": True, "1": True, "sim": True, "yes": True, "y": True,
                        "false": False, "f": False, "0": False, "nao": False, "não": False, "no": False, "n": False
                    }).astype("boolean")
                )
            elif tkey == "category":
                out[col] = s.astype("category")
            else:
                out[col] = s.astype("string")
        return out

    def _detect_delimiter(self, path: Path, *, encoding: str) -> str:
        common = [",", ";", "|", "\t"]
        with open(path, "r", encoding=encoding, newline="") as f:
            sample = f.read(64 * 1024) or ""
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters="".join(common))
                return dialect.delimiter
            except Exception:
                counts = {d: sample.count(d) for d in common}
                best = max(counts, key=counts.get)
                return best if counts[best] > 0 else ";"

    def _is_ndjson(self, path: Path, *, encoding: str) -> bool:
        try:
            with open(path, "r", encoding=encoding) as f:
                lines = [next(f) for _ in range(20)]
        except Exception:
            return False
        objs = 0; total = 0
        for ln in lines:
            s = ln.strip()
            if not s:
                continue
            total += 1
            if s.startswith("{") and s.endswith("}"):
                objs += 1
        return total >= 3 and objs / max(total, 1) >= 0.6

    @staticmethod
    def _ext(path: Union[str, Path]) -> str:
        s = str(path).lower()
        return s[-5:] if s.endswith(".xlsx") else os.path.splitext(s)[1].lower()

    @staticmethod
    def _kind_from_ext(ext: str) -> Optional[str]:
        if ext in {".csv", ".txt"}:
            return "csv"
        if ext in {".xlsx", ".xls"}:
            return "xls"
        if ext == ".json":
            return "json"
        if ext in {".yml", ".yaml"}:
            return "yaml"
        return None

    # ===================== FOLDER =====================

        # ===================== FOLDER =====================
    def read_folder(
        self,
        folder: Union[str, Path],
        pattern: str = "*.*",  # compat: chamadas antigas continuam funcionando
        *,
        schema_filename: Union[str, Path],     # OBRIGATÓRIO
        delimiter: Optional[str] = None,
        sheet_name: Optional[Union[str, int]] = None,
        filename_column: str = "NM_SOURCE_FILE",
        filename_fullpath: bool = False,
        validate_columns: bool = True,
        # novos recursos (opcionais):
        recursive: bool = False,
        patterns: Optional[List[str]] = None,  # múltiplos padrões alternativos
        normalize_names: bool = False,         # lower + sem acento para arquivo e padrões
    ) -> pd.DataFrame:
        """
        Lê vários arquivos (csv/xls/xlsx/json/yaml) de uma pasta e concatena em um DataFrame.
        - Mantém compatibilidade: `pattern="*.csv"` ainda funciona.
        - Novos recursos:
          * `patterns=[...]` permite múltiplos padrões.
          * `normalize_names=True` faz o match em lower+sem acento.
          * `recursive=True` varre subpastas.
        - Aplica schema obrigatório a todos; garante ordem das colunas do schema.
        - Sempre adiciona coluna com o nome do arquivo (ou caminho completo) no FINAL.
        """
        import fnmatch
        import unicodedata

        def _norm(s: str) -> str:
            if s is None:
                return ""
            s = s.lower()
            s = unicodedata.normalize("NFD", s)
            return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

        folder = Path(folder)
        if not folder.exists() or not folder.is_dir():
            raise NotADirectoryError(f"Pasta inválida: {folder}")

        # Monta lista de candidatos
        candidates = list(folder.rglob("*") if recursive else folder.glob("*"))
        files = [p for p in candidates if p.is_file()]
        if not files:
            raise FileNotFoundError(f"Nenhum arquivo encontrado em: {folder}")

        # Schema obrigatório
        if not schema_filename:
            raise ValueError("schema_filename é obrigatório em read_folder().")

        blob = self._load_schema_file(schema_filename)
        schema_map = self._extract_schema_mapping(blob)
        ordered = self._extract_column_names(blob)
        if not schema_map:
            raise ValueError(f"O schema '{schema_filename}' não definiu colunas/tipos válidos.")

        enc = blob.get("encoding", self.default_encoding)
        delim = delimiter if delimiter is not None else blob.get("delimiter")

        # Preparação de padrões
        if patterns and len(patterns) > 0:
            pats = list(patterns)
        else:
            pats = [pattern]  # retrocompat: usa o pattern antigo

        if normalize_names:
            pats_cmp = [_norm(pat) for pat in pats]

            def _matches(p: Path) -> bool:
                name_cmp = _norm(p.name)
                return any(fnmatch.fnmatch(name_cmp, pat_cmp) for pat_cmp in pats_cmp)
        else:
            def _matches(p: Path) -> bool:
                return any(fnmatch.fnmatch(p.name, pat) for pat in pats)

        selected = [p for p in files if _matches(p)]
        if not selected:
            if patterns and len(patterns) > 0:
                pattxt = patterns
            else:
                pattxt = pattern
            raise FileNotFoundError(
                f"Nenhum arquivo elegível em: {folder} "
                f"(patterns={pattxt}, normalize_names={normalize_names}, recursive={recursive})"
            )

        frames: List[pd.DataFrame] = []
        for p in sorted(selected):
            ext = self._ext(p)
            kind = self._kind_from_ext(ext)
            if kind not in {"csv", "xls", "json", "yaml"}:
                self._log("warning", f"Ignorando formato não suportado: {p.name}")
                continue

            df = self.read(
                p,
                schema_filename=schema_filename,
                delimiter=delim,
                encoding=enc,
                sheet_name=sheet_name,
                validate_columns=validate_columns,
            )

            # Garante colunas do schema e cria as ausentes
            if ordered:
                for col in ordered:
                    if col not in df.columns:
                        df[col] = pd.NA
                df = df[ordered + [c for c in df.columns if c not in ordered]]

            # Nome do arquivo no final
            df[filename_column] = str(p) if filename_fullpath else p.name
            cols = [c for c in df.columns if c != filename_column] + [filename_column]
            df = df[cols]
            frames.append(df)

        if not frames:
            raise FileNotFoundError(
                f"Nenhum arquivo legível encontrado em: {folder} "
                f"(patterns={pats}, normalize_names={normalize_names}, recursive={recursive})"
            )

        return pd.concat(frames, ignore_index=True)

    def remove_arquivos(self, dirpath: str, recursivo: bool = False, padroes: list[str] | None = None) -> int:
        """
        Remove arquivos dentro de dirpath.
        - recursivo=True: varre subpastas
        - padroes: ex. ["*.csv", "*.tmp"] (se None, remove TODOS os arquivos)
        Retorna quantos arquivos foram removidos.
        """
        p = Path(dirpath)
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            print(f"Pasta criada: {p}")

        removed = 0
        if padroes:
            it = (p.rglob(pattern) if recursivo else p.glob(pattern) for pattern in padroes)
            paths = (x for gen in it for x in gen)
        else:
            paths = p.rglob("*") if recursivo else p.iterdir()

        for f in paths:
            if f.is_file():
                try:
                    f.unlink()
                    removed += 1
                except Exception as e:
                    print(f"Falha ao remover {f}: {e}")
        return removed