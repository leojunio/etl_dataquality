import hashlib
import chardet
import json
import os
import datetime
import pandas as pd
from pathlib import Path
CWD=Path(os.path.realpath(__file__)).parent.parent

def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def calculate_file_hash(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256_hash.update(chunk)
    return sha256_hash.hexdigest()


def converter_tempo(elapsed_time):
    """Formata um tempo decorrido em PT-BR.
    Aceita: timedelta, int/float (segundos)."""
    saida = "Tempo total decorrido:"

    # Normaliza para segundos (float) para preservar milissegundos
    if isinstance(elapsed_time, datetime.timedelta):
        total_seconds = elapsed_time.total_seconds()
    else:
        total_seconds = float(elapsed_time)

    # Quebra em dias, horas, minutos, segundos e ms
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, rem = divmod(rem, 60)
    seconds = int(rem)
    millis = int(round((rem - seconds) * 1000))

    parts = []

    d = int(days)
    h = int(hours)
    m = int(minutes)
    s = int(seconds)

    if d: parts.append(f"{d} dia{'s' if d != 1 else ''}")
    if h: parts.append(f"{h} hora{'s' if h != 1 else ''}")
    if m: parts.append(f"{m} minuto{'s' if m != 1 else ''}")

    # Mostrar milissegundos quando < 1s; caso contrário, só segundos
    if d == h == m == s == 0 and millis > 0:
        parts.append(f"{millis} ms")
    else:
        if s or not parts:  # garante algo como '0 segundos'
            parts.append(f"{s} segundo{'s' if s != 1 else ''}")

    return f"{saida} " + " ".join(parts)

