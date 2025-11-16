# src/utils_normalization.py

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd


# ----------------------------
# Helpers genéricos
# ----------------------------

def to_snake_case(name: str) -> str:
    """Convierte 'Pub Date' o 'pubDate' a 'pub_date'."""
    if not isinstance(name, str):
        return name
    s = re.sub(r"[^\w]+", "_", name)          # espacios y símbolos -> _
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)  # camelCase -> snake_case
    s = re.sub(r"_+", "_", s)                 # múltiples _ -> 1
    return s.strip("_").lower()


def normalize_columns_snake_case(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [to_snake_case(c) for c in df.columns]
    return df


# ----------------------------
# Fechas
# ----------------------------

def normalize_pub_date_to_iso(date_value: Any) -> str | None:
    """
    Normaliza fechas de Google Books tipo:
      - '1993'
      - '1993-03'
      - '1993-03-01'
    a formato ISO-8601 'YYYY-MM-DD'.

    Si no se puede, devuelve None.
    """
    if date_value is None or (isinstance(date_value, float) and np.isnan(date_value)):
        return None

    s = str(date_value).strip()
    if not s:
        return None

    # solo año
    if re.fullmatch(r"\d{4}", s):
        return f"{s}-01-01"

    # año-mes
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return f"{s}-01"

    # ya parece ISO completo
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s

    # otros formatos raros -> None (o podrías intentar parsear con datetime)
    return None


# ----------------------------
# Idioma
# ----------------------------

LANG_MAP_GOODREADS = {
    # Goodreads suele traer 'English', 'Spanish', etc.
    "english": "en",
    "spanish": "es",
    "español": "es",
    "french": "fr",
    "german": "de",
    # añade los que necesites
}


def normalize_language(value: Any) -> str | None:
    """
    Normaliza idioma a BCP-47 (minúsculas, códigos estándar si se reconocen).
    - 'English' -> 'en'
    - 'en' -> 'en'
    - 'en-US' se queda 'en-us' (puedes dejarlo en minúsculas o respetar mayúsculas de región).
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None

    lower = s.lower()
    if lower in LANG_MAP_GOODREADS:
        return LANG_MAP_GOODREADS[lower]

    # si ya parece un código bcp-47 tipo 'en' o 'en-US', lo pasamos a lower
    # (opcional: podrías respetar mayúscula en región, ej: 'en-US')
    return lower


# ----------------------------
# Moneda y precios
# ----------------------------

def normalize_currency(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    # aquí podrías filtrar solo a códigos conocidos, pero para el ejercicio
    # basta con ponerlo en mayúsculas.
    return s


def normalize_price(value: Any) -> float | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    try:
        return float(value)
    except Exception:
        return None


# ----------------------------
# ID canónico (book_id)
# ----------------------------

def generate_stable_book_id(
    isbn13: Any,
    title: Any,
    author: Any,
    publisher: Any,
    pub_year: Any,
) -> str:
    """
    Devuelve un book_id estable:
      - Si hay isbn13, se usa tal cual.
      - Si no, genera un hash de campos clave.
    """
    if isinstance(isbn13, str) and isbn13.strip():
        return isbn13.strip()

    # clave provisional para el hash
    parts = [
        str(title or "").strip().lower(),
        str(author or "").strip().lower(),
        str(publisher or "").strip().lower(),
        str(pub_year or "").strip(),
    ]
    raw = "|".join(parts)
    # MD5 suficiente para este ejercicio
    return hashlib.md5(raw.encode("utf-8")).hexdigest()
