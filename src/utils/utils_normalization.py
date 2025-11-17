# src/utils_normalization.py

from __future__ import annotations

import ast
import hashlib
import re
from typing import Any, Dict, Iterable, List

import numpy as np
import pandas as pd


def safe_eval(x):
    """Convierte strings 'list-like' o 'dict-like' a Python real."""
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except:
            return x  # si no se puede convertir, se queda como string
    return x


def to_list(x) -> List[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    if isinstance(x, (list, tuple)):
        return [str(v).strip() for v in x if str(v).strip()]
    # si ya viene como string plano, podrías decidir un separador
    if isinstance(x, str):
        # si lleva | lo separamos, si no lo tratamos como único valor
        if "|" in x:
            parts = x.split("|")
        else:
            parts = [x]
        return [p.strip() for p in parts if p.strip()]
    return []


def _compute_title_length(title: Any) -> float | None:
    if not isinstance(title, str):
        return None
    return float(len(title.strip())) if title.strip() else None


def _normalize_authors_google(authors_val: Any) -> str | None:
    """
    Google: a veces autores viene como "['Bill Bryson']" (string).
    Para simplificar, lo dejamos como string plano sin corchetes.
    """
    if authors_val is None or (isinstance(authors_val, float) and np.isnan(authors_val)):
        return None
    s = str(authors_val).strip()
    # quita corchetes y comillas típicos de repr de lista
    s = s.strip("[]")
    s = s.replace("'", "").replace('"', "")
    # separa por coma y limpia espacios
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return "|".join(parts) if parts else None


def _normalize_authors_goodreads(authors_val: Any) -> str | None:
    """
    Goodreads: viene como lista de autores. La convertimos a "autor1|autor2".
    """
    if isinstance(authors_val, (list, tuple)):
        parts = [str(a).strip() for a in authors_val if str(a).strip()]
        return "|".join(parts) if parts else None
    if isinstance(authors_val, str):
        return authors_val.strip()
    return None


def _normalize_categories(val: Any) -> str | None:
    """
    Une categorías en forma "cat1|cat2|cat3".
    Google: puede venir como "['Travel']" o lista.
    Goodreads: lista.
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, (list, tuple)):
        parts = [str(x).strip() for x in val if str(x).strip()]
        return "|".join(parts) if parts else None

    s = str(val).strip()
    if not s:
        return None

    # si viene como "['Travel', 'Humor']"
    s = s.strip("[]")
    s = s.replace("'", "").replace('"', "")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return "|".join(parts) if parts else None

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
    publisher: Any,
    publication_date: Any

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
        str(publisher or "").strip().lower(),
        str(publication_date or "").strip(),
    ]
    raw = "|".join(parts)
    # MD5 suficiente para este ejercicio
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------
# Utilidades genéricas
# ---------------------------------------------------------------------

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_DATE_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
_LANG_RE = re.compile(r"^[a-zA-Z]{2,3}(-[a-zA-Z0-9]{2,8})*$")  # patrón BCP-47
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")  # ISO-4217


def is_non_empty_string(x: Any) -> bool:
    return isinstance(x, str) and x.strip() != ""


def is_valid_url(x: Any) -> bool:
    if not isinstance(x, str):
        return False
    return bool(_URL_RE.match(x.strip()))


def is_positive_number(x: Any, allow_zero: bool = True) -> bool:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return False
    try:
        v = float(x)
    except Exception:
        return False
    if allow_zero:
        return v >= 0
    return v > 0


def is_valid_iso_date(x: Any) -> bool:
    """Fecha ISO estricta YYYY-MM-DD."""
    if not isinstance(x, str):
        return False
    return bool(_DATE_ISO_RE.match(x.strip()))


def pub_date_is_iso_or_false(x: Any) -> bool:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return False
    return is_valid_iso_date(str(x))


def is_valid_language_bcp47(x: Any) -> bool:
    """Valida el patrón BCP-47 (no comprueba lista oficial, solo formato)."""
    if not isinstance(x, str):
        return False
    return bool(_LANG_RE.match(x.strip()))


def is_valid_currency_iso4217(x: Any) -> bool:
    if not isinstance(x, str):
        return False
    return bool(_CURRENCY_RE.match(x.strip()))


def _pub_date_valid(x: Any) -> bool:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return False
    return is_valid_iso_date(str(x))


def _authors_valid(x: Any) -> bool:
    if not isinstance(x, (list, tuple)):
        return False
    if len(x) == 0:
        return False
    return all(is_non_empty_string(a) for a in x)


def _pub_date_valid(x: Any) -> bool:
    # si ya usas pub_date_is_iso_or_false, cámbialo directamente
    return pub_date_is_iso_or_false(x)


def _review_lang_valid(x: Any) -> bool:
    if x is None or isinstance(x, float) and np.isnan(x):
        return True
    if not isinstance(x, dict):
        return False
    for k, v in x.items():
        if not is_non_empty_string(k):
            return False
        if not is_positive_number(v, allow_zero=True):
            return False
    return True


def _genres_valid(x: Any) -> bool:
    if x is None or isinstance(x, float) and np.isnan(x):
        return True
    if not isinstance(x, (list, tuple)):
        return False
    return all(is_non_empty_string(g) for g in x)
