# src/utils_normalization.py

from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, List, Optional

import ast
import hashlib
import math
import re
from typing import Any, List

import numpy as np
import pandas as pd

from const.BCP_47 import LANG_MAP_GOODREADS

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)
_DATE_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD
_LANG_RE = re.compile(r"^[a-zA-Z]{2,3}(-[a-zA-Z0-9]{2,8})*$")  # patrón BCP-47

DATE_PATTERNS = [
    "%Y-%m-%d",
    "%Y-%m",
    "%Y",
    "%B %d, %Y",      # July 16, 2005
    "%b %d, %Y",      # Jul 16, 2005
]


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


def clean(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, str):
        v = v.strip()
        return v or None
    return None


def clean_number(x):
    if x is None:
        return None
    if isinstance(x, float) and math.isnan(x):
        return None
    try:
        return float(x)
    except Exception:
        return None


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

    return lower


def generate_stable_book_id(isbn13, title, publisher, publication_date):
    """
    Genera un ID estable de libro:
    - Si isbn13 existe y es válido (13 dígitos) → usar isbn13
    - Si no, generar un hash estable con (title + publisher + publication_date)
    """

    if pd.notna(isbn13):
        isbn_str = re.sub(r"\D", "", str(isbn13))
        if len(isbn_str) == 13:
            return isbn_str

    def clean(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return ""
        return str(x).strip().lower()

    key = "|".join([
        clean(title),
        clean(publisher),
        clean(publication_date),
    ])

    return hashlib.sha1(key.encode("utf-8")).hexdigest()


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


def _authors_valid(x: Any) -> bool:
    if not isinstance(x, (list, tuple)):
        return False
    if len(x) == 0:
        return False
    return all(is_non_empty_string(a) for a in x)


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


def _norm_text(x: Any) -> str:
    if not isinstance(x, str):
        return ""
    return x.strip().lower()


def _first_author_norm(x: Any) -> str:
    # Goodreads/Google pueden traer lista o string
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    if isinstance(x, (list, tuple)):
        if not x:
            return ""
        return _norm_text(str(x[0]))
    if isinstance(x, str):
        # si viniera tipo "Autor1, Autor2" cogemos el primero
        first = x.split(",")[0]
        return _norm_text(first)
    return _norm_text(str(x))


def _try_parse_date(raw: str) -> Optional[datetime]:
    raw = raw.strip()
    for pattern in DATE_PATTERNS:
        try:
            return datetime.strptime(raw, pattern)
        except ValueError:
            continue
    return None


def normalize_pub_info_to_date(pub_info):
    if not pub_info or not isinstance(pub_info, str):
        return None

    pub_info = pub_info.strip()

    m = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", pub_info)
    if not m:
        return None

    date_str = m.group(1)

    dt = _try_parse_date(date_str)
    if not dt:
        return None

    return dt.strftime("%Y-%m-%d")


def normalize_gb_date(gb_date: Optional[str]) -> Optional[str]:
    """
    Google Books puede devolver:
      - '2015-12-08'
      - '2015-12'
      - '2015'
    Devolvemos ISO 8601 lo más precisa posible (como string).
    """
    if not gb_date:
        return None

    dt = _try_parse_date(gb_date)
    if not dt:
        return None

    # Mantener la granularidad original
    if len(gb_date) == 4:
        return dt.strftime("%Y")          # YYYY
    elif len(gb_date) == 7:
        return dt.strftime("%Y-%m")       # YYYY-MM
    else:
        return dt.strftime("%Y-%m-%d")    # YYYY-MM-DD


def pick_publication_date(gr_pub_info: Optional[str],
                          gr_pub_date: Optional[str],
                          gb_pub_date: Optional[str]) -> Optional[str]:
    """
    Regla simple:
    - Si Google Books trae fecha → prefer-gb
    - Si no, intentar derivar desde Goodreads (pub_info)
    """
    gb_norm = normalize_gb_date(gb_pub_date) if gb_pub_date else None
    if gb_norm:
        return gb_norm

    # Si Goodreads tiene publication_date directa ya normalizada:
    if gr_pub_date:
        gr_dt = _try_parse_date(gr_pub_date)
        if gr_dt:
            return gr_dt.strftime("%Y-%m-%d")

    # Si no, intentar desde pub_info
    return normalize_pub_info_to_date(gr_pub_info)


def normalize_currency_code(currency: Optional[str]) -> Optional[str]:
    """
    Asegura que la moneda esté en formato ISO-4217:
    - upper-case
    - 3 letras
    Si no cumple, devuelve None.
    """
    if not currency:
        return None
    currency = str(currency).strip().upper()
    if re.fullmatch(r"[A-Z]{3}", currency):
        return currency
    return None


def normalize_price(raw_price: Any) -> Optional[float]:
    """
    Convierte el precio a float con punto decimal.
    Acepta strings y números. Si no es parseable, devuelve None.
    """
    if raw_price is None or raw_price == "":
        return None
    try:
        return float(str(raw_price).replace(",", "."))
    except ValueError:
        return None
