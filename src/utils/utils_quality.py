# src/utils_quality.py

from __future__ import annotations

from typing import Dict, Any, Iterable, Tuple
import re

import numpy as np
import pandas as pd


from utils.utils_isbn import is_valid_isbn13


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


def is_valid_language_bcp47(x: Any) -> bool:
    """Valida el patrón BCP-47 (no comprueba lista oficial, solo formato)."""
    if not isinstance(x, str):
        return False
    return bool(_LANG_RE.match(x.strip()))


def is_valid_currency_iso4217(x: Any) -> bool:
    if not isinstance(x, str):
        return False
    return bool(_CURRENCY_RE.match(x.strip()))


def check_required_columns(
    df: pd.DataFrame,
    required: Iterable[str],
    dataset_name: str = ""
) -> None:
    """Lanza ValueError si faltan columnas requeridas."""
    required = list(required)
    missing = [c for c in required if c not in df.columns]
    if missing:
        prefix = f"[{dataset_name}] " if dataset_name else ""
        raise ValueError(f"{prefix}Faltan columnas requeridas: {missing}")


def null_ratio(df: pd.DataFrame, cols: Iterable[str]) -> Dict[str, float]:
    """Devuelve % de nulos por columna (0–1)."""
    ratios = {}
    for c in cols:
        if c in df.columns:
            ratios[c] = float(df[c].isna().mean())
    return ratios


# ---------------------------------------------------------------------
# Validaciones específicas para GOODREADS
# ---------------------------------------------------------------------

def validate_goodreads_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Valida y anota flags de calidad para el JSON de Goodreads.

    Espera un esquema similar a:
      id, url, title, authors, rating_value, desc, pub_info, cover,
      format, num_pages, publication_timestamp, publication_date,
      publisher, isbn, isbn13, language, review_count_by_lang,
      genres, rating_count, review_count, comments

    Devuelve:
      - df con columnas de calidad añadidas (prefijo q_gr_)
      - diccionario de métricas agregadas (para quality_metrics.json)
    """

    df = df.copy()

    # 1. columnas mínimas que esperamos
    required_cols = [
        "id",
        "url",
        "title",
        "authors",
        "rating_value",
        "isbn13",
        "rating_count",
        "review_count",
        "language",
    ]
    check_required_columns(df, required_cols, dataset_name="goodreads")

    # 2. Validaciones por fila (flags)
    df["q_gr_title_valid"] = df["title"].apply(is_non_empty_string)

    df["q_gr_url_valid"] = df["url"].apply(is_valid_url)

    # authors: lista no vacía de strings
    def _authors_valid(x: Any) -> bool:
        if not isinstance(x, (list, tuple)):
            return False
        if len(x) == 0:
            return False
        return all(is_non_empty_string(a) for a in x)

    df["q_gr_authors_valid"] = df["authors"].apply(_authors_valid)

    # rating_value entre 0 y 5
    def _rating_valid(x: Any) -> bool:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return False
        try:
            v = float(x)
        except Exception:
            return False
        return 0.0 <= v <= 5.0

    df["q_gr_rating_valid"] = df["rating_value"].apply(_rating_valid)

    # num_pages positivo (si existe)
    if "num_pages" in df.columns:
        df["q_gr_num_pages_valid"] = df["num_pages"].apply(
            lambda x: is_positive_number(x, allow_zero=False)
        )
    else:
        df["q_gr_num_pages_valid"] = True

    # isbn13 válido (si no nulo)
    def _isbn13_valid(x: Any) -> bool:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return False
        return is_valid_isbn13(str(x))

    df["q_gr_isbn13_not_null"] = df["isbn13"].notna()
    df["q_gr_isbn13_valid"] = df["isbn13"].apply(_isbn13_valid)

    # language formato BCP-47 (aunque venga como "English", lo detectamos)
    # Aquí no podemos ser muy estrictos porque Goodreads puede traer "English".
    # Puedes mapearlo antes a 'en', 'es', etc. En esta fase solo comprobamos que no sea vacío.
    df["q_gr_language_not_null"] = df["language"].apply(is_non_empty_string)

    # rating_count y review_count >= 0
    df["q_gr_rating_count_valid"] = df["rating_count"].apply(
        lambda x: is_positive_number(x, allow_zero=True)
    )
    df["q_gr_review_count_valid"] = df["review_count"].apply(
        lambda x: is_positive_number(x, allow_zero=True)
    )

    # review_count_by_lang dict
    if "review_count_by_lang" in df.columns:
        def _review_lang_valid(x: Any) -> bool:
            if x is None or isinstance(x, float) and np.isnan(x):
                return True  # aceptamos nulo
            if not isinstance(x, dict):
                return False
            # claves string, valores numéricos
            for k, v in x.items():
                if not is_non_empty_string(k):
                    return False
                if not is_positive_number(v, allow_zero=True):
                    return False
            return True

        df["q_gr_review_by_lang_valid"] = df["review_count_by_lang"].apply(
            _review_lang_valid
        )
    else:
        df["q_gr_review_by_lang_valid"] = True

    # genres: lista de strings (si existe)
    if "genres" in df.columns:
        def _genres_valid(x: Any) -> bool:
            if x is None or isinstance(x, float) and np.isnan(x):
                return True
            if not isinstance(x, (list, tuple)):
                return False
            return all(is_non_empty_string(g) for g in x)

        df["q_gr_genres_valid"] = df["genres"].apply(_genres_valid)
    else:
        df["q_gr_genres_valid"] = True

    # 3. Métricas agregadas
    metrics: Dict[str, Any] = {}
    metrics["goodreads_rows"] = int(len(df))

    # % títulos no nulos
    metrics["goodreads_pct_title_not_null"] = float(
        df["q_gr_title_valid"].mean()
    )

    # % isbn13 no nulos y % isbn13 válidos
    metrics["goodreads_pct_isbn13_not_null"] = float(
        df["q_gr_isbn13_not_null"].mean()
    )
    metrics["goodreads_pct_isbn13_valid"] = float(
        df["q_gr_isbn13_valid"].mean()
    )

    # % rating válido
    metrics["goodreads_pct_rating_valid"] = float(
        df["q_gr_rating_valid"].mean()
    )

    # % language no nulo
    metrics["goodreads_pct_language_not_null"] = float(
        df["q_gr_language_not_null"].mean()
    )

    # nulos por campo importante
    metrics["goodreads_nulls"] = null_ratio(
        df, ["title", "isbn13", "rating_value", "language", "num_pages"]
    )

    return df, metrics


# ---------------------------------------------------------------------
# Validaciones específicas para GOOGLE BOOKS
# ---------------------------------------------------------------------

def validate_googlebooks_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Valida y anota flags de calidad para el CSV de Google Books.

    Se asume un esquema aproximado (ajusta nombres según tu CSV real):
      gb_id, url, title, subtitle, authors, desc, cover, page_count,
      publication_date, publisher, isbn10, isbn13, language, categories,
      price_amount, price_currency

    Devuelve:
      - df con columnas de calidad añadidas (prefijo q_gb_)
      - diccionario de métricas agregadas
    """

    df = df.copy()

    required_cols = [
        "isbn13",
        "url",
        "title",
        "authors",
        "publication_date",
        "isbn13",
        "language",
    ]
    # si algún nombre de columna es distinto en tu CSV, cámbialo aquí
    check_required_columns(df, required_cols, dataset_name="googlebooks")

    # Normalizamos tipos básicos (por si authors/cats vienen como texto de lista)
    # authors: si viene como string tipo "['Bill Bryson']" podrías parsearlo antes.
    # Aquí sólo comprobamos que no esté vacío.
    df["q_gb_title_valid"] = df["title"].apply(is_non_empty_string)
    df["q_gb_url_valid"] = df["url"].apply(is_valid_url)

    df["q_gb_authors_not_null"] = df["authors"].apply(is_non_empty_string)

    # publication_date: esperamos que ya esté normalizada a YYYY-MM-DD en la fase de integración;
    # si no lo está aún, esta validación te dirá el % de fechas ya normalizadas.
    def _pub_date_valid(x: Any) -> bool:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return False
        return is_valid_iso_date(str(x))

    df["q_gb_pub_date_valid"] = df["publication_date"].apply(_pub_date_valid)

    # isbn13
    df["q_gb_isbn13_not_null"] = df["isbn13"].notna()
    df["q_gb_isbn13_valid"] = df["isbn13"].apply(
        lambda x: is_valid_isbn13(str(x)) if pd.notna(x) else False
    )

    # language en BCP-47 (Google Books suele cumplir esto)
    df["q_gb_language_valid"] = df["language"].apply(is_valid_language_bcp47)

    # categories: aceptamos cualquier string no vacío
    if "categories" in df.columns:
        df["q_gb_categories_not_null"] = df["categories"].apply(
            lambda x: is_non_empty_string(x) or pd.isna(x)
        )
    else:
        df["q_gb_categories_not_null"] = True

    # precio y moneda (si existen)
    if "price_amount" in df.columns:
        df["q_gb_price_amount_non_negative"] = df["price_amount"].apply(
            lambda x: is_positive_number(x, allow_zero=True) or pd.isna(x)
        )
    else:
        df["q_gb_price_amount_non_negative"] = True

    if "price_currency" in df.columns:
        df["q_gb_price_currency_valid"] = df["price_currency"].apply(
            lambda x: is_valid_currency_iso4217(x) or pd.isna(x)
        )
    else:
        df["q_gb_price_currency_valid"] = True

    if "page_count" in df.columns:
        df["q_gb_page_count_valid"] = df["page_count"].apply(
            lambda x: is_positive_number(x, allow_zero=False) or pd.isna(x)
        )
    else:
        df["q_gb_page_count_valid"] = True

    # 3. Métricas agregadas
    metrics: Dict[str, Any] = {}
    metrics["googlebooks_rows"] = int(len(df))

    metrics["googlebooks_pct_title_not_null"] = float(
        df["q_gb_title_valid"].mean()
    )
    metrics["googlebooks_pct_isbn13_not_null"] = float(
        df["q_gb_isbn13_not_null"].mean()
    )
    metrics["googlebooks_pct_isbn13_valid"] = float(
        df["q_gb_isbn13_valid"].mean()
    )
    metrics["googlebooks_pct_pub_date_valid"] = float(
        df["q_gb_pub_date_valid"].mean()
    )
    metrics["googlebooks_pct_language_valid"] = float(
        df["q_gb_language_valid"].mean()
    )
    if "price_amount" in df.columns:
        metrics["googlebooks_pct_price_amount_non_negative"] = float(
            df["q_gb_price_amount_non_negative"].mean()
        )

    metrics["googlebooks_nulls"] = null_ratio(
        df, ["title", "isbn13", "price_amount", "language", "publication_date"]
    )

    return df, metrics
