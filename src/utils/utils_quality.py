# src/utils_quality.py

from __future__ import annotations

from typing import Callable, Dict, Any, Iterable, Tuple
import re

import numpy as np
import pandas as pd


from utils.utils_isbn import isbn13_valid_or_false
from utils.utils_normalization import _authors_valid, _genres_valid, _review_lang_valid, is_non_empty_string, is_positive_number, is_valid_language_bcp47, is_valid_url, normalize_currency_code, normalize_gb_date, normalize_language, normalize_price, normalize_pub_info_to_date


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


def _rating_valid(x: Any) -> bool:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return False
    try:
        v = float(x)
    except Exception:
        return False
    return 0.0 <= v <= 5.0


def apply_validation_rules(
    df: pd.DataFrame,
    rules: Dict[str, Tuple[str, Callable[[Any], bool]]],
    prefix: str,
) -> pd.DataFrame:
    """
    Aplica reglas de validación a columnas y crea flags booleanos.

    rules: dict con
        key   -> nombre lógico del flag (sin prefijo),
        value -> (nombre_columna, funcion_validadora)

    prefix: se antepone al nombre lógico del flag, por ejemplo 'q_gr_'.
    """
    df = df.copy()
    for flag_name, (col, func) in rules.items():
        full_flag = f"{prefix}{flag_name}"
        if col not in df.columns:
            df[full_flag] = False
        else:
            df[full_flag] = df[col].apply(func)

    return df

# ---------------------------------------------------------------------
# Validaciones específicas para GOODREADS
# ---------------------------------------------------------------------


def validate_goodreads_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()
    required_cols = [
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

    # --- reglas genéricas de validación (una sola vez) ---
    rules = {
        "title_valid": ("title", is_non_empty_string),
        "url_valid": ("url", is_valid_url),
        "authors_valid": ("authors", _authors_valid),
        "rating_valid": ("rating_value", _rating_valid),
        "language_not_null": ("language", is_non_empty_string),
        "rating_count_valid": ("rating_count", lambda x: is_positive_number(x, allow_zero=True)),
        "review_count_valid": ("review_count", lambda x: is_positive_number(x, allow_zero=True)),
        "num_pages_valid": (
            "num_pages", lambda x: is_positive_number(x, allow_zero=False)
        ),
        "isbn13_not_null": ("isbn13", pd.notna),
        "isbn13_valid": ("isbn13", isbn13_valid_or_false),
        "review_by_lang_valid": ("review_count_by_lang", _review_lang_valid),
        "genres_valid": ("genres", _genres_valid)
    }
    if "price" in df.columns:
        df["q_gb_price_not_null"] = df["price"].apply(
            lambda x: is_positive_number(x, allow_zero=True) or pd.isna(x)
        )
    else:
        df["q_gb_price_not_null"] = False

    df = apply_validation_rules(df, rules, prefix="q_gr_")

    # --- métricas ---
    metrics: Dict[str, Any] = {}
    metrics["goodreads_rows"] = int(len(df))
    metrics["goodreads_pct_title_not_null"] = float(
        df["q_gr_title_valid"].mean())
    metrics["goodreads_pct_isbn13_not_null"] = float(
        df["q_gr_isbn13_not_null"].mean())
    metrics["goodreads_pct_isbn13_valid"] = float(
        df["q_gr_isbn13_valid"].mean())

    metrics["goodreads_pct_language_not_null"] = float(
        df["q_gr_language_not_null"].mean())

    metrics["goodreads_nulls"] = null_ratio(
        df, ["title", "isbn13",
             "language", "num_pages", "price"]
    )

    return df, metrics


# ---------------------------------------------------------------------
# Validaciones específicas para GOOGLE BOOKS
# ---------------------------------------------------------------------

def validate_googlebooks_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    df = df.copy()

    required_cols = [
        "isbn13",
        "url",
        "title",
        "authors",
        "language",
    ]
    check_required_columns(df, required_cols, dataset_name="googlebooks")

    rules = {
        "title_valid": ("title", is_non_empty_string),
        "url_valid": ("url", is_valid_url),
        "authors_not_null": ("authors", is_non_empty_string),
        "language_valid": ("language", is_valid_language_bcp47),
        "isbn13_not_null": ("isbn13", pd.notna),
        "isbn13_valid": ("isbn13", isbn13_valid_or_false),
        "num_pages_valid": (
            "num_pages", lambda x: is_positive_number(x, allow_zero=False)
        ),
    }

    df = apply_validation_rules(df, rules, prefix="q_gb_")
    if "price" in df.columns:
        df["q_gb_price_not_null"] = df["price"].apply(
            lambda x: is_positive_number(x, allow_zero=True) or pd.isna(x)
        )
    else:
        df["q_gb_price_not_null"] = False
    metrics: Dict[str, Any] = {}
    metrics["googlebooks_rows"] = int(len(df))
    metrics["googlebooks_pct_title_not_null"] = float(
        df["q_gb_title_valid"].mean())
    metrics["googlebooks_pct_isbn13_not_null"] = float(
        df["q_gb_isbn13_not_null"].mean())
    metrics["googlebooks_pct_isbn13_valid"] = float(
        df["q_gb_isbn13_valid"].mean())

    metrics["googlebooks_nulls"] = null_ratio(
        df, ["title", "isbn13",
             "language", "num_pages", "price"]
    )

    return df, metrics


def safe_apply(df, col, func):
    if col in df:
        df[col] = df[col].apply(lambda x: func(x) if pd.notna(x) else None)


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df["publication_date"] = df["publication_date"].astype("string")
    safe_apply(df, "publication_date", normalize_gb_date)
    safe_apply(df, "pub_info", normalize_pub_info_to_date)
    safe_apply(df, "current", normalize_currency_code)
    safe_apply(df, "price", normalize_price)
    safe_apply(df, "language", normalize_language)
    return df
