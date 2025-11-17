import math
from typing import Any, Dict, List, Optional
import pandas as pd

from utils.utils_normalization import is_non_empty_string, normalize_language


def pick_number(val_gr: float | int, val_gb:  float | int,) -> float | int:
    if val_gr > val_gb:
        return val_gr
    return val_gb


def pick_number(val_gr: Any, val_gb: Any) -> Optional[float]:
    """
    Devuelve el número mayor entre los dos, ignorando nulos.
    Si ambos son nulos, devuelve None.
    """
    def clean(x):
        if x is None:
            return None
        if isinstance(x, float) and math.isnan(x):
            return None
        try:
            return float(x)
        except Exception:
            return None

    n_gr = clean(val_gr)
    n_gb = clean(val_gb)

    if n_gr is None and n_gb is None:
        return None
    if n_gr is None:
        return n_gb
    if n_gb is None:
        return n_gr
    return max(n_gr, n_gb)


def pick_string(val_gr: Any, val_gb: Any) -> Optional[str]:
    """
    Regla genérica para campos string:
      - si uno está vacío/nulo y el otro no → gana el no vacío
      - si los dos existen → gana el más largo
    """
    def clean(v):
        if v is None:
            return None
        if isinstance(v, float) and pd.isna(v):
            return None
        if isinstance(v, str):
            v = v.strip()
            return v or None
        return None

    s_gr = clean(val_gr)
    s_gb = clean(val_gb)

    if s_gr is None and s_gb is None:
        return None
    if s_gr is None:
        return s_gb
    if s_gb is None:
        return s_gr

    return s_gr if len(s_gr) >= len(s_gb) else s_gb


def merge_lists(list_gr: Any, list_gb: Any) -> List[str]:
    """
    Combina listas de strings:
      - acepta listas, tuplas y strings tipo 'a|b|c'
      - une ambas
      - elimina duplicados manteniendo orden
    """
    def to_list(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return []
        if isinstance(x, (list, tuple)):
            return [str(v).strip() for v in x if str(v).strip()]
        if isinstance(x, str):
            # soporta "a|b|c" o "a, b"
            if "|" in x:
                parts = x.split("|")
            else:
                parts = [x]
            return [p.strip() for p in parts if p.strip()]
        return []

    values = to_list(list_gr) + to_list(list_gb)

    seen = set()
    result: List[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            result.append(v)
    return result


def pick_language(lang_gr: Any, lang_gb: Any) -> Optional[str]:
    n_gr = normalize_language(lang_gr)
    n_gb = normalize_language(lang_gb)

    if n_gr is None and n_gb is None:
        return None
    if n_gr is None:
        return n_gb
    if n_gb is None:
        return n_gr

    # si uno es de longitud corta (2–3) y otro es más largo, preferimos el corto
    if len(n_gr) <= 3 and len(n_gb) > 3:
        return n_gr
    if len(n_gb) <= 3 and len(n_gr) > 3:
        return n_gb

    # si los dos son códigos, preferimos Google Books por defecto
    return n_gb


def merge_book_rows(row_gr: pd.Series, row_gb: Optional[pd.Series]) -> Dict[str, Any]:
    """
    Si row_gb es None → se devuelve básicamente row_gr.
    Si row_gb existe → se aplica la lógica de merge (pick_string, merge_lists, etc.)
    """

    merged: Dict[str, Any] = {}

    # ID / isbn13
    isbn13 = row_gr.get("isbn13")
    if row_gb is not None and pd.notna(row_gb.get("isbn13")):
        isbn13 = row_gb.get("isbn13") or isbn13

    merged["id"] = str(isbn13 or row_gr.get("id"))
    merged["isbn13"] = isbn13
    merged["isbn"] = row_gb.get(
        "isbn") if row_gb is not None else row_gr.get("isbn")

    # url
    merged["url"] = (
        pick_string(row_gr.get("url"), row_gb.get("url"))
        if row_gb is not None
        else row_gr.get("url")
    )

    # título
    merged["title"] = (
        pick_string(row_gr.get("title"), row_gb.get("title"))
        if row_gb is not None
        else row_gr.get("title")
    )

    # autores
    merged["authors"] = (
        merge_lists(row_gr.get("authors"), row_gb.get("authors"))
        if row_gb is not None
        else (row_gr.get("authors") or [])
    )

    # rating_value
    merged["rating_value"] = (
        pick_number(row_gr.get("rating_value"), row_gb.get("rating_value"))
        if row_gb is not None
        else row_gr.get("rating_value")
    )

    # desc
    merged["desc"] = (
        pick_string(row_gr.get("desc"), row_gb.get("desc"))
        if row_gb is not None
        else row_gr.get("desc")
    )

    # pub_info / fechas
    merged["pub_info"] = (
        pick_string(row_gr.get("pub_info"), row_gb.get("pub_info"))
        if row_gb is not None
        else row_gr.get("pub_info")
    )
    merged["publication_timestamp"] = (
        pick_number(row_gr.get("publication_timestamp"),
                    row_gb.get("publication_timestamp"))
        if row_gb is not None
        else row_gr.get("publication_timestamp")
    )
    merged["publication_date"] = (
        pick_string(row_gr.get("publication_date"),
                    row_gb.get("publication_date"))
        if row_gb is not None
        else row_gr.get("publication_date")
    )

    # cover
    merged["cover"] = (
        pick_string(row_gr.get("cover"), row_gb.get("cover"))
        if row_gb is not None
        else row_gr.get("cover")
    )

    # formato / num_pages
    merged["format"] = (
        pick_string(row_gr.get("format"), row_gb.get("format"))
        if row_gb is not None
        else row_gr.get("format")
    )
    merged["num_pages"] = (
        pick_number(row_gr.get("num_pages"), row_gb.get("num_pages"))
        if row_gb is not None
        else row_gr.get("num_pages")
    )

    # publisher
    merged["publisher"] = (
        pick_string(row_gr.get("publisher"), row_gb.get("publisher"))
        if row_gb is not None
        else row_gr.get("publisher")
    )

    # idioma
    merged["language"] = (
        pick_language(row_gr.get("language"), row_gb.get("language"))
        if row_gb is not None
        else normalize_language(row_gr.get("language"))
    )

    # review_count_by_lang
    merged["review_count_by_lang"] = row_gr.get("review_count_by_lang") or {}

    # géneros
    merged["genres"] = (
        merge_lists(row_gr.get("genres"), row_gb.get(
            "categories") or row_gb.get("genres"))
        if row_gb is not None
        else (row_gr.get("genres") or [])
    )

    # rating_count / review_count
    merged["rating_count"] = (
        pick_number(row_gr.get("rating_count"), row_gb.get("rating_count"))
        if row_gb is not None
        else row_gr.get("rating_count")
    )
    merged["review_count"] = (
        pick_number(row_gr.get("review_count"), row_gb.get("review_count"))
        if row_gb is not None
        else row_gr.get("review_count")
    )

    # comments (solo Goodreads)
    merged["comments"] = row_gr.get("comments") or []

    return merged


def merge_books(df_gr: pd.DataFrame, df_gb: pd.DataFrame) -> pd.DataFrame:
    """
    Goodreads (df_gr) es el dataset base:
      - si hay mismo isbn13 en Google Books → merge con reglas
      - si no hay isbn13 en Google Books → se conserva solo Goodreads
    """

    # Aseguramos que isbn13 se pueda buscar bien
    df_gr = df_gr.copy()
    df_gb = df_gb.copy()

    # índice por isbn13 en Google Books para lookup rápido
    df_gb_indexed = df_gb.set_index("isbn13", drop=False)

    merged_records: List[Dict[str, Any]] = []

    for _, row_gr in df_gr.iterrows():
        isbn = row_gr.get("isbn13")

        row_gb = None
        # si isbn no es nulo y existe en df_gb, lo cogemos
        if pd.notna(isbn) and isbn in df_gb_indexed.index:
            # si hay duplicados de isbn en Google Books, nos quedamos con la primera fila
            gb_rows = df_gb_indexed.loc[isbn]
            if isinstance(gb_rows, pd.DataFrame):
                row_gb = gb_rows.iloc[0]
            else:
                row_gb = gb_rows

        merged = merge_book_rows(row_gr, row_gb)
        merged_records.append(merged)

    df_merged = pd.DataFrame(merged_records)
    return df_merged
