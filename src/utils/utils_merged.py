import math
from typing import Any, Dict, List, Optional
import pandas as pd

from utils.utils_normalization import _first_author_norm, _norm_text, clean, clean_number, is_non_empty_string, normalize_language, to_list


def pick_number(val_gr: float | int, val_gb:  float | int,) -> float | int:
    if val_gr > val_gb:
        return val_gr
    return val_gb


def pick_number(val_gr: Any, val_gb: Any) -> Optional[float]:
    """
    Devuelve el número mayor entre los dos, ignorando nulos.
    Si ambos son nulos, devuelve None.
    """
    n_gr = clean_number(val_gr)
    n_gb = clean_number(val_gb)

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
    if row_gb is None:
        source_winner = "goodreads"
    else:
        source_winner = "merged"
    merged["source_winner"] = source_winner
    # ID / isbn13
    
    isbn13 = row_gr.get("isbn13")
    if row_gb is not None and pd.notna(row_gb.get("isbn13")):
        isbn13 = row_gb.get("isbn13") or isbn13

    
    merged["isbn13"] = isbn13
    
    merged["id"] = str(isbn13)
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
      - si hay isbn13 en Goodreads y existe en Google Books → merge por isbn13
      - si isbn13 en Goodreads es null → intentar match por (title, author)
      - si no se encuentra nada → se conserva solo Goodreads
    """

    df_gr = df_gr.copy()
    df_gb = df_gb.copy()
    # -----------------------------
    # Índice rápido por isbn13
    # -----------------------------
    df_gb_isbn = df_gb.set_index("isbn13", drop=False)

    # -----------------------------
    # Índice auxiliar por (title_norm, author_norm)
    # para el caso en que isbn13 de Goodreads sea null
    # -----------------------------
    df_gb["title_norm"] = df_gb["title"].apply(_norm_text)
    df_gb["author_norm"] = df_gb["authors"].apply(_first_author_norm)

    # diccionario {(title_norm, author_norm) -> fila de Google Books}
    # si hay duplicados, nos quedamos con la primera ocurrencia
    gb_by_title_author: Dict[tuple, pd.Series] = {}
    for _, row in df_gb.iterrows():
        key = (row["title_norm"], row["author_norm"])
        if key not in gb_by_title_author:
            gb_by_title_author[key] = row

    merged_records: List[Dict[str, Any]] = []

    for _, row_gr in df_gr.iterrows():
        row_gb = None
        isbn = row_gr.get("isbn13")

        # 1) Intento por isbn13, si viene
        if pd.notna(isbn):
            if isbn in df_gb_isbn.index:
                gb_rows = df_gb_isbn.loc[isbn]
                if isinstance(gb_rows, pd.DataFrame):
                    row_gb = gb_rows.iloc[0]
                else:
                    row_gb = gb_rows

        # 2) Si no hay isbn13 en GR o no se encontró en GB, probamos por título+autor
        if row_gb is None:
            title_norm = _norm_text(row_gr.get("title"))
            author_norm = _first_author_norm(row_gr.get("authors"))
            key = (title_norm, author_norm)
            if key in gb_by_title_author:
                row_gb = gb_by_title_author[key]

        # 3) Merge con la lógica que ya tienes
        if row_gr["isbn13"]==None:
            row_gr["isbn13"]=row_gb["isbn13"]
        
        merged = merge_book_rows(row_gr, row_gb)
        merged_records.append(merged)

    df_merged = pd.DataFrame(merged_records)
    return df_merged
