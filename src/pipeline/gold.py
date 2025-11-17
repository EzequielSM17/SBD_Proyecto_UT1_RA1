
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.silver import silver
from setting import BOOKS_DETAIL_URL, DIM_BOOK_URL, DOCS_DIR, QUALITY_JSON_URL, SCHEMA_URL, STANDARD_DIR
from utils.utils_merged import merge_books
from utils.utils_normalization import _compute_title_length, _normalize_authors_goodreads, _normalize_authors_google, _normalize_categories, generate_stable_book_id, normalize_columns_snake_case, normalize_currency, normalize_language, normalize_price, normalize_pub_date_to_iso, safe_eval

BASE_DIR = Path(__file__).resolve().parents[2]


def gold() -> None:
    """
    Capa GOLD:
    - Enriquecimientos ligeros.
    - Deduplicación con reglas de supervivencia.
    - Merge de campos y emisión de artefactos:
        * standard/dim_book.parquet
        * standard/book_source_detail.parquet
        * docs/quality_metrics.json
        * docs/schema.md
    """

    google_silver, goodreads_silver, metadata = silver()

    google = normalize_columns_snake_case(google_silver)
    goodreads = normalize_columns_snake_case(goodreads_silver)

    list_cols = ["authors", "genres", "comments"]
    dict_cols = ["review_count_by_lang"]

    for col in list_cols + dict_cols:
        if col in google.columns:
            google[col] = google[col].apply(safe_eval)

    # prioridad de fuentes (para supervivencia)

    all_sources = pd.concat([google, goodreads], ignore_index=True)

    all_sources["book_id"] = all_sources.apply(
        lambda r: generate_stable_book_id(
            r["isbn13"],
            r["title"],
            r["publisher"],
            r["publication_date"]
        ),
        axis=1,
    )

    # ------------------------------------------------------------------
    # 5) Deduplicación con reglas de supervivencia
    #    - clave: book_id (isbn13 o hash compuesto)
    #    - supervivencia: fuente prioritaria, más completo, más reciente
    #    - merge de listas: autores, categorías
    # ------------------------------------------------------------------

    # completitud: número de campos no nulos en un subconjunto relevante
    completeness_cols = [
        "title",
        "authors",
        "publisher",
        "publication_date",
        "isbn13",
        "language",
        "genres",
        "num_pages",
    ]
    all_sources["completeness_score"] = all_sources[completeness_cols].notna().sum(
        axis=1)

    dim_book = merge_books(goodreads, google)

    dim_book = dim_book.drop_duplicates(
        subset=["isbn13"], keep="first")

    # ------------------------------------------------------------------
    # 7) Actualizar métricas y escribir artefactos
    # ------------------------------------------------------------------
    metadata["integration"] = {
        "dim_book_rows": int(len(dim_book)),
        "book_source_detail_rows": int(len(all_sources)),
        "distinct_book_ids": int(dim_book["isbn13"].nunique()),
        "duplicates_groups": int(
            all_sources["isbn13"].value_counts().gt(1).sum()
        ),
    }

    STANDARD_DIR.mkdir(exist_ok=True)

    dim_book.to_parquet(DIM_BOOK_URL,
                        index=False, engine="pyarrow")

    all_sources["id"] = all_sources["isbn13"]
    all_sources.to_parquet(BOOKS_DETAIL_URL, index=False, engine="pyarrow")

    DOCS_DIR.mkdir(exist_ok=True)
    with open(QUALITY_JSON_URL, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    # DOCS: escribir schema.md (simple, auto-generado)

    def schema_from_df(df: pd.DataFrame, name: str) -> str:
        lines = [f"## {name}", ""]
        lines.append("| campo | tipo |")
        lines.append("|-------|------|")
        for col, dtype in df.dtypes.items():
            lines.append(f"| {col} | {str(dtype)} |")
        lines.append("")
        return "\n".join(lines)

    schema_text = "# Esquema del modelo\n\n"
    schema_text += schema_from_df(dim_book, "dim_book")
    schema_text += "\n"
    schema_text += schema_from_df(all_sources, "book_source_detail")

    with open(SCHEMA_URL, "w", encoding="utf-8") as f:
        f.write(schema_text)
