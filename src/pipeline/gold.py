
import json
from pathlib import Path

import pandas as pd

from const.prevenance import PROVENANCE
from pipeline.silver import silver
from setting import BOOKS_DETAIL_URL, DIM_BOOK_URL, DOCS_DIR, QUALITY_JSON_URL, STANDARD_DIR
from utils.utils_merged import merge_books
from utils.utils_normalization import generate_stable_book_id, normalize_columns_snake_case

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

    # prioridad de fuentes (para supervivencia)

    all_sources = pd.concat([google, goodreads], ignore_index=True)
    cols_to_drop = [c for c in all_sources.columns if c.startswith("q_")]
    all_sources = all_sources.drop(columns=cols_to_drop)
    all_sources["book_id"] = all_sources.apply(
        lambda r: generate_stable_book_id(
            r["isbn13"],
            r["title"],
            r["publisher"],
            r["publication_date"]
        ),
        axis=1,
    )

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
    dim_book["book_id"] = all_sources.apply(
        lambda r: generate_stable_book_id(
            r["isbn13"],
            r["title"],
            r["publisher"],
            r["publication_date"]
        ),
        axis=1,
    )
    dim_book = dim_book.drop_duplicates(
        subset=["isbn13"], keep="first")

    metadata["integration"] = {
        "dim_book_rows": int(len(dim_book)),
        "book_source_detail_rows": int(len(all_sources)),
        "distinct_book_ids": int(dim_book["isbn13"].nunique()),
        "duplicates_groups": int(
            all_sources["isbn13"].value_counts().gt(1).sum()
        ),
    }
    dim_book["provenance"] = json.dumps(PROVENANCE)
    STANDARD_DIR.mkdir(exist_ok=True)
    all_sources.drop(columns=["id"], inplace=True)
    dim_book.drop(columns=["id"], inplace=True)
    dim_book.to_parquet(DIM_BOOK_URL,
                        index=False, engine="pyarrow")

    all_sources.to_parquet(BOOKS_DETAIL_URL, index=False, engine="pyarrow")

    DOCS_DIR.mkdir(exist_ok=True)
    with open(QUALITY_JSON_URL, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
