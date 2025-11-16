import json
import os
from pathlib import Path
from typing import Tuple, Dict, Any

import numpy as np
import pandas as pd

from utils.utils_quality import (
    validate_goodreads_df,
    validate_googlebooks_df,
)
from utils.utils_normalization import (
    normalize_columns_snake_case,
    normalize_pub_date_to_iso,
    normalize_language,
    normalize_currency,
    normalize_price,
    generate_stable_book_id,
)


BASE_DIR = Path(__file__).resolve().parents[1]


def bronze() -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    # Leer archivos
    google_dataset = pd.read_csv("landing/google_books_data.csv")
    good_read_dataset = pd.read_json("landing/goodreads_books.json")

    ts_now = pd.Timestamp.now(tz="UTC")

    # Añadir columnas de metadatos a los dataframes
    google_dataset["_source"] = "google_books_data.csv"
    google_dataset["_ingest_ts"] = ts_now

    good_read_dataset["_source"] = "goodreads_books.json"
    good_read_dataset["_ingest_ts"] = ts_now

    # ---------------------------
    # 📌 METADATOS (3.2)
    # ---------------------------

    metadata: Dict[str, Any] = {
        "google_books": {
            "file": "google_books_data.csv",
            "ingest_ts": str(ts_now),
            "rows": int(len(google_dataset)),
            "columns": list(google_dataset.columns),
            "num_columns": len(google_dataset.columns),
            "dtypes": google_dataset.dtypes.astype(str).to_dict(),
            "file_size_bytes": os.path.getsize("landing/google_books_data.csv"),
        },
        "goodreads": {
            "file": "goodreads_books.json",
            "ingest_ts": str(ts_now),
            "rows": int(len(good_read_dataset)),
            "columns": list(good_read_dataset.columns),
            "num_columns": len(good_read_dataset.columns),
            "dtypes": good_read_dataset.dtypes.astype(str).to_dict(),
            "file_size_bytes": os.path.getsize("landing/goodreads_books.json"),
        },
    }

    return google_dataset, good_read_dataset, metadata


def silver() -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    """
    Capa SILVER (3.3 Chequeos de calidad):

    - Aplica validaciones de calidad a los datasets bronze.
    - Añade columnas de flags (q_*) a cada dataframe.
    - Calcula métricas agregadas y aserciones bloqueantes.
    - Devuelve:
      google_silver, goodreads_silver, metadata_actualizada
    """

    google_bronze, goodreads_bronze, metadata = bronze()

    # ---------------------------
    # 1) Validar con utils_quality
    # ---------------------------

    google_silver, metrics_gb = validate_googlebooks_df(google_bronze)
    goodreads_silver, metrics_gr = validate_goodreads_df(goodreads_bronze)

    # Guardamos métricas de calidad dentro de metadata
    metadata["google_books_quality"] = metrics_gb
    metadata["goodreads_quality"] = metrics_gr

    # ---------------------------
    # 2) Aserciones bloqueantes (ejemplo)
    #    Ajusta los umbrales si quieres
    # ---------------------------

    # Goodreads: al menos 90% de títulos no nulos
    assert (
        metrics_gr["goodreads_pct_title_not_null"] >= 0.90
    ), f"Goodreads: solo {metrics_gr['goodreads_pct_title_not_null']:.2%} títulos no nulos"

    # Goodreads: al menos 80% de isbn13 válidos (si tu scraping lo permite)
    if metrics_gr["goodreads_pct_isbn13_not_null"] > 0:
        assert (
            metrics_gr["goodreads_pct_isbn13_valid"] >= 0.80
        ), (
            "Goodreads: calidad de isbn13 demasiado baja "
            f"({metrics_gr['goodreads_pct_isbn13_valid']:.2%} válidos)"
        )

    # Google Books: al menos 90% de títulos no nulos
    assert (
        metrics_gb["googlebooks_pct_title_not_null"] >= 0.90
    ), (
        "Google Books: porcentaje de títulos no nulos < 90% "
        f"({metrics_gb['googlebooks_pct_title_not_null']:.2%})"
    )

    # Google Books: si tienes fechas normalizadas, exige un % mínimo válidas
    # (puedes relajar si aún no has normalizado a YYYY-MM-DD)
    if metrics_gb["googlebooks_rows"] > 0:
        # ejemplo: exigimos que al menos el 50% tengan fecha válida
        assert (
            metrics_gb["googlebooks_pct_pub_date_valid"] >= 0.50
        ), (
            "Google Books: demasiadas fechas no válidas "
            f"({metrics_gb['googlebooks_pct_pub_date_valid']:.2%})"
        )

    # ---------------------------
    # 3) Flag de "registro válido" por fila (opcional pero útil)
    # ---------------------------

    # Google Books: registro válido si pasan checks básicos
    google_silver["q_record_valid"] = (
        google_silver["q_gb_title_valid"]
        & google_silver["q_gb_isbn13_valid"]
        & google_silver["q_gb_language_valid"]
    )

    # Goodreads: registro válido si pasan checks básicos
    goodreads_silver["q_record_valid"] = (
        goodreads_silver["q_gr_title_valid"]
        & goodreads_silver["q_gr_isbn13_valid"]
        & goodreads_silver["q_gr_rating_valid"]
    )

    # Puedes añadir a metadata cuántos registros válidos hay
    metadata["google_books_quality"]["rows_valid"] = int(
        google_silver["q_record_valid"].sum()
    )
    metadata["goodreads_quality"]["rows_valid"] = int(
        goodreads_silver["q_record_valid"].sum()
    )

    return google_silver, goodreads_silver, metadata


BASE_DIR = Path(__file__).resolve().parents[1]


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

    # ------------------------------------------------------------------
    # 1) Obtener capa SILVER (ya con flags de calidad y metadata)
    # ------------------------------------------------------------------
    google_silver, goodreads_silver, metadata = silver()

    # ------------------------------------------------------------------
    # 2) Normalización básica y enriquecimientos ligeros
    # ------------------------------------------------------------------
    google = normalize_columns_snake_case(google_silver)
    goodreads = normalize_columns_snake_case(goodreads_silver)

    # GOOGLE: fechas, año publicación, idioma, precio, moneda
    google["pub_date_iso"] = google["pub_date"].apply(
        normalize_pub_date_to_iso)
    google["pub_year"] = google["pub_date_iso"].str.slice(0, 4)
    google["language_norm"] = google["language"].apply(normalize_language)
    google["price_amount_norm"] = google["price_amount"].apply(normalize_price)
    google["price_currency_norm"] = google["price_currency"].apply(
        normalize_currency)
    google["authors_norm"] = google["authors"].apply(_normalize_authors_google)
    if "categories" in google.columns:
        google["categories_norm"] = google["categories"].apply(
            _normalize_categories)
    else:
        google["categories_norm"] = None

    google["title_length"] = google["title"].apply(_compute_title_length)
    google["has_price"] = google["price_amount_norm"].notna()
    google["has_isbn13"] = google["isbn13"].notna()

    # GOODREADS: fecha (si tienes publication_date), año, idioma, autores, categorías
    if "publication_date" in goodreads.columns:
        goodreads["pub_date_iso"] = goodreads["publication_date"].apply(
            normalize_pub_date_to_iso)
    else:
        goodreads["pub_date_iso"] = None

    goodreads["pub_year"] = goodreads["pub_date_iso"].astype(
        str).str.slice(0, 4)
    goodreads["language_norm"] = goodreads["language"].apply(
        normalize_language)
    goodreads["authors_norm"] = goodreads["authors"].apply(
        _normalize_authors_goodreads)
    if "genres" in goodreads.columns:
        goodreads["categories_norm"] = goodreads["genres"].apply(
            _normalize_categories)
    else:
        goodreads["categories_norm"] = None

    goodreads["title_length"] = goodreads["title"].apply(_compute_title_length)
    goodreads["has_price"] = False  # no tenemos precio en Goodreads
    goodreads["has_isbn13"] = goodreads["isbn13"].notna()

    # ------------------------------------------------------------------
    # 3) Modelo canónico intermedio (vista unificada por registro)
    # ------------------------------------------------------------------

    # prioridad de fuentes (para supervivencia)
    SOURCE_PRIORITY = {
        "googlebooks": 2,
        "goodreads": 1,
    }

    google_can = pd.DataFrame({
        "source_name": "googlebooks",
        "source_priority": SOURCE_PRIORITY["googlebooks"],
        "source_file": google["_source"],
        "row_number": google.reset_index().index,  # índice original
        "title": google["title"],
        "title_length": google["title_length"],
        "authors": google["authors_norm"],
        "author_principal": google["authors_norm"].str.split("|").str[0],
        "publisher": google.get("publisher"),
        "pub_date_iso": google["pub_date_iso"],
        "pub_year": google["pub_year"],
        "isbn13": google["isbn13"],
        "isbn10": google.get("isbn10"),
        "language": google["language_norm"],
        "categories": google["categories_norm"],
        "price": google["price_amount_norm"],
        "currency": google["price_currency_norm"],
        "page_count": google.get("page_count"),
        "has_price": google["has_price"],
        "has_isbn13": google["has_isbn13"],
        "_ingest_ts": google["_ingest_ts"],
    })

    goodreads_can = pd.DataFrame({
        "source_name": "goodreads",
        "source_priority": SOURCE_PRIORITY["goodreads"],
        "source_file": goodreads["_source"],
        "row_number": goodreads.reset_index().index,
        "title": goodreads["title"],
        "title_length": goodreads["title_length"],
        "authors": goodreads["authors_norm"],
        "author_principal": goodreads["authors_norm"].str.split("|").str[0],
        "publisher": goodreads.get("publisher"),
        "pub_date_iso": goodreads["pub_date_iso"],
        "pub_year": goodreads["pub_year"],
        "isbn13": goodreads["isbn13"],
        "isbn10": goodreads.get("isbn"),
        "language": goodreads["language_norm"],
        "categories": goodreads["categories_norm"],
        "price": None,
        "currency": None,
        "page_count": goodreads.get("num_pages"),
        "has_price": goodreads["has_price"],
        "has_isbn13": goodreads["has_isbn13"],
        "_ingest_ts": goodreads["_ingest_ts"],
    })

    all_sources = pd.concat([google_can, goodreads_can], ignore_index=True)

    # ------------------------------------------------------------------
    # 4) Generar book_id canónico (isbn13 o hash)
    # ------------------------------------------------------------------
    all_sources["book_id"] = all_sources.apply(
        lambda r: generate_stable_book_id(
            r["isbn13"],
            r["title"],
            r["author_principal"],
            r["publisher"],
            r["pub_year"],
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
        "pub_date_iso",
        "isbn13",
        "language",
        "categories",
        "price",
        "currency",
        "page_count",
    ]
    all_sources["completeness_score"] = all_sources[completeness_cols].notna().sum(
        axis=1)

    def merge_group(g: pd.DataFrame) -> pd.Series:
        g = g.sort_values(
            ["source_priority", "completeness_score", "_ingest_ts"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

        base = g.iloc[0].copy()

        # merge autores (uniendo y de-duplicando)
        authors_set = set()
        for val in g["authors"]:
            if val is None or (isinstance(val, float) and np.isnan(val)):
                continue
            for a in str(val).split("|"):
                a = a.strip()
                if a:
                    authors_set.add(a)
        base["authors"] = "|".join(
            sorted(authors_set)) if authors_set else None
        base["author_principal"] = (
            sorted(authors_set)[0] if authors_set else base.get(
                "author_principal")
        )

        # merge categorías
        categories_set = set()
        for val in g["categories"]:
            if val is None or (isinstance(val, float) and np.isnan(val)):
                continue
            for c in str(val).split("|"):
                c = c.strip()
                if c:
                    categories_set.add(c)
        base["categories"] = "|".join(
            sorted(categories_set)) if categories_set else None

        # merge precio (preferimos el último no nulo de la fuente prioritaria)
        # como ya está ordenado por prioridad,completitud,ts, podemos tomar el primero no nulo
        if g["price"].notna().any():
            base["price"] = g.loc[g["price"].notna(), "price"].iloc[0]
            base["currency"] = g.loc[g["price"].notna(), "currency"].iloc[0]

        # la fuente ganadora
        base["source_winner"] = base["source_name"]

        return base

    dim_book = (
        all_sources
        .groupby("book_id", as_index=False)
        .apply(merge_group)
        .reset_index(drop=True)
    )

    # enriquecimiento extra: flags de disponibilidad
    dim_book["has_categories"] = dim_book["categories"].notna()
    dim_book["has_authors"] = dim_book["authors"].notna()

    # ------------------------------------------------------------------
    # 6) book_source_detail: detalle por fuente + flags de calidad
    # ------------------------------------------------------------------
    # Recuperamos silver con sus q_* y les añadimos book_id_candidato
    google_detail = google_silver.copy()
    google_detail = normalize_columns_snake_case(google_detail)
    google_detail["source_name"] = "googlebooks"
    google_detail["source_file"] = "google_books_data.csv"

    goodreads_detail = goodreads_silver.copy()
    goodreads_detail = normalize_columns_snake_case(goodreads_detail)
    goodreads_detail["source_name"] = "goodreads"
    goodreads_detail["source_file"] = "goodreads_books.json"

    # book_id_candidato usando la misma función que dim_book
    def _compute_candidate_id(row: pd.Series) -> str:
        return generate_stable_book_id(
            row.get("isbn13"),
            row.get("title"),
            (row.get("authors_norm") or row.get("authors")),
            row.get("publisher"),
            str(row.get("pub_year")) if "pub_year" in row else None,
        )

    # Añadimos campos auxiliares necesarios
    for df_detail in (google_detail, goodreads_detail):
        if "authors_norm" not in df_detail.columns and "authors" in df_detail.columns:
            # solo por seguridad, no pasa nada si ya está
            df_detail["authors_norm"] = df_detail["authors"]
        if "pub_year" not in df_detail.columns:
            df_detail["pub_year"] = None
        df_detail["book_id_candidato"] = df_detail.apply(
            _compute_candidate_id, axis=1)

    book_source_detail = pd.concat(
        [google_detail, goodreads_detail], ignore_index=True)

    # ------------------------------------------------------------------
    # 7) Actualizar métricas y escribir artefactos
    # ------------------------------------------------------------------
    metadata["integration"] = {
        "dim_book_rows": int(len(dim_book)),
        "book_source_detail_rows": int(len(book_source_detail)),
        "distinct_book_ids": int(dim_book["book_id"].nunique()),
        "duplicates_groups": int(
            all_sources["book_id"].value_counts().gt(1).sum()
        ),
    }

    # STANDARD: escribir Parquet
    standard_dir = BASE_DIR / "standard"
    standard_dir.mkdir(exist_ok=True)

    dim_book.to_parquet(standard_dir / "dim_book.parquet", index=False)
    book_source_detail.to_parquet(
        standard_dir / "book_source_detail.parquet", index=False)

    # DOCS: escribir quality_metrics.json
    docs_dir = BASE_DIR / "docs"
    docs_dir.mkdir(exist_ok=True)
    with open(docs_dir / "quality_metrics.json", "w", encoding="utf-8") as f:
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
    schema_text += schema_from_df(book_source_detail, "book_source_detail")

    with open(docs_dir / "schema.md", "w", encoding="utf-8") as f:
        f.write(schema_text)


if __name__ == "__main__":
    gold()
