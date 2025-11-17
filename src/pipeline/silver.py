from typing import Any, Dict, Tuple

import pandas as pd
from pipeline.bronze import bronze
from utils.utils_quality import validate_goodreads_df, validate_googlebooks_df


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
