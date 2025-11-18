from typing import Any, Dict, Tuple

import pandas as pd
from pipeline.bronze import bronze
from utils.utils_quality import normalize_dataframe, validate_goodreads_df, validate_googlebooks_df


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
    google_normalize = normalize_dataframe(google_bronze)
    goodreads_normalize = normalize_dataframe(google_bronze)
    google_silver, metrics_gb = validate_googlebooks_df(google_normalize)
    goodreads_silver, metrics_gr = validate_goodreads_df(goodreads_normalize)

    metadata["google_books_quality"] = metrics_gb
    metadata["goodreads_quality"] = metrics_gr

    assert (
        metrics_gr["goodreads_pct_title_not_null"] >= 0.90
    ), f"Goodreads: solo {metrics_gr['goodreads_pct_title_not_null']:.2%} títulos no nulos"

    if metrics_gr["goodreads_pct_isbn13_not_null"] > 0:
        assert (
            metrics_gr["goodreads_pct_isbn13_valid"] >= 0.80
        ), (
            "Goodreads: calidad de isbn13 demasiado baja "
            f"({metrics_gr['goodreads_pct_isbn13_valid']:.2%} válidos)"
        )

    assert (
        metrics_gb["googlebooks_pct_title_not_null"] >= 0.90
    ), (
        "Google Books: porcentaje de títulos no nulos < 90% "
        f"({metrics_gb['googlebooks_pct_title_not_null']:.2%})"
    )

    google_silver["q_record_valid"] = (
        google_silver["q_gb_title_valid"]
        & google_silver["q_gb_isbn13_valid"]
        & google_silver["q_gb_language_valid"]
    )

    goodreads_silver["q_record_valid"] = (
        goodreads_silver["q_gr_title_valid"]
        & goodreads_silver["q_gr_isbn13_valid"]
        & goodreads_silver["q_gr_rating_valid"]
    )

    metadata["google_books_quality"]["rows_valid"] = int(
        google_silver["q_record_valid"].sum()
    )
    metadata["goodreads_quality"]["rows_valid"] = int(
        goodreads_silver["q_record_valid"].sum()
    )

    return google_silver, goodreads_silver, metadata
