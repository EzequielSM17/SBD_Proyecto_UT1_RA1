import os
from typing import Any, Dict, Tuple

import pandas as pd

from setting import GOOD_READS_JSON_URL, GOOGLE_CSV_URL


def bronze() -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    # Leer archivos
    google_dataset = pd.read_csv(GOOGLE_CSV_URL)
    good_read_dataset = pd.read_json(GOOD_READS_JSON_URL)

    ts_now = pd.Timestamp.now(tz="UTC")
    good_read_dataset["isbn13"] = good_read_dataset["isbn13"].astype("Int64")
    good_read_dataset["num_pages"] = good_read_dataset["num_pages"].astype(
        "Int64")
    # Añadir columnas de metadatos a los dataframes
    google_dataset["_source"] = "googlebooks_books.csv"
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
            "file_size_bytes": os.path.getsize(GOOGLE_CSV_URL),
        },
        "goodreads": {
            "file": "goodreads_books.json",
            "ingest_ts": str(ts_now),
            "rows": int(len(good_read_dataset)),
            "columns": list(good_read_dataset.columns),
            "num_columns": len(good_read_dataset.columns),
            "dtypes": good_read_dataset.dtypes.astype(str).to_dict(),
            "file_size_bytes": os.path.getsize(GOOD_READS_JSON_URL),
        },
    }

    return google_dataset, good_read_dataset, metadata
