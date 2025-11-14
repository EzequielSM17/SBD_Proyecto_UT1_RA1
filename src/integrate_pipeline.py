from typing import Tuple
import pandas as pd


def bronze() -> Tuple[pd.DataFrame, pd.DataFrame]:
    google_dataset = pd.read_csv("landing/google_books_data.csv")
    good_read_dataset = pd.read_json("landing/goodreads_books.json")
    # Anota metadatos en cada dataset como registrar fuente, fecha de ingesta, esquema detectado, recuentos de filas/columnas, tamaños.
    ts_now = pd.Timestamp.now(tz="ISO8601")

    google_dataset['_source'] = "google_books_data.csv"
    good_read_dataset['_source'] = "goodreads_books.json"
    google_dataset['_ingest_ts'] = ts_now
    good_read_dataset['_ingest_ts'] = ts_now
