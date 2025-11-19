from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()  # carga variables desde .env automáticamente


BOOKS_IDS = [id_book for id_book in range(50, 80)]
BASE_DIR = Path(__file__).resolve().parents[1]
GOOD_READS_BASE_URL = os.getenv("GOOD_READS_BASE_URL")
USER_AGENT = os.getenv("USER_AGENT")
GOOGLE_BOOKS_API_URL = os.getenv("GOOGLE_BOOKS_API_URL")
STANDARD_DIR = BASE_DIR/"standard"
LANDING_DIR = BASE_DIR/"landing"
DOCS_DIR = BASE_DIR/"docs"
DIM_BOOK_URL = STANDARD_DIR/"dim_book.parquet"
BOOKS_DETAIL_URL = STANDARD_DIR/"book_source_detail.parquet"
GOOD_READS_JSON_URL = LANDING_DIR/"goodreads_books.json"
GOOGLE_CSV_URL = LANDING_DIR/"googlebooks_books.csv"
SCHEMA_URL = DOCS_DIR/"schema.md"
QUALITY_JSON_URL = DOCS_DIR/"quality_metrics.json"
SELENIUM = False  # Cambia False si quieres playwright
