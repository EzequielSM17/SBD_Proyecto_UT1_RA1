import time
import pandas as pd
import requests
from dataclasses import asdict
from typing import Optional
from models.Book import BookData  # o desde donde tengas tu dataclass

GOOGLE_BOOKS_API_URL = "https://www.googleapis.com/books/v1/volumes"
# si tienes key, ponla aquí; si no, déjala en None
API_KEY: Optional[str] = None


def fetch_book_from_google(isbn: str) -> BookData:
    params = {"q": f"isbn:{isbn}"}
    if API_KEY:
        params["key"] = API_KEY

    r = requests.get(GOOGLE_BOOKS_API_URL, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()

    # Si no hay resultados, devolvemos un BookData mínimo
    if "items" not in data or not data["items"]:
        return BookData(
            id=isbn,
            url="",
            isbn=isbn,
        )
    if len(data["items"]) > 1:
        item = data["items"][1]
    else:
        item = data["items"][0]
    volume_id = item.get("id", isbn)
    info = item.get("volumeInfo", {})

    title = info.get("title")
    authors = info.get("authors", []) or []

    desc = info.get("description")
    publisher = info.get("publisher")
    # suele ser 'YYYY', 'YYYY-MM' o 'YYYY-MM-DD'
    published_date = info.get("publishedDate")
    page_count = info.get("pageCount")
    categories = info.get("categories", []) or []
    language = info.get("language")

    image_links = info.get("imageLinks", {}) or {}
    cover = image_links.get("thumbnail") or image_links.get("smallThumbnail")

    # Google Books a veces tiene estos campos
    rating_value = info.get("averageRating")
    rating_count = info.get("ratingsCount")

    # ISBNs: vienen en 'industryIdentifiers'
    isbn_10 = None
    isbn_13 = None
    for ident in info.get("industryIdentifiers", []):
        if ident.get("type") == "ISBN_10":
            isbn_10 = ident.get("identifier")
        elif ident.get("type") == "ISBN_13":
            isbn_13 = ident.get("identifier")

    # Construimos el BookData usando tu dataclass
    bd = BookData(
        id=volume_id,
        url=f"https://books.google.com/books?id={volume_id}",
        title=title,
        authors=authors,
        rating_value=rating_value,
        desc=desc,
        pub_info=None,  # aquí podrías guardar published_date crudo si quieres
        cover=cover,
        format=None,  # Google Books no siempre da formato de forma clara
        num_pages=page_count,
        publication_timestamp=None,  # no lo da como timestamp, solo fecha en string
        publication_date=published_date,
        publisher=publisher,
        isbn=isbn_13 or isbn_10 or isbn,  # priorizamos ISBN13
        isbn13=isbn_13,
        language=language,
        review_count_by_lang={},  # Google Books no separa por idioma
        genres=categories,
        rating_count=rating_count,
        review_count=None,
        comments=[],
    )

    return bd


def process_isbns_to_csv(json_path: str, csv_output: str) -> None:
    json_df = pd.read_json(json_path)
    print(f"Encontrados {len(json_df)} ISBNs en {json_path}")
    json_df["isbn13"] = json_df["isbn13"].astype(int)
    books: list[BookData] = []

    for isbn in json_df["isbn13"]:
        try:
            bd = fetch_book_from_google(isbn)
            time.sleep(2)  # para no saturar la API
            books.append(bd)
        except Exception as e:
            print(f"Error con ISBN {isbn}")
            # guardamos algo mínimo para no perder la fila

    # Convertimos lista de BookData -> lista de dicts -> DataFrame
    df = pd.DataFrame(books)

    df.to_csv(csv_output, index=False, encoding="utf-8")
    print(f"CSV guardado en: {csv_output}")


if __name__ == "__main__":
    json_input = "landing/goodreads_books.json"             # tu fichero con ISBNs
    csv_output = "landing/google_books_data.csv"  # salida

    process_isbns_to_csv(json_input, csv_output)
