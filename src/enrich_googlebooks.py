import time
import pandas as pd
import requests
from dataclasses import asdict
from typing import List, Optional
from models.Book import BookData
# o desde donde tengas tu dataclass
from setting import GOOD_READS_JSON_URL, GOOGLE_BOOKS_API_URL, GOOGLE_CSV_URL


def fetch_book_from_google(
    isbn: Optional[str] = None,
    title: Optional[str] = None,
    authors: Optional[List[str]] = None,
) -> BookData:
    """
    Si hay isbn -> busca por isbn
    Si no hay isbn -> busca por título + autor
    """

    if isbn:
        query = f"isbn:{isbn}"
    else:
        if not title:
            raise ValueError(
                "Necesito al menos isbn o título para buscar en Google Books")
        # Autor principal (si viene lista, cogemos el primero)
        author_part = ""
        if authors:
            principal = authors[0] if isinstance(
                authors, list) and authors else str(authors)
            author_part = f"+inauthor:{principal}"
        query = f"intitle:{title}{author_part}"

    params = {"q": query}

    r = requests.get(GOOGLE_BOOKS_API_URL, params=params, timeout=15)
    r.raise_for_status()

    data = r.json()

    if "items" not in data or not data["items"]:
        raise Exception(f"Sin resultados en Google Books para query={query!r}")

    # Cogemos el primer item
    item = data["items"][0]
    volume_id = item.get("id", isbn or title or "unknown")
    info = item.get("volumeInfo", {})

    title_gb = info.get("title")
    authors_gb = info.get("authors", []) or []

    desc = info.get("description")
    publisher = info.get("publisher")
    # 'YYYY' / 'YYYY-MM' / 'YYYY-MM-DD'
    published_date = info.get("publishedDate")
    page_count = info.get("pageCount")
    categories = info.get("categories", []) or []
    language = info.get("language")

    image_links = info.get("imageLinks", {}) or {}
    cover = image_links.get("thumbnail") or image_links.get("smallThumbnail")

    rating_value = info.get("averageRating")
    rating_count = info.get("ratingsCount")

    isbn_10 = None
    isbn_13 = None
    for ident in info.get("industryIdentifiers", []):
        if ident.get("type") == "ISBN_10":
            isbn_10 = ident.get("identifier")
        elif ident.get("type") == "ISBN_13":
            isbn_13 = ident.get("identifier")

    bd = BookData(
        id=volume_id,
        url=r.url,
        title=title_gb,
        authors=authors_gb,
        rating_value=rating_value,
        desc=desc,
        pub_info=None,
        cover=cover,
        format=None,
        num_pages=page_count,
        publication_timestamp=None,
        publication_date=published_date,
        publisher=publisher,
        isbn=isbn_10 or isbn,
        isbn13=isbn_13,
        language=language,
        review_count_by_lang={},
        genres=categories,
        rating_count=rating_count,
        review_count=None,
        comments=[],
        price=None,
    )

    return bd


def process_isbns_to_csv(json_path: str, csv_output: str) -> None:
    json_df = pd.read_json(json_path)

    books: list[BookData] = []

    for _, row in json_df.iterrows():
        isbn13 = row.get("isbn13")
        title = row.get("title")
        authors = row.get("authors")

        # normalizar isbn13 a string o None
        if pd.isna(isbn13):
            isbn13_str = None
        else:
            isbn13_str = int(isbn13)

        try:
            bd = fetch_book_from_google(
                isbn=isbn13_str if isbn13_str else None,
                title=title,
                authors=authors,
            )
            time.sleep(0.5)  # para no saturar la API
            books.append(bd)
        except Exception as e:
            print(
                f"Error buscando libro (isbn={isbn13_str}, title={title!r}): {e}")

    df = pd.DataFrame([asdict(b) for b in books])
    df.to_csv(csv_output, index=False, encoding="utf-8")


if __name__ == "__main__":

    process_isbns_to_csv(GOOD_READS_JSON_URL, GOOGLE_CSV_URL)
