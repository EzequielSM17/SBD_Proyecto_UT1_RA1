from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class BookData:
    # ID del libro (parte final de la URL en Goodreads)
    id: str
    url: str                        # URL completa a la ficha del libro

    title: Optional[str] = None     # Título del libro
    # Autores (pueden ser varios)
    authors: List[str] = field(default_factory=list)

    rating_value: Optional[float] = None  # Nota media (ej. 4.2)
    desc: Optional[str] = None             # Descripción/resumen del libro
    # Texto crudo de publicación (para parsear luego)
    pub_info: Optional[str] = None
    cover: Optional[str] = None            # URL de la imagen de portada

    # Formato (Hardcover, Paperback, Kindle, ...)
    format: Optional[str] = None
    num_pages: Optional[int] = None  # Número de páginas

    # Fechas de publicación: timestamp en ms (crudo) y fecha legible (YYYY-MM-DD)
    publication_timestamp: Optional[int] = None
    publication_date: Optional[str] = None

    publisher: Optional[str] = None  # Editorial
    isbn: Optional[str] = None       # Código ISBN
    isbn13: Optional[str] = None     # Código ISBN-13
    language: Optional[str] = None   # Idioma (ej. "English")

    # Reseñas por idioma, p.ej. {"en": 120, "es": 15}
    review_count_by_lang: Dict[str, int] = field(default_factory=dict)

    genres: List[str] = field(default_factory=list)

    rating_count: Optional[int] = None
    review_count: Optional[int] = None

    comments: List[Dict] = field(default_factory=list)
