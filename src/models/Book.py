from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class BookData:
    id: str
    url: str
    title: Optional[str] = None
    authors: List[str] = field(default_factory=list)
    rating_value: Optional[float] = None
    desc: Optional[str] = None
    pub_info: Optional[str] = None
    cover: Optional[str] = None
    format: Optional[str] = None
    num_pages: Optional[int] = None
    publication_date: Optional[str] = None
    publisher: Optional[str] = None
    isbn: Optional[str] = None
    isbn13: Optional[int] = None  # Primary key
    language: Optional[str] = None
    review_count_by_lang: Dict[str, int] = field(default_factory=dict)
    genres: List[str] = field(default_factory=list)
    rating_count: Optional[int] = None
    review_count: Optional[int] = None
    comments: List[Dict] = field(default_factory=list)
    price: Optional[float] = None
    current: Optional[str] = None
