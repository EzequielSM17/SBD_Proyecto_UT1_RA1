from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
import re
from typing import List, Dict, Optional

from bs4 import BeautifulSoup
import pandas as pd
import requests
import time
from models.Book import BookData
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By


from setting import BOOKS_IDS, GOOD_READS_BASE_URL, GOOD_READS_JSON_URL, LANDING_DIR, USER_AGENT


# Creamos una sesión HTTP reutilizable (más eficiente que requests.get suelto)
SESSION = requests.Session()

# Cabeceras "realistas" para parecer un navegador y evitar bloqueos básicos
SESSION.headers.update({
    "User-Agent": (
        USER_AGENT
    )
})


def make_headless_chrome():
    """
    Crea un navegador Chrome sin ventana (headless) para usar con Selenium.
    Útil cuando la web necesita ejecutar JavaScript para mostrar el contenido.
    """
    try:
        opts = Options()
        # opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")

        print("Iniciando ChromeDriver...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(
            service=service,
            options=opts
        )
        print("ChromeDriver iniciado correctamente.")
        return driver

    except Exception as e:
        print("ERROR AL CREAR CHROMEDRIVER:", e)
        raise


def parse_basic(html: str, book_id: int) -> BookData:
    """
    Extrae los campos básicos directamente del HTML usando BeautifulSoup.
    OJO: si Goodreads cambia sus clases/nodos, habrá que actualizar los selectores.
    """
    soup = BeautifulSoup(html, "lxml")

    title_el = soup.find(class_="Text Text__title1")
    title = title_el.get_text(strip=True) if title_el else None

    authors = [a.get_text(strip=True)
               for a in soup.find_all(class_="ContributorLink__name")]

    rating_el = soup.find(class_="RatingStatistics__rating")
    rating_value = float(rating_el.get_text(strip=True)) if rating_el else None

    desc_el = soup.find(class_="DetailsLayoutRightParagraph__widthConstrained")
    desc = desc_el.get_text(" ", strip=True) if desc_el else None

    pub_info_el = soup.find("p", {"data-testid": "publicationInfo"})
    pub_info = pub_info_el.get_text(" ", strip=True) if pub_info_el else None

    cover_el = soup.find(class_="ResponsiveImage")
    cover = cover_el.get("src") if cover_el else None

    rating_count = None
    review_count = None
    m1 = re.search(r'"ratingCount":(\d+)', html)
    if m1:
        rating_count = int(m1.group(1))
    m2 = re.search(r'"reviewCount":(\d+)', html)
    if m2:
        review_count = int(m2.group(1))

    review_count_by_lang = {}
    lang_matches = re.findall(
        r'"count":(\d+),"isoLanguageCode":"([a-z]{2})"', html)
    for count, lang in lang_matches:
        review_count_by_lang[lang] = review_count_by_lang.get(
            lang, 0) + int(count)

    genres = []
    try:
        genres_block = re.findall(
            r'"bookGenres":.*?}}],"details":', html, flags=re.DOTALL)[0]
        genres_block = genres_block.rstrip(',"details":')
        genres_json = json.loads("{" + genres_block + "}")
        genres = [g["genre"]["name"]
                  for g in genres_json.get("bookGenres", []) if g.get("genre")]
    except Exception:
        pass
    edition_details = soup.find(
        class_="EditionDetails")
    sell_button = soup.find_all(
        class_="Button__container Button__container--block")
    text_price = sell_button[1].find(class_="Button__labelItem")
    try:
        price = text_price.text.split("$")[1]
        current = "USD"
    except Exception:
        price = None
        current = None

    extra_data = edition_details.find_all(
        class_="TruncatedContent__text TruncatedContent__text--small")
    new_extra_data = []
    for item in extra_data:
        texto = str(item.get_text())

        new_extra_data.append(texto)
    try:
        if 5 == len(new_extra_data):
            if "," in new_extra_data[0]:
                format = new_extra_data[0].split(",")[1]
                num_pages = int(new_extra_data[0].split(",")[0].split(" ")[0])
            else:
                format = new_extra_data[0]
                num_pages = None
            format = new_extra_data[0]

            publisher = new_extra_data[1]
            isbns = new_extra_data[2].split(" ")
            isbn13 = int(isbns[0])
            isbn = isbns[2].replace(")", "")
            language = new_extra_data[4]
        elif 4 == len(new_extra_data):
            if "," in new_extra_data[0]:
                format = new_extra_data[0].split(",")[1]
                num_pages = int(new_extra_data[0].split(",")[0].split(" ")[0])
            else:
                format = new_extra_data[0]
                num_pages = None
            format = new_extra_data[0]

            publisher = new_extra_data[1]
            isbn13 = None
            isbn = None
            language = new_extra_data[3]
        elif 3 == len(new_extra_data):
            if "," in new_extra_data[0]:
                format = new_extra_data[0].split(",")[1]
                num_pages = int(new_extra_data[0].split(",")[0].split(" ")[0])
            else:
                format = new_extra_data[0]
                num_pages = None
            format = new_extra_data[0]

            publisher = new_extra_data[1]
            isbn13 = None
            isbn = None
            language = new_extra_data[2]
    except Exception as e:
        print("Error parsing extra data:", e)
    bd = BookData(
        id=book_id, url=f"{GOOD_READS_BASE_URL}{book_id}", title=title, authors=set(authors),
        rating_value=rating_value, desc=desc, pub_info=pub_info, cover=cover,
        review_count_by_lang=review_count_by_lang, genres=genres, publisher=publisher,
        rating_count=rating_count, review_count=review_count, isbn=isbn, format=format,
        language=language, num_pages=num_pages, isbn13=isbn13, price=price, current=current
    )

    return bd


def fetch_book_html_selenium(book_id: int) -> Optional[str]:
    """
    Carga la página con Selenium (más lento, pero ejecuta JS) y devuelve el HTML.
    Usa Selenium solo si Requests no trae lo necesario.
    """

    driver = make_headless_chrome()
    try:
        driver.get(f"{GOOD_READS_BASE_URL}{book_id}")
        boton = driver.find_element(
            By.XPATH,
            "//button[@aria-label='Book details and editions']")
        boton.click()
        time.sleep(2.0)
        html = driver.page_source
        return html
    finally:
        driver.quit()


def get_book(book_id: int) -> BookData:
    """
    Orquestador:
    1) Prueba Requests (salvo que prefer_selenium=True).
    2) Si falla/no hay HTML, usa Selenium.
    3) Parsea básicos + detalles y hace limpiezas al final.
    """

    html = fetch_book_html_selenium(book_id)
    bd = parse_basic(html, book_id)

    return bd


def parse_reviews_from_html(html: str) -> List[Dict]:
    """
    Extrae reseñas individuales desde el HTML de la página del libro.
    Goodreads tiene varios layouts, por eso probamos distintos selectores.
    Devuelve: lista de dicts con {user, date, rating, text}.
    """
    soup = BeautifulSoup(html, "lxml")
    reviews = []

    # Bloques de reseña: probamos dos variantes comunes
    candidates = soup.select(
        '[data-testid="review"]') or soup.select(".ReviewCard")

    for c in candidates:
        text_el = c.select_one(
            '[data-testid="reviewText"]') or c.select_one(".ReviewText__content")
        text = text_el.get_text(" ", strip=True) if text_el else None
        user_el = c.select_one(
            '[data-testid="reviewAuthor"] a') or c.select_one(".ReviewerProfile__name a")
        user = user_el.get_text(strip=True) if user_el else None
        date_el = c.select_one(
            '[data-testid="reviewDate"]') or c.select_one("time")
        date = date_el.get_text(strip=True) if date_el else None

        rating_val = None
        star = c.select_one(
            '[aria-label*="rating"]') or c.select_one('[title*="rating"]')
        if star and star.has_attr("aria-label"):
            m = re.search(r"(\d+(?:\.\d+)?)", star["aria-label"])
            if m:
                rating_val = float(m.group(1))
        elif star and star.has_attr("title"):
            m = re.search(r"(\d+(?:\.\d+)?)", star["title"])
            if m:
                rating_val = float(m.group(1))
        if text:
            reviews.append({"user": user, "date": date,
                           "rating": rating_val, "text": text})

    return reviews


def get_reviews(book_id: int, max_pages: int = 3, delay: float = 1.0) -> List[Dict]:
    """
    Descarga reseñas de varias páginas (?page=2, ?page=3, ...).
    - max_pages: cuántas páginas intentar como máximo.
    - delay: pausa entre peticiones para no saturar el servidor.
    Heurística: si una página no trae resultados (y no es la primera), se detiene.
    """
    out = []

    for page in range(1, max_pages + 1):
        url = f"{GOOD_READS_BASE_URL}{book_id}?page={page}"
        r = SESSION.get(url, timeout=30)
        if r.status_code != 200:
            break  # si falla la petición, paramos
        out.extend(parse_reviews_from_html(r.text))
        time.sleep(delay)

        if page > 1 and len(out) == 0:
            break

    return out


def process_one(book_id: int, with_reviews=True) -> BookData:
    """
    Descarga y parsea un único libro.
    Si with_reviews=True, también intenta traer sus reseñas.
    """
    bd = get_book(book_id)
    if with_reviews:
        try:
            bd.comments = get_reviews(book_id, max_pages=3)  # mejor esfuerzo
        except Exception as e:
            bd.comments = []  # si falla, no rompemos el flujo
    return bd


def process_many(book_ids: List[int], max_workers: int = 8, with_reviews=True) -> List[BookData]:
    """
    Procesa muchos libros en paralelo usando un pool de hilos (ThreadPoolExecutor).
    - book_ids: lista de IDs de libros de Goodreads.
    - max_workers: cuántos hilos simultáneos (más hilos = más rápido, pero más carga).
    - with_reviews: si también se descargan reseñas por cada libro.
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(process_one, bid, with_reviews): bid for bid in book_ids}

        for fut in as_completed(futs):
            bid = futs[fut]
            try:
                resultado = fut.result()
                results.append(resultado)
            except Exception as e:
                print("Error procesando libro",
                      f"{GOOD_READS_BASE_URL}{bid}")

    return results


if __name__ == "__main__":
    sample_ids = BOOKS_IDS
    books = process_many(sample_ids, max_workers=4, with_reviews=True)
    df = pd.DataFrame(books)

    os.makedirs(LANDING_DIR, exist_ok=True)
    df.to_json(GOOD_READS_JSON_URL, orient="records",
               force_ascii=False, indent=2)
