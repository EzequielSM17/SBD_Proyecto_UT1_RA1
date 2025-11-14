from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
import datetime
import json
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

from setting import GOOD_READS_BASE_URL, USER_AGENT


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


def parse_basic(html: str, book_id: str) -> BookData:
    """
    Extrae los campos básicos directamente del HTML usando BeautifulSoup.
    OJO: si Goodreads cambia sus clases/nodos, habrá que actualizar los selectores.
    """
    soup = BeautifulSoup(html, "lxml")

    # Título (si no existe el nodo, queda en None)
    title_el = soup.find(class_="Text Text__title1")
    title = title_el.get_text(strip=True) if title_el else None

    # Autores (puede haber varios → lista de strings)
    authors = [a.get_text(strip=True)
               for a in soup.find_all(class_="ContributorLink__name")]

    # Nota media (float) si existe el elemento
    rating_el = soup.find(class_="RatingStatistics__rating")
    rating_value = float(rating_el.get_text(strip=True)) if rating_el else None

    # Descripción del libro (texto grande; unimos con espacios)
    desc_el = soup.find(class_="DetailsLayoutRightParagraph__widthConstrained")
    desc = desc_el.get_text(" ", strip=True) if desc_el else None

    # Información de publicación (texto crudo; se puede refinar después)
    pub_info_el = soup.find("p", {"data-testid": "publicationInfo"})
    pub_info = pub_info_el.get_text(" ", strip=True) if pub_info_el else None

    # URL de la imagen de portada
    cover_el = soup.find(class_="ResponsiveImage")
    cover = cover_el.get("src") if cover_el else None

    # rating_count y review_count suelen venir embebidos en JSON dentro del HTML
    rating_count = None
    review_count = None
    m1 = re.search(r'"ratingCount":(\d+)', html)  # ... "ratingCount":123
    if m1:
        rating_count = int(m1.group(1))
    m2 = re.search(r'"reviewCount":(\d+)', html)  # ... "reviewCount":45
    if m2:
        review_count = int(m2.group(1))

    # Reseñas por idioma: "count":10,"isoLanguageCode":"en"
    review_count_by_lang = {}
    lang_matches = re.findall(
        r'"count":(\d+),"isoLanguageCode":"([a-z]{2})"', html)
    for count, lang in lang_matches:
        review_count_by_lang[lang] = review_count_by_lang.get(
            lang, 0) + int(count)

    # Géneros: vienen en un bloque JSON; lo recortamos y lo parseamos
    genres = []
    try:
        genres_block = re.findall(
            r'"bookGenres":.*?}}],"details":', html, flags=re.DOTALL)[0]
        genres_block = genres_block.rstrip(',"details":')
        genres_json = json.loads("{" + genres_block + "}")
        genres = [g["genre"]["name"]
                  for g in genres_json.get("bookGenres", []) if g.get("genre")]
    except Exception:
        # Si no está o cambió, seguimos sin romper el flujo
        pass
    edition_details = soup.find(
        class_="EditionDetails")

    extra_data = edition_details.find_all(
        class_="TruncatedContent__text TruncatedContent__text--small")
    new_extra_data = []
    for item in extra_data:
        texto = str(item.get_text())

        new_extra_data.append(texto)
    try:
        if "," in new_extra_data[0]:
            format = new_extra_data[0].split(",")[1]
            num_pages = int(new_extra_data[0].split(",")[0].split(" ")[0])
        else:
            format = new_extra_data[0]
            num_pages = None
        format = new_extra_data[0]

        publisher = new_extra_data[1]
        isbns = new_extra_data[2].split(" ")
        isbn13 = isbns[0]
        isbn = isbns[2].replace(")", "")
        language = new_extra_data[4]
    except Exception as e:
        print("Error parsing extra data:", e)
    # Construimos el objeto con lo básico
    bd = BookData(
        id=book_id, url=f"{GOOD_READS_BASE_URL}{book_id}", title=title, authors=authors,
        rating_value=rating_value, desc=desc, pub_info=pub_info, cover=cover,
        review_count_by_lang=review_count_by_lang, genres=genres, publisher=publisher,
        rating_count=rating_count, review_count=review_count, isbn=isbn, format=format,
        language=language, num_pages=num_pages, isbn13=isbn13
    )

    return bd


def fetch_book_html_selenium(book_id: str) -> Optional[str]:
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
    # cerramos si lo abrimos aquí para no dejar procesos colgados


def get_book(book_id: str) -> BookData:
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
        # Texto de la reseña (unimos con espacios por si hay saltos de línea)
        text_el = c.select_one(
            '[data-testid="reviewText"]') or c.select_one(".ReviewText__content")
        text = text_el.get_text(" ", strip=True) if text_el else None

        # Nombre del usuario (enlace al perfil)
        user_el = c.select_one(
            '[data-testid="reviewAuthor"] a') or c.select_one(".ReviewerProfile__name a")
        user = user_el.get_text(strip=True) if user_el else None

        # Fecha mostrada (a veces viene en <time>)
        date_el = c.select_one(
            '[data-testid="reviewDate"]') or c.select_one("time")
        date = date_el.get_text(strip=True) if date_el else None

        # Puntuación del usuario (si aparece como aria-label o title)
        rating_val = None
        star = c.select_one(
            '[aria-label*="rating"]') or c.select_one('[title*="rating"]')
        if star and star.has_attr("aria-label"):
            # captura 4 o 4.5, etc.
            m = re.search(r"(\d+(?:\.\d+)?)", star["aria-label"])
            if m:
                rating_val = float(m.group(1))
        elif star and star.has_attr("title"):
            # idem desde title
            m = re.search(r"(\d+(?:\.\d+)?)", star["title"])
            if m:
                rating_val = float(m.group(1))

        # Guardamos solo si hay texto (lo mínimo para considerar una reseña)
        if text:
            reviews.append({"user": user, "date": date,
                           "rating": rating_val, "text": text})

    return reviews


def get_reviews(book_id: str, max_pages: int = 3, delay: float = 1.0) -> List[Dict]:
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

        # Parseamos y acumulamos las reseñas de esta página
        out.extend(parse_reviews_from_html(r.text))

        # Pausa cortita entre páginas (amabilidad con el sitio)
        time.sleep(delay)

        # Heurística de corte: si a partir de la 2ª página no llegó nada, paramos
        if page > 1 and len(out) == 0:
            break

    return out


def process_one(book_id: str, with_reviews=True) -> BookData:
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


def process_many(book_ids: List[str], max_workers: int = 8, with_reviews=True) -> List[BookData]:
    """
    Procesa muchos libros en paralelo usando un pool de hilos (ThreadPoolExecutor).
    - book_ids: lista de IDs de libros de Goodreads.
    - max_workers: cuántos hilos simultáneos (más hilos = más rápido, pero más carga).
    - with_reviews: si también se descargan reseñas por cada libro.
    """
    results = []
    # Creamos el pool con N hilos
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        # Lanzamos una tarea (future) por cada ID de libro
        futs = {ex.submit(process_one, bid, with_reviews)
                          : bid for bid in book_ids}

        # Según se vayan completando, recogemos los resultados
        for fut in as_completed(futs):
            bid = futs[fut]  # saber qué book_id correspondía a ese future
            try:
                resultado = fut.result()
                results.append(resultado)  # BookData ya parseado
            except Exception as e:
                # Si falló, guardamos un placeholder mínimo para no perder el orden
                print("Error procesando libro",
                      f"{GOOD_READS_BASE_URL}{bid}")

    return results


if __name__ == "__main__":
    # Ejemplo rápido de uso
    sample_ids = [f"{id_book}" for id_book in range(
        7, 207, 10)]  # IDs de libros en Goodreads
    books = process_many(sample_ids, max_workers=4, with_reviews=True)
    # Convertimos el dataclass BookData a diccionario para poder tabularlo
    df = pd.DataFrame(books)
    df.to_json("landing/goodreads_books.json", orient="records",
               force_ascii=False, indent=2)
