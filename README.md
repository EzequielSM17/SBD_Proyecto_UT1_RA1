# Mini-Pipeline de Libros (Scraping + Enriquecimiento + Integración)

Este proyecto implementa un flujo completo de **Extracción → Enriquecimiento → Integración** para un conjunto de libros obtenidos desde **Goodreads** y **Google Books API**, siguiendo las especificaciones de un mini-pipeline de Big Data.

---

## 1. 🚀 Descripción general

El objetivo del proyecto es consolidar información de libros en un **modelo canónico limpio y deduplicado**, con metadatos, controles de calidad, normalización semántica (fechas, idioma, moneda, ISBN) y trazabilidad por fuente.

El flujo completo consta de tres scripts ejecutados en orden:

### Scraping Goodreads → JSON  
`src/scrape_goodreads.py`

### Enriquecimiento Google Books → CSV  
`src/enrich_googlebooks.py`

### Integración y normalización → Parquet  
`src/integrate_pipeline.py`

---

## 2. 📦 Dependencias

Incluidas en `requirements.txt`:

```bash
pip install -r requirements.txt
``` 

## 3. ▶️ Cómo ejecutar el pipeline

El pipeline debe ejecutarse **en el siguiente orden obligatorio**, ya que cada fase genera los datos necesarios para la siguiente.

Añade las variable de entorno

```.env
GOOD_READS_BASE_URL = URL
USER_AGENT = TU USER_AGENT
GOOGLE_BOOKS_API_URL = URL
```

---


Ejecutar:

### 1️⃣ Scraping Goodreads

```bash
python src/scrape_goodreads.py

``` 


Genera:

landing/goodreads_books.json

### 2️⃣ Enriquecimiento con Google Books API




Ejecutar:

```bash

python src/enrich_googlebooks.py

``` 


Genera:

landing/googlebooks_books.csv

### 3️⃣ Integración, limpieza y normalización

```bash
python src/integrate_pipeline.py
``` 

Genera:

- standard/dim_book.parquet
- standard/book_source_detail.parquet
- docs/quality_metrics.json


```bash
python src/scrape_goodreads.py

python src/enrich_googlebooks.py

python src/integrate_pipeline.py
``` 

## 4. 📄 Metadatos de landing/ y ética de scraping
### 4.1 Fuente y URLs

- Origen del scraping: Goodreads
- Base URL:https://www.goodreads.com/book/show/<book_id>
- Número de libros scrapeados: N (número de elementos en BOOKS_IDS que esta en setting.py)
- Fecha de extracción: YYYY-MM-DD

### 4.2 User-Agent utilizado

Para evitar bloqueos y respetar la transparencia del scraping:

```SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": USER_AGENT
})
```

En setting.py:
```.env
USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.7444.136 Safari/537.36"

```
## 4.3 Selectores utilizados (Goodreads)

En el scraper se combina BeautifulSoup y Selenium para obtener el HTML final y extraer los campos necesarios.
Los principales selectores son:

### Selectores y lógica de extracción (Goodreads)

| Campo                       | Selector / lógica                                                                                     |
|----------------------------|----------------------------------------------------------------------------------------------------------|
| **Título (`title`)**       | `soup.find(class_="Text Text__title1")`                                                                 |
| **Autores (`authors`)**    | `soup.find_all(class_="ContributorLink__name")`                                                         |
| **Rating medio (`rating_value`)** | `soup.find(class_="RatingStatistics__rating")`                                                    |
| **Descripción (`desc`)**   | `soup.find(class_="DetailsLayoutRightParagraph__widthConstrained")`                                     |
| **Información de publicación** | `soup.find("p", {"data-testid": "publicationInfo"})`                                                 |
| **Portada (`cover`)**      | `soup.find(class_="ResponsiveImage")["src"]`                                                            |
| **Nº valoraciones (`rating_count`)** | Regex: `re.search(r'"ratingCount":(\d+)', html)`                                                |
| **Nº reseñas (`review_count`)**     | Regex: `re.search(r'"reviewCount":(\d+)', html)`                                                 |
| **Reseñas por idioma**     | Regex: `re.findall(r'"count":(\d+),"isoLanguageCode":"([a-z]{2})"', html)`                              |
| **Géneros (`genres`)**     | Regex sobre bloque `"bookGenres"` y luego `json.loads(...)`                                             |
| **Precio (`price`)**       | Búsqueda del botón de venta: `.find_all(class_="Button__container Button__container--block")`           |
| **Detalles de edición (`extra_data`)** | `edition_details.find_all(class_="TruncatedContent__text TruncatedContent__text--small")`     |
| **Formato / nº páginas**   | Parseo de `extra_data[0]` (ej. `"320 pages, Paperback"`)                                                |
| **Editorial (`publisher`)** | `extra_data[1]`                                                                                         |
| **ISBN13 / ISBN10**        | Parseo de `extra_data[2]` (ej.: `"9780131103627 (ISBN10 0131103628)"`)                                  |
| **Idioma (`language`)**    | `extra_data[3]` o `extra_data[4]` (según longitud)                                                      |


Para reseñas individuales se usan:

### Selectores y lógica de extracción de reseñas (Goodreads)

| Campo                         | Selector / lógica                                                                                           |
|------------------------------|--------------------------------------------------------------------------------------------------------------|
| **Contenedor de reseña**     | `soup.select('[data-testid="review"]')` **o** `.ReviewCard`                                                 |
| **Texto de la reseña (`text`)** | `c.select_one('[data-testid="reviewText"]')` **o** `.ReviewText__content`                                 |
| **Autor de la reseña (`user`)** | `c.select_one('[data-testid="reviewAuthor"] a')` **o** `.ReviewerProfile__name a`                          |
| **Fecha de la reseña (`date`)** | `c.select_one('[data-testid="reviewDate"]')` **o** `c.select_one("time")`                                  |
| **Rating de la reseña (`rating`)** | `c.select_one('[aria-label*="rating"]')` **o** `c.select_one('[title*="rating"]')` + extracción regex (`(\d+(\.\d+)?)`) |
| **Rating: extracción numérica** | Si atributo `aria-label` → `re.search(r"(\\d+(?:\\.\\d+)?)", star["aria-label"])`                    
## 4.4 Formato de los archivos generados en landing/
### goodreads_books.json

- Formato: JSON
- Codificación: UTF-8
- Estructura: lista de diccionarios con BookData

### googlebooks_books.csv

- Formato: CSV
- Separador: ,
- Codificación: UTF-8

## 5. 🧩 Decisiones clave del pipeline

### ✔ Prioridad de fuentes
- **ID principal:** `isbn13`
- Si `isbn13` no existe → generar **hash estable** usando `(title + author + publisher)`
- Fuente preferente para atributos:
  1. **Google Books**
  2. Goodreads

---

### ✔ Reglas de supervivencia (deduplicación)
- Se conserva el registro con **mayor completitud de campos**
- Prioridad de fuente: **Google Books > Goodreads**
- Autores y géneros: **unión sin duplicados**
- Precio: se selecciona el **más reciente disponible**

---

### ✔ Normalización semántica
- **Fechas:** formato ISO 8601 (`YYYY-MM-DD`)
- **Idioma:** estándar BCP-47 (`en`, `es`, `pt-BR`)
- **Moneda:** ISO 4217 (`USD`, `EUR`…)
- **ISBN:** limpieza de guiones y validación estructural
- **Nombres de columnas:** `snake_case`

---

### ✔ Calidad y aserciones
Las métricas quedan registradas en:  
`docs/quality_metrics.json`

Incluyen:

- `%` de nulos por campo  
- `%` de fechas válidas  
- `%` de idiomas válidos  
- **duplicados detectados**  
- **filas válidas por fuente**  

Estas métricas permiten evaluar la salud de los datos tras la integración.

## 6. 📚 Esquema y modelo canónico (dim_book.parquet)

Este documento describe el **modelo canónico** de libros, el **mapa de campos** desde las fuentes originales (Goodreads y Google Books), las **claves** utilizadas (ID principal y claves provisionales), así como las tablas generadas en la capa `standard/`.



Tabla canónica de libros.  
**Grano:** 1 fila = 1 libro (ID canónico).

Campos principales:

| Campo                     | Tipo           | Null? | Descripción                                                                                         |
|---------------------------|----------------|-------|-----------------------------------------------------------------------------------------------------|
| `book_id`                | string         | NO    | ID canónico del libro. Preferente `isbn13`. Si no existe, hash estable de (título+autor+editorial) |
| `title`                  | string         | SÍ    | Título final del libro (más informativo entre fuentes)                                             |
| `authors`                | array<string>  | SÍ    | Lista de autores (unión sin duplicados de GR + GB)                                                 |
| `publisher`              | string         | SÍ    | Editorial seleccionada (generalmente Google Books si existe)                                       |
| `pub_year`               | Int64          | SÍ    | Año de publicación derivado de `publication_date`                                                  |
| `publication_date`       | string         | SÍ    | Fecha normalizada a ISO-8601 (`YYYY-MM-DD`, `YYYY-MM` o `YYYY`)                                    |
| `language`               | string         | SÍ    | Idioma normalizado a BCP-47 (ej. `en`, `es`, `pt-BR`)                                              |
| `isbn10`                 | string         | SÍ    | ISBN-10 preferente                                                                                  |
| `isbn13`                 | string         | SÍ    | ISBN-13 validado y normalizado                                                                     |
| `num_pages`              | Int64          | SÍ    | Número de páginas (máximo entre GR y GB)                                                           |
| `format`                 | string         | SÍ    | Formato (p.ej. `Paperback`, `Hardcover`, `Audio CD`)                                               |
| `genres`                 | array<string>  | SÍ    | Lista de géneros/categorías (unión GR `genres` + GB `categories`)                                  |
| `rating_value`           | float64        | SÍ    | Rating medio seleccionado (habitualmente Goodreads si existe)                                      |
| `rating_count`           | Int64          | SÍ    | Número de valoraciones (máximo entre GR y GB)                                                      |
| `review_count`           | Int64          | SÍ    | Número de reseñas (si existe en Goodreads)                                                         |
| `price`                  | float64        | SÍ    | Precio numérico (Google Books, normalizado con punto decimal)                                      |
| `cover`             | string         | SÍ    | URL de la portada principal (preferente Google Books)                                              |
| `source_winner`          | string         | NO    | Fuente ganadora a nivel de registro (`"goodreads"`, `"googlebooks"` o `"merged"`)                  |

Campos adicionales del modelo original (`BookData`) se pueden incluir como columnas de detalle: `desc`, `pub_info`, `publication_timestamp`, `review_count_by_lang`, `comments`, etc., siempre que tengan utilidad analítica.


