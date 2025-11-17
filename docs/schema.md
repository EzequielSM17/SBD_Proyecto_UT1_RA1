# Esquema y modelo canónico — books-pipeline

Este documento describe el **modelo canónico** de libros, el **mapa de campos** desde las fuentes originales (Goodreads y Google Books), las **claves** utilizadas (ID principal y claves provisionales), así como las tablas generadas en la capa `standard/`.

---

## 1. Tablas de salida

### 1.1. `standard/dim_book.parquet`

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

---
### 1.2. `standard/book_source_detail.parquet`

Tabla de **detalle por registro de fuente**, generada a partir de la capa SILVER, concatenando los registros de Goodreads y de Google Books con sus campos originales, flags de calidad y enriquecimientos ligeros.

**Grano:** 1 fila = 1 registro original de cualquiera de las dos fuentes.

Campos principales (comunes y derivados):

| Campo                 | Tipo                     | Null? | Descripción                                                                                             |
|-----------------------|--------------------------|-------|---------------------------------------------------------------------------------------------------------|
| `id`                  | int64 / string           | NO    | Identificador técnico del registro, en la práctica coincide con `isbn13` cuando existe                 |
| `book_id`             | string                   | NO    | ID estable del libro (hash de isbn13 o de título+publisher+fecha), usado como clave candidata          |
| `title`               | string                   | SÍ    | Título tal y como viene de la fuente (Goodreads o Google Books)                                        |
| `authors`             | array\<string>           | SÍ    | Lista de autores original o procesada a partir de la fuente                                            |
| `rating_value`        | float64                  | SÍ    | Valor de rating en la fuente (`rating_value` de GR o `averageRating` de GB)                            |
| `desc`                | string                   | SÍ    | Descripción / sinopsis original                                                                         |
| `pub_info`            | string                   | SÍ    | Información de publicación sin normalizar (solo Goodreads)                                             |
| `cover`               | string                   | SÍ    | URL de la portada (Goodreads o Google Books)                                                           |
| `format`              | string                   | SÍ    | Formato textual (`Paperback`, `Audio CD`, etc.)                                                        |
| `num_pages`           | int64                    | SÍ    | Número de páginas reportado por la fuente                                                              |
| `publication_timestamp` | float64 / int64        | SÍ    | Timestamp original si viene de Goodreads                                                               |
| `publication_date`    | string                   | SÍ    | Fecha de publicación en formato libre (`YYYY`, `YYYY-MM`, `YYYY-MM-DD`, etc.)                          |
| `publisher`           | string                   | SÍ    | Editorial según la fuente                                                                              |
| `isbn`                | string                   | SÍ    | ISBN-10 original                                                                                        |
| `isbn13`              | int64 / float64 / string | SÍ    | ISBN-13 original (puede venir como número; se normaliza en otras capas)                                |
| `language`            | string                   | SÍ    | Idioma original (`English`, `en`, etc.)                                                                 |
| `review_count_by_lang`| dict                     | SÍ    | Diccionario lenguaje → número de reseñas (Goodreads), puede contener claves con valor null             |
| `genres`              | array\<string>           | SÍ    | Lista de géneros/categorías original o derivada                                                        |
| `rating_count`        | int64                    | SÍ    | Número de valoraciones según la fuente                                                                 |
| `review_count`        | int64                    | SÍ    | Número de reseñas según la fuente                                                                      |
| `comments`            | array\<struct>           | SÍ    | Lista de comentarios (solo Goodreads), cada uno con `user`, `date`, `rating`, `text`                  |
| `price`               | float64                  | SÍ    | Precio original si viene de Google Books                                                               |
| `source`              | string                   | NO    | Nombre del fichero de origen en `landing/` (`goodreads_books.json` o `googlebooks_books.csv`)          |
| `ingest_ts`           | float64                  | NO    | Timestamp de ingesta (epoch ms) en el momento de la capa BRONZE/SILVER                                 |
| `book_id`             | string                   | NO    | ID estable generado por `generate_stable_book_id`                                                      |
| `completeness_score`  | int64                    | NO    | Número de campos clave no nulos (título, autores, editorial, fecha, isbn13, idioma, géneros, páginas) |


Adicionalmente se pueden incluir todos los campos originales relevantes para trazabilidad y debugging.
La deduplicación y selección del registro ganador se realiza en la tabla `dim_book.parquet`.

---

## 2. Modelo canónico y clave

### 2.1. ID canónico (`book_id`)

- **ID preferente:** `isbn13` normalizado y validado.
- **Clave provisional:** si no existe `isbn13` en ninguna fuente válida, se genera un ID estable:

```text
book_id = md5( lower(trim(title)) + "|" + main_author + "|" + lower(trim(publisher)) )
