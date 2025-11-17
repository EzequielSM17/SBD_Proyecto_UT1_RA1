# Esquema

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


