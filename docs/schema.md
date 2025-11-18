### Esquema canónico `standard/dim_book.parquet` (con reglas de supervivencia)

##  Esquema y modelo canónico (dim_book.parquet) 

| Campo                | Tipo           | Null? | Descripción                                                                                     | Regla        |
|----------------------|----------------|-------|-------------------------------------------------------------------------------------------------|--------------|
| `book_id`            | string         | NO    | ID canónico del libro. Preferente `isbn13` o hash estable (title+author+publisher).            | `prefer-gb` / `fallback` |
| `title`              | string         | SÍ    | Título final del libro.                                                                         | `longest`    |
| `authors`            | array<string>  | SÍ    | Lista unificada de autores.                                                                    | `merge`      |
| `publisher`          | string         | SÍ    | Editorial resultante entre GR y GB.                                                            | `longest`    |
| `pub_year`           | Int64          | SÍ    | Año derivado de `publication_date`.                                                            | `derived`    |
| `publication_date`   | string         | SÍ    | Fecha final normalizada ISO 8601.                                                              | `prefer-gb` / `fallback` |
| `language`           | string         | SÍ    | Idioma en formato BCP-47.                                                                      | `normalize` / `prefer-gb` |
| `isbn10`             | string         | SÍ    | ISBN-10 final (desde GB si disponible).                                                        | `prefer-gb`  |
| `isbn13`             | string         | SÍ    | ISBN-13 seleccionado (GB si existe).                                                           | `prefer-gb`  |
| `num_pages`          | Int64          | SÍ    | Número de páginas mayor entre GR y GB.                                                         | `max`        |
| `format`             | string         | SÍ    | Formato físico/digital.                                                                        | `longest`    |
| `genres`             | array<string>  | SÍ    | Lista combinada de géneros/categorías.                                                         | `merge`      |
| `rating_value`       | float64        | SÍ    | Rating final (mayor entre GR y GB).                                                            | `max`        |
| `rating_count`       | Int64          | SÍ    | Número de valoraciones (máximo entre GR y GB).                                                 | `max`        |
| `review_count`       | Int64          | SÍ    | Número de reseñas (máximo no nulo).                                                            | `max`        |
| `price`              | float64        | SÍ    | Precio final del libro (si hay, viene de Google Books).                                        | `prefer-gb`  |
| `current`            | string         | SÍ    | Moneda normalizada ISO-4217 (`USD`, `EUR`, etc.).                                             | `prefer-gb` / `normalize` |
| `cover`              | string         | SÍ    | URL de la portada seleccionada.                                                                | `prefer-gb`  |
| `source_winner`      | string         | NO    | Fuente ganadora (`goodreads`, `merged`).                                                       | `auto`       |

---

### Campos adicionales (detalle)

| Campo                      | Tipo              | Descripción                                                     | Regla        |
|----------------------------|-------------------|-----------------------------------------------------------------|--------------|
| `url`                      | string            | URL resultante unificada.                                       | `longest`    |
| `desc`                     | string            | Descripción final integrada.                                    | `longest`    |
| `pub_info`                 | string            | Texto original de publicación (sin normalizar).                 | `fallback`   |
| `review_count_by_lang`     | dict              | Reseñas por idioma (solo Goodreads).                           | `inherit-gr` |
| `comments`                 | array             | Comentarios (solo Goodreads).                                   | `inherit-gr` |

---

## 🔎 Glosario de reglas

| Regla        | Significado                                                   |
|--------------|---------------------------------------------------------------|
| `longest`    | Escoge la cadena **más larga** (mayor información).           |
| `max`        | Devuelve el valor numérico **mayor** (ignorando nulos).       |
| `merge`      | Une listas y **elimina duplicados** manteniendo el orden.     |
| `prefer-gb`  | Si Google Books tiene valor → gana GB; si no → Goodreads.     |
| `fallback`   | Si no existe valor principal, usar el alternativo.            |
| `normalize`  | Convierte a formato estándar (fecha ISO, moneda, idioma…).    |
| `auto`       | Determinado automáticamente según tu merge y supervivencia.   |
| `inherit-gr` | Campo exclusivo de Goodreads conservado tal cual.             |

---




