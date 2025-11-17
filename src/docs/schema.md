# Esquema del modelo

## dim_book

| campo | tipo |
|-------|------|
| id | object |
| isbn13 | int64 |
| isbn | int64 |
| url | object |
| title | object |
| authors | object |
| rating_value | float64 |
| desc | object |
| pub_info | object |
| publication_timestamp | object |
| publication_date | object |
| cover | object |
| format | object |
| num_pages | float64 |
| publisher | object |
| language | object |
| review_count_by_lang | object |
| genres | object |
| rating_count | float64 |
| review_count | float64 |
| comments | object |

## book_source_detail

| campo | tipo |
|-------|------|
| id | object |
| url | object |
| title | object |
| authors | object |
| rating_value | float64 |
| desc | object |
| pub_info | object |
| cover | object |
| format | object |
| num_pages | float64 |
| publication_timestamp | float64 |
| publication_date | object |
| publisher | object |
| isbn | int64 |
| isbn13 | int64 |
| language | object |
| review_count_by_lang | object |
| genres | object |
| rating_count | float64 |
| review_count | float64 |
| comments | object |
| source | object |
| ingest_ts | datetime64[us, UTC] |
| q_gb_title_valid | object |
| q_gb_url_valid | object |
| q_gb_authors_not_null | object |
| q_gb_pub_date_valid | object |
| q_gb_language_valid | object |
| q_gb_isbn13_not_null | object |
| q_gb_isbn13_valid | object |
| q_gb_num_pages_valid | object |
| q_gb_price_amount_non_negative | object |
| q_record_valid | bool |
| q_gr_title_valid | object |
| q_gr_url_valid | object |
| q_gr_authors_valid | object |
| q_gr_rating_valid | object |
| q_gr_language_not_null | object |
| q_gr_rating_count_valid | object |
| q_gr_review_count_valid | object |
| q_gr_num_pages_valid | object |
| q_gr_isbn13_not_null | object |
| q_gr_isbn13_valid | object |
| q_gr_review_by_lang_valid | object |
| q_gr_genres_valid | object |
| book_id | object |
| completeness_score | int64 |
