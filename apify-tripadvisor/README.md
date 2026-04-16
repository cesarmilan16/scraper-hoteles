# Actor TripAdvisor (reviews)

**Entrada:** `url` (obligatorio), `maxReviews` (opcional), `delay` opcional `[5, 12]`.

**Dataset:** primer ítem `_type: summary`, luego una fila por review.

Ejemplo de input: `{"url": "https://www.tripadvisor.com/Hotel_Review-...", "maxReviews": 100}`

Despliegue: desde esta carpeta, `apify push` (CLI Apify).
