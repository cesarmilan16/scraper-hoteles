# Actor Booking.com (reviews)

**Entrada:** `url` (obligatorio), `maxReviews` (opcional), `pagename` opcional, `delay` opcional.

**Dataset:** primer ítem `_type: summary`, luego una fila por review.

Ejemplo: `{"url": "https://www.booking.com/hotel/es/...", "maxReviews": 50}`

Despliegue: `apify push` desde esta carpeta.
