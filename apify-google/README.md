# Actor Google Maps / Travel (reviews)

Usa Playwright + Chromium en Docker.

**Entrada:** `url` (obligatorio), `maxReviews` (opcional), `headless` (por defecto `true`).

**Dataset:** primer ítem `_type: summary`, luego una fila por review.

Ejemplo: `{"url": "https://www.google.com/travel/...", "maxReviews": 80, "headless": true}`

Despliegue: `apify push` desde esta carpeta.
