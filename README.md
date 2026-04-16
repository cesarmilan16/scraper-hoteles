# Hotel Reviews Scraper

Extrae **todas** las reviews públicas de hoteles desde **TripAdvisor**, **Booking.com** y **Google Maps**.
100% gratuito — sin APIs de pago ni proxies.

## Instalación

```bash
pip install -r requirements.txt
python -m playwright install chromium   # solo para Google Maps
```

## Scripts

| Script | Plataforma | Método | Velocidad |
|--------|-----------|--------|-----------|
| `tripadvisor_scraper.py` | TripAdvisor | curl_cffi (HTTP) | ~8 min / 500 reviews |
| `booking_scraper.py` | Booking.com | curl_cffi (HTTP) | ~15 min / 4000 reviews |
| `google_scraper.py` | Google Maps | Playwright (navegador) | ~5 min / 200 reviews |

---

## TripAdvisor

```bash
# Hotel por defecto (Alicante Hills)
python tripadvisor_scraper.py

# URL personalizada
python tripadvisor_scraper.py --url "https://www.tripadvisor.es/Hotel_Review-..."

# Varios hoteles
python tripadvisor_scraper.py --url-file hoteles_ta.txt --output resultados_ta/ --resume
```

## Booking.com

```bash
# Por pagename
python booking_scraper.py --pagename alicante-hills

# Por URL completa
python booking_scraper.py --url "https://www.booking.com/hotel/es/alicante-hills.es.html"

# Varios hoteles
python booking_scraper.py --url-file hoteles_bk.txt --output resultados_bk/ --resume
```

## Google Maps

```bash
# Buscar por nombre (recomendado)
python google_scraper.py --url "https://www.google.com/maps/search/Alicante+Hills+Apartments?hl=es"

# Limitar reviews
python google_scraper.py --limit 100

# Modo headless (sin ventana)
python google_scraper.py --headless

# Varios hoteles
python google_scraper.py --url-file hoteles_gm.txt --output resultados_gm/
```

---

## Opciones comunes

| Flag | Descripción | Default |
|---|---|---|
| `--url URL` | URL del hotel | Alicante Hills |
| `--url-file FICHERO` | Fichero con URLs (una por línea) | — |
| `--output PATH` | Salida JSON o directorio | `*_reviews.json` |
| `--limit N` | Máximo de reviews por hotel | Sin límite |
| `--resume` | Continúa desde donde se paró | No |
| `--delay MIN MAX` | Segundos entre páginas (TA/Booking) | Varía |
| `--hotel-delay SECS` | Segundos entre hoteles | `30` |
| `--headless` | Sin ventana (solo Google) | No |

## Formato de salida

Cada JSON contiene las reviews con campos como: `author`, `rating`, `title`, `body`, `date_posted`, etc. Los campos exactos varían por plataforma.

## Tiempos estimados (70 hoteles)

| Plataforma | Tiempo | Coste |
|-----------|--------|-------|
| TripAdvisor | ~9 horas | $0 |
| Booking.com | ~12 horas | $0 |
| Google Maps | ~6 horas | $0 |
