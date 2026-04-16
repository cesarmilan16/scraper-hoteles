# AGENTS.md

Guía breve para personas y agentes de IA que trabajan en este repositorio.

## Qué es

Proyecto de **scrapers en Python** que extraen reseñas públicas de hoteles desde **TripAdvisor**, **Booking.com** y **Google Maps / Google Travel**. La salida principal son ficheros **JSON** con campos como autor, valoración, texto y fecha (el esquema exacto varía por plataforma).

## Dónde está el código

| Ubicación | Rol |
|-----------|-----|
| `tripadvisor_scraper.py`, `booking_scraper.py`, `google_scraper.py` (raíz) | Lógica de extracción y CLI |
| `apify-tripadvisor/`, `apify-booking/`, `apify-google/` | Actores Apify: `main.py`, Docker, `requirements.txt`, copia local del `*_scraper.py` correspondiente |
| `*_reviews.json`, `google_travel_*.json`, etc. | Datos de salida o pruebas; suelen ser grandes |

Si cambias un scraper y despliegas en Apify, revisa que la copia dentro de `apify-*` y el `Dockerfile` sigan siendo coherentes.

## Puesta en marcha mínima

```bash
pip install -r requirements.txt
python -m playwright install chromium   # solo necesario para Google
```

Comandos y flags detallados: **README.md**.

## Convenciones

- Preferir cambios **pequeños y enfocados** en los `*_scraper.py`; mantener la CLI alineada entre scripts donde ya exista un patrón común (`--url`, `--url-file`, `--output`, `--limit`, `--resume`, etc.).
- **Google** usa Playwright y selectores frágiles; cualquier cambio debe probarse contra la página real.
- **TripAdvisor y Booking** usan HTTP (`curl_cffi`); respetar delays y opciones de reanudación salvo petición explícita en contrario.

## Qué evitar

- Introducir credenciales, tokens o claves en el código o en commits.
- Reducir agresivamente delays o límites de cortesía sin que el usuario lo pida.
- Tratar los JSON de salida como fuente de verdad del esquema sin comprobar el código que los genera.

## Cursor: reglas y skills

- **Reglas del proyecto:** `.cursor/rules/` (contexto automático por ficheros y reglas globales).
- **Skills del proyecto:** `.cursor/skills/` (flujo de scrapers y actores Apify).

Para el detalle de instalación, tablas de tiempos y ejemplos por plataforma, usar siempre **README.md** como referencia principal.
