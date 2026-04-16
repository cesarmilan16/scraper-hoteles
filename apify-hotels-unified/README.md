# Actor unificado de reviews hoteleras

Actor único para ejecutar **TripAdvisor**, **Booking** y **Google Maps/Travel** en la misma corrida, con interruptores para activar o desactivar cada fuente.

## Input principal

- `enableTripAdvisor` (`true/false`)
- `tripadvisorUrl`
- `enableBooking` (`true/false`)
- `bookingUrl`
- `bookingPagename` (opcional)
- `enableGoogle` (`true/false`)
- `googleUrl`
- `googleHeadless` (`true/false`, por defecto `true`)
- `maxReviews` (opcional, límite por plataforma)
- `delay` (opcional, `[min, max]` para TripAdvisor y Booking)

Si una fuente está activa, su URL correspondiente es obligatoria.

## Activar/desactivar los 3 actores desde un solo actor

Ejemplo:

```json
{
  "enableTripAdvisor": true,
  "tripadvisorUrl": "https://www.tripadvisor.com/Hotel_Review-...",
  "enableBooking": false,
  "bookingUrl": "",
  "enableGoogle": true,
  "googleUrl": "https://www.google.com/travel/hotels/entity/.../reviews?hl=es-ES",
  "maxReviews": 50,
  "delay": [5, 12]
}
```

Con ese input se ejecutan solo TripAdvisor y Google, y Booking queda desactivado.

## Dataset y output

- `Dataset`: ítems de tipo `_type: "summary"` por plataforma + reseñas con `_type: "review"` y `platform`.
- `Key-value store` (`OUTPUT`): resumen por plataforma con `enabled`, `scrapedCount` y estado.

## Deploy

Desde esta carpeta:

```bash
apify push
```
