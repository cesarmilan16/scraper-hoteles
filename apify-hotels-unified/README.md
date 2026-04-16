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
- `maintenanceMode` (`true/false`, por defecto `false`)
- `checkLimit` (opcional, por defecto `3`)
- `updateLimit` (opcional, por defecto `25`)
- `fingerprintTopN` (opcional, por defecto `3`)
- `maxReviews` (opcional, límite por plataforma)
- `delay` (opcional, `[min, max]` para TripAdvisor y Booking)

Si una fuente está activa, su URL correspondiente es obligatoria.

## Mantenimiento automático (cada 12h)

Con `maintenanceMode=true` el actor hace:

1. Check ligero por plataforma (`checkLimit`).
2. Compara una huella (`fingerprintTopN`) con el estado guardado en key-value store (`STATE`).
3. Solo si detecta cambios (o primera ejecución), scrapea `updateLimit` reviews.

Para tu caso:

```json
{
  "enableTripAdvisor": true,
  "tripadvisorUrl": "https://www.tripadvisor.es/Hotel_Review-...",
  "enableBooking": true,
  "bookingUrl": "https://www.booking.com/hotel/es/alicante-hills.es.html",
  "enableGoogle": true,
  "googleUrl": "https://www.google.com/travel/hotels/entity/.../reviews?hl=es-ES",
  "googleHeadless": true,
  "maintenanceMode": true,
  "checkLimit": 3,
  "updateLimit": 25,
  "fingerprintTopN": 3,
  "delay": [5, 12]
}
```

Programa una Task en Apify cada 12 horas con ese input.

### Estado entre ejecuciones

El modo mantenimiento guarda `STATE` en un **Key-Value Store con nombre** en la nube Apify (`force_cloud`), para que el siguiente run detecte si hubo cambios.

Tras un run, en **Storage → Key-value stores** deberías ver un store llamado `hotel-reviews-maintenance-state` (o el nombre que pongas en `stateKeyValueStoreName`) con la clave `STATE`.

Si no ves el store o cada run sigue con `seeded: true`, sube la última versión del actor (`apify push`) y revisa los logs: líneas `Mantenimiento: STATE cargado` / `STATE guardado`.

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
