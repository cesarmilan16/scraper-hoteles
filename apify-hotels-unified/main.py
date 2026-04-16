"""Actor Apify unificado: TripAdvisor, Booking y Google."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from apify import Actor
from crawlee.storages import KeyValueStore

from booking_scraper import Fetcher as BookingFetcher
from booking_scraper import extract_pagename, scrape_hotel as scrape_booking_hotel
from google_scraper import scrape_hotel as scrape_google_hotel
from tripadvisor_scraper import Fetcher as TripAdvisorFetcher
from tripadvisor_scraper import scrape_hotel as scrape_tripadvisor_hotel


def _parse_max_reviews(raw: Any) -> Optional[int]:
    if raw is None or raw == "":
        return None
    n = int(raw)
    if n < 0:
        raise ValueError("maxReviews debe ser >= 0")
    return n if n > 0 else None


def _parse_delay(raw: Any, default: tuple[float, float] = (5.0, 12.0)) -> tuple[float, float]:
    if not isinstance(raw, list) or len(raw) != 2:
        return default
    return float(raw[0]), float(raw[1])


def _bool(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    return bool(raw)


async def _get_proxy_url(proxy_settings: Any) -> Optional[str]:
    """Crea proxy URL usando Apify Proxy si hay config."""
    if not proxy_settings:
        return None
    try:
        proxy_cfg = await Actor.create_proxy_configuration(
            actor_proxy_input=proxy_settings
        )
        if proxy_cfg:
            url = await proxy_cfg.new_url()
            Actor.log.info(f"Proxy configurado: {url[:40]}...")
            return url
    except Exception as exc:
        Actor.log.warning(f"No se pudo configurar proxy: {exc}")
    return None


async def _run_tripadvisor(
    url: str, max_reviews: Optional[int], delay: tuple[float, float], proxy_url: Optional[str],
) -> dict:
    fd, tmp_name = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    out_path = Path(tmp_name)
    fetcher = TripAdvisorFetcher(proxy_url=proxy_url)

    def run() -> None:
        scrape_tripadvisor_hotel(
            fetcher, url, out_path,
            resume=False, limit=max_reviews, delay_range=delay,
        )

    try:
        await asyncio.to_thread(run)
        data = json.loads(out_path.read_text(encoding="utf-8"))
    finally:
        out_path.unlink(missing_ok=True)
    return data


async def _run_booking(
    url: str, pagename: str, max_reviews: Optional[int],
    delay: tuple[float, float], proxy_url: Optional[str],
) -> dict:
    fd, tmp_name = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    out_path = Path(tmp_name)
    fetcher = BookingFetcher(proxy_url=proxy_url)

    def run() -> None:
        scrape_booking_hotel(
            fetcher, pagename, out_path,
            resume=False, limit=max_reviews, delay_range=delay,
        )

    try:
        await asyncio.to_thread(run)
        data = json.loads(out_path.read_text(encoding="utf-8"))
    finally:
        out_path.unlink(missing_ok=True)
    return data


async def _run_google(
    url: str, max_reviews: Optional[int], headless: bool, proxy_url: Optional[str],
) -> dict:
    fd, tmp_name = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    out_path = Path(tmp_name)

    def run() -> None:
        scrape_google_hotel(url, out_path, max_reviews, headless=headless, proxy_url=proxy_url)

    try:
        await asyncio.to_thread(run)
        data = json.loads(out_path.read_text(encoding="utf-8"))
    finally:
        out_path.unlink(missing_ok=True)
    return data


async def _push_reviews(platform: str, reviews: list[Any]) -> None:
    for i, rev in enumerate(reviews):
        if isinstance(rev, dict):
            await Actor.push_data({"_type": "review", "platform": platform, "index": i + 1, **rev})
        else:
            await Actor.push_data({"_type": "review", "platform": platform, "index": i + 1, "raw": rev})


def _normalize_text(raw: Any) -> str:
    if raw is None:
        return ""
    return str(raw).strip().lower()


def _review_fingerprint(review: dict[str, Any]) -> str:
    payload = {
        "author": _normalize_text(review.get("author")),
        "date": _normalize_text(review.get("date_posted") or review.get("review_date")),
        "rating": str(review.get("rating") or review.get("score") or ""),
        "body": _normalize_text(review.get("body") or review.get("positive") or review.get("title")),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _platform_fingerprint(reviews: list[Any], top_n: int) -> str:
    hashes: list[str] = []
    for item in reviews[:top_n]:
        if isinstance(item, dict):
            hashes.append(_review_fingerprint(item))
        else:
            hashes.append(hashlib.sha1(str(item).encode("utf-8")).hexdigest())
    joined = "|".join(hashes)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def _take_review_signature(reviews: list[Any]) -> dict[str, Any]:
    if not reviews:
        return {}
    first = reviews[0]
    if not isinstance(first, dict):
        return {"raw": str(first)}
    return {
        "author": first.get("author"),
        "date": first.get("date_posted") or first.get("review_date"),
        "rating": first.get("rating") or first.get("score"),
    }


def _parse_positive_int(raw: Any, default: int) -> int:
    if raw is None or raw == "":
        return default
    n = int(raw)
    if n <= 0:
        raise ValueError("El valor debe ser > 0")
    return n


def _state_store_name(inp: dict[str, Any]) -> str:
    raw = (inp.get("stateKeyValueStoreName") or "").strip()
    return raw or "hotel-reviews-maintenance-state"


async def _open_maintenance_state_store(inp: dict[str, Any]) -> KeyValueStore:
    """KV store compartido entre runs. Usa siempre la nube Apify (force_cloud)."""
    store_id = (inp.get("stateKeyValueStoreId") or "").strip()
    if store_id:
        return await Actor.open_key_value_store(id=store_id, force_cloud=True)
    return await Actor.open_key_value_store(name=_state_store_name(inp), force_cloud=True)


async def _load_state(store: KeyValueStore) -> dict[str, Any]:
    state = await store.get_value("STATE")
    if isinstance(state, dict):
        return state
    return {}


def _fp_short(fp: str, length: int = 10) -> str:
    if not fp:
        return "—"
    return fp[:length] + ("…" if len(fp) > length else "")


def _log_maintenance_platform(
    platform: str,
    *,
    check_n: int,
    old_fp: str,
    new_fp: str,
    changed: bool,
    seeded: bool,
    will_scrape_update: bool,
    scraped_n: int,
) -> None:
    Actor.log.info(
        "[%s] mantenimiento | check=%s reseñas | huella %s → %s | cambio=%s primera_vez=%s | "
        "scrape_incremental=%s | reseñas_en_dataset=%s",
        platform.upper(),
        check_n,
        _fp_short(old_fp),
        _fp_short(new_fp),
        changed,
        seeded,
        will_scrape_update,
        scraped_n,
    )


async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}

        run_tripadvisor = _bool(inp.get("enableTripAdvisor"), True)
        run_booking = _bool(inp.get("enableBooking"), True)
        run_google = _bool(inp.get("enableGoogle"), True)

        if not any((run_tripadvisor, run_booking, run_google)):
            raise ValueError("Debes activar al menos una fuente: TripAdvisor, Booking o Google.")

        max_reviews = _parse_max_reviews(inp.get("maxReviews"))

        tripadvisor_url = (inp.get("tripadvisorUrl") or "").strip()
        booking_url = (inp.get("bookingUrl") or "").strip()
        booking_pagename = (inp.get("bookingPagename") or "").strip()
        google_url = (inp.get("googleUrl") or "").strip()

        delay = _parse_delay(inp.get("delay"), default=(5.0, 12.0))
        google_headless = _bool(inp.get("googleHeadless"), True)
        maintenance_mode = _bool(inp.get("maintenanceMode"), False)
        check_limit = _parse_positive_int(inp.get("checkLimit"), default=3)
        update_limit = _parse_positive_int(inp.get("updateLimit"), default=25)
        fingerprint_top_n = _parse_positive_int(inp.get("fingerprintTopN"), default=3)

        proxy_url = await _get_proxy_url(inp.get("proxySettings"))

        if not max_reviews and not maintenance_mode:
            max_reviews = None

        state_store = await _open_maintenance_state_store(inp)
        state = await _load_state(state_store)
        sources_on = [
            n
            for n, on in (
                ("tripadvisor", run_tripadvisor),
                ("booking", run_booking),
                ("google", run_google),
            )
            if on
        ]
        Actor.log.info(
            "Inicio actor | modo=%s | fuentes=%s | maxReviews=%s | delay=%s..%ss | googleHeadless=%s",
            "mantenimiento" if maintenance_mode else "completo",
            ",".join(sources_on) or "ninguna",
            max_reviews if max_reviews is not None else "sin límite",
            delay[0],
            delay[1],
            google_headless,
        )
        if maintenance_mode:
            Actor.log.info(
                "Mantenimiento | checkLimit=%s | updateLimit=%s | fingerprintTopN=%s",
                check_limit,
                update_limit,
                fingerprint_top_n,
            )
            store_label = (
                f"id={inp.get('stateKeyValueStoreId')}"
                if (inp.get("stateKeyValueStoreId") or "").strip()
                else f"name={_state_store_name(inp)}"
            )
            Actor.log.info(
                "Estado persistente | KV %s | claves cargadas: %s",
                store_label,
                list(state.keys()) if state else "(vacío, primera corrida o sin STATE)",
            )
        results: dict[str, dict[str, Any]] = {}

        # --- TripAdvisor ---
        if run_tripadvisor:
            if not tripadvisor_url:
                msg = "`tripadvisorUrl` es obligatorio cuando `enableTripAdvisor` está activo."
                Actor.log.warning(msg)
                results["tripadvisor"] = {"enabled": True, "ok": False, "error": msg}
            else:
                try:
                    if maintenance_mode:
                        check_data = await _run_tripadvisor(tripadvisor_url, check_limit, delay, proxy_url)
                        check_reviews = check_data.get("reviews") or []
                        old_fp = str(state.get("tripadvisor", {}).get("fingerprint") or "")
                        new_fp = _platform_fingerprint(check_reviews, fingerprint_top_n)
                        changed = bool(check_reviews) and old_fp != new_fp
                        initial_seed = not old_fp
                        should_update = changed or initial_seed

                        reviews: list[Any] = []
                        data = check_data
                        if should_update:
                            data = await _run_tripadvisor(tripadvisor_url, update_limit, delay, proxy_url)
                            reviews = data.get("reviews") or []

                        state["tripadvisor"] = {
                            "fingerprint": new_fp,
                            "latest": _take_review_signature(check_reviews),
                            "last_checked_at": data.get("reviews", [{}])[0].get("scraped_at") if data.get("reviews") else None,
                        }

                        await Actor.push_data({
                            "_type": "summary",
                            "platform": "tripadvisor",
                            "mode": "maintenance",
                            "sourceUrl": tripadvisor_url,
                            "changed": changed,
                            "seeded": initial_seed,
                            "scrapedCount": len(reviews),
                            "checkCount": len(check_reviews),
                            "updateLimit": update_limit,
                            "ok": True,
                        })
                        if reviews:
                            await _push_reviews("tripadvisor", reviews)
                        _log_maintenance_platform(
                            "tripadvisor",
                            check_n=len(check_reviews),
                            old_fp=old_fp,
                            new_fp=new_fp,
                            changed=changed,
                            seeded=initial_seed,
                            will_scrape_update=should_update,
                            scraped_n=len(reviews),
                        )
                        results["tripadvisor"] = {
                            "enabled": True,
                            "ok": True,
                            "mode": "maintenance",
                            "changed": changed,
                            "seeded": initial_seed,
                            "checkCount": len(check_reviews),
                            "scrapedCount": len(reviews),
                        }
                    else:
                        Actor.log.info("[TRIPADVISOR] scrape completo | límite=%s", max_reviews or "sin límite")
                        data = await _run_tripadvisor(tripadvisor_url, max_reviews, delay, proxy_url)
                        reviews = data.get("reviews") or []
                        await Actor.push_data({
                            "_type": "summary", "platform": "tripadvisor",
                            "sourceUrl": tripadvisor_url,
                            "scrapedCount": len(reviews),
                            "totalFound": data.get("total_found"),
                            "complete": data.get("complete"), "ok": True,
                        })
                        await _push_reviews("tripadvisor", reviews)
                        results["tripadvisor"] = {
                            "enabled": True, "ok": True,
                            "scrapedCount": len(reviews),
                            "totalFound": data.get("total_found"),
                            "complete": data.get("complete"),
                        }
                        Actor.log.info(
                            "[TRIPADVISOR] fin | reseñas=%s | total_en_web≈%s | complete=%s",
                            len(reviews),
                            data.get("total_found"),
                            data.get("complete"),
                        )
                except Exception as exc:
                    Actor.log.exception("TripAdvisor fallo")
                    await Actor.push_data({"_type": "summary", "platform": "tripadvisor", "ok": False, "error": str(exc)})
                    results["tripadvisor"] = {"enabled": True, "ok": False, "error": str(exc)}
        else:
            results["tripadvisor"] = {"enabled": False, "ok": False}

        # --- Booking ---
        if run_booking:
            if not booking_url:
                msg = "`bookingUrl` es obligatorio cuando `enableBooking` está activo."
                Actor.log.warning(msg)
                results["booking"] = {"enabled": True, "ok": False, "error": msg}
            else:
                try:
                    pagename = booking_pagename or extract_pagename(booking_url)
                    if maintenance_mode:
                        check_data = await _run_booking(booking_url, pagename, check_limit, delay, proxy_url)
                        check_reviews = check_data.get("reviews") or []
                        old_fp = str(state.get("booking", {}).get("fingerprint") or "")
                        new_fp = _platform_fingerprint(check_reviews, fingerprint_top_n)
                        changed = bool(check_reviews) and old_fp != new_fp
                        initial_seed = not old_fp
                        should_update = changed or initial_seed

                        reviews: list[Any] = []
                        data = check_data
                        if should_update:
                            data = await _run_booking(booking_url, pagename, update_limit, delay, proxy_url)
                            reviews = data.get("reviews") or []

                        state["booking"] = {
                            "fingerprint": new_fp,
                            "latest": _take_review_signature(check_reviews),
                            "pagename": pagename,
                            "last_checked_at": data.get("reviews", [{}])[0].get("scraped_at") if data.get("reviews") else None,
                        }

                        await Actor.push_data({
                            "_type": "summary",
                            "platform": "booking",
                            "mode": "maintenance",
                            "sourceUrl": booking_url,
                            "pagename": pagename,
                            "changed": changed,
                            "seeded": initial_seed,
                            "scrapedCount": len(reviews),
                            "checkCount": len(check_reviews),
                            "updateLimit": update_limit,
                            "ok": True,
                        })
                        if reviews:
                            await _push_reviews("booking", reviews)
                        _log_maintenance_platform(
                            "booking",
                            check_n=len(check_reviews),
                            old_fp=old_fp,
                            new_fp=new_fp,
                            changed=changed,
                            seeded=initial_seed,
                            will_scrape_update=should_update,
                            scraped_n=len(reviews),
                        )
                        results["booking"] = {
                            "enabled": True,
                            "ok": True,
                            "mode": "maintenance",
                            "changed": changed,
                            "seeded": initial_seed,
                            "checkCount": len(check_reviews),
                            "scrapedCount": len(reviews),
                            "pagename": pagename,
                        }
                    else:
                        Actor.log.info("[BOOKING] scrape completo | pagename=%s | límite=%s", pagename, max_reviews or "sin límite")
                        data = await _run_booking(booking_url, pagename, max_reviews, delay, proxy_url)
                        reviews = data.get("reviews") or []
                        await Actor.push_data({
                            "_type": "summary", "platform": "booking",
                            "sourceUrl": booking_url, "pagename": pagename,
                            "hotel": data.get("hotel"),
                            "scrapedCount": len(reviews),
                            "complete": data.get("complete"), "ok": True,
                        })
                        await _push_reviews("booking", reviews)
                        results["booking"] = {
                            "enabled": True, "ok": True,
                            "scrapedCount": len(reviews),
                            "pagename": pagename,
                            "complete": data.get("complete"),
                        }
                        Actor.log.info(
                            "[BOOKING] fin | reseñas=%s | pagename=%s | complete=%s",
                            len(reviews),
                            pagename,
                            data.get("complete"),
                        )
                except Exception as exc:
                    Actor.log.exception("Booking fallo")
                    await Actor.push_data({"_type": "summary", "platform": "booking", "ok": False, "error": str(exc)})
                    results["booking"] = {"enabled": True, "ok": False, "error": str(exc)}
        else:
            results["booking"] = {"enabled": False, "ok": False}

        # --- Google ---
        if run_google:
            if not google_url:
                msg = "`googleUrl` es obligatorio cuando `enableGoogle` está activo."
                Actor.log.warning(msg)
                results["google"] = {"enabled": True, "ok": False, "error": msg}
            else:
                try:
                    if maintenance_mode:
                        check_data = await _run_google(google_url, check_limit, google_headless, None)
                        check_reviews = check_data.get("reviews") or []
                        old_fp = str(state.get("google", {}).get("fingerprint") or "")
                        new_fp = _platform_fingerprint(check_reviews, fingerprint_top_n)
                        changed = bool(check_reviews) and old_fp != new_fp
                        initial_seed = not old_fp
                        should_update = changed or initial_seed

                        reviews: list[Any] = []
                        data = check_data
                        if should_update:
                            data = await _run_google(google_url, update_limit, google_headless, None)
                            reviews = data.get("reviews") or []

                        state["google"] = {
                            "fingerprint": new_fp,
                            "latest": _take_review_signature(check_reviews),
                            "last_checked_at": data.get("reviews", [{}])[0].get("scraped_at") if data.get("reviews") else None,
                        }

                        await Actor.push_data({
                            "_type": "summary",
                            "platform": "google",
                            "mode": "maintenance",
                            "sourceUrl": google_url,
                            "changed": changed,
                            "seeded": initial_seed,
                            "scrapedCount": len(reviews),
                            "checkCount": len(check_reviews),
                            "updateLimit": update_limit,
                            "ok": True,
                        })
                        if reviews:
                            await _push_reviews("google", reviews)
                        _log_maintenance_platform(
                            "google",
                            check_n=len(check_reviews),
                            old_fp=old_fp,
                            new_fp=new_fp,
                            changed=changed,
                            seeded=initial_seed,
                            will_scrape_update=should_update,
                            scraped_n=len(reviews),
                        )
                        results["google"] = {
                            "enabled": True,
                            "ok": True,
                            "mode": "maintenance",
                            "changed": changed,
                            "seeded": initial_seed,
                            "checkCount": len(check_reviews),
                            "scrapedCount": len(reviews),
                        }
                    else:
                        Actor.log.info("[GOOGLE] scrape completo | límite=%s | headless=%s", max_reviews or "sin límite", google_headless)
                        data = await _run_google(google_url, max_reviews, google_headless, None)
                        reviews = data.get("reviews") or []
                        await Actor.push_data({
                            "_type": "summary", "platform": "google",
                            "sourceUrl": google_url,
                            "scrapedCount": len(reviews),
                            "scraped": data.get("scraped"),
                            "complete": data.get("complete"),
                            "totalShownInUi": data.get("total_shown_in_ui"), "ok": True,
                        })
                        await _push_reviews("google", reviews)
                        results["google"] = {
                            "enabled": True, "ok": True,
                            "scrapedCount": len(reviews),
                            "complete": data.get("complete"),
                            "totalShownInUi": data.get("total_shown_in_ui"),
                        }
                        Actor.log.info(
                            "[GOOGLE] fin | reseñas=%s | total_UI≈%s | complete=%s",
                            len(reviews),
                            data.get("total_shown_in_ui"),
                            data.get("complete"),
                        )
                except Exception as exc:
                    Actor.log.exception("Google fallo")
                    await Actor.push_data({"_type": "summary", "platform": "google", "ok": False, "error": str(exc)})
                    results["google"] = {"enabled": True, "ok": False, "error": str(exc)}
        else:
            results["google"] = {"enabled": False, "ok": False}

        await state_store.set_value("STATE", state)
        if maintenance_mode:
            Actor.log.info(
                "Estado guardado en KV | claves: %s",
                list(state.keys()),
            )
            total_pushed = sum(
                int(r.get("scrapedCount") or 0)
                for r in results.values()
                if isinstance(r, dict) and r.get("mode") == "maintenance" and r.get("ok")
            )
            any_change = any(
                isinstance(r, dict) and r.get("mode") == "maintenance" and r.get("changed")
                for r in results.values()
            )
            Actor.log.info(
                "Resumen mantenimiento | reseñas nuevas en dataset (suma scrapedCount): %s | hubo_cambio_detectado=%s",
                total_pushed,
                any_change,
            )
        await Actor.set_value("OUTPUT", results)


if __name__ == "__main__":
    asyncio.run(main())
