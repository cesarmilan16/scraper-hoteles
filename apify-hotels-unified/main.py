"""Actor Apify unificado: TripAdvisor, Booking y Google."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from apify import Actor

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

        proxy_url = await _get_proxy_url(inp.get("proxySettings"))

        results: dict[str, dict[str, Any]] = {}

        # --- TripAdvisor ---
        if run_tripadvisor:
            if not tripadvisor_url:
                msg = "`tripadvisorUrl` es obligatorio cuando `enableTripAdvisor` está activo."
                Actor.log.warning(msg)
                results["tripadvisor"] = {"enabled": True, "ok": False, "error": msg}
            else:
                try:
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
                    data = await _run_google(google_url, max_reviews, google_headless, proxy_url)
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
                except Exception as exc:
                    Actor.log.exception("Google fallo")
                    await Actor.push_data({"_type": "summary", "platform": "google", "ok": False, "error": str(exc)})
                    results["google"] = {"enabled": True, "ok": False, "error": str(exc)}
        else:
            results["google"] = {"enabled": False, "ok": False}

        await Actor.set_value("OUTPUT", results)


if __name__ == "__main__":
    asyncio.run(main())
