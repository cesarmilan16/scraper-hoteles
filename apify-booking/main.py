"""Actor Apify: reviews de Booking.com."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from apify import Actor

from booking_scraper import Fetcher, extract_pagename, scrape_hotel


def _parse_max_reviews(raw: Any) -> Optional[int]:
    if raw is None or raw == "":
        return None
    n = int(raw)
    if n < 0:
        raise ValueError("maxReviews debe ser >= 0")
    return n if n > 0 else None


async def main() -> None:
    async with Actor:
        inp = await Actor.get_input() or {}
        url = (inp.get("url") or "").strip()
        if not url:
            raise ValueError("El campo 'url' es obligatorio.")

        pagename = (inp.get("pagename") or "").strip() or extract_pagename(url)
        max_reviews = _parse_max_reviews(inp.get("maxReviews"))
        delay = inp.get("delay") or [5.0, 12.0]
        if not isinstance(delay, list) or len(delay) != 2:
            delay = [5.0, 12.0]
        delay_range = (float(delay[0]), float(delay[1]))

        fd, tmp_name = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        out_path = Path(tmp_name)
        fetcher = Fetcher()

        def run() -> None:
            scrape_hotel(
                fetcher,
                pagename,
                out_path,
                resume=False,
                limit=max_reviews,
                delay_range=delay_range,
            )

        try:
            await asyncio.to_thread(run)
            data = json.loads(out_path.read_text(encoding="utf-8"))
        finally:
            out_path.unlink(missing_ok=True)
        reviews = data.get("reviews") or []

        await Actor.push_data(
            {
                "_type": "summary",
                "sourceUrl": url,
                "pagename": pagename,
                "hotel": data.get("hotel"),
                "scrapedCount": len(reviews),
                "complete": data.get("complete"),
            }
        )
        for i, rev in enumerate(reviews):
            if isinstance(rev, dict):
                await Actor.push_data({"index": i + 1, **rev})
            else:
                await Actor.push_data({"index": i + 1, "raw": rev})

        await Actor.set_value(
            "OUTPUT",
            {
                "scrapedCount": len(reviews),
                "complete": data.get("complete"),
                "pagename": pagename,
            },
        )


if __name__ == "__main__":
    asyncio.run(main())
