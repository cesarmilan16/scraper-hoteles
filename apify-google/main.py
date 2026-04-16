"""Actor Apify: reviews de Google Maps / Google Travel (Playwright)."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Optional

from apify import Actor

from google_scraper import scrape_hotel


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

        max_reviews = _parse_max_reviews(inp.get("maxReviews"))
        headless = inp.get("headless")
        if headless is None:
            headless = True
        headless = bool(headless)

        fd, tmp_name = tempfile.mkstemp(suffix=".json")
        os.close(fd)
        out_path = Path(tmp_name)

        def run() -> None:
            scrape_hotel(url, out_path, max_reviews, headless=headless)

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
                "scrapedCount": len(reviews),
                "scraped": data.get("scraped"),
                "complete": data.get("complete"),
                "totalShownInUi": data.get("total_shown_in_ui"),
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
                "totalShownInUi": data.get("total_shown_in_ui"),
            },
        )


if __name__ == "__main__":
    asyncio.run(main())
