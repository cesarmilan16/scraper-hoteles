#!/usr/bin/env python3
"""
Booking.com Reviews Scraper — 100% gratuito.

Extrae todas las reviews publicas de uno o varios hoteles en Booking.com
usando curl_cffi. Guardado incremental con reanudacion automatica.
"""
import argparse
import datetime
import json
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from curl_cffi import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

REVIEWS_PER_PAGE = 25
REVIEWLIST_URL = "https://www.booking.com/reviewlist.es.html"

DEFAULT_PAGENAME = "alicante-hills"

BLOCKED_INDICATORS = [
    "awsWafCookieDomainList",
    "challenge.js",
    "JavaScript is disabled",
]


# ---------------------------------------------------------------------------
# Modelo
# ---------------------------------------------------------------------------

@dataclass
class Review:
    author:       Optional[str]
    country:      Optional[str]
    score:        Optional[float]
    title:        Optional[str]
    positive:     Optional[str]
    negative:     Optional[str]
    stay_date:    Optional[str]
    review_date:  Optional[str]
    room_type:    Optional[str]
    traveler_type: Optional[str]
    nights:       Optional[str]
    page_num:     int
    scraped_at:   str


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def extract_pagename(url: str) -> str:
    """Extrae el 'pagename' de una URL de Booking (e.g. 'alicante-hills')."""
    m = re.search(r"/hotel/\w+/([\w-]+?)\.(?:es|en|html)", url)
    if m:
        return m.group(1)
    m2 = re.search(r"pagename=([\w-]+)", url)
    if m2:
        return m2.group(1)
    cleaned = re.sub(r"https?://[^/]+/", "", url)
    cleaned = re.sub(r"\..*", "", cleaned)
    return cleaned or "hotel"


def _text(el) -> Optional[str]:
    if el:
        t = el.get_text(" ", strip=True)
        return t if t else None
    return None


# ---------------------------------------------------------------------------
# Parseo de reviews
# ---------------------------------------------------------------------------

def parse_review_block(block: BeautifulSoup, page_num: int) -> Review:
    author = _text(block.select_one("span.bui-avatar-block__title"))
    country = _text(block.select_one("span.bui-avatar-block__subtitle"))

    score: Optional[float] = None
    score_el = block.select_one("div.bui-review-score__badge")
    if score_el:
        raw = score_el.get_text(strip=True).replace(",", ".")
        try:
            score = float(raw)
        except ValueError:
            pass

    title = _text(block.select_one("h3.c-review-block__title"))

    # Positivo / negativo
    labels = block.select("span.bui-u-sr-only")
    bodies = block.select("span.c-review__body")
    positive = negative = None
    for label, body in zip(labels, bodies):
        lt = label.get_text(strip=True).lower()
        bt = body.get_text(" ", strip=True)
        if not bt:
            continue
        if "gust" in lt or "liked" in lt or "positiv" in lt:
            positive = bt
        elif "no gust" in lt or "disliked" in lt or "negativ" in lt:
            negative = bt
        elif positive is None:
            positive = bt
        else:
            negative = bt

    # Si solo hay un body sin labels claros
    if not positive and not negative and bodies:
        positive = bodies[0].get_text(" ", strip=True) or None

    # Fechas
    dates = block.select("span.c-review-block__date")
    stay_date = review_date = None
    for d in dates:
        dt = d.get_text(strip=True)
        if "comentó" in dt.lower() or "reviewed" in dt.lower():
            review_date = re.sub(r"^.*?:\s*", "", dt)
        else:
            stay_date = dt

    # Info adicional
    info_items = [el.get_text(strip=True) for el in block.select("div.bui-list__body")]
    room_type = info_items[0] if len(info_items) > 0 else None
    nights = info_items[1] if len(info_items) > 1 else None
    traveler_type = info_items[2] if len(info_items) > 2 else None

    return Review(
        author=author, country=country, score=score, title=title,
        positive=positive, negative=negative,
        stay_date=stay_date, review_date=review_date,
        room_type=room_type, traveler_type=traveler_type, nights=nights,
        page_num=page_num, scraped_at=now_iso(),
    )


def extract_page_reviews(html: str, page_num: int) -> List[Review]:
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select(".c-review-block")
    return [parse_review_block(b, page_num) for b in blocks]


def get_total_pages(html: str) -> Optional[int]:
    """Extrae el numero total de paginas desde los links de paginacion."""
    soup = BeautifulSoup(html, "html.parser")
    pages = soup.select("a.bui-pagination__link")
    max_page = 1
    for p in pages:
        href = p.get("href", "")
        m = re.search(r"offset=(\d+)", href)
        if m:
            offset = int(m.group(1))
            page = offset // REVIEWS_PER_PAGE + 1
            max_page = max(max_page, page)
    return max_page if max_page > 1 else None


# ---------------------------------------------------------------------------
# Guardado incremental
# ---------------------------------------------------------------------------

def load_output(path: Path) -> Dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_output(
    path: Path, reviews: List[dict], next_offset: int,
    total_pages: Optional[int], pagename: str,
) -> None:
    path.write_text(
        json.dumps({
            "hotel":       pagename,
            "total_pages": total_pages,
            "scraped":     len(reviews),
            "next_offset": next_offset,
            "complete":    total_pages is not None and next_offset >= total_pages * REVIEWS_PER_PAGE,
            "reviews":     reviews,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------

class Fetcher:
    def __init__(self, proxy_url: Optional[str] = None) -> None:
        self._proxy_url = proxy_url
        self._session = self._make_session()
        self._consecutive_blocks = 0

    def _make_session(self) -> requests.Session:
        proxies = (
            {"http": self._proxy_url, "https": self._proxy_url}
            if self._proxy_url
            else None
        )
        return requests.Session(impersonate="chrome", proxies=proxies)

    def _new_session(self) -> None:
        self._session = self._make_session()

    def _is_blocked(self, html: str) -> bool:
        return any(ind in html for ind in BLOCKED_INDICATORS)

    def fetch(self, pagename: str, offset: int, retries: int = 5) -> str:
        url = (
            f"{REVIEWLIST_URL}?cc1=es&dist=1"
            f"&pagename={pagename}&type=total"
            f"&rows={REVIEWS_PER_PAGE}&offset={offset}"
        )

        for attempt in range(1, retries + 1):
            try:
                resp = self._session.get(url, timeout=30)

                if resp.status_code == 200:
                    if self._is_blocked(resp.text):
                        self._consecutive_blocks += 1
                        wait = min(60 * self._consecutive_blocks, 300)
                        print(f"    [WAF] Esperando {wait}s...", file=sys.stderr)
                        time.sleep(wait)
                        self._new_session()
                        if attempt < retries:
                            continue
                        raise RuntimeError("Bloqueado por WAF de Booking")
                    self._consecutive_blocks = 0
                    return resp.text

                if resp.status_code in (202, 403, 429, 500, 503):
                    wait = 10 * attempt + random.uniform(2, 5)
                    print(f"    [HTTP {resp.status_code}] Reintento {attempt}/{retries}, esperando {wait:.0f}s...", file=sys.stderr)
                    if attempt < retries:
                        self._new_session()
                        time.sleep(wait)
                        continue

                raise RuntimeError(f"HTTP {resp.status_code}")

            except RuntimeError:
                raise
            except Exception as exc:
                if attempt < retries:
                    time.sleep(5 * attempt)
                    continue
                raise RuntimeError(f"Error de red: {exc}") from exc

        raise RuntimeError(f"Fallo tras {retries} intentos")


# ---------------------------------------------------------------------------
# Scraping de un hotel
# ---------------------------------------------------------------------------

def scrape_hotel(
    fetcher: Fetcher,
    pagename: str,
    output_path: Path,
    resume: bool,
    limit: Optional[int],
    delay_range: Tuple[float, float],
) -> int:
    existing = load_output(output_path)
    if resume and existing.get("reviews") and existing.get("hotel") == pagename:
        all_reviews = existing["reviews"]
        start_offset = existing.get("next_offset", 0)
        already_done = {r["page_num"] for r in all_reviews}
        print(f"  Reanudando: {len(all_reviews)} reviews, offset={start_offset}", file=sys.stderr)
    else:
        all_reviews = []
        start_offset = 0
        already_done = set()

    # Primera pagina
    first_html = fetcher.fetch(pagename, 0)
    total_pages = get_total_pages(first_html)

    if 1 not in already_done:
        for r in extract_page_reviews(first_html, 1):
            all_reviews.append(asdict(r))

    if total_pages:
        total_est = total_pages * REVIEWS_PER_PAGE
        print(f"  ~{total_est} reviews (~{total_pages} paginas)", file=sys.stderr)
    else:
        total_est = 10000
        print("  Total desconocido, parara cuando no haya reviews.", file=sys.stderr)

    if limit:
        total_est = min(total_est, limit)

    offset = max(start_offset, REVIEWS_PER_PAGE)
    page_num = offset // REVIEWS_PER_PAGE + 1
    empty_streak = 0

    while offset < total_est and (not limit or len(all_reviews) < limit):
        if page_num in already_done:
            offset += REVIEWS_PER_PAGE
            page_num += 1
            continue

        time.sleep(random.uniform(*delay_range))

        try:
            html = fetcher.fetch(pagename, offset)
            reviews = extract_page_reviews(html, page_num)
        except Exception as exc:
            print(f"    [pag {page_num:>3}] Error: {exc}", file=sys.stderr)
            save_output(output_path, all_reviews, offset, total_pages, pagename)
            offset += REVIEWS_PER_PAGE
            page_num += 1
            continue

        if not reviews:
            empty_streak += 1
            if empty_streak >= 3:
                break
        else:
            empty_streak = 0
            for r in reviews:
                all_reviews.append(asdict(r))

        if page_num % 5 == 0 or not reviews:
            save_output(output_path, all_reviews, offset + REVIEWS_PER_PAGE, total_pages, pagename)

        status = f"{len(reviews)} reviews" if reviews else "vacia"
        print(f"    [pag {page_num:>3}] {status:<15} | Total: {len(all_reviews)}", file=sys.stderr)

        offset += REVIEWS_PER_PAGE
        page_num += 1

    save_output(output_path, all_reviews, offset, total_pages, pagename)
    return len(all_reviews)


# ---------------------------------------------------------------------------
# Scraping de multiples hoteles
# ---------------------------------------------------------------------------

def scrape_multiple(
    fetcher: Fetcher,
    inputs: List[str],
    output_dir: Path,
    resume: bool,
    limit: Optional[int],
    delay_range: Tuple[float, float],
    hotel_delay: float,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    total = len(inputs)
    completed = skipped = 0

    for i, raw in enumerate(inputs, 1):
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue

        pagename = extract_pagename(raw) if "/" in raw else raw
        out_path = output_dir / f"{pagename}.json"

        if resume and out_path.exists():
            existing = load_output(out_path)
            if existing.get("complete"):
                skipped += 1
                print(f"\n[{i}/{total}] {pagename} — ya completado ({existing.get('scraped', '?')} reviews). Saltando.", file=sys.stderr)
                continue

        elapsed = time.time() - t0
        print(f"\n[{i}/{total}] {pagename}  ({elapsed/60:.1f} min)", file=sys.stderr)

        try:
            count = scrape_hotel(fetcher, pagename, out_path, resume, limit, delay_range)
            completed += 1
            print(f"  => {count} reviews -> {out_path.name}", file=sys.stderr)
        except Exception as exc:
            print(f"  => ERROR: {exc}. Progreso guardado.", file=sys.stderr)

        if i < total:
            wait = hotel_delay + random.uniform(5, 15)
            print(f"  Pausa {wait:.0f}s...", file=sys.stderr)
            time.sleep(wait)

    elapsed = time.time() - t0
    print(
        f"\n{'='*50}"
        f"\nFinalizado: {completed} hoteles, {skipped} saltados"
        f"\nTiempo total: {elapsed/60:.1f} min"
        f"\nResultados en: {output_dir.resolve()}",
        file=sys.stderr,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Extrae todas las reviews de hoteles en Booking.com (gratis).",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    src = p.add_mutually_exclusive_group()
    src.add_argument("--url", default=None,
                     help="URL de un hotel en Booking.com")
    src.add_argument("--pagename", default=None,
                     help="Pagename del hotel (ej: alicante-hills)")
    src.add_argument("--url-file", default=None, metavar="FICHERO",
                     help="Fichero con URLs o pagenames (uno por linea)")
    p.add_argument("--output", default="booking_reviews.json",
                   help="Salida JSON (1 hotel) o directorio (varios)")
    p.add_argument("--limit", type=int, default=None,
                   help="Max reviews por hotel")
    p.add_argument("--resume", action="store_true",
                   help="Retomar donde se interrumpio")
    p.add_argument("--delay", type=float, nargs=2, default=(3.0, 8.0),
                   metavar=("MIN", "MAX"),
                   help="Segundos entre paginas (default: 3 8)")
    p.add_argument("--hotel-delay", type=float, default=30,
                   help="Segundos entre hoteles (default: 30)")
    return p


def main() -> int:
    args = build_parser().parse_args()
    delay_range: Tuple[float, float] = tuple(args.delay)  # type: ignore[assignment]

    print(
        f"Booking.com Scraper (gratuito)  |  Delay: {delay_range[0]}-{delay_range[1]}s",
        file=sys.stderr,
    )

    fetcher = Fetcher()

    if args.url_file:
        inputs = Path(args.url_file).read_text(encoding="utf-8").splitlines()
        inputs = [u.strip() for u in inputs if u.strip() and not u.strip().startswith("#")]
        print(f"Hoteles: {len(inputs)}", file=sys.stderr)
        scrape_multiple(
            fetcher, inputs, Path(args.output), args.resume,
            args.limit, delay_range, args.hotel_delay,
        )
        return 0

    pagename = args.pagename or (extract_pagename(args.url) if args.url else DEFAULT_PAGENAME)
    try:
        count = scrape_hotel(
            fetcher, pagename, Path(args.output), args.resume,
            args.limit, delay_range,
        )
        print(f"\n{count} reviews guardadas en {Path(args.output).resolve()}")
        return 0
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
