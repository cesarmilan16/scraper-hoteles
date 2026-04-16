#!/usr/bin/env python3
"""
TripAdvisor Reviews Scraper — 100% gratuito.

Extrae todas las reviews publicas de uno o varios hoteles usando curl_cffi
para imitar la huella TLS de Chrome y evitar deteccion anti-bot.
Guardado incremental con reanudacion automatica.
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

REVIEWS_PER_PAGE = 10
BASE_DOMAIN = "https://www.tripadvisor.es"

DEFAULT_URL = (
    f"{BASE_DOMAIN}/Hotel_Review-g1064230-d1757900-Reviews-"
    "Alicante_Hills_Apartments-Alicante_Costa_Blanca_Province_of_Alicante_Valencian_Commun.html"
)

REVIEW_SELECTORS = [
    '[data-test-target="HR_CC_CARD"]',
    "[data-reviewid]",
    'div[class*="review-container"]',
]

BLOCKED_INDICATORS = [
    "El acceso está restringido temporalmente",
    "Access temporarily restricted",
    "comportamiento del navegador nos ha intrigado",
]


# ---------------------------------------------------------------------------
# Modelo
# ---------------------------------------------------------------------------

@dataclass
class Review:
    author:       Optional[str]
    rating:       Optional[int]
    title:        Optional[str]
    body:         Optional[str]
    date_posted:  Optional[str]
    location:     Optional[str]
    travel_tip:   Optional[str]
    stay_date:    Optional[str]
    trip_type:    Optional[str]
    page_num:     int
    source_url:   str
    scraped_at:   str


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _css_text(soup: BeautifulSoup, selector: str, nth: int = 0) -> Optional[str]:
    try:
        nodes = soup.select(selector)
        if nth < len(nodes):
            return nodes[nth].get_text(" ", strip=True) or None
    except Exception:
        pass
    return None


def parse_int(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(\d[\d\.\,]*)", text)
    if not m:
        return None
    return int(re.sub(r"[^\d]", "", m.group(1))) or None


def build_page_url(base_url: str, offset: int) -> str:
    if offset == 0:
        return base_url
    return re.sub(r"(Reviews-)", f"\\1or{offset}-", base_url, count=1)


def get_total_reviews(soup: BeautifulSoup) -> Optional[int]:
    for sel in ("div.JajTY", "div.JRWqg"):
        el = soup.select_one(sel)
        if el:
            m = re.search(r"(\d[\d\.]*)", el.get_text())
            if m:
                return parse_int(m.group(1))
    for tag in soup.find_all(string=re.compile(r"\d+\s*opiniones", re.I)):
        m = re.search(r"(\d[\d\.]*)\s*opiniones", str(tag), re.I)
        if m:
            return parse_int(m.group(1))
    return None


# ---------------------------------------------------------------------------
# Parseo de reviews
# ---------------------------------------------------------------------------

def parse_review_block(block: BeautifulSoup, page_url: str, page_num: int) -> Review:
    author = _css_text(block, "span.RUZll") or _css_text(block, 'a[href*="Profile"]')

    rating: Optional[int] = None
    rt = _css_text(block, "svg.evwcZ title") or _css_text(block, "title")
    if rt:
        m = re.search(r"(\d+)\s+de\s+\d+", rt)
        if m:
            rating = int(m.group(1))

    title = (
        _css_text(block, '[data-test-target="review-title"] span')
        or _css_text(block, '[data-test-target="review-title"]')
        or _css_text(block, "h3")
    )

    meta_raw = _css_text(block, "div.biGQs._P.VImYz.AWdfh") or ""
    dm = re.search(r"opini[oó]n\s+(.+)$", meta_raw)
    date_posted = dm.group(1).strip() if dm else None

    body = (
        _css_text(block, "span.JguWG")
        or _css_text(block, "div.fIrGe._T.bgMZj")
        or _css_text(block, "div._T.FKffI.bmUTE")
    )

    return Review(
        author=author, rating=rating, title=title, body=body,
        date_posted=date_posted,
        location=_css_text(block, "span.qVkLn") or _css_text(block, "div.Mi"),
        travel_tip=_css_text(block, "div.MRPew div.biGQs._P.VImYz.AWdfh"),
        stay_date=_css_text(block, "span.biGQs._P.VImYz.xENVe", nth=0),
        trip_type=_css_text(block, "span.biGQs._P.VImYz.xENVe", nth=1),
        page_num=page_num, source_url=page_url, scraped_at=now_iso(),
    )


def extract_page_reviews(html: str, page_url: str, page_num: int) -> List[Review]:
    soup = BeautifulSoup(html, "html.parser")
    for sel in REVIEW_SELECTORS:
        found = soup.select(sel)
        if found:
            return [
                parse_review_block(BeautifulSoup(str(b), "html.parser"), page_url, page_num)
                for b in found
            ]
    return []


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
    total: Optional[int], base_url: str, completed: bool = False,
) -> None:
    path.write_text(
        json.dumps({
            "hotel_url":   base_url,
            "total_found": total,
            "scraped":     len(reviews),
            "next_offset": next_offset,
            "complete":    completed or (total is not None and next_offset >= total),
            "reviews":     reviews,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Fetcher: sesion con huella TLS de Chrome
# ---------------------------------------------------------------------------

class Fetcher:
    def __init__(self, proxy_url: Optional[str] = None) -> None:
        self._proxy_url = proxy_url
        self._session = self._make_session()
        self._warmed_up = False
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
        self._warmed_up = False

    def _warm_up(self) -> None:
        if self._warmed_up:
            return
        try:
            self._session.get(BASE_DOMAIN, timeout=15)
            time.sleep(random.uniform(1.0, 2.5))
            self._warmed_up = True
        except Exception:
            pass

    def _is_blocked(self, html: str) -> bool:
        return any(ind in html for ind in BLOCKED_INDICATORS)

    def fetch(self, url: str, retries: int = 5) -> str:
        self._warm_up()

        for attempt in range(1, retries + 1):
            try:
                resp = self._session.get(url, timeout=30)

                if resp.status_code == 200:
                    if self._is_blocked(resp.text):
                        self._consecutive_blocks += 1
                        wait = min(60 * self._consecutive_blocks, 300)
                        print(f"    [bloqueado] Esperando {wait}s...", file=sys.stderr)
                        time.sleep(wait)
                        self._new_session()
                        if attempt < retries:
                            continue
                        raise RuntimeError("Bloqueado temporalmente por TripAdvisor")
                    self._consecutive_blocks = 0
                    return resp.text

                if resp.status_code == 403:
                    self._new_session()
                    if attempt < retries:
                        self._warm_up()
                        time.sleep(3 * attempt)
                        continue

                if resp.status_code in (429, 500, 503):
                    wait = 8 * attempt + random.uniform(2, 5)
                    if attempt < retries:
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
    base_url: str,
    output_path: Path,
    resume: bool,
    limit: Optional[int],
    delay_range: Tuple[float, float],
) -> int:
    existing = load_output(output_path)
    if resume and existing.get("reviews") and existing.get("hotel_url", "").split("?")[0] == base_url.split("?")[0]:
        all_reviews = existing["reviews"]
        start_offset = existing.get("next_offset", 0)
        already_done = {r["page_num"] for r in all_reviews}
        print(f"  Reanudando: {len(all_reviews)} reviews, offset={start_offset}", file=sys.stderr)
    else:
        all_reviews = []
        start_offset = 0
        already_done = set()

    first_html = fetcher.fetch(base_url)
    soup = BeautifulSoup(first_html, "html.parser")
    total = get_total_reviews(soup)

    if 1 not in already_done:
        for r in extract_page_reviews(first_html, base_url, 1):
            all_reviews.append(asdict(r))

    effective_total = total or 1000
    if limit:
        effective_total = min(effective_total, limit)

    pages_total = -(-effective_total // REVIEWS_PER_PAGE)
    print(
        f"  {total or '?'} opiniones (~{pages_total} paginas)"
        + (f" | limite: {limit}" if limit else ""),
        file=sys.stderr,
    )

    offset = max(start_offset, REVIEWS_PER_PAGE)
    page_num = offset // REVIEWS_PER_PAGE + 1
    empty_streak = 0

    while offset < effective_total and (not limit or len(all_reviews) < limit):
        if page_num in already_done:
            offset += REVIEWS_PER_PAGE
            page_num += 1
            continue

        time.sleep(random.uniform(*delay_range))

        page_url = build_page_url(base_url, offset)
        try:
            html = fetcher.fetch(page_url)
            reviews = extract_page_reviews(html, page_url, page_num)
        except Exception as exc:
            print(f"    [pag {page_num:>3}] Error: {exc}", file=sys.stderr)
            save_output(output_path, all_reviews, offset, total, base_url)
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
            save_output(output_path, all_reviews, offset + REVIEWS_PER_PAGE, total, base_url)

        pct = f"{len(all_reviews)}/{total}" if total else str(len(all_reviews))
        status = f"{len(reviews)} reviews" if reviews else "vacia"
        print(f"    [pag {page_num:>3}] {status:<15} | Total: {pct}", file=sys.stderr)

        offset += REVIEWS_PER_PAGE
        page_num += 1

    done = (
        (limit is not None and len(all_reviews) >= limit)
        or (total is not None and offset >= total)
        or (limit is None and empty_streak >= 3)
    )
    save_output(output_path, all_reviews, offset, total, base_url, completed=done)
    return len(all_reviews)


# ---------------------------------------------------------------------------
# Scraping de multiples hoteles
# ---------------------------------------------------------------------------

def scrape_multiple(
    fetcher: Fetcher,
    urls: List[str],
    output_dir: Path,
    resume: bool,
    limit: Optional[int],
    delay_range: Tuple[float, float],
    hotel_delay: float,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    total_hotels = len(urls)
    completed = 0
    skipped = 0

    for i, url in enumerate(urls, 1):
        url = url.strip()
        if not url or url.startswith("#"):
            continue

        m = re.search(r"Reviews-(?:or\d+-)?(.+?)\.html", url)
        slug = m.group(1)[:60] if m else f"hotel_{i}"
        slug = re.sub(r"[^a-zA-Z0-9_-]", "_", slug)
        out_path = output_dir / f"{slug}.json"

        if resume and out_path.exists():
            existing = load_output(out_path)
            if existing.get("complete"):
                skipped += 1
                print(
                    f"\n[{i}/{total_hotels}] {slug} — ya completado "
                    f"({existing.get('scraped', '?')} reviews). Saltando.",
                    file=sys.stderr,
                )
                continue

        elapsed = time.time() - t0
        print(
            f"\n[{i}/{total_hotels}] {slug}  (transcurrido: {elapsed/60:.1f} min)",
            file=sys.stderr,
        )

        try:
            count = scrape_hotel(fetcher, url, out_path, resume, limit, delay_range)
            completed += 1
            print(f"  => {count} reviews -> {out_path.name}", file=sys.stderr)
        except Exception as exc:
            print(f"  => ERROR: {exc}. Progreso guardado.", file=sys.stderr)

        if i < total_hotels:
            wait = hotel_delay + random.uniform(5, 15)
            print(f"  Pausa {wait:.0f}s antes del siguiente hotel...", file=sys.stderr)
            time.sleep(wait)

    elapsed = time.time() - t0
    print(
        f"\n{'='*50}"
        f"\nFinalizado: {completed} hoteles scrapeados, {skipped} saltados"
        f"\nTiempo total: {elapsed/60:.1f} min"
        f"\nResultados en: {output_dir.resolve()}",
        file=sys.stderr,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Extrae todas las reviews de hoteles en TripAdvisor (gratis).",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    src = p.add_mutually_exclusive_group()
    src.add_argument("--url", default=None, help="URL de un hotel")
    src.add_argument(
        "--url-file", default=None, metavar="FICHERO",
        help="Fichero con URLs de hoteles (una por linea)",
    )
    p.add_argument("--output", default="reviews.json",
                   help="Salida JSON (1 hotel) o directorio (varios)")
    p.add_argument("--limit", type=int, default=None,
                   help="Max reviews por hotel")
    p.add_argument("--resume", action="store_true",
                   help="Retomar donde se interrumpio")
    p.add_argument("--delay", type=float, nargs=2, default=(5.0, 12.0),
                   metavar=("MIN", "MAX"),
                   help="Segundos entre paginas (default: 5 12)")
    p.add_argument("--hotel-delay", type=float, default=30,
                   help="Segundos entre hoteles (default: 30)")
    return p


def main() -> int:
    args = build_parser().parse_args()
    delay_range: Tuple[float, float] = tuple(args.delay)  # type: ignore[assignment]

    print(
        f"TripAdvisor Scraper (gratuito)  |  Delay: {delay_range[0]}-{delay_range[1]}s",
        file=sys.stderr,
    )

    fetcher = Fetcher()

    if args.url_file:
        urls = Path(args.url_file).read_text(encoding="utf-8").splitlines()
        urls = [u.strip() for u in urls if u.strip() and not u.strip().startswith("#")]
        print(f"Hoteles: {len(urls)}", file=sys.stderr)
        scrape_multiple(
            fetcher, urls, Path(args.output), args.resume,
            args.limit, delay_range, args.hotel_delay,
        )
        return 0

    url = args.url or DEFAULT_URL
    try:
        count = scrape_hotel(
            fetcher, url, Path(args.output), args.resume,
            args.limit, delay_range,
        )
        print(f"\n{count} reviews guardadas en {Path(args.output).resolve()}")
        return 0
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
