#!/usr/bin/env python3
"""
Google Maps / Google Travel — Reviews Scraper (100% gratuito).

Extrae todas las reviews publicas de hoteles desde Google Travel (recomendado)
o Google Maps usando Playwright (Google requiere JS completo).
Por defecto usa la ficha de reseñas en Google Hoteles (Travel).
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
from typing import Dict, List, Optional

from playwright.sync_api import sync_playwright, Page

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

SCROLL_PAUSE_MIN = 1.2
SCROLL_PAUSE_MAX = 2.8
# Rondas sin nuevas reviews antes de parar (Google virtualiza el DOM)
MAX_STALE_ROUNDS_MAPS = 35
MAX_STALE_ROUNDS_TRAVEL = 40
MAX_SCROLL_ITERATIONS = 800

# Alicante Hills — entidad en Google Travel (pestaña /reviews)
DEFAULT_URL = (
    "https://www.google.com/travel/hotels/entity/"
    "CiIIgfrUsbiu__PMARC9076HrqjEml8aCy9nLzF0cjE4eHB4EAE/reviews?hl=es-ES"
)


def is_google_travel_url(url: str) -> bool:
    return "google.com/travel/hotels/entity/" in url


def parse_expected_review_count(page: Page) -> Optional[int]:
    """Intenta leer el total de reseñas mostrado en la UI (ej. 2.529 reseñas)."""
    try:
        txt = page.evaluate("""() => document.body ? document.body.innerText : ''""")
    except Exception:
        return None
    if not txt:
        return None
    for pat in (
        r"([\d\.\s]+)\s*reseñas",
        r"([\d,\.\s]+)\s*reviews",
        r"([\d\.\s]+)\s*reseña",
    ):
        m = re.search(pat, txt, re.I)
        if m:
            n = re.sub(r"[^\d]", "", m.group(1))
            if n:
                return int(n)
    return None


def count_review_cards(page: Page, travel_mode: bool) -> int:
    """Cuenta solo tarjetas que parecen reviews reales."""
    return page.evaluate("""(travel) => {
        if (travel) {
            const nodes = document.querySelectorAll('div.Svr5cf.bKhjM');
            let n = 0;
            nodes.forEach(el => {
                const t = (el.innerText || '').trim();
                if (!t || t.length < 20) return;
                if (/\\d\\s*\\/\\s*5/.test(t) && (/hace\\s/i.test(t) || /ago\\b/i.test(t) || /google|tripadvisor|booking/i.test(t))) {
                    n++;
                }
            });
            return n;
        }
        return document.querySelectorAll('div.jftiEf').length;
    }""", travel_mode)


# ---------------------------------------------------------------------------
# Modelo
# ---------------------------------------------------------------------------

@dataclass
class Review:
    author:       Optional[str]
    rating:       Optional[int]
    body:         Optional[str]
    date_posted:  Optional[str]
    source:       str
    local_guide:  bool
    scraped_at:   str


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def accept_cookies(page: Page) -> None:
    for sel in [
        'button:has-text("Aceptar todo")',
        'button:has-text("Accept all")',
        '[aria-label="Aceptar todo"]',
    ]:
        try:
            btn = page.locator(sel).first
            if btn.is_visible(timeout=2000):
                btn.click()
                time.sleep(2)
                return
        except Exception:
            continue


def click_reviews_tab(page: Page) -> bool:
    """Busca y hace clic en la pestaña 'Reseñas'."""
    try:
        tabs = page.locator('button[role="tab"]').all()
        for t in tabs:
            txt = t.inner_text()
            if "eseña" in txt.lower() or "review" in txt.lower():
                t.click()
                time.sleep(3)
                return True
    except Exception:
        pass
    return False


def open_all_reviews_travel(page: Page) -> None:
    """En Google Travel intenta abrir la vista completa de reseñas."""
    candidates = [
        'button:has-text("Mostrar todas las")',
        'button:has-text("Ver más reseñas")',
        'span:has-text("Mostrar todas las")',
        'span:has-text("Ver más reseñas")',
    ]
    for sel in candidates:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=1500):
                el.click()
                time.sleep(3)
                return
        except Exception:
            continue


def sort_by_newest(page: Page) -> None:
    try:
        sort_btn = page.locator('button[aria-label*="rdenar"], button[aria-label*="Sort"]').first
        if sort_btn.is_visible(timeout=3000):
            sort_btn.click()
            time.sleep(1)
            newest = page.locator('div[data-index="1"], div[role="menuitemradio"]:nth-child(2)').first
            if newest.is_visible(timeout=2000):
                newest.click()
                time.sleep(3)
    except Exception:
        pass


def expand_review_texts(page: Page) -> None:
    try:
        for btn in page.locator('button.w8nwRe').all():
            try:
                if btn.is_visible():
                    btn.click()
                    time.sleep(0.15)
            except Exception:
                continue
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Scroll y carga
# ---------------------------------------------------------------------------

def _scroll_one_step_maps(page: Page, panel) -> None:
    panel.evaluate("el => { el.scrollTop = el.scrollHeight; }")
    try:
        page.keyboard.press("End")
    except Exception:
        pass
    try:
        page.mouse.wheel(0, 2500)
    except Exception:
        pass


def scroll_reviews(
    page: Page,
    max_reviews: Optional[int] = None,
    travel_mode: bool = False,
    expected_total: Optional[int] = None,
) -> int:
    stale_limit = MAX_STALE_ROUNDS_TRAVEL if travel_mode else MAX_STALE_ROUNDS_MAPS
    panel = None
    for sel in ["div.m6QErb.DxyBCb", 'div[role="feed"]', "div.m6QErb"]:
        try:
            el = page.locator(sel).first
            if el.is_visible(timeout=3000):
                panel = el
                break
        except Exception:
            continue

    if expected_total:
        print(f"    Objetivo aproximado segun la pagina: ~{expected_total} reseñas", file=sys.stderr)

    def should_stop(count: int, stale: int, iteration: int) -> bool:
        if iteration >= MAX_SCROLL_ITERATIONS:
            return True
        if max_reviews and count >= max_reviews:
            return True
        if expected_total and count >= int(expected_total * 0.98):
            return True
        if stale >= stale_limit:
            return True
        return False

    if not panel:
        if not travel_mode:
            print("    [warn] No se encontro el panel de scroll", file=sys.stderr)
            return 0
        prev_count = 0
        stale = 0
        iteration = 0
        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(random.uniform(SCROLL_PAUSE_MIN, SCROLL_PAUSE_MAX))
            try:
                open_all_reviews_travel(page)
            except Exception:
                pass
            count = count_review_cards(page, True)
            iteration += 1
            if count == prev_count:
                stale += 1
            else:
                stale = 0
                if count % 100 == 0 or count - prev_count >= 30:
                    print(f"    ... {count} reviews (Travel)", file=sys.stderr)
            prev_count = count
            if should_stop(count, stale, iteration):
                break
        return prev_count

    prev_count = 0
    stale = 0
    iteration = 0

    while True:
        _scroll_one_step_maps(page, panel)
        time.sleep(random.uniform(SCROLL_PAUSE_MIN, SCROLL_PAUSE_MAX))

        if travel_mode:
            try:
                open_all_reviews_travel(page)
            except Exception:
                pass

        count = count_review_cards(page, travel_mode)
        iteration += 1

        if max_reviews and count >= max_reviews:
            break

        if count == prev_count:
            stale += 1
        else:
            stale = 0
            if count % 100 == 0 or count - prev_count >= 30:
                print(f"    ... {count} reviews cargadas", file=sys.stderr)

        prev_count = count

        if should_stop(count, stale, iteration):
            break

    return prev_count


# ---------------------------------------------------------------------------
# Parseo
# ---------------------------------------------------------------------------

def parse_reviews(page: Page, travel_mode: bool = False) -> List[Review]:
    expand_review_texts(page)

    raw = page.evaluate("""(travelMode) => {
        const isTravelCard = (el) => {
            const t = (el.innerText || '').trim();
            if (!t || t.length < 20) return false;
            return /\\d\\s*\\/\\s*5/.test(t) && (
                /hace\\s/i.test(t) || /ago\\b/i.test(t) ||
                /google|tripadvisor|booking/i.test(t)
            );
        };
        if (travelMode) {
            const travelEls = Array.from(document.querySelectorAll('div.Svr5cf.bKhjM')).filter(isTravelCard);
            if (travelEls.length) {
                return travelEls.map(el => {
                const text = (el.innerText || '').trim();
                const lines = text.split('\\n').map(x => x.trim()).filter(Boolean);
                const author = lines.length ? lines[0] : null;
                let dateTxt = null;
                let source = 'Google';
                let rating = null;
                let body = null;

                for (const ln of lines) {
                    const lower = ln.toLowerCase();
                    if (!dateTxt && (lower.includes('hace ') || lower.includes('ago'))) {
                        dateTxt = ln;
                        if (lower.includes('tripadvisor')) source = 'TripAdvisor';
                        else if (lower.includes('booking')) source = 'Booking';
                    }
                    if (rating === null) {
                        const m = ln.match(/(\\d)\\s*\\/\\s*5/);
                        if (m) rating = parseInt(m[1]);
                    }
                }

                // Cuerpo: primera línea larga que no sea autor/fecha/rating/tipo
                for (const ln of lines) {
                    if (ln === author) continue;
                    if (/^(\\d)\\s*\\/\\s*5$/.test(ln)) continue;
                    const lower = ln.toLowerCase();
                    if (lower.includes('hace ') || lower.includes('ago')) continue;
                    if (ln.includes('❘')) continue;
                    if (ln.length >= 25) {
                        body = ln;
                        break;
                    }
                }

                return {
                    author: author,
                    rating: rating,
                    body: body,
                    date_posted: dateTxt,
                    local_guide: text.toLowerCase().includes('local guide'),
                    source: source,
                };
            });
            }
        }

        const els = document.querySelectorAll('div.jftiEf');
        return Array.from(els).map(el => {
            const author = el.querySelector('div.d4r55');
            const body = el.querySelector('span.wiI7pd');
            const badge = el.querySelector('span.RfnDt');

            // Rating: try aria-label first, then "X/5" text
            let rating = null;
            const ratingAria = el.querySelector('span.kvMYJc');
            if (ratingAria) {
                const m = (ratingAria.getAttribute('aria-label') || '').match(/(\\d)/);
                if (m) rating = parseInt(m[1]);
            }
            if (!rating) {
                const ratingText = el.querySelector('.fontBodyLarge.fzvQIb, .DU9Pgb .fzvQIb');
                if (ratingText) {
                    const m = ratingText.innerText.match(/(\\d)[/,]\\s*\\d/);
                    if (m) rating = parseInt(m[1]);
                }
            }

            // Date: try rsqaWe first, then xRkPPb
            let dateTxt = null;
            const dateEl = el.querySelector('span.rsqaWe') || el.querySelector('.xRkPPb');
            if (dateEl) dateTxt = dateEl.innerText.trim();

            // Source (Google, TripAdvisor, etc.)
            let source = 'Google';
            const sourceEl = el.querySelector('.qmhsmd, .xRkPPb');
            if (sourceEl) {
                const st = sourceEl.innerText.toLowerCase();
                if (st.includes('tripadvisor')) source = 'TripAdvisor';
                else if (st.includes('booking')) source = 'Booking';
            }

            return {
                author: author ? author.innerText.trim() : null,
                rating: rating,
                body: body ? body.innerText.trim() : null,
                date_posted: dateTxt,
                local_guide: !!badge,
                source: source,
            };
        });
    }""", travel_mode)

    ts = now_iso()
    return [
        Review(
            author=r.get("author"), rating=r.get("rating"),
            body=r.get("body"), date_posted=r.get("date_posted"),
            source=r.get("source", "Google"),
            local_guide=r.get("local_guide", False), scraped_at=ts,
        )
        for r in raw
    ]


# ---------------------------------------------------------------------------
# Guardado
# ---------------------------------------------------------------------------

def load_output(path: Path) -> Dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_output(
    path: Path, reviews: List[dict], url: str, complete: bool,
    expected_total: Optional[int] = None,
) -> None:
    payload: Dict = {
        "source_url": url,
        "scraped":    len(reviews),
        "complete":   complete,
        "reviews":    reviews,
    }
    if expected_total is not None:
        payload["total_shown_in_ui"] = expected_total
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Scraping de un hotel
# ---------------------------------------------------------------------------

def scrape_hotel(
    url: str,
    output_path: Path,
    limit: Optional[int],
    headless: bool,
) -> int:
    existing = load_output(output_path)
    if existing.get("complete"):
        count = existing.get("scraped", 0)
        print(f"  Ya completado ({count} reviews). Saltando.", file=sys.stderr)
        return count

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            locale="es-ES",
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
            time.sleep(4)
            travel_mode = is_google_travel_url(url)

            accept_cookies(page)
            time.sleep(2)

            # Si llega a una pagina de resultados, click en el primero
            try:
                first = page.locator("a.hfpxzc").first
                if first.is_visible(timeout=2000):
                    first.click()
                    time.sleep(4)
            except Exception:
                pass

            # Click en tab Reseñas (Maps) o abrir reseñas completas (Travel)
            if travel_mode:
                open_all_reviews_travel(page)
            else:
                if not click_reviews_tab(page):
                    print("  [warn] No se encontro tab de Reseñas", file=sys.stderr)

            sort_by_newest(page)

            expected = parse_expected_review_count(page)
            print("  Cargando reviews (scroll hasta agotar o total mostrado)...", file=sys.stderr)
            loaded = scroll_reviews(
                page,
                max_reviews=limit,
                travel_mode=travel_mode,
                expected_total=expected,
            )
            print(f"  {loaded} reviews detectadas en el DOM", file=sys.stderr)

            reviews = parse_reviews(page, travel_mode=travel_mode)
            # Deduplicar porque Google a veces replica tarjetas ocultas/virtualizadas
            deduped: List[Review] = []
            seen = set()
            for r in reviews:
                key = (
                    (r.author or "").strip().lower(),
                    (r.date_posted or "").strip().lower(),
                    (r.body or "").strip().lower(),
                    r.rating,
                    r.source,
                )
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(r)
            reviews = deduped
            if limit:
                reviews = reviews[:limit]

            all_dicts = [asdict(r) for r in reviews]
            n = len(all_dicts)
            done = expected is None or (
                expected > 0 and n >= int(expected * 0.95)
            )
            save_output(output_path, all_dicts, url, complete=done, expected_total=expected)
            if expected and not done:
                print(
                    f"  [aviso] Extraidas {n} de ~{expected} segun la pagina. "
                    "Vuelve a ejecutar sin --headless o mas tarde si Google limita la carga.",
                    file=sys.stderr,
                )
            return n

        except Exception as exc:
            print(f"  Error: {exc}", file=sys.stderr)
            raise
        finally:
            browser.close()


# ---------------------------------------------------------------------------
# Multiples hoteles
# ---------------------------------------------------------------------------

def scrape_multiple(
    urls: List[str],
    output_dir: Path,
    limit: Optional[int],
    headless: bool,
    hotel_delay: float,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    total = len(urls)
    completed = skipped = 0

    for i, url in enumerate(urls, 1):
        url = url.strip()
        if not url or url.startswith("#"):
            continue

        m = re.search(r"search/([^?]+)", url) or re.search(r"place/([^/@]+)", url)
        slug = m.group(1)[:50] if m else f"hotel_{i}"
        slug = re.sub(r"[^a-zA-Z0-9_-]", "_", slug)
        out_path = output_dir / f"{slug}.json"

        existing = load_output(out_path)
        if existing.get("complete"):
            skipped += 1
            print(f"\n[{i}/{total}] {slug} — completado ({existing.get('scraped', '?')} reviews).", file=sys.stderr)
            continue

        elapsed = time.time() - t0
        print(f"\n[{i}/{total}] {slug}  ({elapsed/60:.1f} min)", file=sys.stderr)

        try:
            count = scrape_hotel(url, out_path, limit, headless)
            completed += 1
            print(f"  => {count} reviews -> {out_path.name}", file=sys.stderr)
        except Exception as exc:
            print(f"  => ERROR: {exc}", file=sys.stderr)

        if i < total:
            wait = hotel_delay + random.uniform(10, 25)
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
        description="Extrae reviews de hoteles en Google Maps/Travel (gratis, Playwright).",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    src = p.add_mutually_exclusive_group()
    src.add_argument("--url", default=None,
                     help="URL de Google Maps o Google Travel")
    src.add_argument("--url-file", default=None, metavar="FICHERO",
                     help="Fichero con URLs de Google Maps (una por linea)")
    p.add_argument("--output", default="google_reviews.json",
                   help="Salida JSON (1 hotel) o directorio (varios)")
    p.add_argument("--limit", type=int, default=None,
                   help="Max reviews por hotel")
    p.add_argument("--headless", action="store_true",
                   help="Ejecutar sin ventana del navegador")
    p.add_argument("--hotel-delay", type=float, default=30,
                   help="Segundos entre hoteles (default: 30)")
    return p


def main() -> int:
    args = build_parser().parse_args()
    print("Google Maps / Travel Scraper (gratuito, Playwright)", file=sys.stderr)

    if args.url_file:
        urls = Path(args.url_file).read_text(encoding="utf-8").splitlines()
        urls = [u.strip() for u in urls if u.strip() and not u.strip().startswith("#")]
        print(f"Hoteles: {len(urls)}", file=sys.stderr)
        scrape_multiple(urls, Path(args.output), args.limit, args.headless, args.hotel_delay)
        return 0

    url = args.url or DEFAULT_URL
    try:
        count = scrape_hotel(url, Path(args.output), args.limit, args.headless)
        print(f"\n{count} reviews guardadas en {Path(args.output).resolve()}")
        return 0
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
