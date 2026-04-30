#!/usr/bin/env python3
"""
Google Maps / Google Travel — Reviews Scraper (100% gratuito).

Extrae todas las reviews publicas de hoteles desde Google Travel (recomendado)
o Google Maps usando Playwright (Google requiere JS completo).
Por defecto usa la ficha de reseñas en Google Hoteles (Travel).
"""
import argparse
import datetime
import html
import json
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, unquote, urlparse

from curl_cffi import requests
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
TRAVEL_RPC_ID = "ocp93e"
TRAVEL_RPC_PAGE_SIZE = 10
TRAVEL_RPC_SORT_RELEVANT = 1
TRAVEL_RPC_SORT_NEWEST = 2

# Alicante Hills — entidad en Google Travel (pestaña /reviews)
DEFAULT_URL = (
    "https://www.google.com/travel/hotels/entity/"
    "CiIIgfrUsbiu__PMARC9076HrqjEml8aCy9nLzF0cjE4eHB4EAE/reviews?hl=es-ES"
)
KNOWN_TRAVEL_RPC_ENTITIES = {
    "CiIIgfrUsbiu__PMARC9076HrqjEml8aCy9nLzF0cjE4eHB4EAE": (
        "ChgIgfrUsbiu__PMARoLL2cvMXRyMTh4cHgQAQ"
    ),
}
KNOWN_TRAVEL_FIDS = {
    "CiIIgfrUsbiu__PMARC9076HrqjEml8aCy9nLzF0cjE4eHB4EAE": (
        "0xd6249fdec0195b3:0xcce7fd7386353d01"
    ),
}


def is_google_travel_url(url: str) -> bool:
    return "google.com/travel/hotels/entity/" in url


def known_travel_rpc_entity(url: str) -> Optional[str]:
    m = re.search(r"/travel/hotels/entity/([^/?#]+)", url)
    if not m:
        return None
    return KNOWN_TRAVEL_RPC_ENTITIES.get(m.group(1))


def known_travel_fid(url: str) -> Optional[str]:
    m = re.search(r"/travel/hotels/entity/([^/?#]+)", url)
    if not m:
        return None
    return KNOWN_TRAVEL_FIDS.get(m.group(1))


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
    reviewer_id:  Optional[str] = None
    review_id:    Optional[str] = None
    review_url:   Optional[str] = None
    review_detailed_rating: Optional[dict] = None
    response_from_owner_text: Optional[str] = None
    response_from_owner_date: Optional[str] = None


def review_to_output_dict(review: Review) -> dict:
    data = {
        "author": review.author,
        "rating": review.rating,
        "body": review.body,
        "date_posted": review.date_posted,
        "source": review.source,
        "local_guide": review.local_guide,
        "scraped_at": review.scraped_at,
        "reviewerId": review.reviewer_id,
        "reviewUrl": review.review_url,
        "reviewDetailedRating": review.review_detailed_rating or {},
        "responseFromOwnerText": review.response_from_owner_text,
        "responseFromOwnerDate": review.response_from_owner_date,
    }
    if review.review_id:
        data["reviewId"] = review.review_id
    return data


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
    try:
        clicked = page.evaluate("""() => {
            const btn = [...document.querySelectorAll('button')]
                .find(b => /Aceptar todo|Accept all/i.test(b.innerText || ''));
            if (!btn) return false;
            btn.click();
            return true;
        }""")
        if clicked:
            time.sleep(2)
    except Exception:
        pass


def strip_batchexecute_prefix(text: str) -> str:
    if text.startswith(")]}'"):
        return text[text.find("\n") + 1:]
    return text


def parse_travel_rpc_payload(text: str) -> Optional[List]:
    """Extrae el JSON interno de una respuesta batchexecute de Travel."""
    text = strip_batchexecute_prefix(text)
    for line in text.splitlines():
        if line.startswith(f'[["wrb.fr","{TRAVEL_RPC_ID}"'):
            outer = json.loads(line)
            if outer and outer[0] and len(outer[0]) > 2 and outer[0][2]:
                return json.loads(outer[0][2])
    return None


def clean_review_html(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    value = html.unescape(value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    value = re.sub(r"[ \t]+", " ", value)
    return value.strip() or None


def parse_reviewer_id_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    m = re.search(r"/maps/contrib/(\d+)", url)
    return m.group(1) if m else None


def parse_detailed_rating(raw: List) -> dict:
    """Mapea ratings detallados de hotel desde el bloque de review."""
    if len(raw) <= 7 or not raw[7]:
        return {}
    mapping = {
        1: "Rooms",
        4: "Service",
        5: "Location",
    }
    out = {}
    for item in raw[7]:
        try:
            code = item[0]
            pair = item[1]
            label = mapping.get(code)
            if not label or not pair:
                continue
            out[label] = pair[0]
        except Exception:
            continue
    return out


def parse_owner_response(raw: List) -> tuple[Optional[str], Optional[str]]:
    if len(raw) <= 5 or not raw[5]:
        return None, None
    owner_block = raw[5]
    try:
        parts = owner_block[0] if len(owner_block) > 0 and isinstance(owner_block[0], list) else []
        text_parts = [p.strip() for p in parts if isinstance(p, str) and p.strip()]
        response_text = " ".join(text_parts) if text_parts else None
        response_date = owner_block[1] if len(owner_block) > 1 and isinstance(owner_block[1], str) else None
        return response_text, response_date
    except Exception:
        return None, None


def build_google_review_url(review_id: Optional[str], fid: Optional[str]) -> Optional[str]:
    if not review_id or not fid or ":" not in fid:
        return None
    right = fid.split(":", 1)[1]
    return (
        "https://www.google.com/maps/reviews/data="
        f"!4m8!14m7!1m6!2m5!1s{review_id}!2m1!1s0x0:{right}!3m1!1s2@1:?hl=en"
    )


def review_from_travel_rpc_group(group: List, scraped_at: str, fid: Optional[str]) -> Optional[Review]:
    try:
        source_meta = group[0] or []
        raw = group[1] or []
        author_info = raw[0] or []
        source = source_meta[0] if source_meta else "Google"
        author = author_info[0] if author_info else None
        reviewer_url = author_info[1] if len(author_info) > 1 else None
        reviewer_id = parse_reviewer_id_from_url(reviewer_url)
        date_posted = raw[1] if len(raw) > 1 else None
        rating = None
        if len(raw) > 2 and raw[2]:
            rating = raw[2][0]

        body = None
        if len(raw) > 3 and raw[3]:
            body_block = raw[3][0][0]
            if len(body_block) > 1:
                body = clean_review_html(body_block[1])

        review_id = raw[8] if len(raw) > 8 else None
        review_url = build_google_review_url(review_id, fid)
        response_text, response_date = parse_owner_response(raw)

        return Review(
            author=author,
            rating=rating,
            body=body,
            date_posted=f"{date_posted} en {source}" if date_posted else None,
            source=source or "Google",
            local_guide=False,
            scraped_at=scraped_at,
            reviewer_id=reviewer_id,
            review_id=review_id,
            review_url=review_url,
            review_detailed_rating=parse_detailed_rating(raw),
            response_from_owner_text=response_text,
            response_from_owner_date=response_date,
        )
    except Exception:
        return None


def parse_travel_rpc_reviews(payload: List, fid: Optional[str]) -> tuple[List[Review], Optional[str]]:
    if not payload or not payload[0]:
        return [], None
    page = payload[0]
    groups = page[0] or []
    next_token = page[5] if len(page) > 5 else None
    ts = now_iso()
    reviews = []
    for group in groups:
        review = review_from_travel_rpc_group(group, ts, fid)
        if review:
            reviews.append(review)
    return reviews, next_token


def extract_travel_rpc_entity(post_data: str) -> Optional[str]:
    decoded = unquote(post_data or "")
    m = re.search(r"f\.req=(.*?)(?:&|$)", decoded)
    if not m:
        return None
    outer = json.loads(m.group(1))
    params = json.loads(outer[0][0][1])
    return params[8] if len(params) > 8 else None


def build_travel_rpc_url(travel_url: str) -> str:
    parsed = urlparse(travel_url)
    source_path = quote(parsed.path, safe="")
    return (
        "https://www.google.com/_/TravelFrontendUi/data/batchexecute"
        f"?rpcids={TRAVEL_RPC_ID}"
        f"&source-path={source_path}"
        "&hl=es-ES&soc-app=162&soc-platform=1&soc-device=1&rt=c"
    )


def build_travel_rpc_f_req(entity: str, cursor: Optional[str], sort: int) -> str:
    if cursor:
        params = [
            None, None, None, TRAVEL_RPC_PAGE_SIZE, sort, None, None, "",
            entity, cursor, None, [[]], None, "",
        ]
        mode = "generic"
    else:
        if sort == TRAVEL_RPC_SORT_NEWEST:
            params = [
                None, None, None, TRAVEL_RPC_PAGE_SIZE, sort, None, None, "",
                entity, "", None, [[]], None, "",
            ]
            mode = "generic"
        else:
            params = [None, None, None, None, None, None, None, None, entity, None, None, [[]]]
            mode = "1"
    return json.dumps(
        [[[TRAVEL_RPC_ID, json.dumps(params, separators=(",", ":")), None, mode]]],
        separators=(",", ":"),
    )


def fetch_travel_rpc_page(
    rpc_url: str,
    travel_url: str,
    entity: str,
    cursor: Optional[str],
    sort: int,
    fid: Optional[str],
) -> tuple[List[Review], Optional[str]]:
    resp = requests.post(
        rpc_url,
        data={"f.req": build_travel_rpc_f_req(entity, cursor, sort)},
        impersonate="chrome",
        timeout=30,
        headers={
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
            "referer": travel_url,
        },
    )
    resp.raise_for_status()
    payload = parse_travel_rpc_payload(resp.text)
    if payload is None:
        return [], None
    return parse_travel_rpc_reviews(payload, fid)


def collect_travel_rpc_reviews(
    url: str,
    entity: str,
    limit: Optional[int],
    sort: int,
    fid: Optional[str],
) -> tuple[List[Review], bool]:
    rpc_url = build_travel_rpc_url(url)
    reviews, cursor = fetch_travel_rpc_page(rpc_url, url, entity, None, sort, fid)
    print("  Usando paginacion RPC de Google Travel...", file=sys.stderr)
    print(f"    ... {len(reviews)} reviews", file=sys.stderr)
    empty_pages = 0

    while cursor and (not limit or len(reviews) < limit):
        current_cursor = cursor
        page_reviews, next_cursor = fetch_travel_rpc_page(
            rpc_url, url, entity, cursor, sort, fid
        )
        if not page_reviews:
            empty_pages += 1
            # Google Travel a veces devuelve pagina vacia sin cerrar cursor:
            # lo tratamos como final practico para evitar `complete=false` espurio.
            if empty_pages >= 2 or next_cursor == current_cursor:
                cursor = None
                break
            cursor = next_cursor
            continue
        empty_pages = 0
        reviews.extend(page_reviews)
        cursor = next_cursor
        if len(reviews) % 100 == 0:
            print(f"    ... {len(reviews)} reviews", file=sys.stderr)
        time.sleep(random.uniform(0.25, 0.8))

    return reviews, not cursor


def filter_reviews_by_origin(reviews: List[Review], origin: str) -> List[Review]:
    if origin == "all":
        return reviews
    return [r for r in reviews if (r.source or "").strip().lower() == origin]


def dedupe_reviews(reviews: List[Review]) -> List[Review]:
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
    return deduped


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
    try:
        panel.evaluate("el => { if (el) el.scrollTop = el.scrollHeight; }")
    except Exception:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
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

def scrape_hotel_travel_rpc(
    url: str,
    output_path: Path,
    limit: Optional[int],
    headless: bool,
    sort: int = TRAVEL_RPC_SORT_NEWEST,
    reviews_origin: str = "all",
) -> int:
    """Pagina las reseñas de Google Travel usando el RPC interno que usa la UI."""
    existing = load_output(output_path)
    if existing.get("complete"):
        count = existing.get("scraped", 0)
        print(f"  Ya completado ({count} reviews). Saltando.", file=sys.stderr)
        return count

    entity = known_travel_rpc_entity(url)
    fid = known_travel_fid(url)
    if entity:
        reviews, exhausted = collect_travel_rpc_reviews(url, entity, limit, sort, fid)
        reviews = filter_reviews_by_origin(reviews, reviews_origin)
        reviews = dedupe_reviews(reviews)
        if limit:
            reviews = reviews[:limit]
        all_dicts = [review_to_output_dict(r) for r in reviews]
        save_output(output_path, all_dicts, url, complete=exhausted)
        return len(all_dicts)

    first_post_data: Optional[str] = None
    first_response_text: Optional[str] = None

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

        def capture_request(req) -> None:
            nonlocal first_post_data
            if first_post_data:
                return
            if "TravelFrontendUi/data/batchexecute" in req.url and f"rpcids={TRAVEL_RPC_ID}" in req.url:
                first_post_data = req.post_data or ""

        def capture_response(resp) -> None:
            nonlocal first_response_text
            if first_response_text:
                return
            if "TravelFrontendUi/data/batchexecute" in resp.url and f"rpcids={TRAVEL_RPC_ID}" in resp.url:
                try:
                    first_response_text = resp.body().decode("utf-8", "replace")
                except Exception:
                    first_response_text = None

        page.on("request", capture_request)
        page.on("response", capture_response)
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        time.sleep(2)
        accept_cookies(page)
        time.sleep(4)
        if "consent.google" in page.url:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            time.sleep(2)

        deadline = time.time() + 45
        while time.time() < deadline and (not first_post_data or not first_response_text):
            time.sleep(0.5)

        browser.close()

    if not first_post_data or not first_response_text:
        raise RuntimeError("No se pudo capturar el RPC inicial de Google Travel")

    entity = extract_travel_rpc_entity(first_post_data)
    if not entity:
        raise RuntimeError("No se pudo extraer el entity token de Google Travel")

    first_payload = parse_travel_rpc_payload(first_response_text)
    if first_payload is None:
        raise RuntimeError("No se pudo parsear la primera respuesta RPC de Google Travel")

    reviews, cursor = parse_travel_rpc_reviews(first_payload, fid)
    rpc_url = build_travel_rpc_url(url)
    print("  Usando paginacion RPC de Google Travel...", file=sys.stderr)
    print(f"    ... {len(reviews)} reviews", file=sys.stderr)
    empty_pages = 0
    while cursor and (not limit or len(reviews) < limit):
        current_cursor = cursor
        page_reviews, next_cursor = fetch_travel_rpc_page(
            rpc_url, url, entity, cursor, sort, fid
        )
        if not page_reviews:
            empty_pages += 1
            if empty_pages >= 2 or next_cursor == current_cursor:
                cursor = None
                break
            cursor = next_cursor
            continue
        empty_pages = 0
        reviews.extend(page_reviews)
        cursor = next_cursor
        if len(reviews) % 100 == 0:
            print(f"    ... {len(reviews)} reviews", file=sys.stderr)
        time.sleep(random.uniform(0.25, 0.8))

    reviews = filter_reviews_by_origin(reviews, reviews_origin)
    deduped = dedupe_reviews(reviews)
    if limit:
        deduped = deduped[:limit]

    all_dicts = [review_to_output_dict(r) for r in deduped]
    complete = not cursor
    save_output(output_path, all_dicts, url, complete=complete)
    return len(all_dicts)


def scrape_hotel(
    url: str,
    output_path: Path,
    limit: Optional[int],
    headless: bool,
    use_rpc: bool = False,
    rpc_sort: int = TRAVEL_RPC_SORT_NEWEST,
    reviews_origin: str = "all",
) -> int:
    if use_rpc and is_google_travel_url(url):
        return scrape_hotel_travel_rpc(
            url,
            output_path,
            limit,
            headless,
            sort=rpc_sort,
            reviews_origin=reviews_origin,
        )

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

            all_dicts = [review_to_output_dict(r) for r in reviews]
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
    use_rpc: bool,
    rpc_sort: int,
    reviews_origin: str,
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
            count = scrape_hotel(
                url,
                out_path,
                limit,
                headless,
                use_rpc=use_rpc,
                rpc_sort=rpc_sort,
                reviews_origin=reviews_origin,
            )
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
    p.add_argument("--rpc", action="store_true",
                   help="Google Travel: paginar por RPC interno en vez de scroll visual")
    p.add_argument("--rpc-sort", choices=["newest", "relevant"], default="newest",
                   help="Orden RPC para Google Travel (default: newest)")
    p.add_argument("--reviews-origin", choices=["all", "google"], default="all",
                   help="Origen de reseñas en modo RPC (all o solo google)")
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
        rpc_sort = TRAVEL_RPC_SORT_NEWEST if args.rpc_sort == "newest" else TRAVEL_RPC_SORT_RELEVANT
        scrape_multiple(
            urls,
            Path(args.output),
            args.limit,
            args.headless,
            args.hotel_delay,
            args.rpc,
            rpc_sort,
            args.reviews_origin,
        )
        return 0

    url = args.url or DEFAULT_URL
    try:
        rpc_sort = TRAVEL_RPC_SORT_NEWEST if args.rpc_sort == "newest" else TRAVEL_RPC_SORT_RELEVANT
        count = scrape_hotel(
            url,
            Path(args.output),
            args.limit,
            args.headless,
            use_rpc=args.rpc,
            rpc_sort=rpc_sort,
            reviews_origin=args.reviews_origin,
        )
        print(f"\n{count} reviews guardadas en {Path(args.output).resolve()}")
        return 0
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
