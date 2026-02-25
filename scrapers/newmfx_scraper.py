"""
NewMFX – Stash Performer Scraper
Bypasses the age-gate with a proper session handshake:
  1. GET the landing page → parse CSRF _token
  2. POST to https://newmfx.com with the token → server accepts us and
     redirects to the real site; our cookie jar stores the session cookie
  3. All subsequent GETs reuse the same cookie jar

No external dependencies – stdlib only (Python 3.6+).
"""

import http.cookiejar
import json
import re
import sys
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from html import unescape

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL   = "https://newmfx.com"
CAST_URL   = "https://newmfx.com/cast?orderby=az"
SEARCH_URL = "https://newmfx.com/search?text={}"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Session singleton – built once, reused for every request in one script run
# ---------------------------------------------------------------------------
_opener: urllib.request.OpenerDirector | None = None


def _get_opener() -> urllib.request.OpenerDirector:
    """
    Build (or return) an opener that:
      • carries a cookie jar across all requests
      • follows redirects automatically
      • completes the age-gate POST handshake on first call
    """
    global _opener
    if _opener is not None:
        return _opener

    jar = http.cookiejar.CookieJar()
    _opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(jar),
        urllib.request.HTTPRedirectHandler(),
    )
    _opener.addheaders = [
        ("User-Agent", USER_AGENT),
        ("Referer",    BASE_URL + "/"),
        ("Accept",     "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
        ("Accept-Language", "en-US,en;q=0.9"),
    ]

    _do_age_gate_handshake(_opener)
    return _opener


def _do_age_gate_handshake(opener: urllib.request.OpenerDirector) -> None:
    """
    Fetch the landing page, parse the Laravel CSRF _token, and POST it back.
    After the POST the server redirects to the real site and sets a session
    cookie in our jar automatically.
    """
    debug("Performing age-gate handshake…")
    try:
        # Step 1 – GET landing page (receives XSRF/session cookies from server)
        with opener.open(BASE_URL + "/", timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Already bypassed?
        if 'class="wrap-home"' not in html:
            debug("Age-gate not present – handshake skipped")
            return

        # Step 2 – extract CSRF token from the hidden input
        token_match = re.search(
            r'<input[^>]+name="_token"[^>]+value="([^"]+)"', html, re.IGNORECASE
        )
        if not token_match:
            debug("WARNING: could not find CSRF _token in landing page")
            return
        csrf_token = token_match.group(1)
        debug(f"CSRF token: {csrf_token[:12]}…")

        # Step 3 – POST the token to the landing-page form action
        post_data = urllib.parse.urlencode({"_token": csrf_token}).encode()
        post_req = urllib.request.Request(
            BASE_URL + "/",
            data=post_data,
            headers={
                "Content-Type":  "application/x-www-form-urlencoded",
                "Referer":       BASE_URL + "/",
                "Origin":        BASE_URL,
            },
            method="POST",
        )
        with opener.open(post_req, timeout=20) as resp:
            post_html = resp.read().decode("utf-8", errors="replace")

        if 'class="wrap-home"' in post_html:
            debug("WARNING: still on age-gate after POST – site may have changed")
        else:
            debug("Age-gate bypassed successfully")

    except Exception as exc:
        debug(f"Age-gate handshake failed: {exc}")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def fetch_html(url: str) -> str:
    debug(f"GET {url}")
    opener = _get_opener()
    try:
        with opener.open(url, timeout=20) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        debug(f"HTTP {exc.code} for {url}")
        raise
    except urllib.error.URLError as exc:
        debug(f"URL error for {url}: {exc.reason}")
        raise


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def read_json_input() -> dict:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


def debug(msg: str) -> None:
    sys.stderr.write(f"[NewMFX] {msg}\n")


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def strip_html(value: str) -> str:
    """Remove all HTML tags."""
    if not value:
        return ""
    return re.sub(r"<[^>]+>", "", value).strip()


def normalise(value: str) -> str:
    """Lowercase, remove combining accents, collapse whitespace – for fuzzy matching."""
    if not value:
        return ""
    nkfd = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(c for c in nkfd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", ascii_only).strip().lower()


def make_absolute(url: str) -> str:
    if not url:
        return ""
    url = unescape(url).strip()
    if url.startswith("http"):
        return url
    return BASE_URL + ("" if url.startswith("/") else "/") + url


# ---------------------------------------------------------------------------
# Search result card parsing
# ---------------------------------------------------------------------------

# Primary – card link wrapping a box-image div with an <img alt="name">
_CARD_RE = re.compile(
    r'<a[^>]+href="([^"]+)"[^>]*>\s*'
    r'<div[^>]*class="[^"]*box-image[^"]*"[^>]*>\s*'
    r'<img[^>]+src="([^"]+)"[^>]*alt="([^"]+)"',
    re.IGNORECASE | re.DOTALL,
)

# Fallback – bare /cast/… anchor text
_CAST_LINK_RE = re.compile(
    r'<a[^>]+href="(/cast/[^"]+)"[^>]*>([^<]+)</a>',
    re.IGNORECASE,
)


def extract_search_cards(html: str) -> list[dict]:
    results: list[dict] = []
    seen: set[str] = set()

    for m in _CARD_RE.finditer(html):
        href, src, alt = m.groups()
        canonical = make_absolute(href)
        if canonical in seen:
            continue
        seen.add(canonical)
        results.append({
            "name":  unescape(alt).strip(),
            "url":   canonical,
            "image": make_absolute(src),
        })

    if not results:
        for m in _CAST_LINK_RE.finditer(html):
            href, text = m.groups()
            canonical = make_absolute(href)
            if canonical in seen:
                continue
            seen.add(canonical)
            name = unescape(text).strip()
            if name:
                results.append({"name": name, "url": canonical})

    return results


# ---------------------------------------------------------------------------
# Performer page field extraction
# ---------------------------------------------------------------------------

def _first(pattern: str, html: str, group: int = 1) -> str:
    m = re.search(pattern, html, re.IGNORECASE | re.DOTALL)
    return unescape(m.group(group)).strip() if m else ""


def extract_name(html: str) -> str:
    name = _first(
        r'<div[^>]*class="[^"]*box-title-video[^"]*"[^>]*>.*?<h1[^>]*>([^<]+)</h1>',
        html,
    )
    if not name:
        name = _first(r'<h1[^>]*>([^<]+)</h1>', html)
    return strip_html(name)


def extract_image(html: str) -> str:
    m = re.search(
        r'<section[^>]*class="[^"]*data-cast[^"]*"[^>]*>.*?'
        r'<div[^>]*class="[^"]*box-image[^"]*"[^>]*>.*?'
        r'<img[^>]+src="([^"]+)"',
        html, re.IGNORECASE | re.DOTALL,
    )
    return make_absolute(m.group(1)) if m else ""


def _extract_stat(html: str, label: str) -> str:
    """
    Extract text value associated with a <strong>label</strong> inside a
    topics-cast <li>.  e.g. label='Height' → '170 cm'
    """
    pattern = (
        r'<li[^>]*>.*?<strong[^>]*>[^<]*'
        + re.escape(label)
        + r'[^<]*</strong>\s*:?\s*([^<]+)</li>'
    )
    return _first(pattern, html)


def extract_height(html: str) -> str:
    m = re.search(r"(\d+)", _extract_stat(html, "Height"))
    return m.group(1) if m else ""


def extract_weight(html: str) -> str:
    m = re.search(r"(\d+)", _extract_stat(html, "Weight"))
    return m.group(1) if m else ""


def extract_hair_color(html: str) -> str:
    return re.sub(r"^\s*:\s*", "", _extract_stat(html, "Hair Color"))


def extract_eye_color(html: str) -> str:
    return re.sub(r"^\s*:\s*", "", _extract_stat(html, "Eyes Color"))


def extract_tattoos(html: str) -> str:
    return re.sub(r"^\s*:\s*", "", _extract_stat(html, "Tatoo"))


def extract_details(html: str) -> str:
    """Pipe-joined stat text, mirrors the old XPath concat behaviour."""
    m = re.search(
        r'<div[^>]*class="[^"]*topics-cast[^"]*"[^>]*>(.*?)</div>',
        html, re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return ""
    lis = re.findall(r"<li[^>]*>(.*?)</li>", m.group(1), re.IGNORECASE | re.DOTALL)
    parts = [re.sub(r"\s+", " ", strip_html(li)).strip() for li in lis]
    return " | ".join(p for p in parts if p)


# ---------------------------------------------------------------------------
# High-level scraper operations
# ---------------------------------------------------------------------------

def search_performers(query: str) -> list[dict]:
    url  = SEARCH_URL.format(urllib.parse.quote_plus(query)) if query else CAST_URL
    html = fetch_html(url)

    if 'class="wrap-home"' in html:
        debug("Age-gate still showing after handshake – falling back to CAST_URL")
        html = fetch_html(CAST_URL)

    norm_q  = normalise(query)
    results = []
    for card in extract_search_cards(html):
        if norm_q and norm_q not in normalise(card["name"]):
            continue
        p = {"name": card["name"], "url": card["url"]}
        if card.get("image"):
            p["image"] = card["image"]
        results.append(p)

    debug(f"Search '{query}' → {len(results)} result(s)")
    return results


def scrape_performer_url(url: str) -> dict:
    html = fetch_html(url)

    if 'class="wrap-home"' in html:
        debug("Age-gate showing for performer URL – re-doing handshake")
        global _opener
        _opener = None          # force a fresh handshake
        html = fetch_html(url)

    performer: dict = {"url": url}

    for key, extractor in [
        ("name",       extract_name),
        ("image",      extract_image),
        ("height",     extract_height),
        ("weight",     extract_weight),
        ("hair_color", extract_hair_color),
        ("eye_color",  extract_eye_color),
        ("tattoos",    extract_tattoos),
        ("details",    extract_details),
    ]:
        val = extractor(html)
        if val:
            performer[key] = val

    debug(f"Scraped: {performer.get('name', '?')} from {url}")
    return performer


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) < 2:
        debug("Missing action argument")
        sys.exit(1)

    action  = sys.argv[1]
    payload = read_json_input()

    if action == "performerByName":
        print(json.dumps(search_performers(payload.get("name", ""))))

    elif action == "performerByFragment":
        url = payload.get("url", "")
        print(json.dumps(scrape_performer_url(url) if url else payload))

    elif action == "performerByURL":
        url = payload.get("url", "")
        print(json.dumps(scrape_performer_url(url) if url else {}))

    else:
        debug(f"Unknown action: {action}")
        sys.exit(1)


if __name__ == "__main__":
    main()
