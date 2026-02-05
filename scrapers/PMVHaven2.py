#!/usr/bin/env python3
# PMVHaven2.py
# Updated for new PMVHaven ID/search flow

import json
import os
import re
import sys
import traceback
from datetime import datetime
from importlib import util as importlib_util
from typing import Never, Optional, Dict, Any, Iterable, List, Tuple
import subprocess
from difflib import SequenceMatcher


# ---------------------------
# Logging (file + stderr)
# ---------------------------

def _log_path() -> str:
    log_dir = os.environ.get("STASH_LOG_DIR") or os.path.dirname(os.path.abspath(__file__))
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, "PMVHaven.scraper.log")


def _write_log(level: str, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {msg}\n"
    try:
        with open(_log_path(), "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    try:
        sys.stderr.write(line)
        sys.stderr.flush()
    except Exception:
        pass


def log_debug(msg: str) -> None:
    _write_log("DEBUG", msg)


def log_error(msg: str) -> None:
    _write_log("ERROR", msg)


def jprint(obj: Dict[str, Any]) -> None:
    """Safe JSON print to stdout that prevents crashes."""
    try:
        json_output = json.dumps(obj, ensure_ascii=False)
        if hasattr(sys.stdout, "buffer"):
            sys.stdout.buffer.write(json_output.encode("utf-8"))
            sys.stdout.buffer.write(b"\n")
            sys.stdout.flush()
        else:
            sys.stdout.write(json_output + "\n")
            sys.stdout.flush()
    except Exception as e:
        log_error(f"JSON print failed: {e}")
        sys.stdout.write('{"error":"PMVHaven fatal print error"}')
        sys.stdout.flush()


def fail(message: str) -> Never:
    """Emit a JSON error object and exit."""
    log_error(message)
    jprint({"error": message})
    raise SystemExit(1)


def ensure_requirements(*packages: str) -> None:
    missing = [package for package in packages if importlib_util.find_spec(package) is None]
    if not missing:
        return
    log_debug(f"Installing missing packages: {', '.join(missing)}")
    cmd = [sys.executable, "-m", "pip", "install", *missing]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        log_error(f"Pip install failed: {result.stderr.strip()}")
        fail("Failed to install required Python packages.")


def dig(data: Any, *keys: str) -> Any:
    current = data
    for key in keys:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            return None
    return current


ensure_requirements("requests", "cloudscraper")
import requests  # noqa: E402
import cloudscraper  # noqa: E402


# ---------------------------
# Helpers
# ---------------------------

API_WATCH_TEMPLATE = "https://pmvhaven.com/api/videos/{video_id}/watch-page"
API_SEARCH = "https://pmvhaven.com/api/videos/search"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
DEFAULT_TIMEOUT = (10, 20)
DURATION_TOLERANCE_SECONDS = 10.0
scraper = cloudscraper.create_scraper()


def _json_from_response(resp: requests.Response, context: str) -> Dict[str, Any]:
    """Safely decodes JSON from a response, returning an error dict on failure."""
    try:
        return resp.json()
    except requests.exceptions.JSONDecodeError:
        snippet = (resp.text or "")[:1000]
        ctype = resp.headers.get("Content-Type", "")
        log_error(f"{context}: JSON decode error. HTTP {resp.status_code} CT='{ctype}' Body='{snippet}'")
        return {"error": f"{context}: non-JSON response", "status": resp.status_code}


def _get_json(url: str, context: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    log_debug(f"GET {url} params={params}")
    try:
        headers = {"User-Agent": UA, "Accept": "application/json", "Referer": "https://pmvhaven.com/"}
        resp = scraper.get(url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
    except Exception as e:
        fail(f"{context}: request failed: {e}")

    if not resp.ok:
        log_error(f"{context}: non-OK HTTP {resp.status_code}")
    return _json_from_response(resp, context)


# ---------------------------
# PMVHaven API wrappers
# ---------------------------

def get_watch_page(video_id: str) -> Dict[str, Any]:
    url = API_WATCH_TEMPLATE.format(video_id=video_id)
    return _get_json(url, "get_watch_page")


def search_videos(query: str, limit: int = 32, page: int = 1) -> Dict[str, Any]:
    params = {"limit": limit, "page": page, "q": query}
    return _get_json(API_SEARCH, "search_videos", params=params)


def _extract_scene_id(text: str) -> Optional[str]:
    match = re.search(r"([a-f0-9]{24})", text or "")
    return match.group(1) if match else None


def _extract_storage_key(value: str) -> Optional[str]:
    if not value:
        return None
    name = os.path.basename(value.split("?")[0])
    match = re.search(r"([A-Za-z0-9_-]+\.[A-Za-z0-9]+)$", name)
    if match:
        return match.group(1)
    return name if "." in name else None


def _build_search_query(value: str) -> str:
    if not value:
        return ""
    name = os.path.basename(value)
    name = re.sub(r"\.[^.]+$", "", name)  # remove extension

    # Rule: delete the first set of _-_ and everything before it
    if "_-_" in name:
        parts = name.split("_-_", 1)
        if len(parts) > 1:
            name = parts[1]
    
    # Rule: Cleanse the name by replacing _ or - with whitespace
    name = name.replace("_", " ").replace("-", " ")
    
    # Rule: delete the string of numbers along with everything after it
    # Looking for a long string of numbers (timestamp-like).
    # Type B example uses 13 digits (milliseconds).
    # We'll recognize a sequence of 8+ digits as a timestamp start.
    match = re.search(r"\s(\d{8,})\s", name) # Check for surrounded by space first
    if not match:
         match = re.search(r"\b(\d{8,})", name) # Or just boundary

    if match:
        # Cut off at the start of the match
        name = name[:match.start()]

    # Cleanup extra whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name



def _extract_filename_tokens(filename: str) -> List[str]:
    if not filename:
        return []
    name = os.path.basename(filename.split("?")[0])
    name = re.sub(r"\.[^.]+$", "", name)
    combined = re.search(r"(\d+)_([A-Za-z0-9]+)", name)
    if combined:
        timestamp, suffix = combined.groups()
        return [f"{timestamp}_{suffix}", timestamp, suffix]

    timestamp = re.search(r"\d+", name)
    if timestamp:
        return [timestamp.group(0)]

    if re.fullmatch(r"[A-Za-z0-9]+", name):
        return [name]

    return []


def _iter_strings(obj: Any) -> Iterable[str]:
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for value in obj.values():
            yield from _iter_strings(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _iter_strings(value)


def _video_contains_token(video: Dict[str, Any], tokens: List[str]) -> bool:
    if not tokens:
        return False
    for value in _iter_strings(video):
        for token in tokens:
            if token in value:
                return True
    return False


def _extract_video_candidates(data: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    for key in ("videos", "results", "data"):
        items = data.get(key)
        if isinstance(items, list):
            return items
    nested = data.get("data") if isinstance(data.get("data"), dict) else None
    if nested:
        for key in ("videos", "results"):
            items = nested.get(key)
            if isinstance(items, list):
                return items
    return []


def _get_video_id(item: Dict[str, Any]) -> Optional[str]:
    return item.get("_id") or item.get("id") or item.get("videoId")


def _coerce_duration(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _extract_candidate_duration(video: Dict[str, Any]) -> Optional[float]:
    for key in ("duration", "durationSeconds", "length", "runtime", "runTime", "time"):
        duration = _coerce_duration(video.get(key))
        if duration is not None:
            return duration
    return None


def _duration_matches(candidate_duration: Optional[float], local_durations: List[float]) -> bool:
    if candidate_duration is None or not local_durations:
        return False
    return any(abs(candidate_duration - local) <= DURATION_TOLERANCE_SECONDS for local in local_durations)


def _extract_local_durations(params: Dict[str, Any]) -> List[float]:
    durations: List[float] = []

    def _collect(value: Any) -> None:
        duration = _coerce_duration(value)
        if duration is not None:
            durations.append(duration)

    if not isinstance(params, dict):
        return durations

    _collect(params.get("duration"))
    scene = params.get("scene")
    if isinstance(scene, dict):
        _collect(scene.get("duration"))
        fingerprints = scene.get("fingerprints")
    else:
        fingerprints = params.get("fingerprints")

    if isinstance(fingerprints, list):
        for fingerprint in fingerprints:
            if isinstance(fingerprint, dict):
                _collect(fingerprint.get("duration"))

    return durations


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value or "").strip("-")
    return slug.lower()


def _pick_image(video: Dict[str, Any]) -> str:
    if thumb := video.get("thumbnailUrl"):
        return thumb
    for item in video.get("thumbnails", []) or []:
        if isinstance(item, str):
            return item
    return ""


def _build_selection_options(candidates: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    options = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        video_id = _get_video_id(item)
        title = item.get("title") or ""
        slug = _slugify(title)
        url = f"https://pmvhaven.com/video/{slug}_{video_id}" if slug and video_id else ""
        thumb = _pick_image(item)
        options.append(
            {
                "title": title,
                "id": video_id or "",
                "url": url,
                "thumbnail": thumb,
            }
        )

    return {"results": options}


def _normalize_names(value: Any) -> List[str]:
    if isinstance(value, list):
        names = value
    elif isinstance(value, str):
        names = [item.strip() for item in value.split(",")]
    else:
        names = []
    return [name for name in names if isinstance(name, str) and name.strip()]


def _extract_studio_url(video: Dict[str, Any]) -> Optional[str]:
    for key in ("creatorUrl", "creatorURL", "creatorPage", "creatorLink"):
        url = video.get(key)
        if isinstance(url, str) and url.strip():
            return url
    return None


def _build_scene(video: Dict[str, Any]) -> Dict[str, Any]:
    title = video.get("title") or ""
    video_id = video.get("_id") or ""
    slug = _slugify(title)
    url = f"https://pmvhaven.com/video/{slug}_{video_id}" if slug and video_id else f"https://pmvhaven.com/video/{video_id}"

    tags = _normalize_names(video.get("tags") or [])
    performers = _normalize_names(video.get("starsTags") or [])
    creator = video.get("creator") or []
    if isinstance(creator, list):
        studio_name = creator[0] if creator else None
    else:
        studio_name = creator
    studio_url = _extract_studio_url(video)

    scene = {
        "title": title,
        "url": url,
        "image": _pick_image(video),
        "date": (video.get("uploadDate") or video.get("isoDate") or "").split("T")[0],
        "performers": [{"name": name} for name in performers],
        "tags": [{"name": name} for name in tags],
    }

    if description := video.get("description"):
        scene["details"] = description
    if studio_name or studio_url:
        studio: Dict[str, Any] = {}
        if studio_name:
            studio["name"] = studio_name
        if studio_url:
            studio["url"] = studio_url
        scene["studio"] = studio

    return scene


def _get_video_details(video_id: str) -> Dict[str, Any]:
    data = get_watch_page(video_id)
    if "error" in data:
        return data

    video = dig(data, "data", "video")
    if not isinstance(video, dict):
        fail(f"Video data not found in watch-page response: {data}")
    return video


def _get_video_by_id(video_id: str) -> Dict[str, Any]:
    video = _get_video_details(video_id)
    if "error" in video:
        return video
    return _build_scene(video)


def _trim_query(query: str) -> str:
    words = [word for word in query.split() if word]
    if len(words) <= 1:
        return ""
    return " ".join(words[1:])


def _filter_candidates_by_duration(
    candidates: List[Dict[str, Any]],
    local_durations: List[float],
) -> List[Dict[str, Any]]:
    if not local_durations:
        return candidates
    matched: List[Dict[str, Any]] = []
    fallback: List[Dict[str, Any]] = []
    for item in candidates:
        duration = _extract_candidate_duration(item)
        if duration is None:
            fallback.append(item)
            continue
        if _duration_matches(duration, local_durations):
            matched.append(item)
    if matched:
        return matched
    if fallback:
        return fallback
    return candidates


def _fetch_candidate_details(candidate: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Given a candidate object from search results, fetch the full video details
    (the 'watch page' data), which contains tags, description, etc.
    """
    video_id = _get_video_id(candidate)
    if not video_id:
        return None
    
    # helper: _get_video_details calls get_watch_page -> returns raw video dict
    details = _get_video_details(video_id)
    if "error" in details:
        # If we can't get details, we can't really use this candidate effectively
        return None
        
    return details


def _calculate_score(video: Dict[str, Any], query_title: str, local_durations: List[float]) -> float:
    """
    Calculate a relevance score for the video.
    """
    score = 0.0
    
    # 1. Title Similarity
    video_title = video.get("title", "")
    if query_title and video_title:
        # Use SequenceMatcher to get a ratio between 0 and 1
        ratio = SequenceMatcher(None, query_title.lower(), video_title.lower()).ratio()
        score += (ratio * 100)  # max 100
        
    # 2. Duration Match
    duration = _extract_candidate_duration(video)
    if local_durations and _duration_matches(duration, local_durations):
        score += 50.0  # Big boost for matching duration
        
    # 3. Recency (optional tie-breaker, small boost for newer videos?)
    # ... skipping for now to keep it simple
    
    return score


def _handle_search_results(
    candidates: List[Dict[str, Any]],
    tokens: List[str],
    local_durations: List[float],
) -> Dict[str, Any]:
    # Prune candidates using basic filters first (optional, but saves requests)
    # For now, let's fetch details for top X candidates to ensure accuracy
    
    # If we have too many candidates, maybe filter by tokens first?
    filtered_candidates = candidates
    if len(candidates) > 10 and tokens:
         # Rough pre-filter to reduce API calls
         filtered = [c for c in candidates if _video_contains_token(c, tokens)]
         if filtered:
             filtered_candidates = filtered
    
    # Limit to top 5 to avoid long waiting times since we are sequential now
    candidates_to_process = filtered_candidates[:5] 

    query_title = " ".join(tokens) if tokens else ""

    results_with_details = []
    
    # Sequential fetching
    for c in candidates_to_process:
        try:
            details = _fetch_candidate_details(c)
            if details:
                results_with_details.append(details)
        except Exception as exc:
            log_error(f"Failed to fetch details for candidate {_get_video_id(c)}: {exc}")

    if not results_with_details:
        fail("No valid video details found from search results")

    # Score candidates
    scored_candidates = []
    for video in results_with_details:
        score = _calculate_score(video, query_title, local_durations)
        scored_candidates.append((score, video))

    # Sort by score desc
    scored_candidates.sort(key=lambda x: x[0], reverse=True)

    if not scored_candidates:
         return _build_selection_options(candidates[:3])

    best_score, best_video = scored_candidates[0]
    
    match_duration = _extract_candidate_duration(best_video)
    if local_durations and not _duration_matches(match_duration, local_durations):
        # If best match fails duration check, look for next best that passes
        for score, video in scored_candidates:
            if _duration_matches(_extract_candidate_duration(video), local_durations):
                return _build_scene(video)
        
        # If no strict duration match found, fall back to selection options
        # If no strict duration match found, fall back to selection options
        # Sort candidates by score and take top 3
        top_candidates = sorted(scored_candidates, key=lambda x: x[0], reverse=True)[:3]
        return _build_selection_options([v for s, v in top_candidates])

    return _build_scene(best_video)


def _search_videos_with_retries(query: str, tokens: List[str], local_durations: List[float]) -> Dict[str, Any]:
    current_query = query
    for attempt in range(3):
        data = search_videos(current_query)
        if "error" in data:
            return data

        candidates = list(_extract_video_candidates(data))
        if candidates:
            return _handle_search_results(candidates, tokens, local_durations)

    fail(f"No search results for query '{query}' after retries")


def sceneByFragment(params: Dict[str, Any]) -> Dict[str, Any]:
    filename = dig(params, "filename") or ""
    title = dig(params, "title") or ""
    local_durations = _extract_local_durations(params)

    scene_id = _extract_scene_id(filename) or _extract_scene_id(title)
    if scene_id:
        return _get_video_by_id(scene_id)

    storage_key = _extract_storage_key(filename) or _extract_storage_key(title)
    query_source = storage_key or title or filename
    query = _build_search_query(query_source)
    tokens = _extract_filename_tokens(filename)

    if not query:
        fail("Did not find a usable search query from fragment input")

    return _search_videos_with_retries(query, tokens, local_durations)


def sceneByURL(params: Dict[str, Any]) -> Dict[str, Any]:
    url = dig(params, "url") or ""
    local_durations = _extract_local_durations(params)
    scene_id = _extract_scene_id(url)
    if scene_id:
        return _get_video_by_id(scene_id)

    slug = url.rstrip("/").split("/")[-1]
    slug = slug.split("?")[0]
    if slug:
        query = _build_search_query(slug)
        return _search_videos_with_retries(query, [], local_durations)

    fail(f"Did not find scene ID or slug from URL {url}")


if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            fail("Missing scrape method argument")
        calledFunction = sys.argv[1]
        raw_input = sys.stdin.read()
        if not raw_input.strip():
            fail("No JSON input provided on stdin")
        try:
            params = json.loads(raw_input)
        except json.JSONDecodeError as exc:
            fail(f"Invalid JSON input: {exc}")
        result: Dict[str, Any] = {}

        if calledFunction == "sceneByURL":
            result = sceneByURL(params)
        elif calledFunction == "sceneByFragment":
            result = sceneByFragment(params)
        else:
            fail(f"Unknown scrape method '{calledFunction}'")

        jprint(result)

    except SystemExit:
        raise
    except Exception as e:
        tb = traceback.format_exc()
        log_error(f"Unhandled exception: {e}\n{tb}")
        jprint({"error": f"Unhandled exception in scraper: {type(e).__name__}: {e}"})
        raise SystemExit(1)
