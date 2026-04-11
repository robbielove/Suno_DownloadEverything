"""
Suno aligned-lyrics fetcher + LRC converter.

Shared between Suno_downloader.py (live download path) and
suno_aligned_lyrics_backfill.py (catch-up for previously downloaded clips).

Suno exposes word-level timings at:
    GET https://studio-api-prod.suno.com/api/gen/{clip_uuid}/aligned_lyrics
    Authorization: Bearer <__session JWT>

Response shape:
    { "data": [ [word_obj, word_obj, ...], [float, ...], float ] }

Only data[0] is used — list of word-timing objects:
    {"word": "Lasagne ", "start_s": 13.245, "end_s": 13.883, "p_align": 0.99, "success": true}

Word text already includes the original formatting (trailing spaces and
newlines) so line detection is "word contains \\n" rather than heuristic.

Output is enhanced LRC (Apple Music / Musixmatch compatible):
    [mm:ss.xx] <mm:ss.xx>word <mm:ss.xx>word <mm:ss.xx>word
"""

import json
import re

import requests


ALIGNED_LYRICS_URL = "https://studio-api-prod.suno.com/api/gen/{uuid}/aligned_lyrics"
DEFAULT_TIMEOUT = 15


def fetch_aligned_lyrics(clip_uuid, token, proxies=None, timeout=DEFAULT_TIMEOUT):
    """
    Fetch raw aligned-lyrics JSON for a single clip. Returns None if the
    clip has no lyrics (instrumental), 404s, or any other non-fatal error.
    Raises RuntimeError on auth failure so the caller can refresh the token.
    """
    url = ALIGNED_LYRICS_URL.format(uuid=clip_uuid)
    headers = {
        "Authorization": f"Bearer {token}",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://suno.com/",
        "Origin": "https://suno.com",
    }

    resp = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)

    if resp.status_code == 401:
        raise RuntimeError("Suno aligned_lyrics: 401 unauthorized — token refresh needed")
    if resp.status_code == 404:
        return None  # clip has no lyrics (instrumental) or was removed
    if resp.status_code != 200:
        return None  # transient — skip this one, try again next run

    try:
        return resp.json()
    except ValueError:
        return None


def aligned_to_words(aligned_json):
    """
    Extract the flat word list from a raw aligned_lyrics payload. Returns
    a list of dicts {word, start_s, end_s, p_align} or empty list on any
    malformed input.
    """
    if not isinstance(aligned_json, dict):
        return []
    data = aligned_json.get("data")
    if not isinstance(data, list) or not data:
        return []
    words = data[0]
    if not isinstance(words, list):
        return []
    return [w for w in words if isinstance(w, dict) and "word" in w and "start_s" in w]


def words_to_lines(words):
    """
    Group flat word list back into lines by detecting \\n inside the word
    text. Suno's aligned_lyrics preserves the original line breaks in the
    word strings, so splitting on that rebuilds the lyric structure.
    """
    lines = []
    current = []
    for w in words:
        current.append(w)
        if "\n" in w.get("word", ""):
            lines.append(current)
            current = []
    if current:
        lines.append(current)
    return lines


def fmt_lrc_time(seconds):
    """Format seconds as mm:ss.xx for LRC timestamps."""
    m = int(seconds // 60)
    s = seconds - m * 60
    return f"{m:02d}:{s:05.2f}"


def to_enhanced_lrc(aligned_json, title=None, artist=None):
    """
    Convert a raw aligned_lyrics JSON payload to an enhanced LRC string.

    Format per line:
        [mm:ss.xx] <mm:ss.xx>word <mm:ss.xx>word ...

    Optional metadata lines at the top:
        [ti:title]
        [ar:artist]

    Returns an empty string if the payload has no usable word data.
    """
    words = aligned_to_words(aligned_json)
    if not words:
        return ""

    lines = words_to_lines(words)

    out = []
    if title:
        out.append(f"[ti:{title}]")
    if artist:
        out.append(f"[ar:{artist}]")
    if out:
        out.append("")

    for line_words in lines:
        if not line_words:
            continue
        line_start = line_words[0]["start_s"]
        parts = [f"[{fmt_lrc_time(line_start)}]"]
        for w in line_words:
            clean = re.sub(r"\s+", " ", w["word"].replace("\n", " ")).strip()
            if not clean:
                continue
            parts.append(f"<{fmt_lrc_time(w['start_s'])}>{clean}")
        out.append(" ".join(parts))

    return "\n".join(out) + "\n"


def save_lrc_alongside(mp3_path, lrc_text):
    """
    Write `{same basename}.lrc` next to an mp3 file. No-op on empty text.
    """
    if not lrc_text:
        return False
    if not mp3_path.lower().endswith(".mp3"):
        return False
    lrc_path = mp3_path[:-4] + ".lrc"
    with open(lrc_path, "w", encoding="utf-8") as f:
        f.write(lrc_text)
    return True
