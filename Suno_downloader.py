import argparse
import base64
from datetime import datetime
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from threading import Lock

import requests
from colorama import Fore, Style, init
from mutagen.id3 import ID3, APIC, TIT2, TPE1, error
from mutagen.mp3 import MP3

from suno_lrc import fetch_aligned_lyrics, save_lrc_alongside, to_enhanced_lrc

init(autoreset=True)

FILENAME_BAD_CHARS = r'[<>:"/\\|?*\x00-\x1F]'
STATE_FILE = "suno_download_state.json"
SUNO_API_BASE = "https://studio-api.prod.suno.com/api/feed/v2"
CLERK_TOKEN_URL = "https://auth.suno.com/v1/client/sessions/{sid}/tokens"

state_lock = Lock()
print_lock = Lock()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def log(message, color=Fore.WHITE):
    """Thread-safe logging with timestamp."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with print_lock:
        print(f"{Fore.CYAN}[{ts}]{Style.RESET_ALL} {color}{message}{Style.RESET_ALL}")


def sanitize_filename(name, maxlen=200):
    safe = re.sub(FILENAME_BAD_CHARS, "_", name)
    safe = safe.strip(" .")
    return safe[:maxlen] if len(safe) > maxlen else safe


def make_filename(title, uuid):
    """Title [abcd1234].mp3  — first 8 chars of UUID for uniqueness."""
    return f"{sanitize_filename(title)} [{uuid[:8]}].mp3"


def decode_jwt(token):
    """Decode JWT payload without verification."""
    payload = token.split('.')[1]
    payload += '=' * (4 - len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def pick_proxy_dict(proxies_list):
    if not proxies_list:
        return None
    proxy = random.choice(proxies_list)
    return {"http": proxy, "https": proxy}


def retry_with_backoff(max_retries=10, initial_delay=1, backoff_factor=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exc = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    if attempt < max_retries - 1:
                        log(f"  -> Attempt {attempt + 1} failed: {e}", Fore.YELLOW)
                        log(f"  -> Retrying in {delay}s...", Fore.YELLOW)
                        time.sleep(delay)
                        delay *= backoff_factor
                    else:
                        log(f"  -> All {max_retries} attempts failed", Fore.RED)
            raise last_exc
        return wrapper
    return decorator


def set_file_timestamp(filepath, created_at_str):
    if not created_at_str:
        return
    try:
        dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
        ts = dt.timestamp()
        os.utime(filepath, (ts, ts))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# State (flat UUID -> filepath dict)
# ---------------------------------------------------------------------------

def load_state(directory):
    """Load state, merging legacy nested format into flat UUID->path dict."""
    state_path = os.path.join(directory, STATE_FILE)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        log(f"Warning: Could not load state file: {e}", Fore.YELLOW)
        return {}

    state = {}
    # Legacy nested format
    if 'downloaded_songs' in data and isinstance(data['downloaded_songs'], dict):
        state.update(data['downloaded_songs'])
    # Flat UUID entries (skip metadata keys)
    skip = {'downloaded_songs', 'last_page_processed', 'last_run_timestamp', 'total_pages_at_last_run', '_meta'}
    for k, v in data.items():
        if k not in skip and isinstance(v, str):
            state[k] = v
    return state


def load_meta(directory):
    """Load _meta block from state file."""
    state_path = os.path.join(directory, STATE_FILE)
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('_meta', {})
    except Exception:
        return {}


def save_state(directory, state, meta=None):
    state_path = os.path.join(directory, STATE_FILE)
    with state_lock:
        try:
            out = dict(state)
            if meta:
                out['_meta'] = meta
            with open(state_path, 'w', encoding='utf-8') as f:
                json.dump(out, f, indent=2, ensure_ascii=False)
        except Exception as e:
            log(f"Warning: Could not save state: {e}", Fore.RED)


# ---------------------------------------------------------------------------
# Token manager — auto-refresh via Clerk
# ---------------------------------------------------------------------------

class TokenManager:
    def __init__(self, token=None, client_cookie=None, from_chrome=False):
        self._token = token
        self._client_cookie = client_cookie
        self._session_id = None
        self._lock = Lock()

        if from_chrome and not client_cookie:
            self._client_cookie = self._read_chrome_cookie()

        if self._token:
            self._extract_session_id()

    def _extract_session_id(self):
        try:
            payload = decode_jwt(self._token)
            self._session_id = payload.get('sid')
        except Exception:
            pass

    def get_token(self):
        """Return a valid token, refreshing if expiring within 5 min."""
        with self._lock:
            if self._token:
                try:
                    payload = decode_jwt(self._token)
                    if time.time() > payload.get('exp', 0) - 300:
                        log("Token expiring soon, refreshing...", Fore.YELLOW)
                        if self._refresh():
                            return self._token
                        log("Refresh failed, using existing token", Fore.YELLOW)
                except Exception:
                    pass
            elif self._client_cookie and self._session_id:
                self._refresh()
            return self._token

    def _refresh(self):
        """Refresh JWT via Clerk API using __client cookie."""
        if not self._client_cookie or not self._session_id:
            return False
        url = CLERK_TOKEN_URL.format(sid=self._session_id)
        try:
            resp = requests.post(url,
                cookies={'__client': self._client_cookie},
                headers={'Origin': 'https://suno.com', 'Referer': 'https://suno.com/'},
                timeout=10)
            resp.raise_for_status()
            data = resp.json()
            new_jwt = data.get('jwt') or data.get('token')
            if new_jwt:
                self._token = new_jwt
                self._extract_session_id()
                exp = datetime.fromtimestamp(decode_jwt(new_jwt)['exp'])
                log(f"Token refreshed, expires {exp}", Fore.GREEN)
                return True
            log(f"Refresh response had no JWT: {list(data.keys())}", Fore.RED)
        except Exception as e:
            log(f"Token refresh failed: {e}", Fore.RED)
        return False

    def _read_chrome_cookie(self):
        """Read __client cookie from Chrome on macOS."""
        try:
            import subprocess
            import sqlite3
            import shutil
            import tempfile
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
            from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
            from cryptography.hazmat.primitives import hashes

            result = subprocess.run(
                ['security', 'find-generic-password', '-w', '-s', 'Chrome Safe Storage', '-a', 'Chrome'],
                capture_output=True, text=True)
            if result.returncode != 0:
                log("Could not get Chrome keychain password", Fore.RED)
                return None

            password = result.stdout.strip().encode()
            kdf = PBKDF2HMAC(algorithm=hashes.SHA1(), length=16, salt=b'saltysalt', iterations=1003)
            key = kdf.derive(password)

            # Try all Chrome profiles
            chrome_dir = os.path.expanduser('~/Library/Application Support/Google/Chrome')
            for profile in ['Default'] + [f'Profile {i}' for i in range(1, 10)]:
                src = os.path.join(chrome_dir, profile, 'Cookies')
                if not os.path.exists(src):
                    continue
                tmp = tempfile.mktemp(suffix='.db')
                shutil.copy2(src, tmp)
                conn = sqlite3.connect(tmp)
                cursor = conn.execute(
                    "SELECT encrypted_value FROM cookies WHERE host_key LIKE '%clerk.suno%' AND name='__client'")
                row = cursor.fetchone()
                conn.close()
                os.unlink(tmp)
                if not row:
                    continue
                ev = bytes(row[0])
                if ev[:3] == b'v10':
                    iv = b' ' * 16
                    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
                    decryptor = cipher.decryptor()
                    decrypted = decryptor.update(ev[3:]) + decryptor.finalize()
                    pad_len = decrypted[-1]
                    decrypted = decrypted[:-pad_len]
                    text = decrypted.decode('utf-8', errors='replace')
                    jwt_match = re.search(r'(eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)', text)
                    if jwt_match:
                        log(f"Extracted __client cookie from Chrome ({profile})", Fore.GREEN)
                        return jwt_match.group(1)
            log("No valid __client cookie found in any Chrome profile", Fore.RED)
        except ImportError:
            log("cryptography package needed for --from-chrome (pip install cryptography)", Fore.RED)
        except Exception as e:
            log(f"Chrome cookie extraction failed: {e}", Fore.RED)
        return None

    def extract_jwt(self, raw_input):
        """Extract a JWT from raw text, cookie string, etc."""
        raw_input = raw_input.strip()
        if re.match(r'^ey[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$', raw_input):
            return raw_input
        m = re.search(r'(ey[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)', raw_input)
        if m:
            return m.group(1)
        if len(raw_input) > 10 and ' ' not in raw_input:
            return raw_input
        return None


# ---------------------------------------------------------------------------
# Media helpers (unchanged from original)
# ---------------------------------------------------------------------------

@retry_with_backoff(max_retries=3, initial_delay=2, backoff_factor=2)
def embed_metadata(mp3_path, image_url=None, title=None, artist=None,
                   proxies_list=None, token=None, timeout=15):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = requests.get(image_url, proxies=pick_proxy_dict(proxies_list),
                     headers=headers, timeout=timeout)
    r.raise_for_status()
    image_bytes = r.content
    mime = r.headers.get("Content-Type", "image/jpeg").split(";")[0]

    audio = MP3(mp3_path, ID3=ID3)
    try:
        audio.add_tags()
    except error:
        pass
    if title:
        audio.tags["TIT2"] = TIT2(encoding=3, text=title)
    if artist:
        audio.tags["TPE1"] = TPE1(encoding=3, text=artist)
    for key in list(audio.tags.keys()):
        if key.startswith("APIC"):
            del audio.tags[key]
    audio.tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=image_bytes))
    audio.save(v2_version=3)


@retry_with_backoff(max_retries=10, initial_delay=2, backoff_factor=2)
def download_file(url, filename, proxies_list=None, token=None, timeout=30):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    with requests.get(url, stream=True, proxies=pick_proxy_dict(proxies_list),
                      headers=headers, timeout=timeout) as r:
        r.raise_for_status()
        with open(filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    return filename


# ---------------------------------------------------------------------------
# Rename existing files from vXX to UUID
# ---------------------------------------------------------------------------

def rename_existing_files(directory, state):
    """Rename files from 'Title vXX.mp3' to 'Title [uuid8].mp3'."""
    log("Renaming existing files to UUID format...", Fore.CYAN)
    renamed = 0
    skipped = 0
    missing = 0
    errors = 0

    for uuid, filepath in list(state.items()):
        if not os.path.exists(filepath):
            missing += 1
            continue

        old_name = os.path.basename(filepath)

        # Already UUID format?
        if re.search(r'\[[0-9a-f]{8}\]\.mp3$', old_name, re.IGNORECASE):
            skipped += 1
            continue

        # Strip vXX suffix and .mp3 to get base title
        title = old_name
        if title.endswith('.mp3'):
            title = title[:-4]
        title = re.sub(r'\s+v\d+$', '', title)

        new_name = make_filename(title, uuid)
        new_path = os.path.join(directory, new_name)

        # Collision guard (same title + same first 8 UUID chars — near impossible)
        if os.path.exists(new_path) and os.path.abspath(new_path) != os.path.abspath(filepath):
            new_name = f"{sanitize_filename(title)} [{uuid[:12]}].mp3"
            new_path = os.path.join(directory, new_name)

        if os.path.abspath(new_path) == os.path.abspath(filepath):
            skipped += 1
            continue

        try:
            os.rename(filepath, new_path)
            state[uuid] = new_path
            renamed += 1
        except Exception as e:
            log(f"  Failed: {old_name} -> {new_name}: {e}", Fore.RED)
            errors += 1

    save_state(directory, state)
    log(f"Rename done: {renamed} renamed, {skipped} already OK, {missing} missing, {errors} errors", Fore.GREEN)
    return renamed


# ---------------------------------------------------------------------------
# Incremental page fetcher (page 1 = newest, stop at known)
# ---------------------------------------------------------------------------

def fetch_page(page_num, token_mgr, proxies_list=None, page_delay=2.0):
    """Fetch one page with retry + rate-limit handling."""
    if page_delay > 0:
        time.sleep(page_delay)

    url = f"{SUNO_API_BASE}?hide_disliked=true&hide_gen_stems=true&hide_studio_clips=true&page={page_num}"
    delay = 2
    for attempt in range(10):
        token = token_mgr.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = requests.get(url, headers=headers,
                                proxies=pick_proxy_dict(proxies_list), timeout=15)
            if resp.status_code == 429:
                log(f"  429 on page {page_num}, waiting {delay}s...", Fore.YELLOW)
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue
            if resp.status_code in [401, 403]:
                log(f"  Auth error on page {page_num}, refreshing token...", Fore.YELLOW)
                token_mgr._refresh()
                continue
            resp.raise_for_status()
            data = resp.json()
            return data if isinstance(data, list) else data.get("clips", [])
        except requests.exceptions.RequestException as e:
            if attempt < 9:
                log(f"  Page {page_num} attempt {attempt+1}: {e}", Fore.YELLOW)
                time.sleep(delay)
                delay = min(delay * 2, 60)
            else:
                raise
    return []


def discover_new_songs(token_mgr, state, proxies_list=None, page_delay=2.0,
                       max_pages=None, stop_after=3, start_page=0):
    """
    Fetch pages starting from start_page (default 1 = newest songs).
    Stop after `stop_after` consecutive pages with zero new songs.
    Returns (list of new song dicts, last_page_scanned).
    """
    if start_page > 0:
        log(f"Resuming scan from page {start_page}...", Fore.CYAN)
    else:
        log("Scanning for new songs (newest first, from page 0)...", Fore.CYAN)
    new_songs = []
    consecutive_known = 0
    page_num = start_page - 1
    pages_scanned = 0

    while True:
        page_num += 1
        pages_scanned += 1
        if max_pages and pages_scanned > max_pages:
            log(f"Reached --max-pages limit ({max_pages})", Fore.YELLOW)
            break

        log(f"  Page {page_num}...", Fore.CYAN)
        try:
            clips = fetch_page(page_num, token_mgr, proxies_list, page_delay)
        except Exception as e:
            log(f"  Failed to fetch page {page_num}: {e}", Fore.RED)
            break

        if not clips:
            log(f"  Empty page {page_num} — reached end of library.", Fore.CYAN)
            break

        page_new = 0
        for clip in clips:
            uuid = clip.get("id")
            title = clip.get("title")
            audio_url = clip.get("audio_url")
            if not (uuid and title and audio_url):
                continue
            if uuid in state:
                continue
            page_new += 1
            new_songs.append({
                "uuid": uuid,
                "title": title,
                "audio_url": audio_url,
                "image_url": clip.get("image_url"),
                "display_name": clip.get("display_name"),
                "created_at": clip.get("created_at", ""),
            })

        log(f"  Page {page_num}: {len(clips)} clips, {page_new} new", Fore.GREEN)

        if page_new == 0:
            consecutive_known += 1
            if consecutive_known >= stop_after:
                log(f"  {stop_after} consecutive known pages — caught up!", Fore.GREEN)
                break
        else:
            consecutive_known = 0

    log(f"Found {len(new_songs)} new songs across {pages_scanned} pages (pages {start_page}-{page_num})", Fore.CYAN)
    return new_songs, page_num


# ---------------------------------------------------------------------------
# Download songs
# ---------------------------------------------------------------------------

def download_songs(new_songs, token_mgr, directory, state, proxies_list=None,
                   max_workers=10, download_delay=0.5, with_thumbnail=True):
    """Download a list of new songs in parallel with UUID-based filenames."""
    if not new_songs:
        log("Nothing to download!", Fore.GREEN)
        return 0, 0

    log(f"Downloading {len(new_songs)} songs ({max_workers} workers, {download_delay}s delay)...", Fore.CYAN)
    downloaded = 0
    failed = 0
    dl_lock = Lock()

    def process_one(song):
        uuid = song["uuid"]
        title = song["title"]

        if download_delay > 0:
            time.sleep(download_delay)

        filename = make_filename(title, uuid)
        filepath = os.path.join(directory, filename)

        # Collision (different UUID, same title, same 8-char prefix — astronomically rare)
        if os.path.exists(filepath):
            filename = f"{sanitize_filename(title)} [{uuid[:12]}].mp3"
            filepath = os.path.join(directory, filename)

        try:
            token = token_mgr.get_token()
            download_file(song["audio_url"], filepath, proxies_list, token=token)

            if with_thumbnail and song.get("image_url"):
                try:
                    embed_metadata(filepath, image_url=song["image_url"],
                                   token=token, artist=song.get("display_name"),
                                   title=title, proxies_list=proxies_list)
                except Exception:
                    pass  # thumbnail failure is non-fatal

            # Fetch word-level timings and save as .lrc next to the MP3.
            # Non-fatal — instrumental clips and any API hiccups just
            # leave the song without an .lrc file.
            try:
                aligned = fetch_aligned_lyrics(uuid, token)
                if aligned:
                    lrc = to_enhanced_lrc(
                        aligned,
                        title=title,
                        artist=song.get("display_name"),
                    )
                    if lrc:
                        save_lrc_alongside(filepath, lrc)
            except Exception:
                pass  # lyric fetch failure is non-fatal

            if song.get("created_at"):
                set_file_timestamp(filepath, song["created_at"])

            log(f"  Saved: {filename}", Fore.GREEN)
            return uuid, filepath, True
        except Exception as e:
            log(f"  Failed: {title} [{uuid[:8]}] — {e}", Fore.RED)
            return uuid, None, False

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one, s): s for s in new_songs}
        for future in as_completed(futures):
            uuid, filepath, success = future.result()
            if success and filepath:
                with dl_lock:
                    state[uuid] = filepath
                    downloaded += 1
                    if downloaded % 10 == 0:
                        save_state(directory, state)
            else:
                with dl_lock:
                    failed += 1

    save_state(directory, state)
    return downloaded, failed


# ---------------------------------------------------------------------------
# Full-library download (legacy mode, fetches all pages)
# ---------------------------------------------------------------------------

def download_full_library(token_mgr, directory, state, proxies_list=None,
                          max_workers=10, page_delay=2.0, download_delay=0.5,
                          with_thumbnail=True, page_workers=5):
    """Fetch ALL pages and download everything (for first run)."""
    log("Full library mode — finding last page...", Fore.CYAN)
    token = token_mgr.get_token()

    # Binary search for last page
    low, high = 1, 2
    while True:
        url = f"{SUNO_API_BASE}?hide_disliked=true&hide_gen_stems=true&hide_studio_clips=true&page={high}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                            proxies=pick_proxy_dict(proxies_list), timeout=10)
        if resp.status_code in [401, 403]:
            token_mgr._refresh()
            token = token_mgr.get_token()
            continue
        data = resp.json()
        clips = data if isinstance(data, list) else data.get("clips", [])
        if clips:
            low = high
            high *= 2
            log(f"  Page {low} exists, trying {high}...", Fore.CYAN)
            time.sleep(0.3)
        else:
            break

    while low < high:
        mid = (low + high + 1) // 2
        url = f"{SUNO_API_BASE}?hide_disliked=true&hide_gen_stems=true&hide_studio_clips=true&page={mid}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                            proxies=pick_proxy_dict(proxies_list), timeout=10)
        data = resp.json()
        clips = data if isinstance(data, list) else data.get("clips", [])
        if clips:
            low = mid
        else:
            high = mid - 1
        time.sleep(0.3)

    last_page = low
    log(f"Last page: {last_page}", Fore.GREEN)

    # Fetch all pages with throttle
    log(f"Fetching {last_page} pages ({page_workers} workers, {page_delay}s delay)...", Fore.CYAN)
    all_songs = []
    pages_data = {}
    pages_lock = Lock()

    def fetch_one(pg):
        clips = fetch_page(pg, token_mgr, proxies_list, page_delay)
        songs = []
        for clip in clips:
            uuid = clip.get("id")
            title = clip.get("title")
            audio_url = clip.get("audio_url")
            if uuid and title and audio_url:
                songs.append({
                    "uuid": uuid, "title": title, "audio_url": audio_url,
                    "image_url": clip.get("image_url"),
                    "display_name": clip.get("display_name"),
                    "created_at": clip.get("created_at", ""),
                })
        with pages_lock:
            pages_data[pg] = songs
        log(f"  Page {pg}/{last_page}: {len(songs)} songs", Fore.GREEN)

    with ThreadPoolExecutor(max_workers=page_workers) as executor:
        futures = {executor.submit(fetch_one, p): p for p in range(last_page, 0, -1)}
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                pg = futures[f]
                log(f"  Page {pg} failed: {e}", Fore.RED)

    # Combine oldest-first
    for pg in range(last_page, 0, -1):
        if pg in pages_data:
            all_songs.extend(pages_data[pg])

    # Filter to new only
    new_songs = [s for s in all_songs if s["uuid"] not in state]
    log(f"Total: {len(all_songs)} songs, {len(new_songs)} new", Fore.CYAN)

    return download_songs(new_songs, token_mgr, directory, state, proxies_list,
                          max_workers, download_delay, with_thumbnail)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Suno bulk downloader with UUID filenames and incremental sync")

    # Token sources (at least one required)
    token_group = parser.add_argument_group("authentication")
    token_group.add_argument("--token", type=str, help="Suno Bearer JWT token")
    token_group.add_argument("--client-cookie", type=str,
                             help="Clerk __client cookie value for auto-refresh")
    token_group.add_argument("--from-chrome", action="store_true",
                             help="Auto-extract __client cookie from Chrome (macOS)")

    parser.add_argument("--directory", type=str, default="suno-downloads",
                        help="Download directory (default: suno-downloads)")
    parser.add_argument("--proxy", type=str, help="Proxy (comma-separated)")
    parser.add_argument("--max-workers", type=int, default=10,
                        help="Parallel download workers (default: 10)")
    parser.add_argument("--page-delay", type=float, default=2.0,
                        help="Seconds between page fetches (default: 2.0)")
    parser.add_argument("--download-delay", type=float, default=0.5,
                        help="Seconds between song downloads per worker (default: 0.5)")
    parser.add_argument("--with-thumbnail", action="store_true", default=True)
    parser.add_argument("--no-thumbnail", dest="with_thumbnail", action="store_false")

    # Modes
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--full", action="store_true",
                            help="Full library download (fetch all pages, not just new)")
    mode_group.add_argument("--rename", action="store_true",
                            help="Rename existing files from vXX to [uuid] format, then exit")

    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max pages to scan in incremental mode")
    parser.add_argument("--stop-after", type=int, default=3,
                        help="Stop after N consecutive fully-known pages (default: 3)")
    parser.add_argument("--start-page", type=int, default=None,
                        help="Start scanning from this page number (resume from where you left off)")
    parser.add_argument("--resume", action="store_true",
                        help="Auto-resume from last_page_scanned saved in state file")
    parser.add_argument("--page-workers", type=int, default=5,
                        help="Parallel page-fetch workers for --full mode (default: 5)")

    args = parser.parse_args()

    # Require at least one auth source (unless --rename only)
    if not args.rename and not args.token and not args.client_cookie and not args.from_chrome:
        parser.error("Provide --token, --client-cookie, or --from-chrome")

    start_time = datetime.now()
    log("=" * 60, Fore.CYAN)
    log("SUNO DOWNLOADER v2", Fore.CYAN)
    log("=" * 60, Fore.CYAN)

    # Create directory
    os.makedirs(args.directory, exist_ok=True)

    # Load state (handles legacy nested format)
    state = load_state(args.directory)
    log(f"State: {len(state)} songs tracked", Fore.CYAN)

    # Rename mode
    if args.rename:
        rename_existing_files(args.directory, state)
        return

    # Token manager
    token_mgr = TokenManager(
        token=args.token,
        client_cookie=args.client_cookie,
        from_chrome=args.from_chrome,
    )

    # Extract JWT from raw input if needed
    if args.token:
        clean = token_mgr.extract_jwt(args.token)
        if clean:
            token_mgr._token = clean
            token_mgr._extract_session_id()

    # Verify we have a usable token
    test_token = token_mgr.get_token()
    if not test_token:
        log("No valid token available. Provide --token or --client-cookie.", Fore.RED)
        sys.exit(1)

    try:
        payload = decode_jwt(test_token)
        exp = datetime.fromtimestamp(payload['exp'])
        log(f"Token valid until {exp}", Fore.CYAN)
        if args.client_cookie or args.from_chrome:
            log("Auto-refresh enabled", Fore.GREEN)
    except Exception:
        log("Token present but could not decode — proceeding anyway", Fore.YELLOW)

    proxies_list = args.proxy.split(",") if args.proxy else None

    log(f"Settings: workers={args.max_workers}, page_delay={args.page_delay}s, "
        f"dl_delay={args.download_delay}s, thumbnails={args.with_thumbnail}", Fore.CYAN)

    # Determine start page
    start_page = 0
    if args.start_page is not None:
        start_page = args.start_page
    elif args.resume:
        meta = load_meta(args.directory)
        saved_page = meta.get('last_page_scanned', 0)
        if saved_page > 0:
            start_page = saved_page
            log(f"Resuming from saved page {start_page}", Fore.GREEN)
        else:
            log("No saved page in state — starting from page 1", Fore.YELLOW)

    # Run
    last_page_scanned = 0
    if args.full:
        downloaded, failed = download_full_library(
            token_mgr, args.directory, state, proxies_list,
            args.max_workers, args.page_delay, args.download_delay,
            args.with_thumbnail, args.page_workers)
    else:
        new_songs, last_page_scanned = discover_new_songs(
            token_mgr, state, proxies_list, args.page_delay,
            args.max_pages, args.stop_after, start_page)
        downloaded, failed = download_songs(
            new_songs, token_mgr, args.directory, state, proxies_list,
            args.max_workers, args.download_delay, args.with_thumbnail)

    # Save meta (last page scanned) for --resume next time
    meta = load_meta(args.directory)
    meta['last_page_scanned'] = last_page_scanned or meta.get('last_page_scanned', 0)
    meta['last_run_timestamp'] = datetime.now().isoformat()
    save_state(args.directory, state, meta)

    # Summary
    duration = datetime.now() - start_time
    log("=" * 60, Fore.CYAN)
    log("DOWNLOAD COMPLETE", Fore.GREEN)
    log(f"Downloaded: {downloaded} | Failed: {failed} | Time: {duration}", Fore.CYAN)
    log(f"Total tracked: {len(state)} songs", Fore.CYAN)
    if last_page_scanned:
        log(f"Last page scanned: {last_page_scanned} (use --resume to continue)", Fore.CYAN)
    log("=" * 60, Fore.CYAN)


if __name__ == "__main__":
    main()
