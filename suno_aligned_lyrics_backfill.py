#!/usr/bin/env python3
"""
Backfill aligned-lyrics (.lrc) files for previously downloaded Suno clips.

Reads suno_download_state.json, iterates clip UUIDs, fetches aligned_lyrics
from the Suno API, and writes an enhanced LRC file next to each MP3.

Idempotent: if a .lrc already exists for a clip, it's skipped unless --force.

Usage:
    python3 suno_aligned_lyrics_backfill.py                       # default dir
    python3 suno_aligned_lyrics_backfill.py --dir /Volumes/Ai/... # override
    python3 suno_aligned_lyrics_backfill.py --force               # rewrite existing
    python3 suno_aligned_lyrics_backfill.py --limit 50            # first 50 only
    python3 suno_aligned_lyrics_backfill.py --workers 10          # concurrency

Needs the same TokenManager setup as the main downloader — it reuses
it for Clerk JWT refresh on long runs.
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from colorama import Fore, Style, init

from Suno_downloader import TokenManager, load_state, log
from suno_lrc import fetch_aligned_lyrics, save_lrc_alongside, to_enhanced_lrc

init(autoreset=True)


def backfill(directory, token_mgr, force=False, limit=None, workers=5, delay=0.3):
    state = load_state(directory)
    skip = {"_meta", "downloaded_songs", "last_page_processed",
            "last_run_timestamp", "total_pages_at_last_run"}

    items = [(uuid, path) for uuid, path in state.items()
             if uuid not in skip and isinstance(path, str)]

    log(f"Total candidates in state: {len(items)}", Fore.CYAN)

    # Filter to those that need LRC. Limit is applied AFTER the
    # disk-existence filter so --limit 50 always means "fetch 50 real
    # candidates", not "scan the first 50 state entries (which may all
    # be stale)".
    todo = []
    already = 0
    missing_mp3 = 0
    for uuid, mp3_path in items:
        if not os.path.exists(mp3_path):
            missing_mp3 += 1
            continue
        lrc_path = mp3_path[:-4] + ".lrc" if mp3_path.lower().endswith(".mp3") else mp3_path + ".lrc"
        if os.path.exists(lrc_path) and not force:
            already += 1
            continue
        todo.append((uuid, mp3_path))
        if limit and len(todo) >= limit:
            break

    log(f"Missing mp3 on disk: {missing_mp3}", Fore.YELLOW)
    log(f"Already have .lrc:   {already}", Fore.GREEN)
    log(f"To fetch:            {len(todo)}", Fore.CYAN)

    if not todo:
        return 0, 0, 0

    saved = 0
    empty = 0
    failed = 0
    lock = Lock()

    def process_one(uuid_path):
        uuid, mp3_path = uuid_path
        if delay > 0:
            time.sleep(delay)
        try:
            token = token_mgr.get_token()
            aligned = fetch_aligned_lyrics(uuid, token)
            if aligned is None:
                return uuid, "empty"
            lrc = to_enhanced_lrc(aligned)
            if not lrc:
                return uuid, "empty"
            save_lrc_alongside(mp3_path, lrc)
            return uuid, "saved"
        except RuntimeError as e:
            # Auth error — let the worker retry next run
            return uuid, f"auth:{e}"
        except Exception as e:
            return uuid, f"error:{e}"

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_one, ip): ip for ip in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            uuid, outcome = fut.result()
            with lock:
                if outcome == "saved":
                    saved += 1
                elif outcome == "empty":
                    empty += 1
                else:
                    failed += 1
                    log(f"  [{uuid[:8]}] {outcome}", Fore.RED)

                if i % 50 == 0:
                    log(f"  progress: {i}/{len(todo)} "
                        f"(saved={saved} empty={empty} failed={failed})", Fore.CYAN)

    return saved, empty, failed


def main():
    parser = argparse.ArgumentParser(description="Backfill aligned-lyrics .lrc files for downloaded Suno clips")
    parser.add_argument("--dir", default="/Volumes/Ai/Suno_DownloadEverything/suno-downloads",
                        help="Download directory containing suno_download_state.json and .mp3 files")
    parser.add_argument("--force", action="store_true",
                        help="Rewrite existing .lrc files")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N clips (for testing)")
    parser.add_argument("--workers", type=int, default=5,
                        help="Concurrent fetches (default 5)")
    parser.add_argument("--delay", type=float, default=0.3,
                        help="Per-request delay in seconds (default 0.3)")
    parser.add_argument("--bearer",
                        help="Bearer JWT from Suno cookies (__session). "
                             "If omitted, TokenManager is used (reads from cookies keychain).")
    args = parser.parse_args()

    if not os.path.isdir(args.dir):
        log(f"Directory not found: {args.dir}", Fore.RED)
        sys.exit(1)

    # Build a minimal TokenManager that just returns the bearer directly
    # when one was passed on the CLI. Otherwise fall back to the downloader's
    # Chrome keychain extraction.
    if args.bearer:
        class StaticToken:
            def __init__(self, token): self._token = token
            def get_token(self): return self._token
            def invalidate(self): pass
        token_mgr = StaticToken(args.bearer)
    else:
        token_mgr = TokenManager()

    start = time.time()
    saved, empty, failed = backfill(
        directory=args.dir,
        token_mgr=token_mgr,
        force=args.force,
        limit=args.limit,
        workers=args.workers,
        delay=args.delay,
    )
    elapsed = time.time() - start

    log("", Fore.WHITE)
    log("=" * 50, Fore.CYAN)
    log(f"Saved:   {saved}", Fore.GREEN)
    log(f"Empty:   {empty} (instrumental or removed)", Fore.YELLOW)
    log(f"Failed:  {failed}", Fore.RED)
    log(f"Elapsed: {elapsed:.1f}s", Fore.CYAN)


if __name__ == "__main__":
    main()
