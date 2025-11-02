import argparse
import os
import random
import re
import sys
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from colorama import Fore, init
from mutagen.id3 import ID3, APIC, TIT2, TPE1, error
from mutagen.mp3 import MP3

init(autoreset=True)

FILENAME_BAD_CHARS = r'[<>:"/\\|?*\x00-\x1F]'
MAX_RETRIES = 10
PROGRESS_FILE = "suno_progress.json"

def sanitize_filename(name, maxlen=200):
    safe = re.sub(FILENAME_BAD_CHARS, "_", name)
    safe = safe.strip(" .")
    return safe[:maxlen] if len(safe) > maxlen else safe

def pick_proxy_dict(proxies_list):
    if not proxies_list: return None
    proxy = random.choice(proxies_list)
    return {"http": proxy, "https": proxy}

def save_progress(page_num):
    try:
        with open(PROGRESS_FILE, 'w') as f:
            json.dump({"last_page": page_num}, f)
    except Exception as e:
        print(f"{Fore.YELLOW}Warning: Could not save progress: {e}")

def load_progress():
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                data = json.load(f)
                return data.get("last_page", 0)
    except Exception as e:
        print(f"{Fore.YELLOW}Warning: Could not load progress: {e}")
    return 0

def clear_progress():
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
    except Exception as e:
        print(f"{Fore.YELLOW}Warning: Could not clear progress: {e}")

def embed_metadata(mp3_path, image_url=None, title=None, artist=None, proxies_list=None, token=None, timeout=15, max_retries=MAX_RETRIES):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    proxy_dict = pick_proxy_dict(proxies_list)
    
    for attempt in range(max_retries):
        try:
            r = requests.get(image_url, proxies=proxy_dict, headers=headers, timeout=timeout)
            r.raise_for_status()
            image_bytes = r.content
            mime = r.headers.get("Content-Type", "image/jpeg").split(";")[0]
            break
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"{Fore.YELLOW}  -> Thumbnail download attempt {attempt + 1}/{max_retries} failed: {e}. Retrying...")
                time.sleep(2)
            else:
                raise
    
    audio = MP3(mp3_path, ID3=ID3)
    try: audio.add_tags()
    except error: pass

    if title: audio.tags["TIT2"] = TIT2(encoding=3, text=title)
    if artist: audio.tags["TPE1"] = TPE1(encoding=3, text=artist)

    for key in list(audio.tags.keys()):
        if key.startswith("APIC"): del audio.tags[key]

    audio.tags.add(APIC(encoding=3, mime=mime, type=3, desc="Cover", data=image_bytes))
    audio.save(v2_version=3)

def fetch_page_with_retry(page_num, base_api_url, headers, proxies_list, max_retries=MAX_RETRIES):
    """Fetch a single page with retry logic"""
    api_url = f"{base_api_url}{page_num}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(api_url, headers=headers, proxies=pick_proxy_dict(proxies_list), timeout=15)
            if response.status_code in [401, 403]:
                print(f"{Fore.RED}Authorization failed (status {response.status_code}). Your token is likely expired or incorrect.")
                # Return None to signal auth failure (not retryable)
                return None, True
            response.raise_for_status()
            data = response.json()
            clips = data if isinstance(data, list) else data.get("clips", [])
            return clips, False
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"{Fore.YELLOW}Page {page_num} fetch attempt {attempt + 1}/{max_retries} failed: {e}. Retrying...")
                time.sleep(2)
            else:
                print(f"{Fore.RED}Failed to fetch page {page_num} after {max_retries} attempts: {e}")
                raise
    return [], False

def determine_total_pages(base_api_url, headers, proxies_list):
    """Determine the total number of pages by finding the first empty page"""
    print(f"{Fore.CYAN}Determining total number of pages...")
    page = 1
    
    while True:
        clips, auth_failed = fetch_page_with_retry(page, base_api_url, headers, proxies_list)
        if auth_failed:
            return 0, True
        
        if not clips:
            print(f"{Fore.GREEN}Total pages found: {page - 1}")
            return page - 1, False
        
        print(f"{Fore.MAGENTA}Page {page} has {len(clips)} clips...")
        page += 1
        time.sleep(1)  # Small delay between page checks

def extract_private_song_info(token_string, proxies_list=None):
    print(f"{Fore.CYAN}Extracting private songs using Authorization Token...")
    base_api_url = "https://studio-api.prod.suno.com/api/feed/v2?hide_disliked=true&hide_gen_stems=true&hide_studio_clips=true&page="
    headers = {"Authorization": f"Bearer {token_string}"}

    # Determine total pages first
    total_pages, auth_failed = determine_total_pages(base_api_url, headers, proxies_list)
    
    if auth_failed:
        # Prompt for new token
        print(f"{Fore.YELLOW}Please enter a new token:")
        new_token = input().strip()
        if new_token:
            token_string = new_token
            headers = {"Authorization": f"Bearer {token_string}"}
            total_pages, auth_failed = determine_total_pages(base_api_url, headers, proxies_list)
            if auth_failed:
                print(f"{Fore.RED}New token also failed. Exiting.")
                return {}
        else:
            return {}
    
    if total_pages == 0:
        print(f"{Fore.YELLOW}No pages found.")
        return {}
    
    # Download all pages in parallel
    print(f"{Fore.CYAN}Downloading all {total_pages} pages in parallel...")
    all_pages_data = {}
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_page = {
            executor.submit(fetch_page_with_retry, page, base_api_url, headers, proxies_list): page 
            for page in range(1, total_pages + 1)
        }
        
        for future in as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                clips, auth_failed = future.result()
                if auth_failed:
                    print(f"{Fore.RED}Authorization failed during parallel download. Please restart with a valid token.")
                    return {}
                all_pages_data[page_num] = clips
                print(f"{Fore.GREEN}Page {page_num}/{total_pages} downloaded ({len(clips)} clips)")
            except Exception as e:
                print(f"{Fore.RED}Failed to download page {page_num}: {e}")
                return {}
    
    # Process all clips from all pages
    song_info = {}
    for page_num in sorted(all_pages_data.keys()):
        clips = all_pages_data[page_num]
        for clip in clips:
            uuid, title, audio_url, image_url = clip.get("id"), clip.get("title"), clip.get("audio_url"), clip.get("image_url")
            if (uuid and title and audio_url) and uuid not in song_info:
                song_info[uuid] = {
                    "title": title, 
                    "audio_url": audio_url, 
                    "image_url": image_url, 
                    "display_name": clip.get("display_name"),
                    "uuid": uuid
                }
    
    print(f"{Fore.GREEN}Total unique songs extracted: {len(song_info)}")
    return song_info

def get_unique_filename(filename):
    if not os.path.exists(filename): return filename
    name, extn = os.path.splitext(filename)
    counter = 2
    while True:
        new_filename = f"{name} v{counter}{extn}"
        if not os.path.exists(new_filename): return new_filename
        counter += 1

def download_file(url, filename, proxies_list=None, token=None, timeout=30, max_retries=MAX_RETRIES):
    # This function now correctly handles finding a unique filename before saving
    unique_filename = get_unique_filename(filename)
    
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    
    for attempt in range(max_retries):
        try:
            with requests.get(url, stream=True, proxies=pick_proxy_dict(proxies_list), headers=headers, timeout=timeout) as r:
                r.raise_for_status()
                with open(unique_filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk: f.write(chunk)
            return unique_filename
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"{Fore.YELLOW}  -> Download attempt {attempt + 1}/{max_retries} failed: {e}. Retrying...")
                time.sleep(2)
            else:
                raise
    return unique_filename

def main():
    parser = argparse.ArgumentParser(description="Bulk download your private suno songs")
    parser.add_argument("--token", type=str, required=True, help="Your Suno session Bearer Token.")
    parser.add_argument("--proxy", type=str, help="Proxy with protocol (comma-separated).")
    parser.add_argument("--directory", type=str, default="suno-downloads", help="Local directory for saving files.")
    parser.add_argument("--with-thumbnail", action="store_true", help="Embed the song's thumbnail.")
    args = parser.parse_args()

    songs = extract_private_song_info(args.token, args.proxy.split(",") if args.proxy else None)

    if not songs:
        print(f"{Fore.RED}No songs found. Please check your token.")
        sys.exit(1)

    if not os.path.exists(args.directory):
        os.makedirs(args.directory)

    print(f"\n{Fore.CYAN}--- Starting Download Process ({len(songs)} songs to check) ---")
    for uuid, obj in songs.items():
        title = obj["title"] or uuid
        fname = sanitize_filename(title) + ".mp3"
        out_path = os.path.join(args.directory, fname)

        print(f"Processing: {Fore.GREEN}🎵 {title} {Fore.CYAN}[UUID: {uuid}]")
        try:
            # FIX: The old 'if os.path.exists' check was removed from here.
            # We now call download_file directly and let it handle unique filenames.
            
            print(f"  -> Downloading...")
            saved_path = download_file(obj["audio_url"], out_path, token=args.token, proxies_list=args.proxy.split(",") if args.proxy else None)
            
            if args.with_thumbnail and obj.get("image_url"):
                print(f"  -> Embedding thumbnail...")
                embed_metadata(saved_path, image_url=obj["image_url"], token=args.token, artist=obj.get("display_name"), title=title, proxies_list=args.proxy.split(",") if args.proxy else None)
            
            # Let the user know if a new version was created
            if os.path.basename(saved_path) != os.path.basename(out_path):
                print(f"{Fore.YELLOW}  -> Saved as new version: {os.path.basename(saved_path)}")

        except Exception as e:
            print(f"{Fore.RED}Failed on {title} [UUID: {uuid}]: {e}")

    print(f"\n{Fore.BLUE}Download process complete. Files are in '{args.directory}'.")
    sys.exit(0)


if __name__ == "__main__":
    main()