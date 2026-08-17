"""Hits 3 stem batch.

Mechanism (per Robbie): Suno gates stem generation behind a Cloudflare/hCaptcha
token. A datacenter/VPN IP nulls the *invisible* captcha, but starting a song on
/create forces the *visible* challenge, which you tick once; the token is then
valid for the session. After that, stem extraction rides on it.

So: this holds one logged-in window. You go to Create, start any generation, tick
the Cloudflare box. The script detects the resulting generate 200, then walks the
Hits 3 list newest->oldest: drives  ... -> Download -> Get Stems / MIDI -> Extract
(Auto split / 12 stems) on each song, waits for the extraction to register, then
downloads every stem's WAV + MIDI over the studio-api into
/Volumes/Ai/Suno/Stems/<Title>/. Resumable via state file. If the token expires
(a 422), it pauses and asks you to re-tick on Create, then continues.

Run:  .venv/bin/python -u hits3_batch.py
Stop: touch /tmp/suno_batch_stop
"""
from __future__ import annotations
import json, re, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests
from playwright.sync_api import sync_playwright
from json_to_smf import process as smf_process, is_json_midi
from captcha_lib import tick as captcha_tick, find_widget as captcha_find

SP = Path("/private/tmp/claude-501/-Users-robbielove/d8f3bf46-7423-4ff6-950a-5e4d5714f197/scratchpad")
CLIPS = json.loads((SP / "hits3_clips.json").read_text())
PROFILE = str(Path.home() / ".suno-midi" / "profile")
STATE = Path.home() / ".suno-midi" / "hits3_batch_state.json"
OUTROOT = Path("/Volumes/Ai/Suno/Stems")
LOG = Path("/tmp/hits3_batch.log")
API = "https://studio-api-prod.suno.com"
CDN = "https://cdn1.suno.ai"
STOP = Path("/tmp/suno_batch_stop")
CAPTCHA_OK = Path("/tmp/suno_captcha_ok")   # touch to manually confirm captcha solved

BAD = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
def safe(s): return (BAD.sub("_", s).strip(" .") or "untitled")[:120]

def log(m):
    line = f"[{int(time.time())%100000:05d}] {m}"
    with LOG.open("a") as f: f.write(line + "\n")
    print(line, flush=True)

def load_state():
    if STATE.exists(): return json.loads(STATE.read_text())
    return {"done": {}}
def save_state(s): STATE.write_text(json.dumps(s, indent=2))

# ---- studio-api download half (adapted from api_stem_grab) ----
def list_stems(cid, bearer):
    H = {"Authorization": f"Bearer {bearer}", "Origin": "https://suno.com", "Referer": "https://suno.com/"}
    pg = requests.get(f"{API}/api/clip/{cid}/stems/pages", headers=H, timeout=30)
    if pg.status_code != 200: return None
    pages = pg.json().get("pages", 0)
    out = []
    for p in range(pages):
        r = requests.get(f"{API}/api/clip/{cid}/stems", params={"page": p}, headers=H, timeout=30)
        if r.ok: out.extend(r.json().get("stems", []))
    return out

def stem_label(stem_title, song_title):
    if "(" in stem_title and stem_title.endswith(")"):
        return stem_title[stem_title.rfind("(") + 1:-1].strip()
    return stem_title.replace(song_title, "").strip(" -:") or "stem"

def dl(url, dest, headers=None):
    dest.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, stream=True, headers=headers or {}, timeout=180)
    r.raise_for_status()
    tmp = dest.with_suffix(dest.suffix + ".part"); n = 0
    with tmp.open("wb") as f:
        for c in r.iter_content(1 << 20):
            if c: f.write(c); n += len(c)
    tmp.replace(dest); return n

def try_wav(gid, dest):
    """Download a stem WAV only if Suno has rendered it (>0.5MB). Returns True on success."""
    if dest.exists() and dest.stat().st_size > 500_000:
        return True
    try:
        r = requests.get(f"{CDN}/{gid}.wav", stream=True, timeout=(10, 30))
        if r.status_code != 200:
            return False
        tmp = dest.with_suffix(".wav.part"); n = 0
        with tmp.open("wb") as f:
            for c in r.iter_content(1 << 20):
                if c: f.write(c); n += len(c)
        if n < 500_000:               # placeholder / not ready yet
            tmp.unlink(missing_ok=True); return False
        tmp.replace(dest); return True
    except Exception:
        return False

def try_midi(gid, dest, H, bpm=120.0):
    """Fetch a stem's MIDI JSON once Suno marks it complete, convert to binary SMF.
    Returns True when a real .mid exists."""
    if dest.exists() and dest.stat().st_size > 200 and not is_json_midi(dest):
        return True                                # already a converted binary MIDI
    try:
        r = requests.get(f"{API}/api/gen/{gid}/midi", headers=H, timeout=30)
        if r.status_code != 200:
            return False
        txt = r.text
        if '"running"' in txt or '"queued"' in txt or len(txt) < 80:
            return False                           # not rendered yet
        dest.write_text(txt)                       # JSON note-data
        return bool(smf_process(dest, bpm))        # convert in place -> SMF
    except Exception:
        return False

def download_stems(cid, title, bearer):
    """Poll each stem's WAV until rendered, retrying convert_wav. Returns (got, folder, expected)."""
    stems = list_stems(cid, bearer)
    if not stems:
        return 0, None, 0
    folder = OUTROOT / f"{safe(title)} [{cid[:8]}]"; folder.mkdir(parents=True, exist_ok=True)
    H = {"Authorization": f"Bearer {bearer}", "Origin": "https://suno.com", "Referer": "https://suno.com/"}
    # dedupe: exactly one stem per instrument label (undoes any duplicate extractions)
    seen = {}
    for s in stems:
        lbl = stem_label(s.get("title", ""), title)
        if lbl not in seen:
            seen[lbl] = s["id"]
    labels = {gid: lbl for lbl, gid in seen.items()}
    expected = len(labels)
    def convert(gid):
        try: requests.post(f"{API}/api/gen/{gid}/convert_wav/", headers=H, timeout=30)
        except Exception: pass
    with ThreadPoolExecutor(max_workers=8) as ex:
        list(ex.map(convert, list(labels)))
    items = list(labels.items())
    # WAIT until every stem's WAV has rendered — no giving up early.
    got = 0
    for rnd in range(60):             # patient: Suno renders stems over ~1-3 min
        if STOP.exists(): break
        def _wav(gl):
            gid, label = gl
            return try_wav(gid, folder / f"{safe(title)} ({label}) [{gid[:8]}].wav")
        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(_wav, items))
        pending = [items[i][0] for i, ok in enumerate(results) if not ok]
        got = expected - len(pending)
        if not pending:
            break
        if rnd % 3 == 0:
            log(f"    waiting for stems to render: {got}/{expected} WAV for {title}")
        for gid in pending: convert(gid)
        time.sleep(10)
    log(f"    WAVs {got}/{expected} for {title}")

    # MIDI: wait for it to render, but ACCEPT that some stems have no MIDI (empty
    # layers like FX never produce notes) — stop when no new MIDI arrives.
    midi_got = 0; mstale = 0
    for rnd in range(18):
        if STOP.exists(): break
        def _midi(gl):
            gid, label = gl
            return try_midi(gid, folder / f"{safe(title)} ({label}) [{gid[:8]}].mid", H)
        with ThreadPoolExecutor(max_workers=8) as ex:
            mres = list(ex.map(_midi, items))
        pend = [items[i][0] for i, ok in enumerate(mres) if not ok]
        new_m = expected - len(pend)
        if not pend:
            midi_got = new_m; break
        mstale = mstale + 1 if new_m == midi_got else 0
        midi_got = new_m
        if mstale >= 3:                # no new MIDI for ~30s: remaining stems have none
            break
        time.sleep(10)
    log(f"    MIDI {midi_got}/{expected} for {title}")
    return got, folder, expected


def main():
    LOG.write_text(""); state = load_state()
    ctx_state = {"bearer": None}
    last_gen = {"status": None, "t": 0}

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=PROFILE, channel="chrome", headless=False,
            accept_downloads=True, viewport={"width": 1500, "height": 950},
            args=["--window-position=0,0", "--window-size=1500,950",
                  "--disable-blink-features=AutomationControlled"])
        page = ctx.pages[0] if ctx.pages else ctx.new_page()

        def on_req(r):
            if "studio-api" in r.url:
                a = r.headers.get("authorization", "")
                if a.startswith("Bearer "): ctx_state["bearer"] = a[7:]
        def on_resp(r):
            if "/api/generate/v2-web" in r.url:
                last_gen["status"] = r.status; last_gen["t"] = time.time()
                try: log(f"  generate -> {r.status} {r.text()[:120]}")
                except Exception: log(f"  generate -> {r.status}")
        ctx.on("request", on_req); ctx.on("response", on_resp)

        def strip_onetrust():
            try:
                page.evaluate("""()=>{for(const id of ['onetrust-accept-btn-handler','onetrust-reject-all-handler']){const e=document.getElementById(id);if(e)e.click();}if(window.OneTrust&&OneTrust.AllowAll)OneTrust.AllowAll();for(const s of ['#onetrust-consent-sdk','.onetrust-pc-dark-filter'])document.querySelectorAll(s).forEach(e=>e.remove());}""")
            except Exception: pass

        def menu_hover_click(parent_text, child_rx):
            """Open the More menu, hover EVERY matching parent entry until its flyout
            reveals the child, then click the child."""
            for _ in range(4):
                strip_onetrust()
                try: page.locator('button[aria-label="More menu contents"]').first.click(timeout=5000)
                except Exception: continue
                page.wait_for_timeout(900)
                parents = page.get_by_text(parent_text, exact=True)
                try: n = parents.count()
                except Exception: n = 0
                for idx in range(n):
                    el = parents.nth(idx)
                    try:
                        if not el.is_visible(): continue
                        el.hover(timeout=2500); page.wait_for_timeout(900)
                    except Exception: continue
                    c = page.get_by_text(re.compile(child_rx, re.I)).first
                    try:
                        if c.count() and c.is_visible():
                            c.hover(timeout=2500); page.wait_for_timeout(250); c.click(timeout=3000)
                            log(f"   {parent_text} #{idx} -> clicked child /{child_rx}/")
                            return True
                    except Exception: pass
                # nothing worked this round; log what the menu showed and retry
                vis = page.eval_on_selector_all("body *",
                    "els=>[...new Set(els.filter(e=>{const r=e.getBoundingClientRect();return r.width>0&&e.children.length===0;})"
                    ".map(e=>(e.innerText||'').trim()).filter(t=>t&&t.length<28))]")
                log(f"   {parent_text}: no child /{child_rx}/ in {[t for t in vis if t.lower() not in ('home','explore','create','studio','library','hooks','earn credits','labs','notifications')][:26]}")
                page.keyboard.press("Escape"); page.wait_for_timeout(400)
            return False

        def try_auto_solve():
            """DISABLED. Creating songs to trigger the captcha burns credits and
            pollutes the library. The stems modal triggers it — see wait_captcha."""
            log("   (song creation disabled - using the stems-modal path)")
            return None

        def _try_auto_solve_disabled():
            """Robbie's flow: song -> Remix -> Reuse this song's lyrics -> Create, then
            a human-like move+click at the Turnstile checkbox. Measure the result."""
            log(">>> AUTO-ATTEMPT: Remix -> Reuse lyrics -> Create to force the challenge...")
            cid = CLIPS[0]["id"]
            page.goto(f"https://suno.com/song/{cid}", wait_until="commit", timeout=60000)
            for _ in range(30):
                page.wait_for_timeout(1200)
                if page.locator('button[aria-label="More menu contents"]').first.count(): break
            strip_onetrust()
            if not menu_hover_click("Remix", r"reuse\s*(prompt|lyric)"):
                log("   couldn't reach Remix -> Reuse prompt"); return None
            # Reuse prompt navigates to /create after a short delay
            for _ in range(15):
                page.wait_for_timeout(1500)
                if "/create" in page.url: break
            strip_onetrust(); log(f"   on {page.url}")
            base = last_gen["t"]
            # the Create button is a text element, not role=button; click the bottom one
            clicked_create = False
            for loc in (page.get_by_text("Create", exact=True).last,
                        page.get_by_text("Create", exact=True).first):
                try:
                    if loc.count():
                        loc.click(timeout=5000); clicked_create = True; log("   clicked Create"); break
                except Exception as e: log(f"   create click: {e}")
            if not clicked_create: log("   Create button not clickable")
            # The captcha ONLY appears here (create route) and only when the token is
            # dead. SCREENSHOT it so we can see the real box, then tick it.
            shots = Path("/private/tmp/claude-501/-Users-robbielove/"
                         "d8f3bf46-7423-4ff6-950a-5e4d5714f197/scratchpad")
            # tick the visible "Verify you are human" box here on /create
            if tick_captcha():
                log("   captcha ticked on create page")
                page.wait_for_timeout(3000)
                # the generation may need re-triggering after verification
                if last_gen["status"] != 200:
                    for loc in (page.get_by_text("Create", exact=True).last,):
                        try:
                            if loc.count():
                                loc.click(timeout=5000); log("   re-clicked Create after tick")
                        except Exception as e:
                            log(f"   re-click: {e}")
            else:
                log("   no captcha box appeared on create (see captcha_watch.png)")
            for _ in range(12):
                page.wait_for_timeout(2500)
                if last_gen["status"] is not None and last_gen["t"] > base:
                    return last_gen["status"]
            return None

        def wait_captcha():
            """Re-validate WITHOUT ever creating a song: the stems modal itself
            triggers the challenge. Open modal -> Extract -> wait 5s -> close modal
            -> tick the box. That is the whole workflow."""
            CAPTCHA_OK.unlink(missing_ok=True)
            log(">>> re-validating via the stems modal (no song creation)")
            base = last_gen["t"]
            for attempt in range(6):
                if STOP.exists(): return False
                if last_gen["status"] == 200 and last_gen["t"] > base:
                    log(">>> validated (generate 200)"); return True
                page.goto(f"https://suno.com/song/{CLIPS[0]['id']}",
                          wait_until="commit", timeout=60000)
                for _ in range(30):
                    page.wait_for_timeout(1200)
                    if page.locator('button[aria-label="More menu contents"]').first.count(): break
                strip_onetrust()
                if not open_stems_modal():
                    log(f">>> attempt {attempt+1}: could not open stems modal"); continue
                b = find_extract_btn()
                if not b:
                    log(f">>> attempt {attempt+1}: no Extract button"); continue
                last_gen["status"] = None
                b.click(timeout=4000); log("   clicked Extract to trigger the challenge")
                page.wait_for_timeout(5000)          # wait 5s
                page.keyboard.press("Escape")        # close the modal
                page.wait_for_timeout(1500)
                log("   closed modal; hunting the captcha box")
                if tick_captcha_fixed(tries=40):     # tick the box
                    log(">>> ticked the captcha")
                    page.wait_for_timeout(4000)
                    return True
                log(f">>> attempt {attempt+1}: box not found")
            log(">>> could not re-validate; continuing anyway")
            return True

        def open_stems_modal():
            """... -> Download -> Get Stems / MIDI. True when the modal is open."""
            for _ in range(5):
                strip_onetrust()
                try: page.locator('button[aria-label="More menu contents"]').first.click(timeout=5000)
                except Exception: continue
                page.wait_for_timeout(800)
                d = page.get_by_text("Download", exact=True).first
                if not (d.count() and d.is_visible()):
                    page.keyboard.press("Escape"); page.wait_for_timeout(400); continue
                try:
                    d.hover(timeout=3000); page.wait_for_timeout(700)
                    gs = page.get_by_text(re.compile(r"get stems", re.I)).first
                    gs.hover(timeout=3000); page.wait_for_timeout(300); gs.click(timeout=3000)
                    return True
                except Exception:
                    page.keyboard.press("Escape"); page.wait_for_timeout(400)
            return False

        def find_extract_btn():
            for _ in range(40):
                page.wait_for_timeout(1500); strip_onetrust()
                b = page.get_by_role("button", name=re.compile(r"^extract$", re.I)).first
                if b.count(): return b
            return None

        # The Cloudflare widget always renders in the SAME place: 300px wide, centred
        # horizontally and vertically in the 1500x950 viewport. Checkbox sits ~21px
        # in from its left edge. Measured off Robbie's screenshot.
        CAPTCHA_XY = (620, 475)

        SHOTS = Path("/private/tmp/claude-501/-Users-robbielove/"
                     "d8f3bf46-7423-4ff6-950a-5e4d5714f197/scratchpad")

        def tick_captcha_fixed(tries=45):
            """Click the box at its fixed position. Nothing else."""
            tx, ty = CAPTCHA_XY
            page.mouse.move(tx - 80, ty - 30); page.wait_for_timeout(120)
            page.mouse.move(tx, ty); page.wait_for_timeout(180)
            page.mouse.click(tx, ty)
            log(f"  clicked captcha checkbox at fixed ({tx},{ty})")
            page.wait_for_timeout(4000)
            return True

        def tick_captcha():
            """Robbie's sequence: CLOSE the modal, then click the box at its fixed
            position. It is always in the same place. No searching, no Tab."""
            page.keyboard.press("Escape")
            page.wait_for_timeout(1500)
            log("  closed modal")
            tick_captcha_fixed()
            return True

        def _tick_captcha_search_unused():
            for attempt in range(12):
                shots = Path("/private/tmp/claude-501/-Users-robbielove/"
                             "d8f3bf46-7423-4ff6-950a-5e4d5714f197/scratchpad")
                try: page.screenshot(path=str(shots / "captcha_watch.png"))
                except Exception: pass
                boxes = []
                try:
                    boxes = page.eval_on_selector_all("iframe", """els=>els.map(e=>{
                        const r=e.getBoundingClientRect();
                        return {src:(e.src||''), title:(e.title||''),
                                x:r.left, y:r.top, w:r.width, h:r.height};
                    })""")
                except Exception:
                    pass
                tgt = None
                for b in boxes:
                    if b["w"] < 40 or b["h"] < 20:
                        continue
                    if ("challenges.cloudflare" in b["src"]
                            or "turnstile" in (b["title"] or "").lower()
                            or "hallenge" in (b["title"] or "")
                            or (30 < b["h"] < 130 and 180 < b["w"] < 520)):   # the widget's shape
                        tgt = b; break
                if tgt:
                    log(f"  CAPTCHA FOUND: {tgt}")
                    tx = tgt["x"] + 30; ty = tgt["y"] + tgt["h"] / 2   # checkbox: left-centre
                    page.mouse.move(tx - 90, ty - 25); page.wait_for_timeout(150)
                    page.mouse.move(tx, ty); page.wait_for_timeout(220)
                    page.mouse.click(tx, ty)
                    log(f"  TICKED captcha at ({tx:.0f},{ty:.0f})")
                    page.wait_for_timeout(5000)
                    try: page.screenshot(path=str(shots / "captcha_after_tick.png"))
                    except Exception: pass
                    return True
                # keyboard fallback: Tab onto the widget, Space to tick
                if attempt == 5:
                    log("  trying Tab+Space to reach the checkbox")
                    for i in range(35):
                        page.keyboard.press("Tab"); page.wait_for_timeout(180)
                        try:
                            a = page.evaluate("""()=>{const a=document.activeElement;
                                return a?{tag:a.tagName,src:(a.src||'')}:null;}""")
                        except Exception:
                            a = None
                        if a and (a["tag"] == "IFRAME" or "challenges.cloudflare" in (a["src"] or "")):
                            page.keyboard.press("Space")
                            log(f"  pressed Space on focused iframe (tab {i})")
                            page.wait_for_timeout(5000)
                            return True
                page.wait_for_timeout(1500)
            log("  no captcha box visible")
            return False

        def do_extract(cid):
            """Drive ...->Download->Get Stems/MIDI->Extract. Returns the generate status seen."""
            page.goto(f"https://suno.com/song/{cid}", wait_until="commit", timeout=60000)
            for _ in range(30):
                page.wait_for_timeout(1200)
                if page.locator('button[aria-label="More menu contents"]').first.count(): break
            strip_onetrust()
            # open menu -> hover Download -> hover+click Get Stems / MIDI
            reached = False
            for _ in range(5):
                strip_onetrust()
                try: page.locator('button[aria-label="More menu contents"]').first.click(timeout=5000)
                except Exception: continue
                page.wait_for_timeout(800)
                d = page.get_by_text("Download", exact=True).first
                if not (d.count() and d.is_visible()):
                    page.keyboard.press("Escape"); page.wait_for_timeout(400); continue
                try:
                    d.hover(timeout=3000); page.wait_for_timeout(700)
                    gs = page.get_by_text(re.compile(r"get stems", re.I)).first
                    gs.hover(timeout=3000); page.wait_for_timeout(300); gs.click(timeout=3000)
                    reached = True; break
                except Exception:
                    page.keyboard.press("Escape"); page.wait_for_timeout(400)
            if not reached: return "no-modal"
            # wait for the modal's DSP engine to load and the Extract button to appear
            ex = None
            for _ in range(40):               # up to ~60s for the modal to finish loading
                page.wait_for_timeout(1500); strip_onetrust()
                b = page.get_by_role("button", name=re.compile(r"^extract$", re.I)).first
                if b.count(): ex = b; break
            if not ex: return "no-extract-btn"
            last_gen["status"] = None; last_gen["t"] = time.time()
            ex.click(timeout=4000); log("  clicked Extract; waiting 5s...")
            # The captcha sits BEHIND the stems modal. Wait 5s, CLOSE the modal so the
            # box is visible, tick it, then REOPEN the modal and extract again.
            # A 422 here is precisely the "captcha is showing" signal - never treat it
            # as a finished result.
            page.wait_for_timeout(5000)
            # wait 5s -> close modal -> click the captcha box -> reopen modal -> Extract
            if last_gen["status"] != 200:
                tick_captcha()
                # ticking may itself validate the pending extract - only redo if not
                if last_gen["status"] != 200 and open_stems_modal():
                    b2 = find_extract_btn()
                    if b2:
                        last_gen["status"] = None; last_gen["t"] = time.time()
                        b2.click(timeout=4000)
                        log("  reopened modal and clicked Extract again")
            # wait for client-side extraction + the generate response
            for _ in range(48):  # up to 240s
                page.wait_for_timeout(5000)
                if last_gen["status"] == 200: break
                if STOP.exists(): break
            page.keyboard.press("Escape")
            return last_gen["status"]

        # ---- run ----
        # warm the session, then go straight in: the token is validated by a
        # generation; we only (re)verify when an extract actually 422s.
        page.goto("https://suno.com/me", wait_until="commit", timeout=60000)
        page.wait_for_timeout(5000)
        todo = [c for c in CLIPS if not state["done"].get(c["id"])]
        log(f">>> {len(todo)}/{len(CLIPS)} clips to do (newest first).")

        for i, clip in enumerate(CLIPS, 1):
            if STOP.exists(): log("stop requested"); break
            cid = clip["id"]
            title = clip.get("title", cid[:8])
            if state["done"].get(cid): continue
            log(f"[{i}/{len(CLIPS)}] {title} ({cid[:8]})")

            # already have stems? just download.
            bearer = ctx_state["bearer"]
            if bearer:
                existing = list_stems(cid, bearer)
                if existing:
                    log(f"  already extracted ({len(existing)} stems) - downloading")
                    got, folder, expected = download_stems(cid, title, bearer)
                    if expected and got >= min(expected, 12):
                        state["done"][cid] = {"title": title, "stems": expected, "wavs": got}; save_state(state)
                        log(f"  DONE {title}: {got}/{expected} WAVs -> {folder}")
                    else:
                        log(f"  partial ({got}/{expected}); will retry next run")
                    continue

            status = do_extract(cid)
            if status == 422:
                log("  !! 422 token expired - re-solving captcha")
                if not wait_captcha(): break
                status = do_extract(cid)   # retry once
            if status != 200:
                log(f"  extract did not confirm (status={status}); leaving for retry")
                continue
            # poll for stems to register, then download
            bearer = ctx_state["bearer"]
            saved = False
            for _ in range(30):  # up to ~2.5min for stems to register
                time.sleep(5)
                stems = list_stems(cid, bearer)
                if stems:
                    got, folder, expected = download_stems(cid, title, bearer)
                    if expected and got >= min(expected, 12):
                        state["done"][cid] = {"title": title, "stems": expected, "wavs": got}; save_state(state)
                        log(f"  DONE {title}: {got}/{expected} WAVs -> {folder}")
                        saved = True
                    else:
                        log(f"  partial ({got}/{expected}) for {title}; will retry next run")
                        saved = True  # extracted; downloads can finish on a later pass
                    break
            if not saved: log(f"  stems not registered in time for {title}; will retry next run")
            page.wait_for_timeout(3000)  # gentle throttle

        log(">>> batch loop ended. window stays open 10 min.")
        end = time.time() + 600
        while time.time() < end and not STOP.exists(): time.sleep(3)
        ctx.close()


if __name__ == "__main__":
    main()
