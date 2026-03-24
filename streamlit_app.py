#!/usr/bin/env python3
"""
SLC Video Merger – Streamlit Edition
All text is rendered by Pillow (no FFmpeg drawtext = no escaping bugs).
FFmpeg only does: overlay PNG on video, normalise, transitions, concatenate.

OneDrive upload uses Microsoft OAuth2 (device-code flow).
No app registration key needed — just a Client ID from Azure.

Railway Edition: per-session temp dirs, multi-user safe.
"""

import os, json, subprocess, tempfile, time, shutil, threading, uuid
from pathlib import Path
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

from PIL import Image, ImageDraw, ImageFont
import numpy as np
import streamlit as st

try:
    import msal, requests
    ONEDRIVE_AVAILABLE = True
except ImportError:
    ONEDRIVE_AVAILABLE = False

# ── Embedded SLC logo (base64) — written to assets/ on startup ───────────
_SLC_LOGO_B64 = "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"

st.set_page_config(page_title="SLC Video Merger", page_icon="🎬", layout="wide")

BASE_DIR  = Path(__file__).parent
INTRO_TPL = BASE_DIR / "assets" / "intro_template.mp4"
SLC_LOGO  = BASE_DIR / "assets" / "slc_logo.png"
TOKEN_CACHE_FILE = Path("/tmp/ms_token_cache.json")

# ── Per-session temp dir (Railway multi-user safety) ─────────────────────
TMP_ROOT = Path("/tmp/slc_merger")
TMP_ROOT.mkdir(parents=True, exist_ok=True)

def get_session_dir() -> Path:
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())
    d = TMP_ROOT / st.session_state.session_id
    d.mkdir(parents=True, exist_ok=True)
    return d

def cleanup_old_sessions(max_age=3600):
    now = time.time()
    for d in TMP_ROOT.iterdir():
        if d.is_dir() and now - d.stat().st_mtime > max_age:
            shutil.rmtree(d, ignore_errors=True)

threading.Thread(target=cleanup_old_sessions, daemon=True).start()

# ── Watermark / badge cover ───────────────────────────────────────────────
WM_BR_X, WM_BR_Y, WM_BR_W, WM_BR_H = 1655, 960, 240, 72
WM_TOP_X, WM_TOP_Y, WM_TOP_W, WM_TOP_H = 760, 48, 390, 72
BOX_RADIUS = 10
WM_EC_X, WM_EC_Y, WM_EC_W, WM_EC_H = 448, 310, 1024, 420
EC_RADIUS  = 14
LOGO_H, LOGO_RIGHT_MARGIN, LOGO_BOTTOM_MARGIN = 44, 113, 53

# ── OneDrive settings ─────────────────────────────────────────────────────
MS_CLIENT_ID = "772dd850-50bd-4c97-9152-d1b3e78fb737"
MS_SCOPES    = ["https://graph.microsoft.com/Files.ReadWrite", "https://graph.microsoft.com/User.Read"]
ONEDRIVE_FOLDER_URL = "https://globaledulinkuk-my.sharepoint.com/:f:/g/personal/content_gamification_imperiallearning_co_uk/IgDpo-qQQhSNS5aOw2lBAFo-ASQb3KWLDkHS9kp6sIHuy0s?e=3Ualc4"
MS_AUTHORITY = "https://login.microsoftonline.com/globaledulinkuk.onmicrosoft.com"

TEAL, WHITE = (96, 204, 190), (255, 255, 255)


def _font(name):
    for c in [str(BASE_DIR / "fonts" / name),
              f"/usr/share/fonts/truetype/google-fonts/{name}",
              "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]:
        if os.path.exists(c): return c
    return None

BOLD, MEDIUM = _font("Poppins-Bold.ttf"), _font("Poppins-Medium.ttf")


def _ft(path, size):
    try:    return ImageFont.truetype(path, size) if path else ImageFont.load_default()
    except: return ImageFont.load_default()


def _make_logo_composite(logo_path, box, W=1920, H=1080, bg=(249,249,249,255)):
    brx, bry, brw, brh = box
    img  = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle([brx, bry, brx+brw, bry+brh], radius=BOX_RADIUS, fill=bg)
    logo_h_px = brh - 12
    logo_img  = Image.open(str(logo_path)).convert("RGBA")
    ratio     = logo_img.width / logo_img.height
    logo_w_px = int(logo_h_px * ratio)
    if logo_w_px > brw - 12:
        logo_w_px = brw - 12
        logo_h_px = int(logo_w_px / ratio)
    logo_img  = logo_img.resize((logo_w_px, logo_h_px), Image.LANCZOS)
    cx, cy = brx + brw // 2, bry + brh // 2
    img.paste(logo_img, (cx - logo_w_px // 2, cy - logo_h_px // 2), logo_img)
    out = Path(str(logo_path)).parent / "logo_composite.png"
    img.save(str(out), "PNG")
    return out


def _make_box_png(boxes, path, W=1920, H=1080, colour=(255,255,255,255)):
    img  = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    for (x, y, w, h, r) in boxes:
        draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=colour)
    img.save(str(path), "PNG")
    return path


def render_intro_overlay(course, unit_num, unit_title, W=1920, H=1080):
    img  = Image.new("RGBA", (W, H), (0,0,0,0))
    draw = ImageDraw.Draw(img)
    pad  = W - 200
    csz  = 52; cfn = _ft(BOLD, csz)
    while csz > 28:
        bb = draw.textbbox((0,0), course, font=cfn)
        if bb[2]-bb[0] <= pad: break
        csz -= 2; cfn = _ft(BOLD, csz)
    c_asc, c_desc = cfn.getmetrics(); c_h = c_asc+c_desc
    ufn  = _ft(BOLD, 28); utxt = unit_num.upper()
    bb   = draw.textbbox((0,0), utxt, font=ufn)
    badge_w = bb[2]-bb[0]+70; badge_h = 56
    has_title = bool(unit_title and unit_title.strip()); title_h = 0
    if has_title:
        tsz = 30; tfn = _ft(MEDIUM, tsz)
        while tsz > 20:
            bb = draw.textbbox((0,0), unit_title, font=tfn)
            if bb[2]-bb[0] <= pad: break
            tsz -= 2; tfn = _ft(MEDIUM, tsz)
        t_asc, t_desc = tfn.getmetrics(); title_h = t_asc+t_desc
    gap1 = 45; gap2 = 25
    block_h = c_h+gap1+badge_h+(gap2+title_h if has_title else 0)
    start_y = (H//2-60)-block_h//2
    draw.text((W//2, start_y+c_h//2), course, fill=WHITE, font=cfn, anchor="mm")
    bx = (W-badge_w)//2; by = start_y+c_h+gap1
    draw.rounded_rectangle([bx,by,bx+badge_w,by+badge_h], radius=14, fill=TEAL+(230,))
    draw.text((bx+badge_w//2, by+badge_h//2), utxt, fill=WHITE, font=ufn, anchor="mm")
    if has_title:
        ty2 = by+badge_h+gap2
        draw.text((W//2, ty2+title_h//2), unit_title, fill=WHITE, font=tfn, anchor="mm")
    return img


def render_end_overlay(W=1920, H=1080):
    img  = Image.new("RGBA", (W, H), (0,0,0,0))
    draw = ImageDraw.Draw(img)
    fn   = _ft(BOLD, 42); bb = draw.textbbox((0,0), "END", font=fn)
    bw, bh = bb[2]-bb[0]+90, 72; bx, by = (W-bw)//2, (H-bh)//2-20
    draw.rounded_rectangle([bx,by,bx+bw,by+bh], radius=16, fill=TEAL+(230,))
    draw.text((bx+bw//2, by+bh//2), "END", fill=WHITE, font=fn, anchor="mm")
    return img


def _ff(cmd, timeout=600):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        err = r.stderr.strip().split("\n")
        raise RuntimeError("\n".join(err[-6:]) if len(err)>6 else r.stderr)
    return r


def _probe_resolution(path):
    r = subprocess.run(["ffprobe","-v","error","-select_streams","v:0",
        "-show_entries","stream=width,height","-of","csv=p=0",str(path)],
        capture_output=True, text=True)
    try:    w, h = r.stdout.strip().split(","); return (int(w), int(h))
    except: return (1920, 1080)


def _probe_duration(path):
    r = subprocess.run(["ffprobe","-v","error","-show_entries","format=duration",
        "-of","default=noprint_wrappers=1:nokey=1",str(path)],
        capture_output=True, text=True)
    if r.returncode!=0 or not r.stdout.strip():
        raise RuntimeError(f"Cannot read duration: {path}")
    return float(r.stdout.strip())


def _has_audio(path):
    r = subprocess.run(["ffprobe","-v","error","-select_streams","a",
        "-show_entries","stream=index","-of","csv=p=0",str(path)],
        capture_output=True, text=True)
    return bool(r.stdout.strip())


def _is_end_card_frame(arr):
    """
    Return True if a (H, W, 3) uint8 numpy frame looks like a NotebookLM end card.
    End cards are visually uniform (low std-dev) AND either:
      - Mostly bright/white (mean > 190), or
      - Mostly a single saturated colour (NotebookLM uses solid purple/teal bg variants)
    """
    if arr is None or arr.size == 0:
        return False
    f = arr.astype(float)
    mean_brightness = f.mean()
    std_all         = f.std()           # low = uniform solid colour
    # Bright white-ish end card
    if mean_brightness > 190 and std_all < 60:
        return True
    # Dark solid-colour end card (e.g. NotebookLM's purple variant)
    if std_all < 45 and mean_brightness > 30:
        return True
    return False


def _detect_end_card_start(path):
    """
    Scan EVERY SECOND of the video from the end backwards.
    The moment we find a second that is NOT an end card, we know the
    end card starts at the next second — that's our trim point.

    This is the most reliable possible approach: it works regardless of
    end card colour, duration, or fade style.

    Steps:
      1. Dump all frames at 1 fps into a temp folder using a single
         FFmpeg call (much faster than one ffmpeg call per second).
      2. Walk the frame list BACKWARDS.
      3. First non-end-card frame found → trim = that timestamp + 1s.
      4. If every frame in the last 60s looks like end card, trim at
         total - 60s (safety guard against all-white slide decks).
    """
    total = _probe_duration(path)
    if total < 15:
        return total  # too short to trim

    # How far back to scan: last 40% of video, max 90s
    scan_secs = min(90, max(20, int(total * 0.40)))
    scan_from = max(0.0, total - scan_secs)

    fd_dir = tempfile.mkdtemp()
    try:
        # Single FFmpeg call: extract 1 frame/sec from scan window
        subprocess.run([
            "ffmpeg", "-y",
            "-ss", f"{scan_from:.2f}", "-i", str(path),
            "-vf", "fps=1,scale=320:180",
            "-q:v", "5",
            os.path.join(fd_dir, "f%04d.jpg")
        ], capture_output=True, timeout=120)

        # Collect and sort frames
        frames = sorted([
            f for f in os.listdir(fd_dir) if f.endswith(".jpg")
        ])

        if not frames:
            return max(0.0, total - 8.0)

        # Load all frame arrays
        frame_data = []
        for fname in frames:
            try:
                arr = np.array(Image.open(os.path.join(fd_dir, fname)).convert("RGB"))
                frame_data.append(arr)
            except Exception:
                frame_data.append(None)

        n = len(frame_data)

        # Walk BACKWARDS — find first non-end-card frame
        for i in range(n - 1, -1, -1):
            if not _is_end_card_frame(frame_data[i]):
                # This frame is content — end card starts at i+1
                trim_t = scan_from + i + 1
                # Sanity: must leave at least 10s of content
                if trim_t > 10 and trim_t < total - 1:
                    return trim_t
                break

        # All frames looked like end card — trim at scan_from + 1s
        # (protects against all-white slide decks being fully trimmed)
        return max(10.0, scan_from + 1.0)

    except Exception:
        return max(0.0, total - 8.0)
    finally:
        try:
            import shutil
            shutil.rmtree(fd_dir, ignore_errors=True)
        except Exception:
            pass


def _overlay_on_template(png_path, out_path, y_expr, timeout=90):
    """
    Two-step overlay: PNG -> silent video first, then overlay two videos.
    Avoids -loop 1 mixed with video input which causes audio frame drops
    (drop=N, frame=0) regardless of where -t is placed.
    """
    dur = _probe_duration(str(INTRO_TPL))
    png_vid = Path(str(out_path).replace(".mp4", "_pngvid.mp4"))

    # Step 1: PNG -> fixed-duration silent video (no audio stream)
    _ff([
        "ffmpeg", "-y",
        "-loop", "1", "-framerate", "30", "-t", f"{dur:.4f}",
        "-i", str(png_path),
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
        "-pix_fmt", "yuva420p",   # keep alpha
        "-vf", "format=rgba",
        "-an",
        str(png_vid)
    ], timeout=30)

    # Step 2: overlay png_vid on INTRO_TPL — both are finite videos, no sync issues
    _ff([
        "ffmpeg", "-y",
        "-i", str(INTRO_TPL),
        "-i", str(png_vid),
        "-filter_complex",
        f"[1:v]format=rgba[ovr];[0:v][ovr]overlay=x=0:y='{y_expr}'[out]",
        "-map", "[out]", "-map", "0:a?",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-ac", "2",
        "-r", "30", "-pix_fmt", "yuv420p",
        str(out_path)
    ], timeout=timeout)

    # Cleanup intermediate
    try: png_vid.unlink()
    except: pass


def make_intro(course, unit_num, unit_title, tmp):
    png = tmp / "intro_overlay.png"; out = tmp / "intro.mp4"
    render_intro_overlay(course, unit_num, unit_title).save(str(png), "PNG")
    _overlay_on_template(png, out, "if(lt(t\\,0.8)\\,300*pow(1-t/0.8\\,2)\\,0)")
    return out


def make_outro(tmp):
    png = tmp / "end_overlay.png"; out = tmp / "outro.mp4"
    render_end_overlay().save(str(png), "PNG")
    _overlay_on_template(png, out, "if(lt(t\\,0.8)\\,250*pow(1-t/0.8\\,2)\\,0)")
    return out


def normalise(inp, out):
    ha = _has_audio(inp)
    cmd = ["ffmpeg", "-y", "-i", str(inp)]
    if not ha:
        cmd += ["-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo"]
    cmd += [
        "-vf",
        "scale=1920:1080:force_original_aspect_ratio=decrease,"
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black",
        "-r", "30",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k", "-ar", "48000", "-ac", "2",
        "-pix_fmt", "yuv420p",
        "-async", "1",       # fix audio/video drift on unusual inputs
        "-vsync", "cfr",     # force constant frame rate
    ]
    if not ha:
        cmd += ["-shortest"]
    cmd += [str(out)]
    try:
        _ff(cmd)
    except RuntimeError:
        # Fallback: strip and re-mux audio separately to avoid sync issues
        cmd2 = ["ffmpeg", "-y", "-i", str(inp),
            "-vf",
            "scale=1920:1080:force_original_aspect_ratio=decrease,"
            "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black",
            "-r", "30", "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-an", "-pix_fmt", "yuv420p", str(out)]
        _ff(cmd2)
    return Path(out)


def _detect_top_watermark_end(path, max_scan=120.0):
    """
    Detect how long the NotebookLM top badge is visible.

    Fixes over old version:
    - Scans first 5 seconds to find the badge (not just t=0),
      so videos starting with a black/dark/title frame still work.
    - Relaxed diff threshold (25 not 12) to tolerate JPEG noise.
    - Requires 3 consecutive changed frames before declaring badge gone,
      so a single compression artifact doesn't trigger a false exit.
    - Always covers at least the first slide change even if badge detection
      fails — NotebookLM always shows badge on slide 1.
    """
    try:    src_w, src_h = _probe_resolution(path)
    except: src_w, src_h = 1920, 1080
    sx, sy = src_w / 1920, src_h / 1080
    rx  = max(0, int(WM_TOP_X * sx))
    ry  = max(0, int(WM_TOP_Y * sy))
    rw  = max(1, int(WM_TOP_W * sx))
    rh  = max(1, int(WM_TOP_H * sy))

    def _grab(t):
        fd, tf = tempfile.mkstemp(suffix=".jpg"); os.close(fd)
        try:
            subprocess.run(["ffmpeg", "-y", "-ss", f"{t:.2f}", "-i", str(path),
                "-vframes", "1", tf], capture_output=True, timeout=8)
            img = Image.open(tf).convert("RGB")
            return np.array(img)[ry:ry+rh, rx:rx+rw].astype(float)
        except: return None
        finally:
            try: os.unlink(tf)
            except: pass

    total = _probe_duration(path)

    # ── Step 1: find a reference frame where badge IS present ────────
    # Scan t=0, 0.5, 1.0 … up to 5s to find a near-white badge region
    ref = None
    for probe_t in [t * 0.5 for t in range(11)]:  # 0, 0.5 … 5.0
        candidate = _grab(probe_t)
        if candidate is not None and candidate.size > 0:
            if (candidate > 180).mean() > 0.50:  # region is mostly light = badge present
                ref = candidate
                break

    # If badge never found in first 5s, cover first 3s conservatively
    if ref is None:
        return 3.0

    # ── Step 2: scan forward until badge consistently disappears ─────
    scan_end      = min(max_scan, total - 2.0)
    t             = 0.5
    last_badge_t  = probe_t   # last time we confirmed badge present
    consecutive_gone = 0

    while t <= scan_end:
        frame = _grab(t)
        if frame is not None and frame.size > 0:
            diff = np.abs(frame - ref).mean()
            if diff < 25:
                last_badge_t     = t    # badge still there
                consecutive_gone = 0
            else:
                consecutive_gone += 1
                if consecutive_gone >= 3:
                    # Badge gone for 3 consecutive frames — confirmed gone
                    return last_badge_t + 0.5
        t += 0.5

    return min(last_badge_t + 0.5, max_scan)


def remove_notebooklm_watermark(inp, out, src_resolution, tmp, progress_cb=None):
    inp_str, out_str = str(inp), str(out)
    if progress_cb: progress_cb("Detecting end-card start time…")
    duration = _probe_duration(inp_str)
    ecs      = _detect_end_card_start(inp_str)
    trim_at  = None
    # Trim if end card detected AND it leaves at least 5s of real content
    # AND it is not just the fallback value (total-9)
    fallback = max(0.0, duration - 9.0)
    # Detector always returns a trim point — apply it if it leaves enough content
    MIN_CONTENT = 10.0
    if ecs > MIN_CONTENT and ecs < duration - 1.0:
        trim_at = ecs
        if progress_cb: progress_cb(f"✂️ Trimming end card at {trim_at:.1f}s ({duration - trim_at:.1f}s removed)…")
    else:
        if progress_cb: progress_cb("ℹ️ Video too short to trim — keeping full")
    use_logo = SLC_LOGO.exists() and SLC_LOGO.stat().st_size>500
    if progress_cb: progress_cb("Detecting top watermark duration…")
    top_end = _detect_top_watermark_end(inp_str)
    top_png = tmp/"wm_top.png"
    if top_end>0.5:
        if progress_cb: progress_cb(f"   Badge visible until ~{top_end:.1f}s")
        _make_box_png([(WM_TOP_X,WM_TOP_Y,WM_TOP_W,WM_TOP_H,BOX_RADIUS)], top_png, colour=(249,249,249,255))
        use_top = True; en_top = f"lte(t\\,{top_end:.2f})"
    else:
        if progress_cb: progress_cb("   No top badge detected — skipping")
        Image.new("RGBA",(1920,1080),(0,0,0,0)).save(str(top_png),"PNG")
        use_top = False; en_top = "0"
    if use_logo:
        comp_png = _make_logo_composite(SLC_LOGO, (WM_BR_X,WM_BR_Y,WM_BR_W,WM_BR_H))
        fc = (f"[1:v]format=rgba[comp];[0:v][comp]overlay=x=0:y=0[v1];"
              f"[2:v]format=rgba[top];[v1][top]overlay=x=0:y=0:enable='{en_top}'[vout]")
        cmd = ["ffmpeg","-y","-i",inp_str,"-i",str(comp_png),"-i",str(top_png)]
    else:
        br_png = tmp/"wm_br.png"
        _make_box_png([(WM_BR_X,WM_BR_Y,WM_BR_W,WM_BR_H,BOX_RADIUS)], br_png, colour=(249,249,249,255))
        fc = (f"[1:v]format=rgba[br];[0:v][br]overlay=x=0:y=0[v1];"
              f"[2:v]format=rgba[top];[v1][top]overlay=x=0:y=0:enable='{en_top}'[vout]")
        cmd = ["ffmpeg","-y","-i",inp_str,"-i",str(br_png),"-i",str(top_png)]
    extra = ["-t",f"{trim_at:.2f}"] if trim_at else []
    audio_map = "-map 0:a" if _has_audio(inp_str) else "-f lavfi -i anullsrc=r=48000:cl=stereo -map 3:a"
    cmd += ["-filter_complex",fc,"-map","[vout]"]+extra+[
        "-c:v","libx264","-preset","ultrafast","-crf","23",
        "-c:a","aac","-b:a","128k","-ar","48000","-ac","2",
        "-r","30","-pix_fmt","yuv420p"]
    if trim_at: cmd += [out_str]
    else:       cmd += ["-shortest", out_str]
    _ff(cmd, timeout=max(900,int(duration*25)))
    return Path(out)


def add_notebooklm_transition(intro, main, out, duration=1.0, direction="left"):
    tm = {"left":"wipeleft","right":"wiperight","up":"wipeup","down":"wipedown"}
    wipe = tm.get(direction,"wipeleft"); intro_d = _probe_duration(intro)
    half = max(0.25, min(duration/2, intro_d-0.05))
    cc = ("color=c=0x7B2CBF:s=1920x1080:r=30,"
          "drawbox=x=0:y=0:w=576:h=1080:color=0x7B2CBF:t=fill,"
          "drawbox=x=576:y=0:w=461:h=1080:color=0x4285F4:t=fill,"
          "drawbox=x=1037:y=0:w=346:h=1080:color=0x7EDFC3:t=fill,"
          "drawbox=x=1383:y=0:w=537:h=1080:color=0xB7E4C7:t=fill")
    _ff(["ffmpeg","-y","-i",str(intro),"-i",str(main),
         "-f","lavfi","-t",f"{duration}","-i",cc,
         "-f","lavfi","-t",f"{duration}","-i","anullsrc=r=48000:cl=stereo",
         "-filter_complex",
         "[0:v]fps=30,format=yuv420p,settb=AVTB[v0];"
         "[1:v]fps=30,format=yuv420p,settb=AVTB[v1];"
         "[2:v]fps=30,format=yuv420p,settb=AVTB[vc];"
         f"[v0][vc]xfade=transition={wipe}:duration={half}:offset={max(intro_d-half,0):.3f}[vx];"
         f"[vx][v1]xfade=transition={wipe}:duration={half}:offset={intro_d:.3f}[vout];"
         f"[0:a][3:a]acrossfade=d={half}:c1=tri:c2=tri[ax];"
         f"[ax][1:a]acrossfade=d={half}:c1=tri:c2=tri[aout]",
         "-map","[vout]","-map","[aout]",
         "-c:v","libx264","-preset","ultrafast","-crf","23",
         "-c:a","aac","-b:a","128k","-ar","48000","-ac","2",
         "-r","30","-pix_fmt","yuv420p",str(out)], timeout=180)
    return Path(out)


def concat(parts, out, tmp):
    lst = tmp/"list.txt"
    with open(lst,"w") as f:
        for p in parts: f.write(f"file '{Path(p).resolve()}'\n")
    try:
        _ff(["ffmpeg","-y","-f","concat","-safe","0","-i",str(lst),"-c","copy",str(out)])
    except RuntimeError:
        _ff(["ffmpeg","-y","-f","concat","-safe","0","-i",str(lst),
             "-c:v","libx264","-preset","ultrafast","-crf","23",
             "-c:a","aac","-b:a","128k","-pix_fmt","yuv420p",str(out)])
    return Path(out)


def preview_frame(course, unit_num, unit_title):
    if not INTRO_TPL.exists(): raise FileNotFoundError(f"Missing: {INTRO_TPL}")
    fd, tp = tempfile.mkstemp(suffix=".png"); os.close(fd)
    try:
        subprocess.run(["ffmpeg","-y","-i",str(INTRO_TPL),"-ss","3","-vframes","1",tp],
                       capture_output=True, timeout=10)
        bg = Image.open(tp).convert("RGBA"); bg.load()
    finally:
        try: os.unlink(tp)
        except: pass
    comp = Image.alpha_composite(bg, render_intro_overlay(course,unit_num,unit_title)).convert("RGB")
    buf = BytesIO(); comp.save(buf,"JPEG",quality=90); buf.seek(0)
    return buf


# ── OneDrive OAuth2 ───────────────────────────────────────────────────────
def _get_token_cache():
    cache = msal.SerializableTokenCache()
    if TOKEN_CACHE_FILE.exists():
        try: cache.deserialize(TOKEN_CACHE_FILE.read_text()); return cache
        except: pass
    if st.session_state.get("_ms_token_cache"):
        try: cache.deserialize(st.session_state["_ms_token_cache"])
        except: pass
    return cache

def _save_token_cache(cache):
    if cache.has_state_changed:
        s = cache.serialize()
        try: TOKEN_CACHE_FILE.parent.mkdir(parents=True,exist_ok=True); TOKEN_CACHE_FILE.write_text(s)
        except: pass
        st.session_state["_ms_token_cache"] = s

def _get_msal_app(cache=None):
    return msal.PublicClientApplication(MS_CLIENT_ID, authority=MS_AUTHORITY, token_cache=cache)

def _get_access_token():
    try:
        cache = _get_token_cache(); app = _get_msal_app(cache)
        accounts = app.get_accounts()
        if accounts:
            result = app.acquire_token_silent(MS_SCOPES, account=accounts[0])
            if result and "access_token" in result:
                _save_token_cache(cache); return result["access_token"]
    except: TOKEN_CACHE_FILE.unlink(missing_ok=True)
    return None

def _start_device_flow():
    cache = _get_token_cache(); app = _get_msal_app(cache)
    flow = app.initiate_device_flow(scopes=MS_SCOPES)
    st.session_state["ms_flow"] = flow; st.session_state["ms_cache"] = cache
    return flow

def _complete_device_flow():
    flow = st.session_state.get("ms_flow"); cache = st.session_state.get("ms_cache")
    if not flow or not cache: return False, "No active auth flow."
    app = _get_msal_app(cache); result = app.acquire_token_by_device_flow(flow)
    if "access_token" in result:
        _save_token_cache(cache)
        st.session_state.pop("ms_flow",None); st.session_state.pop("ms_cache",None)
        return True, result["access_token"]
    return False, result.get("error_description") or result.get("error") or str(result)

def _onedrive_upload(data, filename, folder_name, token, status_cb=None, **kwargs):
    h = {"Authorization":f"Bearer {token}","Content-Type":"application/json"}
    def _cb(s):
        if status_cb: status_cb(s)
    folder_id = None; drive_prefix = "me/drive"
    folder_url = kwargs.get("folder_url","").strip()
    if folder_url:
        _cb("🔗 Resolving folder from URL…")
        try:
            import base64 as _b64
            b64 = _b64.urlsafe_b64encode(folder_url.encode()).rstrip(b"=").decode()
            for ep in [
                f"https://graph.microsoft.com/v1.0/shares/u!{b64}/root?$select=id,name,webUrl,parentReference",
                f"https://graph.microsoft.com/v1.0/shares/u!{b64}/driveItem?$select=id,name,webUrl,parentReference",
            ]:
                sr = requests.get(ep,headers=h,timeout=20); _cb(f"   → HTTP {sr.status_code}")
                if sr.status_code==200:
                    item=sr.json(); folder_id=item["id"]
                    drv=item.get("parentReference",{}).get("driveId","")
                    drive_prefix=f"drives/{drv}" if drv else "me/drive"
                    _cb(f"✅ Folder resolved: '{item.get('name','?')}'"); break
            if not folder_id: _cb(f"⚠️ URL resolve failed ({sr.status_code})")
        except Exception as ex: _cb(f"⚠️ URL error: {ex}")
    if not folder_id:
        _cb("🔍 Searching personal OneDrive…")
        r = requests.get(f"https://graph.microsoft.com/v1.0/me/drive/root/search(q='{folder_name}')?$select=id,name,webUrl,folder,parentReference",headers=h,timeout=20)
        if r.status_code==200:
            hits=[i for i in r.json().get("value",[]) if folder_name.lower() in i.get("name","").lower()]
            if hits:
                item=hits[0]; folder_id=item["id"]
                drv=item.get("parentReference",{}).get("driveId","")
                drive_prefix=f"drives/{drv}" if drv else "me/drive"
                _cb(f"✅ Found in personal OneDrive: '{item['name']}'")
    if not folder_id:
        return False,(f"❌ Folder '{folder_name}' not found. Paste the folder URL in the OneDrive folder URL field.")
    safe_name = filename.replace(" ","_")
    urls_to_try = [
        f"https://graph.microsoft.com/v1.0/{drive_prefix}/items/{folder_id}:/{safe_name}:/createUploadSession",
        f"https://graph.microsoft.com/v1.0/me/drive/items/{folder_id}:/{safe_name}:/createUploadSession",
    ]
    r2 = None; errors = []
    for i, session_url in enumerate(urls_to_try):
        try:
            r2 = requests.post(session_url,headers=h,json={"item":{"@microsoft.graph.conflictBehavior":"rename"}},timeout=30)
            _cb(f"   Upload session HTTP {r2.status_code}")
            if r2.status_code in (200,201): break
            else: errors.append(f"Format {i+1} → HTTP {r2.status_code}"); r2=None
        except Exception as ex: errors.append(f"Format {i+1} → {ex}"); r2=None
    if r2 is None: return False, f"❌ All upload session attempts failed:\n{chr(10).join(errors)}"
    upload_url = r2.json().get("uploadUrl")
    if not upload_url: return False, f"❌ No uploadUrl in response"
    CHUNK=5*1024*1024; total=len(data); uploaded=0; file_web_url=None; last_pct=-1
    while uploaded<total:
        chunk=data[uploaded:uploaded+CHUNK]; chunk_end=uploaded+len(chunk)-1
        pct=int(uploaded/total*100)
        if pct//10!=last_pct//10: _cb(f"⬆️ Uploading… {pct}% ({uploaded//1048576}/{total//1048576} MB)"); last_pct=pct
        r3=requests.put(upload_url,data=chunk,timeout=180,
            headers={"Content-Length":str(len(chunk)),"Content-Range":f"bytes {uploaded}-{chunk_end}/{total}","Content-Type":"video/mp4"})
        if r3.status_code in (200,201):
            try: file_web_url=r3.json().get("webUrl","")
            except: file_web_url=""
        elif r3.status_code!=202: return False,f"Upload failed at byte {uploaded} (HTTP {r3.status_code}): {r3.text[:300]}"
        uploaded+=len(chunk)
    _cb(f"✅ Upload complete! ({total//1048576} MB)")
    return True, file_web_url or "https://onedrive.live.com"


def _check_template():
    if not INTRO_TPL.exists(): st.error(f"❌ Intro template not found: `{INTRO_TPL}`"); st.stop()
    if INTRO_TPL.stat().st_size < 10000: st.error("❌ Intro template appears corrupt."); st.stop()

def _ensure_logo():
    if not SLC_LOGO.exists() or SLC_LOGO.stat().st_size < 100:
        import base64
        SLC_LOGO.parent.mkdir(parents=True, exist_ok=True)
        SLC_LOGO.write_bytes(base64.b64decode(_SLC_LOGO_B64))

_check_template()
_ensure_logo()

# ── CSS ───────────────────────────────────────────────────────────────────
st.markdown("""<style>
.stApp{background:linear-gradient(135deg,#0a2a3c 0%,#0d3b54 30%,#0f4c6e 60%,#1a3a5c 100%)}
header[data-testid="stHeader"]{background:rgba(10,42,60,.85);backdrop-filter:blur(10px)}
.stButton>button[kind="primary"],.stDownloadButton>button{background:#60ccbe!important;color:#0a2a3c!important;border:none!important;border-radius:12px!important;font-weight:600!important;padding:.6rem 2rem!important}
.stButton>button[kind="primary"]:hover,.stDownloadButton>button:hover{background:#4dbcad!important;box-shadow:0 4px 20px rgba(96,204,190,.3)!important}
.stTextInput>div>div>input{background:rgba(255,255,255,.08)!important;border:1px solid rgba(255,255,255,.15)!important;border-radius:10px!important;color:#fff!important}
.stTextInput>div>div>input:focus{border-color:#60ccbe!important;box-shadow:0 0 0 3px rgba(96,204,190,.15)!important}
section[data-testid="stFileUploader"]{border:2px dashed rgba(96,204,190,.4)!important;border-radius:14px!important;background:rgba(96,204,190,.03)!important}
.fb{display:inline-block;background:rgba(96,204,190,.12);border:1px solid rgba(96,204,190,.3);padding:6px 18px;border-radius:8px;font-size:14px;color:rgba(255,255,255,.85)}
.fa{display:inline-block;color:#60ccbe;font-size:18px;margin:0 6px}
.sn{display:inline-flex;align-items:center;justify-content:center;width:28px;height:28px;border-radius:50%;background:#60ccbe;color:#0a2a3c;font-weight:700;font-size:13px;margin-right:10px}
.st{color:#60ccbe;font-size:15px;font-weight:600;text-transform:uppercase;letter-spacing:1.5px}
.ok{text-align:center;padding:24px;background:rgba(96,204,190,.08);border:1px solid rgba(96,204,190,.25);border-radius:16px;margin:16px 0}
.ok h3{color:#60ccbe;margin-bottom:4px}
hr{border-color:rgba(96,204,190,.15)!important}
video{border-radius:12px;border:1px solid rgba(96,204,190,.2)}
.auth-box{background:rgba(255,255,255,.05);border:1px solid rgba(96,204,190,.3);border-radius:12px;padding:16px;margin:12px 0;font-size:14px}
</style>""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────
st.markdown("""<div style="display:flex;align-items:center;gap:16px;margin-bottom:8px">
  <h1 style="margin:0;font-size:28px">🎬 SLC Video Merger</h1>
  <span style="background:#60ccbe;color:#0a2a3c;font-size:11px;font-weight:700;
        padding:3px 12px;border-radius:20px;text-transform:uppercase">Fast</span>
</div>""", unsafe_allow_html=True)
st.markdown("""<div style="text-align:center;margin:8px 0 24px">
  <span class="fb">🎬 Custom Intro</span><span class="fa">→</span>
  <span class="fb">🟪🟦🟩⬜ Transition</span><span class="fa">→</span>
  <span class="fb">📹 NotebookLM Video</span><span class="fa">→</span>
  <span class="fb">🔚 Outro</span>
</div>""", unsafe_allow_html=True)

# ── 1 Intro ───────────────────────────────────────────────────────────────
st.markdown('<div><span class="sn">1</span><span class="st">Intro Customisation</span></div>', unsafe_allow_html=True)
course_name = st.text_input("Course Name", placeholder="e.g. Level 3 Diploma in Sports Development (RQF)")
c1, _ = st.columns(2)
with c1:
    unit_number = st.text_input("Unit / Chapter Number", placeholder="e.g. UNIT 03 | CHAPTER 06")
if st.button("👁 Preview Intro", type="secondary"):
    if course_name and unit_number:
        with st.spinner("Rendering…"):
            st.image(preview_frame(course_name, unit_number, ""), caption="Intro Preview", use_column_width=True)
    else:
        st.warning("Enter course name and unit number first.")
st.markdown("---")

# ── 2 Upload ──────────────────────────────────────────────────────────────
st.markdown('<div><span class="sn">2</span><span class="st">Upload NotebookLM Video</span></div>', unsafe_allow_html=True)
vid = st.file_uploader("Upload your NotebookLM video", type=["mp4","mov","webm","avi","mkv"], help="Up to 500 MB")
if vid:
    st.success(f"📁 **{vid.name}** — {vid.size/1048576:.1f} MB")
st.markdown("---")

# ── OneDrive connect ──────────────────────────────────────────────────────
if ONEDRIVE_AVAILABLE:
    _token = _get_access_token()
    if _token:
        st.markdown('<div><span class="sn">☁</span><span class="st">OneDrive</span></div>', unsafe_allow_html=True)
        st.success("✅ Connected to OneDrive — videos will upload automatically after merging.")
        if st.button("🔄 Switch account / Re-connect", type="secondary", key="od_reset"):
            TOKEN_CACHE_FILE.unlink(missing_ok=True)
            st.session_state.pop("ms_flow",None); st.session_state.pop("ms_cache",None)
            st.rerun()
    else:
        st.markdown('<div><span class="sn">☁</span><span class="st">OneDrive — One-time Setup</span></div>', unsafe_allow_html=True)
        st.markdown('<p style="font-size:13px;color:rgba(255,255,255,.7);margin-bottom:8px">Sign in once with the department Microsoft account — stays connected for all users.</p>', unsafe_allow_html=True)
        if "ms_flow" not in st.session_state:
            if st.button("🔑 Connect Department Microsoft Account"):
                with st.spinner("Starting sign-in…"):
                    _start_device_flow()
                st.rerun()
        else:
            flow = st.session_state["ms_flow"]
            st.markdown(f"""<div class="auth-box">
            <strong>Step 1</strong> — Open: <a href="{flow['verification_uri']}" target="_blank" style="color:#60ccbe">{flow['verification_uri']}</a><br><br>
            <strong>Step 2</strong> — Enter code: <code style="background:#1a3a5c;padding:4px 12px;border-radius:6px;font-size:18px;letter-spacing:3px;color:#60ccbe">{flow['user_code']}</code><br><br>
            <strong>Step 3</strong> — Sign in, then click below.
            </div>""", unsafe_allow_html=True)
            if st.button("✅ I've signed in — complete connection"):
                with st.spinner("Completing…"):
                    ok, result = _complete_device_flow()
                if ok: st.success("✅ Connected!"); st.rerun()
                else:  st.error(f"Sign-in failed: {result}")
st.markdown("---")

# ── 3 Merge ───────────────────────────────────────────────────────────────
st.markdown('<div><span class="sn">3</span><span class="st">Generate Final Video</span></div>', unsafe_allow_html=True)

if st.button("🎬 Merge & Download", type="primary", use_container_width=True):
    if not course_name: st.error("Enter a course name."); st.stop()
    if not unit_number: st.error("Enter a unit number."); st.stop()
    if not vid:         st.error("Upload a video.");      st.stop()

    t0 = time.time()
    session_dir = get_session_dir()

    with tempfile.TemporaryDirectory(dir=str(session_dir)) as td:
        tmp = Path(td); bar = st.progress(0, "Starting…"); msg = st.empty()
        try:
            raw = tmp/"raw.mp4"; raw.write_bytes(vid.getvalue())
            src_res = _probe_resolution(str(raw))

            msg.info("⏳ **Step 1 / 4** — Building intro, outro and normalising…")
            bar.progress(10)
            results, errors = {}, {}

            def _job(name, fn, *args):
                try:    results[name] = fn(*args)
                except Exception as e: errors[name] = e

            with ThreadPoolExecutor(max_workers=3) as pool:
                pool.submit(_job,"intro",make_intro,course_name,unit_number,"",tmp)
                pool.submit(_job,"outro",make_outro,tmp)
                pool.submit(_job,"norm",normalise,raw,tmp/"norm.mp4")

            if errors:
                raise RuntimeError("; ".join(f"{k}: {v}" for k,v in errors.items()))

            msg.info(f"⏳ **Step 2 / 4** — Replacing watermarks ({src_res[0]}×{src_res[1]})…")
            bar.progress(40)
            norm_clean = remove_notebooklm_watermark(
                results["norm"], tmp/"norm_clean.mp4", src_res, tmp,
                progress_cb=lambda s: msg.info(f"⏳ **Step 2 / 4** — {s}"))

            msg.info("⏳ **Step 3 / 4** — Adding 4-colour transition…")
            bar.progress(65)
            with_trans = add_notebooklm_transition(results["intro"], norm_clean, tmp/"intro_and_main.mp4")

            msg.info("⏳ **Step 4 / 4** — Merging final segments…")
            bar.progress(85)
            final = concat([with_trans, results["outro"]], tmp/"final.mp4", tmp)

            bar.progress(100); secs = time.time()-t0
            data = final.read_bytes(); mb = len(data)/1048576
            msg.empty(); bar.empty()

            safec = course_name[:30].replace(" ","_")
            safeu = unit_number.replace(" ","_").replace("|","")
            filename = f"SLC_Video_{safec}_{safeu}.mp4"
            st.session_state["video_data"]     = data
            st.session_state["video_filename"] = filename
            st.session_state["video_mb"]       = mb
            st.session_state["video_secs"]     = secs

        except Exception as e:
            bar.empty(); msg.empty()
            st.error(f"**Processing failed:**\n\n```\n{e}\n```")

# ── Result + download + upload ────────────────────────────────────────────
if st.session_state.get("video_data"):
    data     = st.session_state["video_data"]
    filename = st.session_state["video_filename"]
    mb       = st.session_state["video_mb"]
    secs     = st.session_state["video_secs"]

    st.markdown(f"""<div class="ok">
        <div style="font-size:48px;margin-bottom:8px">✅</div>
        <h3>Video Ready!</h3>
        <p style="color:rgba(255,255,255,.5);font-size:13px">{secs:.1f}s &nbsp;•&nbsp; {mb:.1f} MB</p>
    </div>""", unsafe_allow_html=True)

    st.video(data, format="video/mp4")
    st.download_button("⬇ Download Final Video", data, filename, "video/mp4", use_container_width=True)

    current_token = _get_access_token() if ONEDRIVE_AVAILABLE else None
    if current_token:
        st.markdown("---")
        st.markdown('<div style="margin:8px 0"><span class="sn">☁</span><span class="st">Save to OneDrive</span></div>', unsafe_allow_html=True)
        if st.button("☁ Upload to OneDrive", use_container_width=True):
            prog = st.progress(0,"Starting upload…"); stat = st.empty()
            def _log(s):
                stat.info(s)
                if "%" in s:
                    try: pct=int(s.split("%")[0].split()[-1]); prog.progress(pct,s)
                    except: pass
            ok, result = _onedrive_upload(data,filename,"",current_token,status_cb=_log,folder_url=ONEDRIVE_FOLDER_URL)
            prog.empty()
            if ok:   stat.empty(); st.success(f"✅ Uploaded **{filename}**! [Open file]({result})")
            else:    stat.empty(); st.error(result)
