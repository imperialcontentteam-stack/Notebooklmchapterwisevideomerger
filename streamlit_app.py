#!/usr/bin/env python3
"""
SLC Video Merger – Streamlit Edition  (Queue + PaddleOCR build)
Watermark detection: PaddleOCR scans frames to find watermark regions
dynamically. Falls back to hardcoded coordinates if PaddleOCR is not
installed or detects nothing.
"""

import os, subprocess, tempfile, time, uuid, shutil
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

_SLC_LOGO_B64 = "iVBORw0KGgoAAAANSUhEUgAAAHcAAABNCAYAAACc2PtBAAAtpElEQVR4nO29WY8kyZXv9ztm5mvsuWctXc1ukk1OX1KjucQVpAcBepAAfWJ9AAkC9HAx94ozHK69VFdlVVbusftiZnpwN0/PrC2bPcTVDHmAzIjwcHeLsGNn/x8LqWpHn4QPvwbAK7wAqNvz/DvO618iqvfKIR4Qh/LgpHded/67jzefCVT/ADT3e+e4vVF7z5VX773m3wupj5/yDpIPM/L7DPs+xvbJ37lG4e9d9zd6N5kfdvktkwNDgjTcldTbE5oH34jfvbV1n6lBOptRbpnb/H/YAhN/e9++tP97l1r4gcy9z4z+RPYZ30zkXbV8/9r3D6J6V/YYLHfH+BD9NTDyXdQx9522FfDthPbf97Q2sX+iuG7q+5MpuLvy2dpYj3qnalX+3ZbirVPfWjCB3Fvn37HPvnnT8fbi/PdGP1At39LbTHHvdHxoj3luFexb9C4G9zkh4eqPS243/v3Hf+eMBTDvk9j75O8xQflWIltG9L3nO2r4jpvrOsm5le53M/Itp+rOa6FbHm85d3fHda1H/tdIf560HC729FRAAAAA="

st.set_page_config(page_title="SLC Video Merger", page_icon="🎬", layout="wide")

BASE_DIR  = Path(__file__).parent
INTRO_TPL = BASE_DIR / "assets" / "intro_template.mp4"
SLC_LOGO  = BASE_DIR / "assets" / "slc_logo.png"
TOKEN_CACHE_FILE = Path("/tmp/ms_token_cache.json")

# Hardcoded fallback watermark positions (1920x1080 reference)
WM_BR_X, WM_BR_Y, WM_BR_W, WM_BR_H = 1655, 960, 240, 72
WM_TOP_X, WM_TOP_Y, WM_TOP_W, WM_TOP_H = 760, 48, 390, 72
BOX_RADIUS = 10

# OCR settings
WATERMARK_KEYWORDS = frozenset({
    "notebooklm", "notebook lm", "notebook", "generated with",
    "audio overview", "google",
})
OCR_CONF_THRESHOLD = 0.45
OCR_BOX_PADDING    = 22

_ocr_instance = None  # lazy singleton

MS_CLIENT_ID        = "772dd850-50bd-4c97-9152-d1b3e78fb737"
MS_SCOPES           = ["https://graph.microsoft.com/Files.ReadWrite",
                       "https://graph.microsoft.com/User.Read"]
ONEDRIVE_FOLDER_URL = (
    "https://globaledulinkuk-my.sharepoint.com/:f:/g/personal/"
    "content_gamification_imperiallearning_co_uk/"
    "IgDpo-qQQhSNS5aOw2lBAFo-ASQb3KWLDkHS9kp6sIHuy0s?e=3Ualc4"
)
MS_AUTHORITY = "https://login.microsoftonline.com/globaledulinkuk.onmicrosoft.com"
TEAL, WHITE  = (96, 204, 190), (255, 255, 255)


# ── Fonts ─────────────────────────────────────────────────────────────────
def _font(name):
    for c in [str(BASE_DIR/"fonts"/name),
              f"/usr/share/fonts/truetype/google-fonts/{name}",
              "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]:
        if os.path.exists(c): return c
    return None

BOLD, MEDIUM = _font("Poppins-Bold.ttf"), _font("Poppins-Medium.ttf")

def _ft(path, size):
    try:    return ImageFont.truetype(path, size) if path else ImageFont.load_default()
    except: return ImageFont.load_default()


# ══════════════════════════════════════════════════════════════════════════
#  PADDLEOCR — LAZY INIT + DYNAMIC WATERMARK DETECTION
# ══════════════════════════════════════════════════════════════════════════

def _get_ocr():
    """Return cached PaddleOCR instance, or None if not installed.
    Uses only v2.x-compatible parameters so it works on Streamlit Cloud
    with paddleocr==2.7.3 + paddlepaddle==2.5.2.
    """
    global _ocr_instance
    if _ocr_instance is not None:
        return _ocr_instance
    try:
        from paddleocr import PaddleOCR  # type: ignore
        _ocr_instance = PaddleOCR(
            use_angle_cls=False,
            lang="en",
            use_gpu=False,
            show_log=False,
        )
        return _ocr_instance
    except Exception:
        return None


def _extract_frame_jpg(video_path: str, t: float, out_path: Path) -> bool:
    """Extract one frame at time t (seconds) → JPEG. Returns True on success."""
    r = subprocess.run(
        ["ffmpeg", "-y", "-ss", f"{t:.2f}", "-i", video_path,
         "-vframes", "1", str(out_path)],
        capture_output=True, timeout=12,
    )
    return out_path.exists() and out_path.stat().st_size > 0


def _ocr_boxes_from_frame(frame_path: Path):
    """
    Run PaddleOCR on a 1920x1080 frame.
    Returns list of (x, y, w, h, BOX_RADIUS) for watermark text regions,
    or None if PaddleOCR is unavailable.
    """
    ocr = _get_ocr()
    if ocr is None:
        return None

    try:
        result = ocr.ocr(str(frame_path), cls=False)
    except Exception:
        return None

    if not result or not result[0]:
        return []

    boxes = []
    for line in result[0]:
        if not line or len(line) < 2:
            continue
        box_pts, (text, conf) = line
        if conf < OCR_CONF_THRESHOLD:
            continue
        if not any(kw in text.lower().strip() for kw in WATERMARK_KEYWORDS):
            continue
        xs = [p[0] for p in box_pts];  ys = [p[1] for p in box_pts]
        x  = max(0,      int(min(xs)) - OCR_BOX_PADDING)
        y  = max(0,      int(min(ys)) - OCR_BOX_PADDING)
        w  = min(1920-x, int(max(xs) - min(xs)) + OCR_BOX_PADDING * 2)
        h  = min(1080-y, int(max(ys) - min(ys)) + OCR_BOX_PADDING * 2)
        boxes.append((x, y, w, h, BOX_RADIUS))

    return boxes


def _boxes_near(b1, b2, px=120) -> bool:
    return abs(b1[0]-b2[0]) < px and abs(b1[1]-b2[1]) < px


def _detect_watermarks_ocr(video_path: str, top_end: float, tmp: Path, progress_cb=None):
    """
    Scan two frames with PaddleOCR to classify watermarks:
      - frame at t=0.5s  → all watermarks (top badge + persistent)
      - frame at t=top_end+3s → only persistent watermarks

    Returns (top_only_boxes, persistent_boxes) or None if OCR unavailable.
    Each box is (x, y, w, h, radius).
    """
    if _get_ocr() is None:
        return None

    def _cb(s):
        if progress_cb: progress_cb(s)

    _cb("🔍 OCR: extracting frame at t=0.5s…")
    f0 = tmp / "ocr_f0.jpg"
    if not _extract_frame_jpg(video_path, 0.5, f0):
        _cb("⚠️ OCR: frame extract failed");  return []

    boxes_t0 = _ocr_boxes_from_frame(f0)
    if boxes_t0 is None:
        return None
    _cb(f"🔍 OCR t=0: found {len(boxes_t0)} region(s) — "
        + ", ".join(f"xy({b[0]},{b[1]}) {b[2]}×{b[3]}" for b in boxes_t0))

    t1 = (top_end + 3.0) if top_end > 1.0 else 15.0
    _cb(f"🔍 OCR: extracting frame at t={t1:.0f}s…")
    f1 = tmp / "ocr_f1.jpg"
    boxes_t1 = []
    if _extract_frame_jpg(video_path, t1, f1):
        r = _ocr_boxes_from_frame(f1)
        if r is not None:
            boxes_t1 = r
        _cb(f"🔍 OCR t={t1:.0f}s: found {len(boxes_t1)} region(s) — "
            + ", ".join(f"xy({b[0]},{b[1]}) {b[2]}×{b[3]}" for b in boxes_t1))

    # Classify: persistent = present at both timestamps; top_only = only at t=0
    persistent = list(boxes_t1)
    top_only   = []
    for b in boxes_t0:
        if not any(_boxes_near(b, p) for p in boxes_t1):
            top_only.append(b)
        elif not any(_boxes_near(b, p) for p in persistent):
            persistent.append(b)

    _cb(f"🔍 OCR classified: {len(top_only)} time-limited, {len(persistent)} persistent")
    return top_only, persistent


# ══════════════════════════════════════════════════════════════════════════
#  PILLOW PNG BUILDERS
# ══════════════════════════════════════════════════════════════════════════

def _make_logo_composite(logo_path, box, W=1920, H=1080, bg=(249,249,249,255)):
    brx, bry, brw, brh = box
    img  = Image.new("RGBA", (W, H), (0,0,0,0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle([brx, bry, brx+brw, bry+brh], radius=BOX_RADIUS, fill=bg)
    logo_h = brh - 12
    logo   = Image.open(str(logo_path)).convert("RGBA")
    ratio  = logo.width / logo.height
    logo_w = int(logo_h * ratio)
    if logo_w > brw - 12:
        logo_w = brw - 12;  logo_h = int(logo_w / ratio)
    logo = logo.resize((logo_w, logo_h), Image.LANCZOS)
    img.paste(logo, (brx + brw//2 - logo_w//2, bry + brh//2 - logo_h//2), logo)
    out = Path(str(logo_path)).parent / "logo_composite.png"
    img.save(str(out), "PNG");  return out


def _make_box_png(boxes, path, W=1920, H=1080, colour=(255,255,255,255)):
    img  = Image.new("RGBA", (W, H), (0,0,0,0))
    draw = ImageDraw.Draw(img)
    for (x, y, w, h, r) in boxes:
        draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=colour)
    img.save(str(path), "PNG");  return path


def _build_persistent_cover(persistent_boxes, tmp, use_logo):
    out = tmp / "persist_cover.png"
    if not persistent_boxes:
        Image.new("RGBA", (1920,1080), (0,0,0,0)).save(str(out), "PNG");  return out
    if use_logo:
        logo_box = max(persistent_boxes, key=lambda b: b[2]*b[3])
        comp = _make_logo_composite(SLC_LOGO, logo_box[:4])
        if len(persistent_boxes) > 1:
            base = Image.open(str(comp)).convert("RGBA")
            draw = ImageDraw.Draw(base)
            for b in persistent_boxes:
                if b is not logo_box:
                    draw.rounded_rectangle([b[0],b[1],b[0]+b[2],b[1]+b[3]], radius=b[4], fill=(249,249,249,255))
            base.save(str(out), "PNG")
        else:
            shutil.copy(str(comp), str(out))
    else:
        _make_box_png(persistent_boxes, out, colour=(249,249,249,255))
    return out


def _build_toponly_cover(top_boxes, tmp):
    out = tmp / "top_cover.png"
    if top_boxes:
        _make_box_png(top_boxes, out, colour=(249,249,249,255))
    else:
        Image.new("RGBA", (1920,1080), (0,0,0,0)).save(str(out), "PNG")
    return out


# ══════════════════════════════════════════════════════════════════════════
#  PILLOW TEXT OVERLAYS
# ══════════════════════════════════════════════════════════════════════════

def render_intro_overlay(course, unit_num, unit_title, W=1920, H=1080):
    img  = Image.new("RGBA", (W,H), (0,0,0,0));  draw = ImageDraw.Draw(img)
    pad  = W-200;  csz = 52;  cfn = _ft(BOLD, csz)
    while csz > 28:
        bb = draw.textbbox((0,0), course, font=cfn)
        if bb[2]-bb[0] <= pad: break
        csz -= 2;  cfn = _ft(BOLD, csz)
    c_asc, c_desc = cfn.getmetrics();  c_h = c_asc+c_desc
    ufn  = _ft(BOLD, 28);  utxt = unit_num.upper()
    bb   = draw.textbbox((0,0), utxt, font=ufn)
    badge_w = bb[2]-bb[0]+70;  badge_h = 56
    has_title = bool(unit_title and unit_title.strip());  title_h = 0
    if has_title:
        tsz = 30;  tfn = _ft(MEDIUM, tsz)
        while tsz > 20:
            bb = draw.textbbox((0,0), unit_title, font=tfn)
            if bb[2]-bb[0] <= pad: break
            tsz -= 2;  tfn = _ft(MEDIUM, tsz)
        t_asc, t_desc = tfn.getmetrics();  title_h = t_asc+t_desc
    gap1 = 45;  gap2 = 25
    block_h = c_h+gap1+badge_h+(gap2+title_h if has_title else 0)
    start_y = (H//2-60)-block_h//2
    draw.text((W//2, start_y+c_h//2), course, fill=WHITE, font=cfn, anchor="mm")
    bx = (W-badge_w)//2;  by = start_y+c_h+gap1
    draw.rounded_rectangle([bx,by,bx+badge_w,by+badge_h], radius=14, fill=TEAL+(230,))
    draw.text((bx+badge_w//2, by+badge_h//2), utxt, fill=WHITE, font=ufn, anchor="mm")
    if has_title:
        ty2 = by+badge_h+gap2
        draw.text((W//2, ty2+title_h//2), unit_title, fill=WHITE, font=tfn, anchor="mm")
    return img


def render_end_overlay(W=1920, H=1080):
    img  = Image.new("RGBA", (W,H), (0,0,0,0));  draw = ImageDraw.Draw(img)
    fn   = _ft(BOLD, 42);  bb = draw.textbbox((0,0), "END", font=fn)
    bw, bh = bb[2]-bb[0]+90, 72;  bx, by = (W-bw)//2, (H-bh)//2-20
    draw.rounded_rectangle([bx,by,bx+bw,by+bh], radius=16, fill=TEAL+(230,))
    draw.text((bx+bw//2, by+bh//2), "END", fill=WHITE, font=fn, anchor="mm")
    return img


# ══════════════════════════════════════════════════════════════════════════
#  FFMPEG HELPERS
# ══════════════════════════════════════════════════════════════════════════

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
    try:    w,h = r.stdout.strip().split(",");  return int(w),int(h)
    except: return 1920,1080

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

def _detect_end_card_start(path):
    total = _probe_duration(path);  t = max(0.0, total-20.0)
    while t < total-1.0:
        fd, tf = tempfile.mkstemp(suffix=".jpg");  os.close(fd)
        try:
            subprocess.run(["ffmpeg","-y","-ss",f"{t:.2f}","-i",str(path),"-vframes","1",tf],
                           capture_output=True, timeout=8)
            a = np.array(Image.open(tf))
            if (a.mean(axis=2)>230).sum()/(a.shape[0]*a.shape[1]) > 0.95: return t
        except: pass
        finally:
            try: os.unlink(tf)
            except: pass
        t += 0.5
    return max(0.0, total-9.0)

def _detect_top_watermark_end(path, max_scan=120.0):
    try:    src_w, src_h = _probe_resolution(path)
    except: src_w, src_h = 1920, 1080
    sx=src_w/1920; sy=src_h/1080
    rx=max(0,int(WM_TOP_X*sx)); ry=max(0,int(WM_TOP_Y*sy))
    rw=max(1,int(WM_TOP_W*sx)); rh=max(1,int(WM_TOP_H*sy))
    def _grab(t):
        fd, tf = tempfile.mkstemp(suffix=".jpg");  os.close(fd)
        try:
            subprocess.run(["ffmpeg","-y","-ss",f"{t:.2f}","-i",str(path),"-vframes","1",tf],
                           capture_output=True, timeout=8)
            return np.array(Image.open(tf).convert("RGB"))[ry:ry+rh,rx:rx+rw].astype(float)
        except: return None
        finally:
            try: os.unlink(tf)
            except: pass
    ref = _grab(0.0)
    if ref is None or ref.size==0 or (ref>200).mean()<0.60: return 0.0
    total=_probe_duration(path); scan_end=min(max_scan,total-2.0)
    step=0.5; t=step; last_t=0.0
    while t<=scan_end:
        frame=_grab(t)
        if frame is not None and frame.size>0:
            if np.abs(frame-ref).mean()<12: last_t=t
            else: return last_t+step
        t+=step
    return min(last_t+step,max_scan)


def make_intro(course, unit_num, unit_title, tmp):
    png=str(tmp/"intro_overlay.png"); out=str(tmp/"intro.mp4")
    render_intro_overlay(course,unit_num,unit_title).save(png,"PNG")
    y="if(lt(t\\,0.8)\\,300*pow(1-t/0.8\\,2)\\,0)"
    _ff(["ffmpeg","-y","-i",str(INTRO_TPL),"-loop","1","-i",png,"-filter_complex",
        f"[1:v]format=rgba[ovr];[0:v][ovr]overlay=x=0:y='{y}':shortest=1[out]",
        "-map","[out]","-map","0:a?","-c:v","libx264","-preset","ultrafast",
        "-crf","23","-c:a","aac","-b:a","128k","-ar","48000","-ac","2",
        "-r","30","-pix_fmt","yuv420p",out], timeout=60)
    return Path(out)

def make_outro(tmp):
    png=str(tmp/"end_overlay.png"); out=str(tmp/"outro.mp4")
    render_end_overlay().save(png,"PNG")
    y="if(lt(t\\,0.8)\\,250*pow(1-t/0.8\\,2)\\,0)"
    _ff(["ffmpeg","-y","-i",str(INTRO_TPL),"-loop","1","-i",png,"-filter_complex",
        f"[1:v]format=rgba[ovr];[0:v][ovr]overlay=x=0:y='{y}':shortest=1[out]",
        "-map","[out]","-map","0:a?","-c:v","libx264","-preset","ultrafast",
        "-crf","23","-c:a","aac","-b:a","128k","-ar","48000","-ac","2",
        "-r","30","-pix_fmt","yuv420p",out], timeout=60)
    return Path(out)

def normalise(inp, out):
    ha=_has_audio(inp); cmd=["ffmpeg","-y","-i",str(inp)]
    if not ha: cmd+=["-f","lavfi","-i","anullsrc=r=48000:cl=stereo"]
    cmd+=["-vf",
        "scale=1920:1080:force_original_aspect_ratio=decrease,"
        "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black",
        "-r","30","-c:v","libx264","-preset","ultrafast","-crf","23",
        "-c:a","aac","-b:a","128k","-ar","48000","-ac","2","-pix_fmt","yuv420p"]
    if not ha: cmd+=["-shortest"]
    cmd+=[str(out)]; _ff(cmd); return Path(out)


# ══════════════════════════════════════════════════════════════════════════
#  WATERMARK REMOVAL  — OCR-first, hardcoded fallback
# ══════════════════════════════════════════════════════════════════════════

def remove_notebooklm_watermark(inp, out, src_resolution, tmp, progress_cb=None):
    """
    Cover NotebookLM watermarks using one of two methods:

    1. PaddleOCR (if installed): scans t=0.5s and t≈top_end+3s frames from
       the normalised 1920×1080 video. Text matching WATERMARK_KEYWORDS is
       boxed. Boxes present only at t=0 are treated as time-limited (top badge);
       boxes at both timestamps are treated as persistent.

    2. Hardcoded fallback: uses fixed pixel coordinates (WM_BR_* / WM_TOP_*).
       Activated when PaddleOCR is not installed OR OCR finds no matches.
    """
    inp_str, out_str = str(inp), str(out)

    if progress_cb: progress_cb("Detecting end-card start time…")
    ecs = _detect_end_card_start(inp_str);  duration = _probe_duration(inp_str)
    trim_at = None
    if ecs < duration-2.0:
        trim_at = ecs
        if progress_cb: progress_cb(f"✂️ Trimming end card at {ecs:.1f}s…")

    if progress_cb: progress_cb("Detecting top watermark duration…")
    top_end = _detect_top_watermark_end(inp_str)
    if progress_cb:
        progress_cb(f"   Top badge: ~{top_end:.1f}s" if top_end>0.5 else "   No top badge (pixel diff)")

    # ── OCR detection ──────────────────────────────────────────────────
    ocr_result = _detect_watermarks_ocr(inp_str, top_end, tmp, progress_cb)
    ocr_method = False

    if ocr_result is not None:
        top_boxes, persistent_boxes = ocr_result
        ocr_method = True
        if not top_boxes and not persistent_boxes:
            if progress_cb: progress_cb("⚠️ OCR found no watermarks — falling back to hardcoded")
            ocr_method = False

    if not ocr_method:
        msg = ("ℹ️ PaddleOCR not installed — using hardcoded positions"
               if _get_ocr() is None
               else "ℹ️ Using hardcoded positions (OCR found nothing)")
        if progress_cb: progress_cb(msg)
        top_boxes        = ([(WM_TOP_X,WM_TOP_Y,WM_TOP_W,WM_TOP_H,BOX_RADIUS)] if top_end>0.5 else [])
        persistent_boxes = [(WM_BR_X,WM_BR_Y,WM_BR_W,WM_BR_H,BOX_RADIUS)]

    src = "OCR" if ocr_method else "hardcoded"
    if progress_cb:
        progress_cb(f"📦 Cover boxes ({src}): "
                    f"{len(persistent_boxes)} persistent, {len(top_boxes)} time-limited")

    # ── Build cover PNGs ───────────────────────────────────────────────
    use_logo    = SLC_LOGO.exists() and SLC_LOGO.stat().st_size > 500
    persist_png = _build_persistent_cover(persistent_boxes, tmp, use_logo)
    top_png     = _build_toponly_cover(top_boxes, tmp)

    # ── FFmpeg overlay ─────────────────────────────────────────────────
    en_top = (f"lte(t\\,{top_end:.2f})" if top_boxes and top_end>0.5 else "0")
    fc = (
        "[1:v]format=rgba[persist];"
        "[0:v][persist]overlay=x=0:y=0[v1];"
        "[2:v]format=rgba[topov];"
        f"[v1][topov]overlay=x=0:y=0:enable='{en_top}'[vout]"
    )
    cmd = ["ffmpeg","-y","-i",inp_str,"-i",str(persist_png),"-i",str(top_png),
           "-filter_complex",fc,"-map","[vout]","-map","0:a"]
    if trim_at is not None: cmd += ["-t",f"{trim_at:.2f}"]
    cmd += ["-c:v","libx264","-preset","ultrafast","-crf","23",
            "-c:a","aac","-b:a","128k","-ar","48000","-ac","2",
            "-r","30","-pix_fmt","yuv420p"]
    if trim_at is None: cmd += ["-shortest"]
    cmd += [out_str]
    _ff(cmd, timeout=max(900, int(duration*25)))
    return Path(out)


def add_notebooklm_transition(intro, main, out, duration=1.0, direction="left"):
    tm={"left":"wipeleft","right":"wiperight","up":"wipeup","down":"wipedown"}
    wipe=tm.get(direction,"wipeleft"); intro_d=_probe_duration(intro)
    half=max(0.25,min(duration/2,intro_d-0.05))
    cc=("color=c=0x7B2CBF:s=1920x1080:r=30,"
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
    lst=tmp/"list.txt"
    with open(lst,"w") as f:
        for p in parts: f.write(f"file '{Path(p).resolve()}'\n")
    try: _ff(["ffmpeg","-y","-f","concat","-safe","0","-i",str(lst),"-c","copy",str(out)])
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
        bg=Image.open(tp).convert("RGBA"); bg.load()
    finally:
        try: os.unlink(tp)
        except: pass
    comp=Image.alpha_composite(bg,render_intro_overlay(course,unit_num,unit_title)).convert("RGB")
    buf=BytesIO(); comp.save(buf,"JPEG",quality=90); buf.seek(0); return buf


# ══════════════════════════════════════════════════════════════════════════
#  ONEDRIVE
# ══════════════════════════════════════════════════════════════════════════

def _get_token_cache():
    cache=msal.SerializableTokenCache()
    if TOKEN_CACHE_FILE.exists():
        try: cache.deserialize(TOKEN_CACHE_FILE.read_text()); return cache
        except: pass
    if st.session_state.get("_ms_token_cache"):
        try: cache.deserialize(st.session_state["_ms_token_cache"])
        except: pass
    return cache

def _save_token_cache(cache):
    if cache.has_state_changed:
        s=cache.serialize()
        try: TOKEN_CACHE_FILE.parent.mkdir(parents=True,exist_ok=True); TOKEN_CACHE_FILE.write_text(s)
        except: pass
        st.session_state["_ms_token_cache"]=s

def _get_msal_app(cache=None):
    return msal.PublicClientApplication(MS_CLIENT_ID, authority=MS_AUTHORITY, token_cache=cache)

def _get_access_token():
    try:
        cache=_get_token_cache(); app=_get_msal_app(cache)
        accounts=app.get_accounts()
        if accounts:
            result=app.acquire_token_silent(MS_SCOPES,account=accounts[0])
            if result and "access_token" in result:
                _save_token_cache(cache); return result["access_token"]
    except: TOKEN_CACHE_FILE.unlink(missing_ok=True)
    return None

def _start_device_flow():
    cache=_get_token_cache(); app=_get_msal_app(cache)
    flow=app.initiate_device_flow(scopes=MS_SCOPES)
    st.session_state["ms_flow"]=flow; st.session_state["ms_cache"]=cache; return flow

def _complete_device_flow():
    flow=st.session_state.get("ms_flow"); cache=st.session_state.get("ms_cache")
    if not flow or not cache: return False,"No active auth flow."
    app=_get_msal_app(cache); result=app.acquire_token_by_device_flow(flow)
    if "access_token" in result:
        _save_token_cache(cache)
        st.session_state.pop("ms_flow",None); st.session_state.pop("ms_cache",None)
        return True,result["access_token"]
    return False, result.get("error_description") or result.get("error") or str(result)

def _onedrive_upload(data, filename, folder_name, token, status_cb=None, **kwargs):
    h={"Authorization":f"Bearer {token}","Content-Type":"application/json"}
    def _cb(s):
        if status_cb: status_cb(s)
    folder_id=None; drive_prefix="me/drive"; folder_url=kwargs.get("folder_url","").strip()
    if folder_url:
        _cb("🔗 Resolving folder…")
        try:
            import base64 as _b64
            b64=_b64.urlsafe_b64encode(folder_url.encode()).rstrip(b"=").decode()
            for ep in [
                f"https://graph.microsoft.com/v1.0/shares/u!{b64}/root?$select=id,name,webUrl,parentReference",
                f"https://graph.microsoft.com/v1.0/shares/u!{b64}/driveItem?$select=id,name,webUrl,parentReference",
            ]:
                sr=requests.get(ep,headers=h,timeout=20)
                if sr.status_code==200:
                    item=sr.json(); folder_id=item["id"]
                    drv=item.get("parentReference",{}).get("driveId","")
                    drive_prefix=f"drives/{drv}" if drv else "me/drive"
                    _cb(f"✅ Folder: '{item.get('name','?')}'"); break
        except Exception as ex: _cb(f"⚠️ {ex}")
    if not folder_id:
        return False, f"❌ Folder not found. Provide the folder URL."
    safe=filename.replace(" ","_")
    urls=([f"https://graph.microsoft.com/v1.0/{drive_prefix}/items/{folder_id}:/{safe}:/createUploadSession",
           f"https://graph.microsoft.com/v1.0/{drive_prefix}/items/{folder_id}:/{filename}:/createUploadSession"]
          if drive_prefix!="me/drive" else
          [f"https://graph.microsoft.com/v1.0/me/drive/items/{folder_id}:/{safe}:/createUploadSession"])
    r2=None
    for i,url in enumerate(urls):
        try:
            r2=requests.post(url,headers=h,json={"item":{"@microsoft.graph.conflictBehavior":"rename"}},timeout=30)
            if r2.status_code in(200,201): break
            r2=None
        except: r2=None
    if r2 is None: return False,"❌ Could not create upload session."
    upload_url=r2.json().get("uploadUrl")
    if not upload_url: return False,"❌ No uploadUrl in response"
    CHUNK=5*1024*1024; total=len(data); uploaded=0; file_url=None; last_pct=-1
    while uploaded<total:
        chunk=data[uploaded:uploaded+CHUNK]; end=uploaded+len(chunk)-1
        pct=int(uploaded/total*100)
        if pct//10!=last_pct//10:
            _cb(f"⬆️ {pct}% ({uploaded//1048576}/{total//1048576} MB)"); last_pct=pct
        r3=requests.put(upload_url,data=chunk,timeout=180,
            headers={"Content-Length":str(len(chunk)),"Content-Range":f"bytes {uploaded}-{end}/{total}","Content-Type":"video/mp4"})
        if r3.status_code in(200,201):
            try: file_url=r3.json().get("webUrl","")
            except: file_url=""
        elif r3.status_code!=202:
            return False,f"Upload failed at {uploaded} bytes (HTTP {r3.status_code})"
        uploaded+=len(chunk)
    _cb(f"✅ Done ({total//1048576} MB)"); return True, file_url or "https://onedrive.live.com"


# ══════════════════════════════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════════════════════════════

def _check_template():
    if not INTRO_TPL.exists(): st.error(f"❌ Intro template not found: `{INTRO_TPL}`"); st.stop()
    if INTRO_TPL.stat().st_size<10000: st.error("❌ Intro template appears corrupt."); st.stop()

def _ensure_logo():
    if not SLC_LOGO.exists() or SLC_LOGO.stat().st_size<100:
        import base64
        SLC_LOGO.parent.mkdir(parents=True,exist_ok=True)
        SLC_LOGO.write_bytes(base64.b64decode(_SLC_LOGO_B64))

_check_template(); _ensure_logo()


# ══════════════════════════════════════════════════════════════════════════
#  QUEUE HELPERS
# ══════════════════════════════════════════════════════════════════════════

def _new_item(course_name, unit_number, orig_filename, video_bytes):
    return {"id":uuid.uuid4().hex[:8],"course_name":course_name,"unit_number":unit_number,
            "orig_filename":orig_filename,"size_mb":len(video_bytes)/1048576,
            "video_bytes":video_bytes,"status":"pending","result_data":None,
            "result_filename":None,"error":None,"secs":None,"mb_out":None,
            "od_url":None,"ocr_used":None}

def _process_item(item, bar_slot, msg_slot):
    item=dict(item); item["status"]="processing"; t0=time.time()
    try:
        with tempfile.TemporaryDirectory() as td:
            tmp=Path(td); raw=tmp/"raw.mp4"; raw.write_bytes(item["video_bytes"])
            src_res=_probe_resolution(str(raw))
            msg_slot.info("⏳ **1/4** — Building intro, outro, normalising…"); bar_slot.progress(10)
            results,errors={},{}
            def _job(name,fn,*args):
                try: results[name]=fn(*args)
                except Exception as e: errors[name]=e
            with ThreadPoolExecutor(max_workers=3) as pool:
                pool.submit(_job,"intro",make_intro,item["course_name"],item["unit_number"],"",tmp)
                pool.submit(_job,"outro",make_outro,tmp)
                pool.submit(_job,"norm",normalise,raw,tmp/"norm.mp4")
            if errors: raise RuntimeError("; ".join(f"{k}: {v}" for k,v in errors.items()))
            ocr_log=[]
            msg_slot.info(f"⏳ **2/4** — OCR watermark detection ({src_res[0]}×{src_res[1]})…")
            bar_slot.progress(40)
            norm_clean=remove_notebooklm_watermark(
                results["norm"],tmp/"norm_clean.mp4",src_res,tmp,
                progress_cb=lambda s:(ocr_log.append(s),msg_slot.info(f"⏳ **2/4** — {s}")))
            item["ocr_used"]=any("OCR" in m and "hardcoded" not in m and "not installed" not in m
                                 for m in ocr_log)
            msg_slot.info("⏳ **3/4** — Adding 4-colour transition…"); bar_slot.progress(65)
            with_trans=add_notebooklm_transition(results["intro"],norm_clean,tmp/"intro_and_main.mp4")
            msg_slot.info("⏳ **4/4** — Merging final segments…"); bar_slot.progress(85)
            final=concat([with_trans,results["outro"]],tmp/"final.mp4",tmp)
            bar_slot.progress(100); data=final.read_bytes()
            safec=item["course_name"][:30].replace(" ","_")
            safeu=item["unit_number"].replace(" ","_").replace("|","")
            fn=f"SLC_Video_{safec}_{safeu}.mp4"
            item.update({"status":"done","result_data":data,"result_filename":fn,
                         "secs":time.time()-t0,"mb_out":len(data)/1048576})
    except Exception as e:
        item["status"]="failed"; item["error"]=str(e)
    return item


if "queue" not in st.session_state: st.session_state["queue"]=[]


# ══════════════════════════════════════════════════════════════════════════
#  CSS
# ══════════════════════════════════════════════════════════════════════════

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
hr{border-color:rgba(96,204,190,.15)!important}
video{border-radius:12px;border:1px solid rgba(96,204,190,.2)}
.auth-box{background:rgba(255,255,255,.05);border:1px solid rgba(96,204,190,.3);border-radius:12px;padding:16px;margin:12px 0;font-size:14px}
.q-row{background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:10px;padding:10px 14px;margin-bottom:8px}
.badge-pending{background:rgba(255,200,80,.15);color:#ffc850;border:1px solid rgba(255,200,80,.3);padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600}
.badge-processing{background:rgba(96,204,190,.15);color:#60ccbe;border:1px solid rgba(96,204,190,.4);padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600}
.badge-done{background:rgba(80,200,120,.15);color:#50c878;border:1px solid rgba(80,200,120,.3);padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600}
.badge-failed{background:rgba(255,80,80,.15);color:#ff5050;border:1px solid rgba(255,80,80,.3);padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600}
.ocr-on{color:#60ccbe;font-size:11px;font-weight:600;background:rgba(96,204,190,.12);border:1px solid rgba(96,204,190,.3);padding:1px 8px;border-radius:10px}
.ocr-off{color:rgba(255,255,255,.4);font-size:11px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.1);padding:1px 8px;border-radius:10px}
</style>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════════════════════════════════

_ocr_avail=_get_ocr() is not None
_ocr_badge=(
    '<span style="background:#60ccbe;color:#0a2a3c;font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px">🔍 OCR Active</span>'
    if _ocr_avail else
    '<span style="background:rgba(255,200,80,.2);color:#ffc850;font-size:11px;font-weight:700;padding:3px 10px;border-radius:20px" title="pip install paddleocr paddlepaddle">📍 Hardcoded Positions</span>'
)
st.markdown(f"""<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px">
  <h1 style="margin:0;font-size:28px">🎬 SLC Video Merger</h1>
  <span style="background:#60ccbe;color:#0a2a3c;font-size:11px;font-weight:700;padding:3px 12px;border-radius:20px;text-transform:uppercase">Queue</span>
  {_ocr_badge}
</div>""", unsafe_allow_html=True)

if not _ocr_avail:
    st.info("💡 **PaddleOCR not installed** — watermarks covered using hardcoded positions. "
            "For dynamic detection: `pip install paddleocr paddlepaddle`", icon="ℹ️")

st.markdown("""<div style="text-align:center;margin:8px 0 24px">
  <span class="fb">🎬 Intro</span><span class="fa">→</span>
  <span class="fb">🟪🟦🟩⬜ Transition</span><span class="fa">→</span>
  <span class="fb">📹 NotebookLM (watermarks removed)</span><span class="fa">→</span>
  <span class="fb">🔚 Outro</span>
</div>""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════
#  ONEDRIVE CONNECTION
# ══════════════════════════════════════════════════════════════════════════

if ONEDRIVE_AVAILABLE:
    _token=_get_access_token()
    with st.expander("☁  OneDrive Connection", expanded=not bool(_token)):
        if _token:
            st.success("✅ Connected — processed videos can be uploaded automatically.")
            if st.button("🔄 Switch / Re-connect", type="secondary", key="od_reset"):
                TOKEN_CACHE_FILE.unlink(missing_ok=True)
                st.session_state.pop("ms_flow",None); st.session_state.pop("ms_cache",None)
                st.rerun()
        else:
            st.markdown('<p style="font-size:13px;color:rgba(255,255,255,.7)">Sign in once — stays connected for all users.</p>', unsafe_allow_html=True)
            if "ms_flow" not in st.session_state:
                if st.button("🔑 Connect Department Microsoft Account"):
                    with st.spinner("Starting sign-in…"): _start_device_flow()
                    st.rerun()
            else:
                flow=st.session_state["ms_flow"]
                st.markdown(f"""<div class="auth-box">
                <strong>Step 1</strong> — Open: <a href="{flow['verification_uri']}" target="_blank" style="color:#60ccbe">{flow['verification_uri']}</a><br><br>
                <strong>Step 2</strong> — Enter code: &nbsp;<code style="background:#1a3a5c;padding:4px 12px;border-radius:6px;font-size:18px;letter-spacing:3px;color:#60ccbe">{flow['user_code']}</code><br><br>
                <strong>Step 3</strong> — Sign in, then click below.
                </div>""", unsafe_allow_html=True)
                if st.button("✅ I've signed in — complete connection"):
                    with st.spinner("Completing…"):
                        ok,result=_complete_device_flow()
                    if ok: st.success("✅ Connected!"); st.rerun()
                    else:  st.error(f"Failed: {result}")

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════
#  SECTION 1 — ADD TO QUEUE
# ══════════════════════════════════════════════════════════════════════════

st.markdown('<div><span class="sn">1</span><span class="st">Add Video to Queue</span></div>', unsafe_allow_html=True)
c1,c2=st.columns(2)
with c1: add_course=st.text_input("Course Name",key="add_course",placeholder="e.g. Level 3 Diploma in Sports Development (RQF)")
with c2: add_unit=st.text_input("Unit / Chapter Number",key="add_unit",placeholder="e.g. UNIT 03 | CHAPTER 06")
add_vid=st.file_uploader("Upload NotebookLM Video",type=["mp4","mov","webm","avi","mkv"],help="Up to 500 MB",key="add_vid")
col_prev,col_add=st.columns([1,2])
with col_prev:
    if st.button("👁 Preview Intro",type="secondary",key="btn_preview"):
        if add_course and add_unit:
            with st.spinner("Rendering…"): st.image(preview_frame(add_course,add_unit,""),caption="Intro Preview",use_container_width=True)
        else: st.warning("Enter course name and unit number first.")
with col_add:
    if st.button("➕ Add to Queue",type="primary",use_container_width=True,key="btn_add"):
        if not add_course: st.error("Enter a course name.")
        elif not add_unit: st.error("Enter a unit number.")
        elif not add_vid:  st.error("Upload a video file.")
        else:
            item=_new_item(add_course,add_unit,add_vid.name,add_vid.getvalue())
            st.session_state["queue"].append(item)
            st.success(f"✅ **{add_vid.name}** added ({item['size_mb']:.1f} MB)"); st.rerun()

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════
#  SECTION 2 — QUEUE MANAGER
# ══════════════════════════════════════════════════════════════════════════

queue=st.session_state["queue"]
pc=sum(1 for i in queue if i["status"]=="pending")
dc=sum(1 for i in queue if i["status"]=="done")
fc=sum(1 for i in queue if i["status"]=="failed")

st.markdown(f'<div><span class="sn">2</span><span class="st">Queue</span>'
            f'&nbsp;&nbsp;<span style="font-size:13px;color:rgba(255,255,255,.5)">'
            f'{len(queue)} item{"s" if len(queue)!=1 else ""} &nbsp;·&nbsp; '
            f'🟡 {pc} pending &nbsp;·&nbsp; ✅ {dc} done &nbsp;·&nbsp; ❌ {fc} failed</span></div>',
            unsafe_allow_html=True)

if not queue:
    st.markdown('<p style="color:rgba(255,255,255,.35);font-size:14px;margin:16px 0 0 38px">No videos queued yet — add one above.</p>', unsafe_allow_html=True)
else:
    for idx,item in enumerate(queue):
        status=item["status"]
        badge={"pending":'<span class="badge-pending">🟡 Pending</span>',
               "processing":'<span class="badge-processing">🔵 Processing</span>',
               "done":'<span class="badge-done">✅ Done</span>',
               "failed":'<span class="badge-failed">❌ Failed</span>'}.get(status,status)
        ocr_tag=('&nbsp;<span class="ocr-on">🔍 OCR</span>' if item.get("ocr_used") is True
                 else '&nbsp;<span class="ocr-off">📍 fallback</span>' if item.get("ocr_used") is False
                 else "")
        size_str=f"{item['size_mb']:.1f} MB in"
        if item.get("mb_out"): size_str+=f" · {item['mb_out']:.1f} MB out"
        if item.get("secs"):   size_str+=f" · {item['secs']:.0f}s"
        st.markdown(f"""<div class="q-row">
          <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
            <span style="font-weight:700;color:rgba(255,255,255,.4);font-size:12px">#{idx+1}</span>
            {badge}{ocr_tag}
            <span style="font-weight:600;color:#fff">{item['course_name']}</span>
            <span style="color:rgba(96,204,190,.8);font-size:13px">{item['unit_number']}</span>
            <span style="color:rgba(255,255,255,.35);font-size:12px">{item['orig_filename']} · {size_str}</span>
          </div></div>""", unsafe_allow_html=True)

        bc=st.columns([1,1,1,1,3])
        with bc[0]:
            if status in("pending","failed"):
                if st.button("🗑 Remove",key=f"rm_{item['id']}"):
                    st.session_state["queue"]=[q for q in queue if q["id"]!=item["id"]]; st.rerun()
        with bc[1]:
            if status=="failed" and item.get("error"):
                st.button("⚠️ Error",key=f"err_{item['id']}",help=item["error"],type="secondary")
        with bc[2]:
            if status=="done" and item.get("result_data"):
                st.download_button("⬇ Download",data=item["result_data"],file_name=item["result_filename"],mime="video/mp4",key=f"dl_{item['id']}")
        with bc[3]:
            cur_tok=_get_access_token() if ONEDRIVE_AVAILABLE else None
            if status=="done" and item.get("result_data") and cur_tok:
                if not item.get("od_url"):
                    if st.button("☁ Upload",key=f"od_{item['id']}"):
                        with st.spinner(f"Uploading…"):
                            ok,res=_onedrive_upload(item["result_data"],item["result_filename"],"",cur_tok,folder_url=ONEDRIVE_FOLDER_URL)
                        if ok:
                            for q in st.session_state["queue"]:
                                if q["id"]==item["id"]: q["od_url"]=res
                            st.success(f"✅ [Open]({res})"); st.rerun()
                        else: st.error(res)
                else:
                    st.markdown(f'<a href="{item["od_url"]}" target="_blank" style="color:#50c878;font-size:13px">✅ On OneDrive</a>', unsafe_allow_html=True)

    st.markdown("")
    qc1,qc2,qc3=st.columns([2,1,1])
    with qc1:
        if st.button(f"🎬 Process Queue  ({pc} pending)",type="primary",use_container_width=True,disabled=(pc==0),key="btn_process"):
            overall_bar=st.progress(0,"Starting…"); overall_msg=st.empty()
            item_bar=st.progress(0); item_msg=st.empty()
            pending_ids=[i["id"] for i in st.session_state["queue"] if i["status"]=="pending"]
            total_jobs=len(pending_ids)
            for ji,iid in enumerate(pending_ids):
                ref=next((q for q in st.session_state["queue"] if q["id"]==iid),None)
                if not ref: continue
                overall_msg.info(f"**Job {ji+1}/{total_jobs}** — {ref['course_name']} · {ref['unit_number']}")
                overall_bar.progress(ji/total_jobs,f"Processing {ji+1}/{total_jobs}…")
                item_bar.progress(0)
                updated=_process_item(ref,item_bar,item_msg)
                for q in st.session_state["queue"]:
                    if q["id"]==iid: q.update(updated); break
            overall_bar.progress(1.0,"Queue complete!"); item_bar.empty(); item_msg.empty()
            time.sleep(0.8); overall_bar.empty(); overall_msg.empty(); st.rerun()
    with qc2:
        if st.button("🗑 Clear Done",type="secondary",use_container_width=True,key="btn_clear_done"):
            st.session_state["queue"]=[q for q in queue if q["status"]!="done"]; st.rerun()
    with qc3:
        if st.button("🗑 Clear All",type="secondary",use_container_width=True,key="btn_clear_all"):
            st.session_state["queue"]=[]; st.rerun()

    cur_tok=_get_access_token() if ONEDRIVE_AVAILABLE else None
    uploadable=[q for q in queue if q["status"]=="done" and not q.get("od_url") and q.get("result_data")]
    if uploadable and cur_tok:
        st.markdown("---")
        st.markdown('<div style="margin:4px 0 12px"><span class="sn">☁</span><span class="st">Bulk OneDrive Upload</span></div>', unsafe_allow_html=True)
        if st.button(f"☁ Upload All to OneDrive  ({len(uploadable)} files)",use_container_width=True,key="btn_od_all"):
            bulk_bar=st.progress(0); bulk_msg=st.empty()
            for i,item in enumerate(uploadable):
                bulk_msg.info(f"Uploading {i+1}/{len(uploadable)}: {item['result_filename']}")
                bulk_bar.progress(i/len(uploadable))
                ok,res=_onedrive_upload(item["result_data"],item["result_filename"],"",cur_tok,folder_url=ONEDRIVE_FOLDER_URL)
                for q in st.session_state["queue"]:
                    if q["id"]==item["id"]: q["od_url"]=res if ok else "error"
            bulk_bar.progress(1.0); bulk_msg.success(f"✅ Uploaded {len(uploadable)} files!")
            time.sleep(1); bulk_bar.empty(); bulk_msg.empty(); st.rerun()

st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════
#  SECTION 3 — PREVIEW RESULTS
# ══════════════════════════════════════════════════════════════════════════

done_items=[q for q in st.session_state["queue"] if q["status"]=="done"]
if done_items:
    st.markdown('<div><span class="sn">3</span><span class="st">Preview Results</span></div>', unsafe_allow_html=True)
    for item in done_items:
        method_str=" · 🔍 OCR" if item.get("ocr_used") else " · 📍 fallback"
        with st.expander(f"▶  {item['course_name']} — {item['unit_number']}{method_str}", expanded=False):
            st.video(item["result_data"],format="video/mp4")
            c1,c2=st.columns(2)
            with c1:
                st.download_button("⬇ Download",data=item["result_data"],file_name=item["result_filename"],mime="video/mp4",use_container_width=True,key=f"dlp_{item['id']}")
            with c2:
                if item.get("od_url") and item["od_url"]!="error":
                    st.markdown(f'<a href="{item["od_url"]}" target="_blank" style="color:#50c878">✅ View on OneDrive</a>', unsafe_allow_html=True)
