import os
import json
import re
import time
import uuid
import shutil
import threading
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, jsonify
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DATA_FILE = "data/config.json"
LOG_FILE  = "data/conversion.log"
os.makedirs("data", exist_ok=True)

# ──────────────────────────────────────────────────────────────────────────────
# Default config
# ──────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "watch_dir": "",
    "delete_originals": False,
    "parallel_jobs": 1,
    "consoles": {
        "ps1": {
            "name": "PlayStation 1",
            "enabled": True,
            "folder": "PS1",
            "output_root": "/roms/PS1roms/roms",
            "input_formats": [".cue", ".iso", ".mds", ".7z", ".zip", ".rar"],
            "output_format": "chd",
            "tool": "chdman",
            "options": {}
        },
        "ps2": {
            "name": "PlayStation 2",
            "enabled": True,
            "folder": "PS2",
            "output_root": "",
            "input_formats": [".iso", ".mds", ".7z", ".zip", ".rar"],
            "output_format": "chd",
            "tool": "chdman",
            "options": {}
        },
        "psp": {
            "name": "PSP",
            "enabled": True,
            "folder": "PSP",
            "output_root": "",
            "input_formats": [".iso", ".bin", ".7z", ".zip", ".rar"],
            "output_format": "cso",
            "tool": "maxcso",
            "options": {}
        },
        "saturn": {
            "name": "Sega Saturn",
            "enabled": True,
            "folder": "Saturn",
            "output_root": "",
            "input_formats": [".cue", ".iso", ".mds", ".7z", ".zip", ".rar"],
            "output_format": "chd",
            "tool": "chdman",
            "options": {}
        },
        "dreamcast": {
            "name": "Sega Dreamcast",
            "enabled": True,
            "folder": "Dreamcast",
            "output_root": "",
            "input_formats": [".gdi", ".cdi", ".cue", ".7z", ".zip", ".rar"],
            "output_format": "chd",
            "tool": "chdman",
            "options": {}
        },
        "gamecube": {
            "name": "Nintendo GameCube",
            "enabled": True,
            "folder": "GameCube",
            "output_root": "",
            "input_formats": [".iso", ".gcm", ".7z", ".zip", ".rar"],
            "output_format": "rvz",
            "tool": "dolphin-tool",
            "options": {"compression": "zstd", "block_size": "131072"}
        },
        "wii": {
            "name": "Nintendo Wii",
            "enabled": True,
            "folder": "Wii",
            "output_root": "",
            "input_formats": [".iso", ".wbfs", ".wad", ".7z", ".zip", ".rar"],
            "output_format": "rvz",
            "tool": "dolphin-tool",
            "options": {"compression": "zstd", "block_size": "131072"}
        },
        "xbox_og": {
            "name": "Xbox (Original)",
            "enabled": True,
            "folder": "Xbox",
            "output_root": "",
            "input_formats": [".iso", ".7z", ".zip", ".rar"],
            "output_format": "iso",
            "tool": "extract-xiso",
            "options": {"mode": "repack"}
        },
        "xbox_360": {
            "name": "Xbox 360",
            "enabled": True,
            "folder": "Xbox360",
            "output_root": "",
            "input_formats": [".iso", ".7z", ".zip", ".rar"],
            "output_format": "god",
            "tool": "iso2god",
            "options": {}
        },
        "nes": {
            "name": "NES",
            "enabled": True,
            "folder": "NES",
            "output_root": "",
            "input_formats": [".nes", ".unf", ".7z", ".zip", ".rar"],
            "output_format": "7z",
            "tool": "7z",
            "options": {}
        },
        "snes": {
            "name": "SNES",
            "enabled": True,
            "folder": "SNES",
            "output_root": "",
            "input_formats": [".smc", ".sfc", ".fig", ".swc", ".7z", ".zip", ".rar"],
            "output_format": "7z",
            "tool": "7z",
            "options": {}
        },
        "n64": {
            "name": "Nintendo 64",
            "enabled": True,
            "folder": "N64",
            "output_root": "",
            "input_formats": [".z64", ".n64", ".v64", ".7z", ".zip", ".rar"],
            "output_format": "7z",
            "tool": "7z",
            "options": {}
        },
        "gba": {
            "name": "Game Boy Advance",
            "enabled": True,
            "folder": "GBA",
            "output_root": "",
            "input_formats": [".gba", ".agb", ".7z", ".zip", ".rar"],
            "output_format": "7z",
            "tool": "7z",
            "options": {}
        },
        "nds": {
            "name": "Nintendo DS",
            "enabled": True,
            "folder": "NDS",
            "output_root": "",
            "input_formats": [".nds", ".dsi", ".7z", ".zip", ".rar"],
            "output_format": "7z",
            "tool": "7z",
            "options": {}
        },
        "3ds": {
            "name": "Nintendo 3DS",
            "enabled": True,
            "folder": "3DS",
            "output_root": "",
            "input_formats": [".3ds", ".cia", ".cci", ".7z", ".zip", ".rar"],
            "output_format": "7z",
            "tool": "7z",
            "options": {}
        },
        "arcade": {
            "name": "Arcade (MAME)",
            "enabled": True,
            "folder": "Arcade",
            "output_root": "",
            "input_formats": [".zip", ".7z"],
            "output_format": "zip",
            "tool": "none",
            "options": {}
        }
    }
}

OUTPUT_FORMATS = {
    "chd":  {"label": "CHD (Compressed Hunks of Data)", "tools": ["chdman"]},
    "cso":  {"label": "CSO (Compressed ISO)",           "tools": ["maxcso"]},
    "zso":  {"label": "ZSO (Zstandard ISO)",            "tools": ["maxcso"]},
    "rvz":  {"label": "RVZ (Dolphin Compressed)",       "tools": ["dolphin-tool"]},
    "iso":  {"label": "ISO / Repacked xiso",            "tools": ["extract-xiso", "none"]},
    "god":  {"label": "GoD (Games on Demand / X360)",   "tools": ["iso2god"]},
    "7z":   {"label": "7-Zip Archive",                  "tools": ["7z"]},
    "zip":  {"label": "ZIP Archive",                    "tools": ["7z"]},
}

ARCHIVE_EXTS = {".7z", ".zip", ".rar"}

# ──────────────────────────────────────────────────────────────────────────────
# Disc / game name helpers
# ──────────────────────────────────────────────────────────────────────────────
DISC_PATTERN = re.compile(
    r'[\s_-]*[\(\[](?:disc|disk|cd|side)\s*([0-9]+|[a-zA-Z])[\)\]]',
    re.IGNORECASE
)

def parse_game_name(filename):
    """
    Returns (game_name, disc_number).
    game_name = original name minus disc tag only; all other tags kept.
    disc_number = int or None.
    """
    stem = Path(filename).stem
    m    = DISC_PATTERN.search(stem)
    if not m:
        return stem.strip(), None
    disc_str = m.group(1)
    try:
        disc_num = int(disc_str)
    except ValueError:
        disc_num = ord(disc_str.upper()) - ord('A') + 1
    game_name = DISC_PATTERN.sub('', stem).strip(' -_')
    return game_name, disc_num


def uses_subfolders(output_format):
    """Only CHD consoles get per-game subfolders and M3U files."""
    return output_format == "chd"


# ──────────────────────────────────────────────────────────────────────────────
# Config helpers
# ──────────────────────────────────────────────────────────────────────────────
def load_config():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_CONFIG.copy()

def save_config(cfg):
    with open(DATA_FILE, "w") as f:
        json.dump(cfg, f, indent=2)

# ──────────────────────────────────────────────────────────────────────────────
# Job queue
# ──────────────────────────────────────────────────────────────────────────────
job_queue   = []
job_history = []
queue_lock  = threading.Lock()

def make_job(filepath, cid, cons):
    stem = Path(filepath).stem
    game_name, disc_num = parse_game_name(stem)
    return {
        "id":            str(uuid.uuid4())[:8],
        "src":           filepath,
        "filename":      os.path.basename(filepath),
        "console_id":    cid,
        "console_name":  cons["name"],
        "output_fmt":    cons["output_format"],
        "game_name":     game_name,
        "disc_num":      disc_num,
        "status":        "pending",
        "progress":      0,
        "progress_msg":  "",
        "created":       datetime.now().isoformat(),
        "started":       None,
        "finished":      None,
        "dst":           None,
        "error":         None,
        "command":       None,
        "note":          None,
        "extracted_tmp": None,
    }

def append_log(msg):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    log.info(msg)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

def get_logs(n=300):
    if not os.path.exists(LOG_FILE):
        return []
    with open(LOG_FILE) as f:
        lines = f.readlines()
    return [l.rstrip() for l in lines[-n:]]

# ──────────────────────────────────────────────────────────────────────────────
# M3U generation
# ──────────────────────────────────────────────────────────────────────────────
def try_write_m3u(completed_job):
    if not uses_subfolders(completed_job["output_fmt"]):
        return
    if completed_job["status"] not in ("done", "simulated"):
        return

    game_name  = completed_job["game_name"]
    console_id = completed_job["console_id"]
    output_dir = os.path.dirname(completed_job["dst"]) if completed_job["dst"] else None
    if not output_dir:
        return

    with queue_lock:
        all_jobs = [
            j for j in (job_queue + job_history)
            if j["console_id"] == console_id and j["game_name"] == game_name
        ]

    if any(j["status"] in ("pending", "running") for j in all_jobs):
        return

    done_jobs = [
        j for j in all_jobs
        if j["status"] in ("done", "simulated") and j["dst"]
    ]
    if not done_jobs:
        return

    done_jobs.sort(key=lambda j: j["disc_num"] or 1)

    m3u_path = os.path.join(output_dir, f"{game_name}.m3u")
    try:
        with open(m3u_path, "w", encoding="utf-8") as f:
            for j in done_jobs:
                f.write(os.path.basename(j["dst"]) + "\n")
        n = len(done_jobs)
        append_log(f"📝 M3U written: {game_name}.m3u ({n} disc{'s' if n > 1 else ''})")
    except Exception as e:
        append_log(f"❌ M3U write failed for {game_name}: {e}")


# ──────────────────────────────────────────────────────────────────────────────
# Pre-extraction
# ──────────────────────────────────────────────────────────────────────────────
def extract_archive(job):
    src = job["src"]
    ext = Path(src).suffix.lower()
    if ext not in ARCHIVE_EXTS:
        return src, None

    tmp_dir = f"/tmp/retroconvert_{job['id']}"
    os.makedirs(tmp_dir, exist_ok=True)
    job["progress_msg"] = "Extracting archive…"
    job["progress"]     = 5
    append_log(f"📦 Extracting: {job['filename']} → {tmp_dir}")

    try:
        cmd = ["unrar", "x", "-y", src, tmp_dir] if ext == ".rar" else \
              ["7z", "x", src, f"-o{tmp_dir}", "-y"]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        if result.returncode != 0:
            append_log(f"❌ Extraction failed: {result.stderr[:300]}")
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None, None

        cfg     = load_config()
        cons    = cfg["consoles"].get(job["console_id"], {})
        allowed = [e.lower() for e in cons.get("input_formats", []) if e not in ARCHIVE_EXTS]

        priority_order = [".cue", ".gdi", ".cdi", ".mds", ".iso", ".bin",
                          ".img", ".mdf", ".z64", ".n64", ".v64", ".nes",
                          ".smc", ".sfc", ".gba", ".nds", ".3ds", ".cia"]

        found_files = []
        for root, dirs, files in os.walk(tmp_dir):
            for f in files:
                if Path(f).suffix.lower() in allowed:
                    found_files.append(os.path.join(root, f))

        if not found_files:
            append_log(f"❌ No usable ROM found in archive: {job['filename']}")
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return None, None

        def sort_key(p):
            e = Path(p).suffix.lower()
            try:    return priority_order.index(e)
            except: return 99

        found_files.sort(key=sort_key)
        chosen = found_files[0]
        append_log(f"📂 Extracted: using {os.path.basename(chosen)}")
        job["progress"]     = 10
        job["progress_msg"] = f"Extracted → {os.path.basename(chosen)}"
        return chosen, tmp_dir

    except subprocess.TimeoutExpired:
        append_log(f"⏱ Extraction timeout: {job['filename']}")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None, None
    except FileNotFoundError as e:
        append_log(f"❌ Extraction tool not found: {e}")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Output path resolver
# ──────────────────────────────────────────────────────────────────────────────
def resolve_output_dir(job):
    """
    CHD consoles:
      - If console has output_root set: use that as the base for the game subfolder.
      - Otherwise: walk up from source file to find the console root folder by name.
    Non-CHD consoles: flat output next to the source file.
    """
    src_path = Path(job["src"])

    if not uses_subfolders(job["output_fmt"]):
        return str(src_path.parent)

    cfg      = load_config()
    cons     = cfg["consoles"].get(job["console_id"], {})
    out_root = cons.get("output_root", "").strip()

    if out_root:
        # User explicitly configured the output root — use it directly
        game_folder = os.path.join(out_root, job["game_name"])
        os.makedirs(game_folder, exist_ok=True)
        return game_folder

    # Fallback: walk up from source file to find the console root folder by name
    folder       = cons.get("folder", "").lower()
    console_root = None
    for parent in src_path.parents:
        if parent.name.lower() == folder:
            console_root = str(parent)
            break

    if console_root is None:
        # Last resort: two levels up from source file
        console_root = str(src_path.parent.parent)

    game_folder = os.path.join(console_root, job["game_name"])
    os.makedirs(game_folder, exist_ok=True)
    return game_folder


# ──────────────────────────────────────────────────────────────────────────────
# Command builders
# ──────────────────────────────────────────────────────────────────────────────
def build_command(job, src_file):
    cfg     = load_config()
    console = cfg["consoles"].get(job["console_id"])
    tool    = console["tool"]
    out_dir = resolve_output_dir(job)
    out_fmt = console["output_format"]

    if Path(job["src"]).suffix.lower() in ARCHIVE_EXTS:
        out_stem = Path(src_file).stem
    else:
        out_stem = Path(job["src"]).stem

    if tool == "chdman":
        src_ext = Path(src_file).suffix.lower()
        mode    = "createdvd" if src_ext in (".gdi", ".cdi") else "createcd"
        dst     = os.path.join(out_dir, out_stem + ".chd")
        return ["chdman", mode, "-i", src_file, "-o", dst, "--force"], dst

    elif tool == "maxcso":
        dst = os.path.join(out_dir, out_stem + "." + out_fmt)
        return ["maxcso", src_file, "--output", dst], dst

    elif tool == "dolphin-tool":
        opts = console.get("options", {})
        dst  = os.path.join(out_dir, out_stem + ".rvz")
        return [
            "dolphin-tool", "convert",
            "-f", "rvz",
            "--compression", opts.get("compression", "zstd"),
            "--block_size",  opts.get("block_size", "131072"),
            "-i", src_file, "-o", dst
        ], dst

    elif tool == "extract-xiso":
        dst  = os.path.join(out_dir, out_stem + "_repacked.iso")
        mode = console.get("options", {}).get("mode", "repack")
        if mode == "repack":
            return ["extract-xiso", "-r", src_file, "-d", out_dir], dst
        dst = os.path.join(out_dir, out_stem + "_extracted")
        return ["extract-xiso", "-x", src_file, "-d", dst], dst

    elif tool == "iso2god":
        dst = os.path.join(out_dir, out_stem + "_god")
        os.makedirs(dst, exist_ok=True)
        return ["iso2god", src_file, dst], dst

    elif tool == "7z":
        dst = os.path.join(out_dir, out_stem + ".7z")
        return ["7z", "a", "-mx=9", dst, src_file], dst

    elif tool == "zip":
        dst = os.path.join(out_dir, out_stem + ".zip")
        return ["7z", "a", "-tzip", dst, src_file], dst

    return None, None


# ──────────────────────────────────────────────────────────────────────────────
# Progress parsers
# ──────────────────────────────────────────────────────────────────────────────
def parse_progress(tool, line, job):
    line = line.strip()
    if not line:
        return
    if tool == "chdman":
        m = re.search(r'(\d+)%', line)
        if m:
            job["progress"]     = int(m.group(1))
            job["progress_msg"] = line[:80]
        elif "creating" in line.lower() or "compressing" in line.lower():
            job["progress_msg"] = line[:80]
    elif tool == "maxcso":
        m = re.search(r'([\d.]+)%', line)
        if m:
            job["progress"]     = int(float(m.group(1)))
            job["progress_msg"] = f"{m.group(1)}% compressed"
    elif tool == "dolphin-tool":
        m = re.search(r'(\d+)%', line)
        if m:
            job["progress"]     = int(m.group(1))
            job["progress_msg"] = line[:80]
    elif tool == "7z":
        m = re.match(r'\s*(\d+)%', line)
        if m:
            job["progress"]     = int(m.group(1))
            job["progress_msg"] = f"{m.group(1)}% compressed"
    elif tool in ("extract-xiso", "iso2god"):
        if job["started"]:
            elapsed             = (datetime.now() - datetime.fromisoformat(job["started"])).seconds
            job["progress"]     = min(95, elapsed // 3)
            job["progress_msg"] = line[:80] if line else "Processing…"


# ──────────────────────────────────────────────────────────────────────────────
# Job runner
# ──────────────────────────────────────────────────────────────────────────────
def run_job(job):
    job["status"]   = "running"
    job["started"]  = datetime.now().isoformat()
    job["progress"] = 0
    append_log(f"▶ Starting: {job['filename']} → {job['console_name']}")
    if job["disc_num"]:
        append_log(f"   🗂 Game: {job['game_name']} — Disc {job['disc_num']}")

    src_file, tmp_dir = extract_archive(job)
    job["extracted_tmp"] = tmp_dir
    if src_file is None:
        job["status"] = "error"
        job["error"]  = "Archive extraction failed"
        _cleanup(job)
        return

    cfg  = load_config()
    cons = cfg["consoles"].get(job["console_id"], {})
    tool = cons.get("tool", "none")

    if tool == "none":
        job["status"]       = "skipped"
        job["progress"]     = 100
        job["progress_msg"] = "No conversion needed"
        append_log(f"⏭ Skipped (no-op): {job['filename']}")
        _cleanup(job)
        return

    cmd, dst = build_command(job, src_file)
    if cmd is None:
        job["status"] = "error"
        job["error"]  = "Could not build conversion command"
        _cleanup(job)
        return

    job["command"] = " ".join(str(c) for c in cmd)
    job["dst"]     = dst

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, bufsize=1
        )
        for line in proc.stdout:
            parse_progress(tool, line, job)
        proc.wait(timeout=7200)

        if proc.returncode == 0:
            job["status"]       = "done"
            job["progress"]     = 100
            job["progress_msg"] = "Complete"
            job["finished"]     = datetime.now().isoformat()
            append_log(f"✅ Done: {job['filename']} → {os.path.basename(dst)}")
            if cfg.get("delete_originals") and os.path.exists(job["src"]):
                os.remove(job["src"])
                append_log(f"🗑  Deleted original: {job['filename']}")
        else:
            job["status"]       = "error"
            job["progress_msg"] = "Failed"
            job["error"]        = f"Exit code {proc.returncode}"
            append_log(f"❌ Error (exit {proc.returncode}): {job['filename']}")

    except FileNotFoundError:
        job["status"]       = "simulated"
        job["progress"]     = 100
        job["progress_msg"] = f"Tool '{tool}' not installed — simulated"
        job["finished"]     = datetime.now().isoformat()
        job["note"]         = f"Tool '{cmd[0]}' not found — simulated"
        append_log(f"🔸 Simulated (tool not installed): {job['filename']} → {cmd[0]}")

    except subprocess.TimeoutExpired:
        proc.kill()
        job["status"]       = "error"
        job["progress_msg"] = "Timeout"
        job["error"]        = "Timeout after 2 hours"
        append_log(f"⏱ Timeout: {job['filename']}")

    except Exception as e:
        job["status"]       = "error"
        job["progress_msg"] = "Exception"
        job["error"]        = str(e)
        append_log(f"❌ Exception: {job['filename']}: {e}")

    _cleanup(job)
    try_write_m3u(job)


def _cleanup(job):
    tmp = job.get("extracted_tmp")
    if tmp and os.path.exists(tmp):
        shutil.rmtree(tmp, ignore_errors=True)
        job["extracted_tmp"] = None


# ──────────────────────────────────────────────────────────────────────────────
# Worker thread
# ──────────────────────────────────────────────────────────────────────────────
def worker():
    while True:
        with queue_lock:
            pending = [j for j in job_queue if j["status"] == "pending"]
        if pending:
            job = pending[0]
            run_job(job)
            with queue_lock:
                if job in job_queue:
                    job_queue.remove(job)
                    job_history.insert(0, job)
                    if len(job_history) > 500:
                        job_history.pop()
        else:
            time.sleep(1)

threading.Thread(target=worker, daemon=True).start()


# ──────────────────────────────────────────────────────────────────────────────
# File watcher
# ──────────────────────────────────────────────────────────────────────────────
def find_console_for_file(filepath, cfg):
    parts = Path(filepath).parts
    for cid, cons in cfg["consoles"].items():
        if not cons.get("enabled"):
            continue
        folder = cons.get("folder", "").lower()
        for p in parts:
            if p.lower() == folder:
                ext = Path(filepath).suffix.lower()
                if ext in [e.lower() for e in cons.get("input_formats", [])]:
                    return cid, cons
    return None, None

def enqueue_file(filepath, cfg):
    cid, cons = find_console_for_file(filepath, cfg)
    if not cid:
        return
    with queue_lock:
        already = any(j["src"] == filepath for j in job_queue + job_history)
    if already:
        return
    job = make_job(filepath, cid, cons)
    with queue_lock:
        job_queue.append(job)
    disc_info = f" — Disc {job['disc_num']}" if job["disc_num"] else ""
    append_log(f"📥 Queued: {job['filename']} ({cons['name']}){disc_info}")

def wait_and_enqueue(filepath):
    """Wait until file size is stable before queuing — handles slow network transfers."""
    stable_for = 0
    last_size  = -1
    while stable_for < 3:
        try:
            size = os.path.getsize(filepath)
        except OSError:
            time.sleep(10)
            continue
        if size == last_size and size > 0:
            stable_for += 1
        else:
            stable_for = 0
        last_size = size
        time.sleep(10)
    cfg = load_config()
    enqueue_file(filepath, cfg)

class ROMHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        threading.Thread(target=wait_and_enqueue, args=(event.src_path,), daemon=True).start()

    def on_moved(self, event):
        if event.is_directory:
            return
        threading.Thread(target=wait_and_enqueue, args=(event.dest_path,), daemon=True).start()

observer      = None
observer_lock = threading.Lock()

def start_watcher(watch_dir):
    global observer
    with observer_lock:
        if observer and observer.is_alive():
            observer.stop()
            observer.join()
        if not watch_dir or not os.path.isdir(watch_dir):
            return
        observer = Observer()
        observer.schedule(ROMHandler(), watch_dir, recursive=True)
        observer.start()
        append_log(f"👁 Watching: {watch_dir}")

def scan_existing(watch_dir, cfg):
    count = 0
    for root, dirs, files in os.walk(watch_dir):
        for f in files:
            enqueue_file(os.path.join(root, f), cfg)
            count += 1
    append_log(f"🔍 Scan complete — {count} files checked")


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    cfg = load_config()
    return render_template("index.html", cfg=cfg, output_formats=OUTPUT_FORMATS)

@app.route("/api/config", methods=["GET"])
def api_get_config():
    return jsonify(load_config())

@app.route("/api/config", methods=["POST"])
def api_save_config():
    data    = request.json
    cfg     = load_config()
    old_dir = cfg.get("watch_dir")
    cfg["watch_dir"]        = data.get("watch_dir", "")
    cfg["delete_originals"] = data.get("delete_originals", False)
    cfg["parallel_jobs"]    = int(data.get("parallel_jobs", 1))
    if "consoles" in data:
        for cid, vals in data["consoles"].items():
            if cid in cfg["consoles"]:
                cfg["consoles"][cid].update(vals)
            else:
                cfg["consoles"][cid] = vals
    save_config(cfg)
    if cfg["watch_dir"] != old_dir:
        start_watcher(cfg["watch_dir"])
    return jsonify({"ok": True})

@app.route("/api/queue", methods=["GET"])
def api_queue():
    with queue_lock:
        q = list(job_queue)
        h = list(job_history[:100])
    return jsonify({"queue": q, "history": h})

@app.route("/api/queue/clear", methods=["POST"])
def api_clear_queue():
    with queue_lock:
        to_remove = [j for j in job_queue if j["status"] == "pending"]
        for j in to_remove:
            job_queue.remove(j)
    return jsonify({"ok": True})

@app.route("/api/scan", methods=["POST"])
def api_scan():
    cfg = load_config()
    wd  = cfg.get("watch_dir")
    if not wd or not os.path.isdir(wd):
        return jsonify({"error": "Watch directory not set or invalid"}), 400
    threading.Thread(target=scan_existing, args=(wd, cfg), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/logs", methods=["GET"])
def api_logs():
    n = int(request.args.get("n", 300))
    return jsonify(get_logs(n))

@app.route("/api/status", methods=["GET"])
def api_status():
    cfg = load_config()
    with queue_lock:
        pending  = sum(1 for j in job_queue if j["status"] == "pending")
        running  = sum(1 for j in job_queue if j["status"] == "running")
        done     = sum(1 for j in job_history if j["status"] in ("done", "simulated"))
        errors   = sum(1 for j in job_history if j["status"] == "error")
        active   = [j for j in job_queue if j["status"] == "running"]
    return jsonify({
        "watch_dir":   cfg.get("watch_dir"),
        "watching":    observer is not None and observer.is_alive(),
        "pending":     pending,
        "running":     running,
        "done":        done,
        "errors":      errors,
        "active_jobs": active,
    })

@app.route("/api/job/<jid>/retry", methods=["POST"])
def retry_job(jid):
    with queue_lock:
        job = next((j for j in job_history if j["id"] == jid), None)
        if job:
            cfg  = load_config()
            cons = cfg["consoles"].get(job["console_id"],
                       {"name": job["console_name"], "output_format": job["output_fmt"]})
            new  = make_job(job["src"], job["console_id"], cons)
            job_queue.append(new)
    return jsonify({"ok": True})

@app.route("/api/job/<jid>/cancel", methods=["POST"])
def cancel_job(jid):
    with queue_lock:
        job = next((j for j in job_queue if j["id"] == jid and j["status"] == "pending"), None)
        if job:
            job_queue.remove(job)
    return jsonify({"ok": True})

@app.route("/api/watcher/start", methods=["POST"])
def start_watch():
    cfg = load_config()
    start_watcher(cfg.get("watch_dir", ""))
    return jsonify({"ok": True})

@app.route("/api/watcher/stop", methods=["POST"])
def stop_watch():
    global observer
    with observer_lock:
        if observer and observer.is_alive():
            observer.stop()
            observer.join()
            observer = None
    return jsonify({"ok": True})

if __name__ == "__main__":
    cfg = load_config()
    if cfg.get("watch_dir"):
        start_watcher(cfg["watch_dir"])
    app.run(host="0.0.0.0", port=5000, debug=False)
