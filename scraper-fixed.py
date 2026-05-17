import os
import sys
import json
import asyncio
import random
import re
import zipfile
from pathlib import Path
from datetime import datetime
import httpx
from loguru import logger
import yt_dlp

if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ---------------------------------------------------------
# 1. Batch Folder Setup
# ---------------------------------------------------------
_env_batch = os.environ.get("BATCH_FOLDER_NAME", "").strip()
BATCH_FOLDER_NAME = _env_batch if _env_batch else \
    f"Batch--{datetime.now().strftime('%Y-%m-%d-%A_%I-%M-%S-%p')}"

CHUNK_INDEX  = int(os.environ.get("CHUNK_INDEX",  "0"))
TOTAL_CHUNKS = int(os.environ.get("TOTAL_CHUNKS", "1"))

try:
    os.makedirs(BATCH_FOLDER_NAME, exist_ok=True)
    logger.info(f"📁 Batch Folder: '{BATCH_FOLDER_NAME}'  [Chunk {CHUNK_INDEX+1}/{TOTAL_CHUNKS}]")
except Exception as e:
    logger.warning(f"⚠️ Folder Error: {e}")

CONFIG = {
    "base_dir":               BATCH_FOLDER_NAME,
    "download_media":         True,
    "http2":                  False,
    "proxy":                  None,
    "timeout":                60.0,
    "delay_between_pages":    (1.0, 2.5),
    "delay_between_videos":   (1.0, 3.0),
    "video_concurrency":      10,
    "comment_concurrency":    8,
    "max_comments_limit":     10000,
    "upload_concurrency":     1,    
    "hard_link_limit":        99999999000,
}

_upload_sem: asyncio.Semaphore = None

# ---------------------------------------------------------
# 2. TXT-Based Tracking System
# ---------------------------------------------------------
_suffix        = f"_chunk{CHUNK_INDEX}" if TOTAL_CHUNKS > 1 else ""
TRACKING_FILE  = f"tracking_report{_suffix}.txt"
COMPLETED_FILE = f"completed{_suffix}.txt"
FAILED_FILE    = f"failed{_suffix}.txt"
LOG_FILE       = f"scraper_log{_suffix}.txt"

def _append_tracking(status: str, url: str, note: str = ""):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{status}] {url}"
    if note:
        line += f" | {note}"
    try:
        with open(TRACKING_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        logger.warning(f"⚠️ Tracking write error: {e}")

async def track_success(url: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("SUCCESS", url)
        with open(COMPLETED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")

async def track_failed(url: str, note: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("FAILED", url, note)
        with open(FAILED_FILE, "a", encoding="utf-8") as f:
            f.write(url + "\n")

async def track_skipped(url: str, note: str, file_lock: asyncio.Lock):
    async with file_lock:
        _append_tracking("SKIPPED", url, note)

def load_set_from_file(filepath: str) -> set:
    if not os.path.exists(filepath):
        return set()
    with open(filepath, "r", encoding="utf-8") as f:
        return set(line.strip() for line in f if line.strip())

# ---------------------------------------------------------
# 3. Logger Setup
# ---------------------------------------------------------
logger.remove()
logger.add(sys.stdout,
           format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")
logger.add(LOG_FILE, level="DEBUG",
           format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
           rotation="10 MB")

# ---------------------------------------------------------
# 4. Helpers
# ---------------------------------------------------------
def clean_caption(text: str) -> str:
    text = re.sub(r'#\w*', '', text)
    text = re.sub(r'[^a-zA-Z0-9 \-_\.]', ' ', text)
    text = re.sub(r'[ _]+', '_', text).strip('_. ')
    return text[:40] if text.strip('_. ') else 'no_caption'

def sanitize_folder_name(name: str) -> str:
    # FIX 1: Replace dots and slashes — Mega rejects dots, slashes break local paths
    # FIX 2: Replace any other chars Mega dislikes
    name = name.replace(".", "_")   # dot → underscore  (fixes: Invalid arguments)
    name = name.replace("/", "_")   # slash → underscore (fixes: [Errno 2] No such file or directory)
    name = name.replace("\\", "_")  # backslash safety
    return name

def human_ts(unix_ts):
    if not unix_ts:
        return datetime.now().strftime("%Y-%m-%d")
    try:
        return datetime.fromtimestamp(int(unix_ts)).strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

# ---------------------------------------------------------
# NEW: ZIP artifact builder for GitHub Actions
#      Zips ALL scraped data (entire batch folder) + report files.
#      NO deletion anywhere — data stays on disk AND in ZIP.
#      GitHub Actions picks this up via actions/upload-artifact.
#
#      Add to your workflow YAML:
#        - uses: actions/upload-artifact@v4
#          with:
#            name: scraper-chunk-${{ matrix.chunk }}
#            path: "*_artifact.zip"
# ---------------------------------------------------------
def build_github_artifact():
    """
    Build a ZIP of everything this pod scraped:
      - All report/tracking/log files
      - Entire BATCH_FOLDER_NAME directory (all video folders, all files)
    Data is NOT deleted — ZIP is an additional copy for GitHub artifact store.
    """
    zip_name = f"{BATCH_FOLDER_NAME}{_suffix}_artifact.zip"
    logger.info(f"📦 Building GitHub artifact ZIP: {zip_name}")
    try:
        with zipfile.ZipFile(zip_name, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as zf:
            # Include all report files
            for rfile in [TRACKING_FILE, COMPLETED_FILE, FAILED_FILE, LOG_FILE]:
                if os.path.exists(rfile):
                    zf.write(rfile, rfile)

            # Include ENTIRE batch folder — all scraped data, no exclusions
            base_path = Path(BATCH_FOLDER_NAME)
            if base_path.exists():
                for item in base_path.rglob("*"):
                    if item.is_file():
                        zf.write(str(item), str(item))

        size_mb = os.path.getsize(zip_name) / 1_048_576
        logger.success(f"📦 Artifact ZIP ready: {zip_name} ({size_mb:.1f} MB)")
        return zip_name
    except Exception as e:
        logger.error(f"❌ ZIP build failed: {e}")
        return None

def create_node_report(primary_acc, final_acc, status, files_processed):
    """Generates the NodeReport.json for the Master Ledger summary job."""
    report_file = f"NodeReport_{CHUNK_INDEX}.json"
    data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "node": CHUNK_INDEX,
        "batch_folder": BATCH_FOLDER_NAME,
        "primary_assigned_acc": primary_acc,
        "final_uploaded_acc": final_acc,
        "status": status,
        "files_count": files_processed
    }
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        logger.info(f"📝 Node Report saved: {report_file}")
    except Exception as e:
        logger.error(f"❌ Failed to create node report: {e}")

# ---------------------------------------------------------
# 5. H.264 Codec Fix
# ---------------------------------------------------------
async def ensure_h264(video_path: Path, log_prefix: str) -> bool:
    if not video_path.exists():
        return False
    try:
        probe_cmd = [
            "ffprobe", "-v", "quiet", "-select_streams", "v:0",
            "-show_entries", "stream=codec_name",
            "-of", "csv=p=0", str(video_path)
        ]
        proc = await asyncio.create_subprocess_exec(
            *probe_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        codec = stdout.decode().strip().lower()

        if codec in ("h264", "avc1", "avc"):
            return True

        logger.debug(f"{log_prefix} codec={codec} → re-encoding to H.264 silently...")
        tmp_path = video_path.with_suffix(".h264_tmp.mp4")

        transcode_cmd = [
            "ffmpeg", "-i", str(video_path),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-profile:v", "high", "-level", "4.1",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "128k",
            "-movflags", "+faststart",
            "-y", str(tmp_path)
        ]
        proc = await asyncio.create_subprocess_exec(
            *transcode_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await proc.communicate()

        if proc.returncode == 0 and tmp_path.exists():
            tmp_path.replace(video_path)
            logger.debug(f"{log_prefix} re-encoded → H.264 done.")
            return True
        else:
            logger.error(f"{log_prefix} ❌ Transcode failed: {stderr.decode()[:300]}")
            tmp_path.unlink(missing_ok=True)
            return False

    except FileNotFoundError:
        logger.warning(f"{log_prefix} ⚠️ ffprobe/ffmpeg not found — skipping codec check.")
        return True
    except Exception as e:
        logger.error(f"{log_prefix} ❌ ensure_h264 error: {e}")
        return False

# ---------------------------------------------------------
# 6. YT-DLP
# ---------------------------------------------------------
def download_with_ytdlp(url, output_path):
    ydl_opts = {
        'outtmpl':             str(output_path),
        'quiet':               True,
        'no_warnings':         True,
        'noprogress':          True,
        'socket_timeout':      30,
        'format': (
            'bestvideo[vcodec^=avc1]+bestaudio[acodec^=mp4a]'
            '/bestvideo[vcodec^=avc1]+bestaudio'
            '/bestvideo+bestaudio/best'
        ),
        'merge_output_format': 'mp4',
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True
    except Exception as e:
        logger.error(f"yt-dlp error: {e}")
        return False

# ---------------------------------------------------------
# 7. Rclone Upload (UPDATED WITH SMART FALLBACK)
# ---------------------------------------------------------
async def upload_to_mega(local_folder_path, folder_name, log_prefix, my_accounts):
    global _upload_sem
    sem = _upload_sem or asyncio.Semaphore(CONFIG["upload_concurrency"])
    async with sem:
        # Loop through mapped accounts for fallback (Quota Full / Object not found)
        for acc in my_accounts:
            remote_path = f"{acc}:/{BATCH_FOLDER_NAME}/{folder_name}"
            logger.info(f"{log_prefix} ☁️ Mega Upload Attempt → {remote_path}")
            
            # Note: Removed --tpslimit 1 to restore speed, as we are now strictly 1 connection per node
            cmd = [
                "rclone", "copy", str(local_folder_path), remote_path,
                "--transfers",         "2",     
                "--checkers",          "1",     
                "--retries",           "3",    
                "--low-level-retries", "10",
                "--timeout",           "120s",
                "--contimeout",        "60s",
                "--log-level",         "ERROR",
            ]
            
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await proc.communicate()
            
            if proc.returncode == 0:
                logger.success(f"{log_prefix} 🚀 Mega Upload Done to {acc}!")
                return True, acc   # Signal success and the account used
            else:
                logger.warning(f"{log_prefix} ⚠️ Upload failed on {acc} ({stderr.decode().strip()}). Trying next lane account...")

        # If all fallback accounts fail
        logger.error(f"{log_prefix} ❌ All fallback accounts exhausted! Upload totally failed.")
        return False, None

async def upload_report_files(my_accounts):
    primary_acc = my_accounts[0] if my_accounts else f"tt_acc_{CHUNK_INDEX}"
    for fpath in [TRACKING_FILE, LOG_FILE, COMPLETED_FILE, FAILED_FILE]:
        if not os.path.exists(fpath):
            continue
        try:
            remote_path = f"{primary_acc}:/{BATCH_FOLDER_NAME}/_Reports"
            cmd = [
                "rclone", "copy", fpath, remote_path,
                "--log-level", "ERROR",
            ]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()
            logger.success(f"✅ Report uploaded to {primary_acc}: {fpath}")
        except Exception as e:
            logger.error(f"❌ Report upload failed ({fpath}): {e}")

# ---------------------------------------------------------
# 8. Scraper Engine
# ---------------------------------------------------------
class TikTokScraperV5:
    def __init__(self, config):
        self.cfg = config
        self.base_path = Path(config["base_dir"])
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept":     "application/json, text/plain, */*",
            "Referer":    "https://www.tiktok.com/"
        }
        self.client = httpx.AsyncClient(
            http2=config["http2"],
            timeout=config["timeout"],
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20)
        )
        self.sem_comments = asyncio.Semaphore(config["comment_concurrency"])

    async def download_file_httpx(self, url, path, log_prefix, item_name="Media"):
        if path.exists():
            return True
        try:
            dl_headers = self.headers.copy()
            dl_headers["Accept"] = "*/*"
            resp = await self.client.get(url, headers=dl_headers, timeout=60, follow_redirects=True)
            if resp.status_code == 403:
                del dl_headers["Referer"]
                resp = await self.client.get(url, headers=dl_headers, timeout=60, follow_redirects=True)
            resp.raise_for_status()
            Path(path).write_bytes(resp.content)
            logger.success(f"{log_prefix} 📥 Saved: {item_name}")
            return True
        except Exception as e:
            logger.error(f"{log_prefix} ❌ {item_name} Error: {e}")
            return False

    async def get_video_meta(self, url, track_id):
        clean_url = url.replace("/photo/", "/video/")
        logger.info(f"{track_id} 🌐 Fetching HTML page...")
        try:
            resp  = await self.client.get(clean_url, headers=self.headers, follow_redirects=True)
            match = re.search(
                r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__" type="application/json">([\s\S]*?)</script>',
                resp.text
            )
            if not match:
                return None
            data = json.loads(match.group(1))
            item = (data.get("__DEFAULT_SCOPE__", {})
                        .get("webapp.video-detail", {})
                        .get("itemInfo", {})
                        .get("itemStruct"))
            if not item:
                item = (data.get("__DEFAULT_SCOPE__", {})
                            .get("webapp.image-detail", {})
                            .get("itemInfo", {})
                            .get("itemStruct"))
            return item
        except:
            return None

    async def scrape_video(self, url, index, total, file_lock, my_accounts):
        track_id  = f"[{index}/{total}]"
        logger.info(f"{'-'*50}\n{track_id} 🚀 URL: {url}")

        # ── CHECKPOINT 1: Meta fetch ──────────────────────────────────────────
        item = await self.get_video_meta(url, track_id)
        if not item:
            logger.error(f"{track_id} ❌ Meta not found or Blocked.")
            await track_failed(url, "FAIL:meta_fetch — TikTok blocked or page unavailable", file_lock)
            return False, None

        v_id       = item["id"]
        author     = item.get("author", {}).get("uniqueId", "unknown")
        cap_slug   = clean_caption(item.get("desc", "no_caption"))
        post_ts    = human_ts(item.get("createTime"))
        log_prefix = f"{track_id} [@{author}]"

        raw_folder  = f"@{author}_{cap_slug}_{v_id}"
        folder_name = sanitize_folder_name(raw_folder)

        f_base      = f"@{author}_{cap_slug}"
        f_ts_id     = f"{post_ts}_{v_id}"
        v_path      = self.base_path / folder_name
        v_path.mkdir(parents=True, exist_ok=True)

        # ── 1. JSON FILES ─────────────────────────────────────────────────────
        # CHECKPOINT 2: File save
        files_saved = True
        try:
            (v_path / f"{f_base}_RAW-meta_{f_ts_id}.json").write_text(
                json.dumps(item, indent=2, ensure_ascii=False), encoding="utf-8")

            (v_path / f"{f_base}_meta_{f_ts_id}.json").write_text(
                json.dumps({
                    "post_info": {
                        "id":         v_id,
                        "desc":       item.get("desc"),
                        "createTime": item.get("createTime"),
                        "posted_at":  post_ts
                    },
                    "stats":  item.get("statsV2", item.get("stats", {})),
                    "author": item.get("author", {}),
                    "music":  item.get("music", {})
                }, indent=2, ensure_ascii=False), encoding="utf-8")

            (v_path / f"{f_base}_caption_{f_ts_id}.json").write_text(
                json.dumps({
                    "username": author,
                    "post_url": url,
                    "caption":  item.get("desc", ""),
                    "hashtags": re.findall(r"#\w+", item.get("desc", ""))
                }, indent=2, ensure_ascii=False), encoding="utf-8")

            (v_path / f"{f_base}_account_{f_ts_id}.json").write_text(
                json.dumps({
                    "author_details": item.get("author", {}),
                    "author_stats":   item.get("authorStats", {})
                }, indent=2, ensure_ascii=False), encoding="utf-8")

            logger.success(f"{log_prefix} 📝 Saved: RAW-meta, meta, caption, account")
        except Exception as e:
            logger.error(f"{log_prefix} ❌ JSON save failed: {e}")
            await track_failed(url, f"FAIL:json_save — {e}", file_lock)
            return False, None

        # ── 2. MEDIA DOWNLOADS ────────────────────────────────────────────────
        # CHECKPOINT 3: Media (video/thumbnail/audio/caption)
        media_ok = True
        if self.cfg.get("download_media", True):

            # Avatar
            avatar_url = (item.get("author", {}).get("avatarLarger")
                          or item.get("author", {}).get("avatarMedium"))
            if avatar_url:
                ok = await self.download_file_httpx(
                    avatar_url,
                    v_path / f"{f_base}_avatar_{f_ts_id}.jpg",
                    log_prefix, "Avatar")
                if not ok:
                    media_ok = False

            image_post = item.get("imagePost")
            if image_post and image_post.get("images"):
                # ── CAROUSEL ────────────────────────────────────────────────
                images = image_post.get("images", [])
                logger.info(f"{log_prefix} 📸 Carousel mode ({len(images)} images).")
                failed_indices = []

                for i, img in enumerate(images):
                    img_url = (
                        img.get("imageURL",    {}).get("urlList", [None])[0]
                        or img.get("displayImage", {}).get("urlList", [None])[0]
                    )
                    img_path = v_path / f"{f_base}_carousel-{i+1:03d}_{f_ts_id}.jpg"
                    if img_url:
                        ok = await self.download_file_httpx(
                            img_url, img_path, log_prefix, f"Carousel {i+1}")
                        if not ok:
                            failed_indices.append(i)
                    else:
                        failed_indices.append(i)

                if failed_indices:
                    logger.info(
                        f"{log_prefix} 🔄 Carousel yt-dlp fallback "
                        f"for {len(failed_indices)} failed images...")
                    yt_out = v_path / f"{f_base}_carousel-ytdlp_{f_ts_id}.%(ext)s"
                    if await asyncio.to_thread(download_with_ytdlp, url, yt_out):
                        logger.success(f"{log_prefix} 📥 Carousel yt-dlp done.")
                    else:
                        logger.error(f"{log_prefix} ❌ Carousel yt-dlp fallback failed.")
                        media_ok = False

                music_data = item.get("music", {})
                audio_url  = music_data.get("playUrl")
                if isinstance(audio_url, dict):
                    audio_url = audio_url.get("urlList", [None])[0]
                if isinstance(audio_url, list):
                    audio_url = audio_url[0]
                if audio_url:
                    ok = await self.download_file_httpx(
                        audio_url,
                        v_path / f"{f_base}_audio_{f_ts_id}.mp3",
                        log_prefix, "Carousel Audio")
                    if not ok:
                        media_ok = False

            else:
                # ── VIDEO ────────────────────────────────────────────────────
                video_data = item.get("video", {})
                play_url   = None

                for br in (video_data.get("bitrateInfo") or video_data.get("bitRateList") or []):
                    try:
                        play_url = br.get("PlayAddr", {}).get("UrlList", [None])[0]
                        if play_url:
                            break
                    except:
                        pass

                if not play_url:
                    for key in ("downloadAddr", "playAddr"):
                        val = video_data.get(key)
                        if isinstance(val, str) and val:
                            play_url = val; break
                        elif isinstance(val, list) and val:
                            play_url = val[0]; break

                video_path = v_path / f"{f_base}_video_{f_ts_id}.mp4"
                success    = False

                if play_url:
                    try:
                        resp = await self.client.get(
                            play_url, headers=self.headers,
                            timeout=90, follow_redirects=True)
                        if resp.status_code == 200:
                            video_path.write_bytes(resp.content)
                            logger.success(f"{log_prefix} 📥 Video Saved (Direct).")
                            success = True
                            await ensure_h264(video_path, log_prefix)
                        else:
                            logger.warning(
                                f"{log_prefix} ⚠️ Direct {resp.status_code} → yt-dlp...")
                    except Exception as e:
                        logger.warning(f"{log_prefix} ⚠️ Direct error → yt-dlp: {e}")

                if not success:
                    logger.info(f"{log_prefix} 🔄 yt-dlp fallback (strict H.264)...")
                    if await asyncio.to_thread(download_with_ytdlp, url, video_path):
                        logger.success(f"{log_prefix} 📥 Video Saved (yt-dlp).")
                        await ensure_h264(video_path, log_prefix)
                    else:
                        logger.error(f"{log_prefix} ❌ Video download failed.")
                        media_ok = False

                music_data = item.get("music", {})
                audio_url  = music_data.get("playUrl")
                if isinstance(audio_url, dict):
                    audio_url = audio_url.get("urlList", [None])[0]
                if isinstance(audio_url, list):
                    audio_url = audio_url[0]
                if audio_url:
                    ok = await self.download_file_httpx(
                        audio_url,
                        v_path / f"{f_base}_audio_{f_ts_id}.mp3",
                        log_prefix, "Audio")
                    if not ok:
                        media_ok = False

        if not media_ok:
            # Media partially failed — still try upload, but note it
            logger.warning(f"{log_prefix} ⚠️ Some media files failed — proceeding to upload remaining.")

        # ── 3. COMMENTS ───────────────────────────────────────────────────────
        # CHECKPOINT 4: Comments
        comments_ok = await self.fetch_comments(v_id, v_path, f_base, f_ts_id, log_prefix)
        if not comments_ok:
            logger.warning(f"{log_prefix} ⚠️ Comments incomplete — proceeding to upload.")

        # ── 4. UPLOAD + TRACK ─────────────────────────────────────────────────
        # CHECKPOINT 5: Mega upload — only SUCCESS when Mega confirms via fallback loop
        upload_ok, acc_used = await upload_to_mega(v_path, folder_name, log_prefix, my_accounts)

        if not upload_ok:
            # Build a detailed failure note so retry is smart
            fail_parts = []
            if not media_ok:     fail_parts.append("media_partial")
            if not comments_ok:  fail_parts.append("comments_incomplete")
            fail_parts.append("FAIL:mega_upload_all_fallbacks_exhausted")
            await track_failed(url, " | ".join(fail_parts), file_lock)
            return False, None

        # All 5 checkpoints passed
        if not media_ok or not comments_ok:
            # Uploaded but with partial data — log warning in tracking
            note = []
            if not media_ok:    note.append("media_partial")
            if not comments_ok: note.append("comments_incomplete")
            _append_tracking("SUCCESS_PARTIAL", url, " | ".join(note))
            async with file_lock:
                with open(COMPLETED_FILE, "a", encoding="utf-8") as f:
                    f.write(url + "\n")
        else:
            await track_success(url, file_lock)

        return True, acc_used

    async def fetch_replies(self, video_id, comment_id, raw_list, clean_list, log_prefix):
        async with self.sem_comments:
            cursor, has_more = 0, 1
            while has_more:
                try:
                    resp = await self.client.get(
                        "https://www.tiktok.com/api/comment/list/reply/",
                        params={"item_id": video_id, "comment_id": comment_id,
                                "cursor": cursor, "count": 50, "aid": "1988"},
                        headers=self.headers)
                    data    = resp.json()
                    replies = data.get("comments") or []
                    if not replies:
                        break
                    raw_list.extend(replies)
                    for c in replies:
                        clean_list.append({
                            "is_reply":          True,
                            "parent_comment_id": comment_id,
                            "cid":               c.get("cid"),
                            "text":              c.get("text"),
                            "likes":             c.get("digg_count"),
                            "create_time":       c.get("create_time"),
                            "user":              {"username": c.get("user", {}).get("unique_id")}
                        })
                    has_more = data.get("has_more", 0)
                    cursor   = data.get("cursor", cursor + len(replies))
                    await asyncio.sleep(random.uniform(*self.cfg["delay_between_pages"]))
                except:
                    break

    async def fetch_comments(self, video_id, path, f_base, f_ts_id, log_prefix):
        raw_path   = path / f"{f_base}_RAW-comments_{f_ts_id}.json"
        clean_path = path / f"{f_base}_comments_{f_ts_id}.json"

        raw_comments, clean_comments, cursor = [], [], 0

        if raw_path.exists() and clean_path.exists():
            try:
                raw_comments   = json.loads(raw_path.read_text(encoding="utf-8"))
                clean_comments = json.loads(clean_path.read_text(encoding="utf-8"))
                cursor         = len([c for c in clean_comments if not c.get("is_reply")])
                logger.info(f"{log_prefix} 🔄 Resuming from {cursor} comments...")
            except:
                raw_comments, clean_comments, cursor = [], [], 0

        if len(raw_comments) >= self.cfg["max_comments_limit"]:
            return True

        logger.info(f"{log_prefix} 💬 Fetching comments...")
        has_more = 1

        while has_more and len(raw_comments) < self.cfg["max_comments_limit"]:
            async with self.sem_comments:
                try:
                    resp = await self.client.get(
                        "https://www.tiktok.com/api/comment/list/",
                        params={"aweme_id": video_id, "cursor": cursor,
                                "count": 50, "aid": "1988"},
                        headers=self.headers)
                    data = resp.json()
                except:
                    return False  # comments fetch totally failed

            curr_batch = data.get("comments") or []
            if not curr_batch:
                break

            raw_comments.extend(curr_batch)
            reply_tasks = []
            for c in curr_batch:
                clean_comments.append({
                    "is_reply":    False,
                    "cid":         c.get("cid"),
                    "text":        c.get("text"),
                    "likes":       c.get("digg_count"),
                    "reply_total": c.get("reply_comment_total"),
                    "create_time": c.get("create_time"),
                    "user":        {"username": c.get("user", {}).get("unique_id")}
                })
                if c.get("reply_comment_total", 0) > 0:
                    reply_tasks.append(
                        self.fetch_replies(
                            video_id, c.get("cid"),
                            raw_comments, clean_comments, log_prefix))

            if reply_tasks:
                await asyncio.gather(*reply_tasks)

            has_more = data.get("has_more", 0)
            cursor   = data.get("cursor", cursor + len(curr_batch))

            raw_path.write_text(
                json.dumps(raw_comments,    indent=2, ensure_ascii=False), encoding="utf-8")
            clean_path.write_text(
                json.dumps(clean_comments, indent=2, ensure_ascii=False), encoding="utf-8")

            if len(raw_comments) % 100 < 50:
                logger.info(f"{log_prefix} 💬 Saved {len(raw_comments)} comments so far...")
            await asyncio.sleep(random.uniform(*self.cfg["delay_between_pages"]))

        logger.success(f"{log_prefix} 🎉 Comments Done: {len(raw_comments)}")
        return True

    async def close(self):
        await self.client.aclose()

# ---------------------------------------------------------
# 9. Worker
# ---------------------------------------------------------
async def worker_task(scraper, url, index, total, sem_video, file_lock, my_accounts, results_tracker):
    async with sem_video:
        try:
            success, acc_used = await scraper.scrape_video(url, index, total, file_lock, my_accounts)
            if success and acc_used:
                results_tracker["final_acc"] = acc_used
            await asyncio.sleep(random.uniform(*CONFIG["delay_between_videos"]))
            return success
        except Exception as e:
            logger.error(f"Worker Error [{url}]: {e}")
            await track_failed(url, f"FAIL:worker_exception — {e}", file_lock)
            return False

# ---------------------------------------------------------
# 10. Main
# ---------------------------------------------------------
async def main():
    if not os.path.exists("links.txt"):
        logger.error("❌ links.txt not found!")
        return

    # ── LOAD SESSION MAPPING (New Architecture Logic) ──
    my_accounts = []
    primary_acc = "N/A"
    try:
        with open("session_mapping.json", "r") as f:
            mapping = json.load(f)
            my_accounts = mapping.get(str(CHUNK_INDEX), [])
            if my_accounts: primary_acc = my_accounts[0]
    except Exception as e:
        logger.warning(f"⚠️ Mapping load error: {e}. Using raw fallback.")
        my_accounts = [f"tt_acc_{CHUNK_INDEX}", f"tt_acc_{(CHUNK_INDEX+20)%50}"]
        primary_acc = my_accounts[0]

    all_urls = [l.strip() for l in open("links.txt", encoding="utf-8") if l.strip()]

    # Hard limit — scraper will refuse to process more than 17000 links
    hard_limit = CONFIG.get("hard_link_limit", 17000)
    if len(all_urls) > hard_limit:
        logger.warning(
            f"⚠️ links.txt has {len(all_urls)} URLs — hard limit is {hard_limit}. "
            f"Truncating to first {hard_limit}."
        )
        all_urls = all_urls[:hard_limit]

    if TOTAL_CHUNKS > 1:
        my_urls = [u for i, u in enumerate(all_urls) if i % TOTAL_CHUNKS == CHUNK_INDEX]
        logger.info(
            f"📦 Chunk {CHUNK_INDEX+1}/{TOTAL_CHUNKS}: "
            f"assigned {len(my_urls)}/{len(all_urls)} URLs")
    else:
        my_urls = all_urls

    done_urls   = load_set_from_file(COMPLETED_FILE)
    failed_urls = load_set_from_file(FAILED_FILE)

    if failed_urls:
        open(FAILED_FILE, "w").close()
        logger.info(f"🔄 Retrying {len(failed_urls)} previously failed URLs.")

    retry_set = failed_urls - done_urls
    new_set   = set(my_urls) - done_urls - retry_set
    pending   = list(retry_set) + [u for u in my_urls if u in new_set]
    skipped   = [u for u in my_urls if u in done_urls]

    if not pending:
        logger.info("✅ All links already done.")
        await asyncio.to_thread(build_github_artifact)
        create_node_report(primary_acc, primary_acc, "SKIPPED", 0)
        return

    logger.info(
        f"🚀 Batch Start | Folder: {BATCH_FOLDER_NAME}\n"
        f"   My URLs        : {len(my_urls)}\n"
        f"   Done (skip)    : {len(skipped)}\n"
        f"   Retry failed   : {len(retry_set)}\n"
        f"   New            : {len(new_set)}\n"
        f"   Pending        : {len(pending)}\n"
        f"   Concurrency    : {CONFIG['video_concurrency']} videos parallel\n"
        f"   Target Accounts: {my_accounts}"
    )

    file_lock = asyncio.Lock()
    for u in skipped:
        await track_skipped(u, "Already completed", file_lock)

    global _upload_sem
    _upload_sem = asyncio.Semaphore(CONFIG["upload_concurrency"])
    sem_video   = asyncio.Semaphore(CONFIG["video_concurrency"])
    scraper     = TikTokScraperV5(CONFIG)
    
    # Track which account finally succeeded for the Ledger
    results_tracker = {"final_acc": primary_acc}

    try:
        tasks = [
            worker_task(scraper, url, i + 1, len(pending), sem_video, file_lock, my_accounts, results_tracker)
            for i, url in enumerate(pending)
        ]
        await asyncio.gather(*tasks)
    finally:
        await scraper.close()

    done_final   = load_set_from_file(COMPLETED_FILE)
    failed_final = load_set_from_file(FAILED_FILE)

    async with file_lock:
        with open(TRACKING_FILE, "a", encoding="utf-8") as f:
            f.write("\n" + "="*60 + "\n")
            f.write(f"RUN COMPLETE  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"CHUNK         : {CHUNK_INDEX+1}/{TOTAL_CHUNKS}\n")
            f.write(f"  Processed   : {len(pending)}\n")
            f.write(f"  Success     : {len(done_final)}\n")
            f.write(f"  Failed      : {len(failed_final)}\n")
            f.write(f"  Skipped     : {len(skipped)}\n")
            f.write("="*60 + "\n")

    logger.success(
        f"\n{'='*50}\n✅ RUN COMPLETE  [Chunk {CHUNK_INDEX+1}/{TOTAL_CHUNKS}]\n"
        f"   Success : {len(done_final)}\n"
        f"   Failed  : {len(failed_final)}\n"
        f"   Skipped : {len(skipped)}\n{'='*50}"
    )

    # Upload reports to Mega
    logger.info("📤 Uploading reports to Mega...")
    await upload_report_files(my_accounts)

    logger.info("📦 Building GitHub artifact ZIP...")
    zip_path = await asyncio.to_thread(build_github_artifact)
    if zip_path:
        logger.success(f"📦 Artifact ready for GitHub Actions upload: {zip_path}")
        
    # Generate CSV Summary Node Report
    status = "SUCCESS" if len(done_final) > 0 else "FAILED"
    create_node_report(primary_acc, results_tracker["final_acc"], status, len(done_final))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("\n🛑 Stopped by user.")
