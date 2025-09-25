from __future__ import annotations
import argparse
import json
import re
import subprocess
import shutil
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import whisper
import yt_dlp


def clean_filename(name: str) -> str:
    return re.sub(r'[\\/:"*?<>|]+', "_", str(name)).strip()


def is_valid_youtube_url(url: str) -> bool:
    return re.match(r"^https?://(www\.)?(youtube\.com|youtu\.be)/", url or "") is not None


def is_supported_video_url(url: str) -> bool:
    return any((url or "").lower().endswith(ext) for ext in [".mp4", ".m4a", ".webm"])


def run_ffmpeg_to_wav(input_src: str, wav_path: Path, ffmpeg_path: str) -> bool:
    cmd = [ffmpeg_path, "-y", "-i", input_src, "-ar", "16000", "-ac", "1", str(wav_path)]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError as e:
        print(f"ffmpeg 轉檔失敗：{e}")
        return False


def download_via_ytdlp_to_wav(url: str, tmp_dir: Path, out_wav: Path, ffmpeg_path: str) -> bool:
    outtmpl = str(tmp_dir / "%(id)s.%(ext)s")
    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": outtmpl,
        "quiet": True,
        "noprogress": True,
        "ffmpeg_location": ffmpeg_path,  # 讓 yt-dlp 後處理能找到 ffmpeg
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
        vid = info.get("id")
        ext = info.get("ext", "m4a")
        src = tmp_dir / f"{vid}.{ext}"
        if not src.exists():
            print(f"下載後找不到音檔：{src}")
            return False
        ok = run_ffmpeg_to_wav(str(src), out_wav, ffmpeg_path)
        try:
            src.unlink(missing_ok=True)
        except Exception:
            pass
        return ok
    except Exception as e:
        print(f"yt-dlp 下載失敗：{e}")
        return False


def download_mp3_to_wav(url: str, tmp_dir: Path, out_wav: Path, ffmpeg_path: str) -> bool:
    import requests, urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    mp3_tmp = tmp_dir / "tmp.mp3"
    try:
        with requests.get(url, stream=True, verify=False, timeout=60) as r:
            r.raise_for_status()
            with open(mp3_tmp, "wb") as f:
                for chunk in r.iter_content(1024 * 1024):
                    if chunk:
                        f.write(chunk)
        ok = run_ffmpeg_to_wav(str(mp3_tmp), out_wav, ffmpeg_path)
        try:
            mp3_tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return ok
    except Exception as e:
        print(f"下載 mp3 失敗：{e}")
        try:
            mp3_tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def direct_media_to_wav(url: str, out_wav: Path, ffmpeg_path: str) -> bool:
    return run_ffmpeg_to_wav(url, out_wav, ffmpeg_path)


def download_to_wav(url: str, tmp_dir: Path, out_wav: Path, ffmpeg_path: str) -> bool:
    if is_valid_youtube_url(url):
        print("來源：YouTube → yt-dlp + ffmpeg")
        return download_via_ytdlp_to_wav(url, tmp_dir, out_wav, ffmpeg_path)
    if is_supported_video_url(url):
        print("來源：直鏈 mp4/m4a/webm → ffmpeg")
        return direct_media_to_wav(url, out_wav, ffmpeg_path)
    if (url or "").lower().endswith(".mp3"):
        print("來源：直鏈 mp3 → 下載後 ffmpeg 轉檔")
        return download_mp3_to_wav(url, tmp_dir, out_wav, ffmpeg_path)
    print("來源：一般播放頁 → 嘗試 yt-dlp 解析")
    return download_via_ytdlp_to_wav(url, tmp_dir, out_wav, ffmpeg_path)


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    p = argparse.ArgumentParser(description="從 Excel 批量抓連結並輸出 ASR JSON（含時間戳、語言）")
    p.add_argument("-e", "--excel", required=True, help="Excel 檔案路徑")
    p.add_argument(
        "-s",
        "--sheet",
        default=0,
        type=lambda x: int(x) if str(x).isdigit() else x,
        help="Sheet 名稱或 0-based index (預設 0)",
    )
    p.add_argument("-o", "--outdir", default="./asr_segments", help="JSON 輸出根資料夾 (預設 ./asr_segments)")
    p.add_argument("--ffmpeg", default=None, help="ffmpeg 可執行檔路徑（未指定則使用系統 PATH）")
    p.add_argument("--model", default="large", help='Whisper 型號（如 "small", "medium", "large", "large-v3"）')
    args = p.parse_args()

    # 檢查/定位 ffmpeg
    ffmpeg_path = args.ffmpeg or shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError("系統找不到 ffmpeg，請安裝 ffmpeg 或透過 --ffmpeg 指定路徑")

    out_root = Path(args.outdir)
    audio_tmp = out_root / "audio_tmp"
    audio_tmp.mkdir(parents=True, exist_ok=True)
    out_root.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(args.excel, sheet_name=args.sheet)
    if "symbol" not in df.columns or "audio_link" not in df.columns:
        raise KeyError("找不到 symbol 或 audio_link 欄位！")

    print(f"載入 Whisper 模型（{args.model}；自動語言偵測）…")
    model = whisper.load_model(args.model)

    counters = defaultdict(int)
    total = len(df)

    for idx, row in df.iterrows():
        symbol = clean_filename(str(row["symbol"]).strip())
        url = str(row["audio_link"]).strip()

        if not url.lower().startswith("http"):
            print(f"[{idx+1}/{total}] 無效連結，跳過：{url}")
            continue

        counters[symbol] += 1
        tag = symbol if counters[symbol] == 1 else f"{symbol}_{counters[symbol]-1}"

        wav_path = audio_tmp / f"{tag}.wav"
        json_path = out_root / f"{tag}.json"

        print(f"[{idx+1}/{total}] 處理 {symbol} → {url}")

        try:
            ok = download_to_wav(url, audio_tmp, wav_path, ffmpeg_path)
            if not ok or not wav_path.exists():
                print("下載/轉檔失敗，跳過")
                continue

            print("Whisper 進行語音辨識（自動語言偵測）…")
            res = model.transcribe(str(wav_path), task="transcribe", language=None, verbose=False)

            lang = res.get("language", "unknown")
            segments = res.get("segments", []) or []
            duration = None
            try:
                duration = float(res.get("duration")) if res.get("duration") is not None else None
            except Exception:
                duration = None

            out_obj = {
                "symbol": symbol,
                "source_url": url,
                "language": lang,
                "duration_sec": duration,
                "created_at": now_iso(),
                "model": f"whisper-{args.model}",
                "segments": [
                    {
                        "i": int(s.get("id", i)),
                        "start": float(s.get("start", 0.0)),
                        "end": float(s.get("end", 0.0)),
                        "text": (s.get("text") or "").strip(),
                    }
                    for i, s in enumerate(segments)
                ],
                "full_text": (res.get("text") or "").strip(),
            }

            json_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"已儲存：{json_path}（語言：{lang}）")

        except Exception as e:
            print(f"第 {idx+1} 筆處理失敗：{e}")

    print("全部完成。JSON 目錄：", out_root.resolve())
    print("暫存音檔目錄：", audio_tmp.resolve(), "（可視需要刪除）")


if __name__ == "__main__":
    main()
    
"""
python -m earnings_call.VideoToText \
  --excel '/Users/fiiconan/Desktop/tw_stock/earnings_call/0925_test.xlsx' \
  --sheet '工作表1' \
  --outdir './asr_segments' \
  --model 'large-v3'
"""
