import os, json, re, time
from typing import List
from openai import OpenAI, APIStatusError
from dotenv import load_dotenv

try:
    from opencc import OpenCC
    cc = OpenCC('s2twp')  # 簡→繁（臺灣）
except Exception:
    cc = None

# ===== 參數 =====
load_dotenv()
API_KEY = os.getenv("GROK_API")
if not API_KEY:
    raise RuntimeError("找不到環境變數 GROK_API，請先 export GROK_API='gsk_xxx'")

JSON_PATH = "/Users/fiiconan/Desktop/tw_stock/asr_segments/2317_2025Q1.json"
OUT_PATH  = "/Users/fiiconan/Desktop/tw_stock/asr_segments/2317_2025Q1_cleaned.txt"
CKPT_PATH = "/Users/fiiconan/Desktop/tw_stock/asr_segments/2317_2025Q1_progress.jsonl"

# 一次跑完（不續跑）
FRESH_RUN = True

# 模型：8B，並放大完成上限
MODEL = "llama-3.1-8b-instant"
MAX_CHARS_PER_CHUNK = 8000
MAX_OUTPUT_TOKENS   = 131072
SLEEP_BETWEEN_CALLS = 1.0

client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=API_KEY)

# ===== Prompt 強化：嚴禁多餘字樣 =====
SYS = (
    "你是台灣的逐字稿編輯助手，務必使用繁體中文（臺灣），全形標點。"
    "只做『段落化與補標點』，保留原句原意與詞序，不得摘要、不得改寫、不增刪資訊。"
    "嚴禁輸出與正文無關的任何語句、符號或標記：不得出現「分段如下」「第X段」「⚠️」「註：」「—」「#」「```」等。"
    "不得輸出標題、小標、清單符號、編號、程式碼區塊或多餘括號說明。"
)

USER_TMPL = (
    "⚠️ 全程使用繁體中文（臺灣），請使用全形中文標點（。、，；：？！「」）。\n"
    "任務：將以下逐字稿僅做『段落化與補標點』，保留原句原意與詞序，不得摘要、不得改寫內容。\n"
    "只允許移除『連續口頭禪或連續重複詞』（例：就是就是、那個那個、我們我們）。\n"
    "每段約 2–6 句，主題自然分段，不添加評論或小標，不加任何額外說明。\n\n{content}"
)

FILLERS = ["就是", "那個", "然後", "好", "嗯", "呃"]

# ===== 清理器：移除模型可能產生的奇怪文字 =====
CLEAN_PATTERNS = [
    r"^```+.*?$",                      # 開頭或整行的 code fence
    r"```+$",
    r"^#{1,6}\s.*?$",                  # Markdown 標題
    r"^-{2,}\s*$",                     # 分隔線
    r"^[>]\s?.*$",                     # 引言
    r"^\s*（?⚠️.*?）?\s*$",            # 警語
    r"^(?:第[一二三四五六七八九十百千0-9]+[段節章部篇]|段落如下)[:：]\s*",  # 標題化前綴
]

def sanitize_output(text: str) -> str:
    # 去掉常見包裝/警語/標題/代碼框
    lines = text.splitlines()
    kept = []
    for ln in lines:
        drop = False
        for pat in CLEAN_PATTERNS:
            if re.match(pat, ln.strip()):
                drop = True
                break
        if not drop:
            kept.append(ln)
    out = "\n".join(kept)

    # 移除殘留 code fence 與多餘符號
    out = re.sub(r"`{3,}.*?`{3,}", "", out, flags=re.S)
    out = re.sub(r"[#>`]{2,}", "", out)
    out = re.sub(r"\s+\Z", "", out)

    # 過度空白行壓縮
    out = re.sub(r"\n{3,}", "\n\n", out)

    # 最後再保險轉繁
    if cc:
        out = cc.convert(out)
    return out.strip()

def light_dedupe(s: str) -> str:
    s = re.sub(r'([^\s，。、！？；：]{1,4})\1{1,3}', r'\1', s)
    for w in FILLERS:
        s = re.sub(fr'(?:{w}){{2,}}', w, s)
    return s

def load_segments(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    joined = " ".join(seg["text"] for seg in data["segments"])
    if cc:
        joined = cc.convert(joined)
    joined = light_dedupe(joined)
    return joined

def chunk_by_chars(text: str, max_chars: int) -> List[str]:
    chunks, cur = [], 0
    while cur < len(text):
        end = min(cur + max_chars, len(text))
        slice_ = text[cur:end]
        cut = max(
            slice_.rfind("。"), slice_.rfind("！"), slice_.rfind("？"),
            slice_.rfind("\n"), slice_.rfind("，"), slice_.rfind(" ")
        )
        if cut < max_chars // 2:
            cut = len(slice_)
        chunk = slice_[:cut].strip()
        if chunk:
            chunks.append(chunk)
        cur += len(chunk)
    return chunks

def call_model(content: str, max_tokens: int) -> str:
    user = USER_TMPL.format(content=content)
    delay = 2
    for _ in range(6):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": SYS},
                    {"role": "user", "content": user},
                ],
                temperature=0.2,
                max_tokens=max_tokens,
                # 盡量避免模型包裝
                frequency_penalty=0.1,
                presence_penalty=0.0,
            )
            out = resp.choices[0].message.content or ""
            # 不要把「長度上限」警語寫進正文
            return sanitize_output(out)
        except APIStatusError as e:
            msg = str(e)
            if any(k in msg for k in ["rate_limit", "TPM", "Too Many Requests", "Request too large"]) \
               or (getattr(e, "status_code", None) and 500 <= int(e.status_code) < 600):
                time.sleep(delay); delay = min(delay * 2, 30); continue
            raise
        except Exception:
            time.sleep(delay); delay = min(delay * 2, 30); continue
    raise RuntimeError("重試多次仍失敗。")

def main():
    # 一次跑完：清理舊檔、不使用進度檔
    if FRESH_RUN:
        for p in (OUT_PATH, CKPT_PATH):
            try:
                if os.path.exists(p): os.remove(p)
            except Exception:
                pass

    full_text = load_segments(JSON_PATH)
    chunks = chunk_by_chars(full_text, MAX_CHARS_PER_CHUNK)
    print(f"總段數：{len(chunks)}（一次從頭跑到尾）")

    outputs = []
    for i, chunk in enumerate(chunks, 1):
        print(f"[{i}/{len(chunks)}] 呼叫模型…")
        out = call_model(chunk, MAX_OUTPUT_TOKENS)
        outputs.append(out)
        time.sleep(SLEEP_BETWEEN_CALLS)

    final_text = "\n\n".join(outputs).strip()
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(final_text)

    print(f"完成 ✅ 已輸出到：{OUT_PATH}")

if __name__ == "__main__":
    main()