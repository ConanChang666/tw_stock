# stock_information/translate_texts.py
from typing import Optional, Dict, List
import re, time
from functools import lru_cache

# 繁轉簡
try:
    from opencc import OpenCC
    _cc_t2s = OpenCC("t2s")
except Exception:
    _cc_t2s = None

def to_zh_cn(text: Optional[str]) -> Optional[str]:
    if not text: return None
    return _cc_t2s.convert(text) if _cc_t2s else text

MODEL = "HPLT/translate-zh_hant-en-v1.0-hplt_opus"
MAX_TOKENS = 800
MAX_LENGTH = 512
BATCH_SIZE = 32

@lru_cache(maxsize=1)
def _hf_tokenizer():
    print("[translate] loading tokenizer ...")
    from transformers import AutoTokenizer
    return AutoTokenizer.from_pretrained(MODEL)

@lru_cache(maxsize=1)
def _hf_pipeline():
    print("[translate] loading model (this can take a while on first run) ...")
    import torch
    from transformers import pipeline, AutoModelForSeq2SeqLM
    device = 0 if torch.backends.mps.is_available() else -1
    kw = {"torch_dtype": torch.float16} if device == 0 else {}
    tok = _hf_tokenizer()
    model = AutoModelForSeq2SeqLM.from_pretrained(MODEL, **kw)
    return pipeline("translation", model=model, tokenizer=tok, device=device)

def _smart_split(text: str) -> List[str]:
    if not text.strip(): return []
    tok = _hf_tokenizer()
    sents = re.split(r'(?<=[。．\.!?！？])\s*', text)
    sents = [s for s in sents if s.strip()]
    chunks, cur = [], []
    def flush():
        if cur:
            chunks.append(tok.decode(cur, skip_special_tokens=True))
            cur.clear()
    for s in sents:
        ids = tok.encode(s, add_special_tokens=False)
        if len(ids) > MAX_TOKENS:
            for i in range(0, len(ids), MAX_TOKENS):
                flush()
                chunks.append(tok.decode(ids[i:i+MAX_TOKENS], skip_special_tokens=True))
            continue
        if len(cur) + len(ids) <= MAX_TOKENS:
            cur.extend(ids)
            if len(cur) > int(MAX_TOKENS*0.9): flush()
        else:
            flush(); cur.extend(ids)
    flush()
    compact = []
    for part in chunks:
        ids = tok.encode(part, add_special_tokens=False)
        if compact:
            prev = compact[-1]
            pids = tok.encode(prev, add_special_tokens=False)
            if len(pids)+len(ids) <= MAX_TOKENS:
                compact[-1] = tok.decode(pids+ids, skip_special_tokens=True)
                continue
        compact.append(part)
    return compact

def to_en(text: Optional[str]) -> Optional[str]:
    if not text: return None
    pipe = _hf_pipeline()
    parts = _smart_split(text)
    out = []
    print(f"[translate] translating: {len(parts)} chunks")
    for i in range(0, len(parts), BATCH_SIZE):
        batch = parts[i:i+BATCH_SIZE]
        res = pipe(batch, max_length=MAX_LENGTH)
        out.extend([r["translation_text"] if isinstance(r, dict) else r for r in res])
    return " ".join(out).strip()

def build_multilang_name(zh_tw: Optional[str], en_from_field: Optional[str]) -> Dict[str, Optional[str]]:
    return {"zh_tw": zh_tw or None, "zh_cn": to_zh_cn(zh_tw) if zh_tw else None, "en": en_from_field or None}

def build_multilang_address(zh_tw: Optional[str], en_street: Optional[str], en_county: Optional[str]) -> Dict[str, Optional[str]]:
    en = " ".join([en_street or "", en_county or ""]).strip() or None
    return {"zh_tw": zh_tw or None, "zh_cn": to_zh_cn(zh_tw) if zh_tw else None, "en": en}

def build_multilang_description(zh_tw: Optional[str], en_prefill: Optional[str] = None) -> Dict[str, Optional[str]]:
    en_text = en_prefill or to_en(zh_tw)
    return {"zh_tw": zh_tw or None, "zh_cn": to_zh_cn(zh_tw) if zh_tw else None, "en": en_text}