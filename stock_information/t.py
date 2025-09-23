from typing import Optional, Dict
try:
    from opencc import OpenCC
    _cc = OpenCC('t2s')   # 繁 -> 簡
except Exception:
    _cc = None

def to_zh_cn(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    if _cc is None:
        # 沒安裝 opencc 時的降級
        return text  # 或者 return None
    return _cc.convert(text)

def to_en(text: Optional[str]) -> Optional[str]:
    """
    英文翻譯的占位：之後改成你自己的翻譯流程 / API。
    你也可以在主流程上游直接給英文字段，就不用翻譯。
    """
    return None

def build_multilang_name(zh_tw: Optional[str], en_from_field: Optional[str]) -> Dict[str, Optional[str]]:
    return {
        "zh_tw": zh_tw or None,
        "zh_cn": to_zh_cn(zh_tw) if zh_tw else None,
        "en": en_from_field or None
    }

def build_multilang_address(zh_tw: Optional[str], en_street: Optional[str], en_county: Optional[str]) -> Dict[str, Optional[str]]:
    en = " ".join([en_street or "", en_county or ""]).strip() or None
    return {
        "zh_tw": zh_tw or None,
        "zh_cn": to_zh_cn(zh_tw) if zh_tw else None,
        "en": en
    }

def build_multilang_description(zh_tw: Optional[str], en_prefill: Optional[str] = None) -> Dict[str, Optional[str]]:
    return {
        "zh_tw": zh_tw or None,
        "zh_cn": to_zh_cn(zh_tw) if zh_tw else None,
        "en": en_prefill or to_en(zh_tw)
    }