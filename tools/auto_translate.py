# tools/auto_translate.py
import os, sys, json, time, urllib.request, urllib.error, argparse, io, re
from babel.messages.pofile import read_po, write_po
# --- add/replace these near the top with your other regexes ---
import re

# exact placeholders in source strings
PY_TOKEN = re.compile(r'%\(([A-Za-z0-9_]+)\)s')
BRACE_TOKEN = re.compile(r'\{([A-Za-z0-9_]+)\}')

# compact legacy tokens: PH_0, __PH_0__, __PH_PY_1__, etc.
PH_SENTINEL_ANY = re.compile(r'(?:__|_)?PH(?:_[A-Z]+)?_(\d+)(?:__|_)?')

# robust "spaced" matcher: catches `_ _ PH _ PY _ 0 _ _` even with non-breaking spaces
WHITES = r'[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]'
SPACED_PH = re.compile(fr'(?:{WHITES}|_)*PH(?:{WHITES}|_)*(?:[A-Z]+)?(?:{WHITES}|_)*(\d+)(?:{WHITES}|_)*(?:{WHITES}|_)*')

def extract_placeholders(src: str):
    """
    Return (tokens, kind): tokens are the exact substrings to protect/restore.
    Prefer Python-style; fallback to brace style; otherwise empty list.
    """
    if not src:
        return [], "none"
    py = [m.group(0) for m in PY_TOKEN.finditer(src)]
    if py:
        return py, "py"
    br = [m.group(0) for m in BRACE_TOKEN.finditer(src)]
    if br:
        return br, "br"
    return [], "none"

def protect_placeholders(text: str):
    """
    Replace placeholders with __PH_i__ for safer MT.
    Returns (protected_text, original_placeholders_list).
    """
    tokens, _kind = extract_placeholders(text or "")
    if not tokens:
        return text, []
    out = text
    # Replace in first-occurrence order for deterministic mapping.
    for i, tok in enumerate(tokens):
        out = out.replace(tok, f"__PH_{i}__")
    return out, tokens

def restore_placeholders(translated: str, tokens):
    """Restore original placeholders by their index."""
    if not tokens or not translated:
        return translated
    out = translated
    for i, tok in enumerate(tokens):
        out = out.replace(f"__PH_{i}__", tok).replace(f"_PH_{i}__", tok).replace(f"PH_{i}", tok)
    return out

def repair_legacy_tokens(text: str, tokens):
    """
    Map junk PH tokens back to the real placeholders by index.
    If the source had no placeholders, just strip PH junk and
    leave the rest of the string unchanged (no global normalization).
    """
    if not text:
        return text

    out = text  # <-- do NOT normalize globally

    if tokens:
        # compact forms like "__PH_PY_0__", "_PH_0__", "PH_0"
        def repl_compact(m):
            try:
                j = int(m.group(1))
            except Exception:
                j = 0
            if not (0 <= j < len(tokens)):
                j = 0
            return tokens[j]

        out = PH_SENTINEL_ANY.sub(repl_compact, out)

        # spaced variants like "_ _ PH _ PY _ 0 _ _"
        def repl_spaced(m):
            try:
                j = int(m.group(1))
            except Exception:
                j = 0
            if not (0 <= j < len(tokens)):
                j = 0
            return tokens[j]

        out = SPACED_PH.sub(repl_spaced, out)
        return out

    # Source had no placeholders — remove PH junk only; keep spaces intact
    out = PH_SENTINEL_ANY.sub('', out)
    out = SPACED_PH.sub('', out)
    return out

# --- Robust PO writer ---------------------------------------------------------

def write_po_robust(path, catalog, width=80):
    """
    Some Babel versions write str, others bytes. Try text first; fall back to binary.
    """
    try:
        with open(path, "w", encoding="utf-8", newline="") as f:
            write_po(f, catalog, width=width)
    except TypeError:
        with open(path, "wb") as f:
            write_po(f, catalog, width=width)

# --- LibreTranslate client ----------------------------------------------------

def _lt_post(url, payload, timeout=60):
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )
    return urllib.request.urlopen(req, timeout=timeout)

def lt_translate_single(endpoint_base: str, text: str, target: str, source: str):
    url = endpoint_base.rstrip("/") + "/translate"
    payload = {"q": text, "source": source, "target": target, "format": "text"}
    for attempt in range(5):
        try:
            with _lt_post(url, payload) as resp:
                data = json.load(resp)
                if isinstance(data, dict) and "translatedText" in data:
                    return data["translatedText"]
                if isinstance(data, list):
                    if not data:
                        return ""
                    if isinstance(data[0], dict) and "translatedText" in data[0]:
                        return data[0]["translatedText"]
                    if isinstance(data[0], str):
                        return data[0]
                return ""
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(1.3 * (attempt + 1))
                continue
            raise
        except urllib.error.URLError:
            time.sleep(1.3 * (attempt + 1))
    return ""

def lt_translate_batch(endpoint_base: str, texts, target: str, source: str):
    if not texts:
        return []
    url = endpoint_base.rstrip("/") + "/translate"
    payload = {"q": texts, "source": source, "target": target, "format": "text"}
    try:
        with _lt_post(url, payload) as resp:
            data = json.load(resp)
    except Exception:
        return [lt_translate_single(endpoint_base, t, target, source) for t in texts]

    out = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "translatedText" in item:
                out.append(item["translatedText"])
            elif isinstance(item, str):
                out.append(item)
            else:
                out.append("")
    elif isinstance(data, dict) and "translatedText" in data and len(texts) == 1:
        out = [data["translatedText"]]
    else:
        return [lt_translate_single(endpoint_base, t, target, source) for t in texts]

    if len(out) != len(texts):
        return [lt_translate_single(endpoint_base, t, target, source) for t in texts]
    return out

# --- Core translate/repair logic ---------------------------------------------

def translate_catalog(po_path, endpoint, target_lang, source_lang="en",
                      batch_size=50, verbose=False, repair_only=False):
    with io.open(po_path, "r", encoding="utf-8") as f:
        catalog = read_po(f, locale=target_lang)

    changed = 0
    repaired = 0

    # Pass 1: repair legacy PH tokens in already-filled entries
    for msg in list(catalog):
        if not msg.id:
            continue

        if msg.pluralizable and isinstance(msg.string, dict):
            nm = dict(msg.string)
            for k, v in list(nm.items()):
                if not isinstance(v, str) or not v:
                    continue
                # pick singular source for index 0, plural for others
                src = msg.id[0] if k == 0 else msg.id[1]
                tokens, _ = extract_placeholders(src)
                fixed = repair_legacy_tokens(v, tokens)
                if fixed != v:
                    nm[k] = fixed
                    repaired += 1
            msg.string = nm
        else:
            if isinstance(msg.string, str) and msg.string:
                src = msg.id if isinstance(msg.id, str) else msg.id[0]
                tokens, _ = extract_placeholders(src)
                fixed = repair_legacy_tokens(msg.string, tokens)
                if fixed != msg.string:
                    msg.string = fixed
                    repaired += 1

    if repair_only:
        if changed or repaired:
            write_po_robust(po_path, catalog, width=80)
        return changed, repaired

    # Pass 2: translate empty entries
    tasks = []   # (msg, part, protected_text, tokens)
    for msg in list(catalog):
        if not msg.id:
            continue

        if msg.pluralizable:
            has_any = False
            if isinstance(msg.string, dict):
                has_any = any(bool(v) for v in msg.string.values())
            if has_any:
                continue
            s_sing, s_plu = msg.id
            p_sing, t_sing = protect_placeholders(s_sing)
            p_plu,  t_plu  = protect_placeholders(s_plu)
            tasks.append((msg, "sing", p_sing, t_sing))
            tasks.append((msg, "plur", p_plu,  t_plu))
        else:
            if msg.string:
                continue
            src = msg.id if isinstance(msg.id, str) else msg.id[0]
            p_src, toks = protect_placeholders(src)
            tasks.append((msg, "singular", p_src, toks))

    i = 0
    while i < len(tasks):
        batch = tasks[i:i+batch_size]
        texts = [t[2] for t in batch]
        if verbose:
            print(f"[{target_lang}] batch {i}..{i+len(batch)-1}")
        outs = lt_translate_batch(endpoint, texts, target_lang, source_lang)

        if len(outs) != len(batch):
            if verbose:
                print(f"[{target_lang}] WARN: batch mismatch; retrying per-item")
            outs = []
            for (_, _, ptxt, _tok) in batch:
                outs.append(lt_translate_single(endpoint, ptxt, target_lang, source_lang))

        for (m, part, ptxt, toks), tr in zip(batch, outs):
            tr = tr or ""
            tr = restore_placeholders(tr, toks)
            # Final safety: if any legacy tokens slipped, repair again
            src = m.id if isinstance(m.id, str) else (m.id[0] if part in ("sing", "singular") else m.id[1])
            tr = repair_legacy_tokens(tr, extract_placeholders(src)[0])

            if m.pluralizable:
                if not m.string or not isinstance(m.string, dict):
                    m.string = {}
                if part == "sing":
                    m.string[0] = tr
                else:
                    for k in range(1, 6):
                        m.string[k] = tr
                changed += 1
            else:
                m.string = tr
                changed += 1

        i += batch_size

    if changed or repaired:
        write_po_robust(po_path, catalog, width=80)
    return changed, repaired

# --- CLI ---------------------------------------------------------------------

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--trans-dir",
        default="echorepo/translations",
        help="translations dir with <lang>/LC_MESSAGES/messages.po",
    )
    ap.add_argument(
        "--langs",
        nargs="+",
        help="space-separated lang codes; if omitted, auto-detect from --trans-dir",
    )
    ap.add_argument(
        "--endpoint",
        # default to service name in docker compose
        default=os.environ.get("LT_URL", "http://libretranslate:5000"),
        help="LibreTranslate base URL (no trailing /translate)",
    )
    ap.add_argument("--source", default="en", help="source language code")
    ap.add_argument("--batch", type=int, default=60, help="batch size")
    ap.add_argument("--verbose", action="store_true", help="verbose logs")
    ap.add_argument(
        "--repair-only",
        action="store_true",
        help="only repair legacy PH tokens; do not call MT",
    )
    args = ap.parse_args()

    # 1) auto-detect langs if user didn't pass --langs
    langs = args.langs
    if not langs:
        langs = []
        # NOTE: this was the line with the typo
        for name in os.listdir(args.trans_dir):
            lang_dir = os.path.join(args.trans_dir, name)
            po_path = os.path.join(lang_dir, "LC_MESSAGES", "messages.po")
            if os.path.isfile(po_path):
                langs.append(name)
        langs.sort()

    total_changed = 0
    total_repaired = 0

    for lang in langs:
        po = os.path.join(args.trans_dir, lang, "LC_MESSAGES", "messages.po")
        if not os.path.isfile(po):
            print(f"[{lang}] missing: {po} — skipping")
            continue

        print(
            f"[{lang}] translating empty entries via {args.endpoint} …"
            if not args.repair_only
            else f"[{lang}] repairing placeholder tokens …"
        )

        ch, rep = translate_catalog(
            po,
            args.endpoint,
            lang,
            args.source,
            args.batch,
            args.verbose,
            args.repair_only,
        )
        print(f"[{lang}] filled {ch} entries; repaired {rep} existing")
        total_changed += ch
        total_repaired += rep

    print(f"ALL DONE — total filled: {total_changed}; repaired: {total_repaired}")
    if total_changed == 0 and total_repaired == 0:
        print("Note: If zero, verify LT_URL, languages, and that msgids exist/are empty.")
        
if __name__ == "__main__":
    main()
