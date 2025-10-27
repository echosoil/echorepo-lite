#!/usr/bin/env python3
import os, re, argparse, subprocess, sys, requests, polib

LANGS = ["cs","nl","fi","fr","de","el","it","pl","pt","ro","sk","es"]

LT_LANG = {  # LibreTranslate codes (source assumed English)
    "cs":"cs","nl":"nl","fi":"fi","fr":"fr","de":"de","el":"el",
    "it":"it","pl":"pl","pt":"pt","ro":"ro","sk":"sk","es":"es",
}

DO_NOT_TRANSLATE = ["ECHOrepo", "Keycloak", "QR"]

PH_PATTERNS = [
    re.compile(r'%\([^)]+\)s'),   # %(name)s
    re.compile(r'%[sdif]'),       # %s %d %i %f
    re.compile(r'\{[^}]+\}'),     # {n} {km}
]

def shell(*args):
    print("+", " ".join(args), flush=True)
    subprocess.check_call(args)

def mask_placeholders(text):
    mapping, out, idx = {}, text, 0
    for pat in PH_PATTERNS:
        for m in pat.finditer(text):
            ph = m.group(0)
            if ph in mapping.values():  # already masked
                continue
            key = f"__PH_{idx}__"
            mapping[key] = ph
            out = out.replace(ph, key)
            idx += 1
    return out, mapping

def mask_tokens(text, tokens):
    mapping, out = {}, text
    for i, tok in enumerate(tokens):
        if tok in out:
            key = f"__KEEP_{i}__"
            mapping[key] = tok
            out = out.replace(tok, key)
    return out, mapping

def unmask(text, mapping):
    for k, v in mapping.items():
        text = text.replace(k, v)
    return text

def translate_libre(text, target_lang, endpoint):
    r = requests.post(f"{endpoint}/translate", json={
        "q": text,
        "source": "en",
        "target": target_lang,
        "format": "text",
    }, timeout=60)
    r.raise_for_status()
    return r.json()["translatedText"]

def safe_translate(source, lang, endpoint):
    masked, ph_map = mask_placeholders(source)
    masked, keep_map = mask_tokens(masked, DO_NOT_TRANSLATE)
    translated = translate_libre(masked, LT_LANG[lang], endpoint)
    translated = unmask(translated, keep_map)
    translated = unmask(translated, ph_map)
    return translated

def ensure_po(pot, trans_dir, lang):
    po = os.path.join(trans_dir, lang, "LC_MESSAGES", "messages.po")
    if not os.path.exists(po):
        os.makedirs(os.path.dirname(po), exist_ok=True)
        shell("pybabel", "init", "-i", pot, "-d", trans_dir, "-l", lang)
    else:
        shell("pybabel", "update", "-i", pot, "-d", trans_dir, "-l", lang)
    return po

def process_entry(entry, lang, endpoint):
    if entry.obsolete:
        return False
    if entry.msgid_plural:
        changed = False
        for idx_key in sorted(entry.msgstr_plural.keys(), key=int):
            if entry.msgstr_plural[idx_key]:
                continue
            src = entry.msgid if idx_key == "0" else entry.msgid_plural
            entry.msgstr_plural[idx_key] = safe_translate(src, lang, endpoint)
            changed = True
        if "fuzzy" in entry.flags:
            entry.flags.remove("fuzzy")
        return changed
    else:
        if entry.msgstr:
            return False
        entry.msgstr = safe_translate(entry.msgid, lang, endpoint)
        if "fuzzy" in entry.flags:
            entry.flags.remove("fuzzy")
        return True

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pot", default="messages.pot")
    ap.add_argument("--trans-dir", default="echorepo/translations")
    ap.add_argument("--langs", nargs="*", default=LANGS)
    ap.add_argument("--endpoint", default=os.environ.get("LT_ENDPOINT","http://localhost:5000"))
    args = ap.parse_args()

    if not os.path.exists(args.pot):
        print(f"ERROR: {args.pot} not found", file=sys.stderr)
        sys.exit(2)

    pos = []
    for lang in args.langs:
        if lang not in LT_LANG:
            print(f"Skip {lang}: not supported mapping", file=sys.stderr)
            continue
        po_path = ensure_po(args.pot, args.trans_dir, lang)
        pos.append((lang, po_path))

    for lang, po_path in pos:
        po = polib.pofile(po_path)
        changed = False
        for e in po:
            changed |= process_entry(e, lang, args.endpoint)
        if changed:
            po.save(po_path)
            print(f"Saved {po_path}")
        else:
            print(f"No changes for {po_path}")

    shell("pybabel", "compile", "-d", args.trans_dir)

if __name__ == "__main__":
    main()
