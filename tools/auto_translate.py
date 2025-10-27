#!/usr/bin/env python3
import os, re, argparse, subprocess, sys
import polib

# ---- CONFIG ----
LANGS = ["cs","nl","fi","fr","de","el","it","pl","pt","ro","sk","es"]

# DeepL uses uppercased codes; "pt-pt" for European Portuguese is "PT-PT"
DEEPL_LANG = {
    "cs":"CS","nl":"NL","fi":"FI","fr":"FR","de":"DE","el":"EL","it":"IT",
    "pl":"PL","pt":"PT-PT","ro":"RO","sk":"SK","es":"ES"
}

DO_NOT_TRANSLATE = ["ECHOrepo", "Keycloak", "QR"]  # keep as-is

PH_PATTERNS = [
    re.compile(r'%\([^)]+\)s'),      # Python named: %(n)s
    re.compile(r'%[sdif]'),          # %s %d %i %f
    re.compile(r'\{[^}]+\}'),        # {n} {km} {name}
]

def shell(*args):
    print("+", " ".join(args), flush=True)
    subprocess.check_call(args)

def mask_tokens(text, tokens, prefix="__KEEP__"):
    mapping = {}
    out = text
    for i, tok in enumerate(tokens):
        if tok in out:
            key = f"{prefix}{i}__"
            mapping[key] = tok
            out = out.replace(tok, key)
    return out, mapping

def mask_placeholders(text):
    mapping = {}
    out = text
    idx = 0
    for pat in PH_PATTERNS:
        for m in pat.finditer(text):
            ph = m.group(0)
            # avoid double-masking the same span
            if ph not in mapping.values():
                key = f"__PH_{idx}__"
                mapping[key] = ph
                out = out.replace(ph, key)
                idx += 1
    return out, mapping

def unmask(text, mapping):
    for k, v in mapping.items():
        text = text.replace(k, v)
    return text

# ---- Translators ----
def translate_deepl(text, target_lang):
    import deepl
    api_key = os.environ.get("DEEPL_API_KEY")
    if not api_key:
        raise RuntimeError("Set DEEPL_API_KEY in your environment")
    tr = deepl.Translator(api_key)
    # Preserve punctuation/whitespace as much as possible
    res = tr.translate_text(text, target_lang=target_lang, preserve_formatting=True)
    return res.text

def translate_text(text, lang, provider="deepl"):
    if provider == "deepl":
        return translate_deepl(text, DEEPL_LANG[lang])
    raise RuntimeError(f"Unknown provider: {provider}")

# ---- PO helpers ----
def ensure_po(pot, trans_dir, lang):
    po = os.path.join(trans_dir, lang, "LC_MESSAGES", "messages.po")
    if not os.path.exists(po):
        os.makedirs(os.path.dirname(po), exist_ok=True)
        shell("pybabel", "init", "-i", pot, "-d", trans_dir, "-l", lang)
    else:
        shell("pybabel", "update", "-i", pot, "-d", trans_dir, "-l", lang)
    return po

def process_entry(entry, lang, provider):
    # Skip already translated non-fuzzy entries
    if entry.obsolete:
        return False
    if entry.msgid_plural:
        changed = False
        for idx_key in sorted(entry.msgstr_plural.keys(), key=int):
            if entry.msgstr_plural[idx_key]:
                continue
            source = entry.msgid if idx_key == "0" else entry.msgid_plural
            translated = safe_translate(source, lang, provider)
            entry.msgstr_plural[idx_key] = translated
            changed = True
        # clear fuzzy flag
        if "fuzzy" in entry.flags:
            entry.flags.remove("fuzzy")
        return changed
    else:
        if entry.msgstr:
            return False
        translated = safe_translate(entry.msgid, lang, provider)
        entry.msgstr = translated
        if "fuzzy" in entry.flags:
            entry.flags.remove("fuzzy")
        return True

def safe_translate(source, lang, provider):
    # mask placeholders & protected tokens
    masked, ph_map = mask_placeholders(source)
    masked, keep_map = mask_tokens(masked, DO_NOT_TRANSLATE, prefix="__KEEP__")
    translated = translate_text(masked, lang, provider=provider)
    # unmask back
    translated = unmask(translated, keep_map)
    translated = unmask(translated, ph_map)
    return translated

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pot", default="messages.pot", help="Path to messages.pot")
    ap.add_argument("--trans-dir", default="echorepo/translations", help="Translations root")
    ap.add_argument("--langs", nargs="*", default=LANGS)
    ap.add_argument("--provider", default="deepl", choices=["deepl"])
    args = ap.parse_args()

    if not os.path.exists(args.pot):
        print(f"ERROR: {args.pot} not found", file=sys.stderr)
        sys.exit(2)

    # Ensure each PO exists and is updated
    pos = []
    for lang in args.langs:
        if args.provider == "deepl" and lang not in DEEPL_LANG:
            print(f"Skip {lang}: no provider mapping", file=sys.stderr)
            continue
        po_path = ensure_po(args.pot, args.trans_dir, lang)
        pos.append((lang, po_path))

    # Translate missing strings
    for lang, po_path in pos:
        po = polib.pofile(po_path)
        changed = False
        for e in po:
            changed |= process_entry(e, lang, args.provider)
        if changed:
            po.save(po_path)
            print(f"Saved {po_path}")
        else:
            print(f"No changes for {po_path}")

    # Compile all .mo
    shell("pybabel", "compile", "-d", args.trans_dir)

if __name__ == "__main__":
    main()
