# tools/auto_translate.py
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Iterable

from babel.messages.pofile import read_po, write_po


# ---------------------------------------------------------------------------
# Placeholder handling
# ---------------------------------------------------------------------------

PY_TOKEN = re.compile(r"%\(([A-Za-z0-9_]+)\)s")
BRACE_TOKEN = re.compile(r"\{([A-Za-z0-9_]+)\}")

# LibreTranslate occasionally mutates protected tokens, e.g.
# __PH_0__ -> _PH_0 / PH_0 or spaced variants.
PH_SENTINEL_ANY = re.compile(r"(?:__|_)?PH(?:_[A-Z]+)?_(\d+)(?:__|_)?")
WHITES = r"[\s\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]"
SPACED_PH = re.compile(
    rf"(?:{WHITES}|_)*PH(?:{WHITES}|_)*(?:[A-Z]+)?(?:{WHITES}|_)*"
    rf"(\d+)(?:{WHITES}|_)*(?:{WHITES}|_)*"
)


def _to_text(value) -> str:
    """Normalize an API response value into a string."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        if not value:
            return ""
        return _to_text(value[0])
    return str(value)


def extract_placeholders(src: str) -> list[str]:
    """
    Return placeholders in source order.

    Unlike the old implementation, this protects BOTH %(name)s and {name}
    if the same source string happens to contain both styles.
    """
    if not src:
        return []

    found: list[tuple[int, str]] = []
    for rx in (PY_TOKEN, BRACE_TOKEN):
        found.extend((m.start(), m.group(0)) for m in rx.finditer(src))
    found.sort(key=lambda x: x[0])
    return [token for _, token in found]


def protect_placeholders(text: str) -> tuple[str, list[str]]:
    tokens = extract_placeholders(text or "")
    if not tokens:
        return text, []

    out = text
    # Replace one occurrence at a time so repeated placeholders remain stable.
    for i, token in enumerate(tokens):
        out = out.replace(token, f"__PH_{i}__", 1)
    return out, tokens


def restore_placeholders(translated: str, tokens: list[str]) -> str:
    if not tokens:
        return _to_text(translated)

    out = _to_text(translated)
    for i, token in enumerate(tokens):
        for variant in (f"__PH_{i}__", f"_PH_{i}__", f"PH_{i}"):
            out = out.replace(variant, token)
    return out


def repair_legacy_tokens(text: str, tokens: list[str]) -> str:
    """
    Repair placeholder sentinels that were damaged by an older MT run.
    """
    out = _to_text(text)
    if not out:
        return out

    if tokens:
        def repl(m):
            try:
                idx = int(m.group(1))
            except Exception:
                idx = 0
            if not 0 <= idx < len(tokens):
                idx = 0
            return tokens[idx]

        out = PH_SENTINEL_ANY.sub(repl, out)
        out = SPACED_PH.sub(repl, out)
        return out

    # No placeholders existed in the source; remove stray old PH markers only.
    out = PH_SENTINEL_ANY.sub("", out)
    out = SPACED_PH.sub("", out)
    return out


# ---------------------------------------------------------------------------
# PO handling
# ---------------------------------------------------------------------------

def write_po_robust(path: str, catalog, width: int = 80) -> None:
    try:
        with open(path, "w", encoding="utf-8", newline="") as f:
            write_po(f, catalog, width=width)
    except TypeError:
        with open(path, "wb") as f:
            write_po(f, catalog, width=width)


def _plural_values(msg, num_plurals: int) -> list[str]:
    """
    Babel represents plural Message.string as a tuple, not a dict.
    Normalize it to a mutable list of exactly num_plurals items.
    """
    if isinstance(msg.string, (tuple, list)):
        values = [_to_text(v) for v in msg.string]
    elif isinstance(msg.string, str) and msg.string:
        values = [msg.string]
    else:
        values = []

    if len(values) < num_plurals:
        values.extend([""] * (num_plurals - len(values)))
    return values[:num_plurals]


def repair_existing_catalog(catalog) -> int:
    """Repair old placeholder damage in already translated entries."""
    repaired = 0
    num_plurals = max(1, int(getattr(catalog, "num_plurals", 2) or 2))

    for msg in list(catalog):
        if not msg.id:
            continue

        if msg.pluralizable:
            values = _plural_values(msg, num_plurals)
            source_singular, source_plural = msg.id
            changed = False

            for idx, old in enumerate(values):
                if not old:
                    continue
                src = source_singular if idx == 0 else source_plural
                fixed = repair_legacy_tokens(old, extract_placeholders(src))
                if fixed != old:
                    values[idx] = fixed
                    repaired += 1
                    changed = True

            if changed:
                msg.string = tuple(values)
        else:
            old = _to_text(msg.string)
            if not old:
                continue
            src = msg.id if isinstance(msg.id, str) else msg.id[0]
            fixed = repair_legacy_tokens(old, extract_placeholders(src))
            if fixed != old:
                msg.string = fixed
                repaired += 1

    return repaired


# ---------------------------------------------------------------------------
# LibreTranslate HTTP client
# ---------------------------------------------------------------------------

def _lt_post(url: str, payload: dict, timeout: int = 60):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    return urllib.request.urlopen(req, timeout=timeout)


def _http_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        return exc.read().decode("utf-8", errors="replace")
    except Exception:
        return ""


def get_supported_languages(endpoint_base: str) -> set[str] | None:
    url = endpoint_base.rstrip("/") + "/languages"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.load(resp)
    except Exception as exc:
        print(f"WARN: could not read LibreTranslate /languages: {exc}", file=sys.stderr)
        return None

    codes = set()
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and item.get("code"):
                codes.add(str(item["code"]))
    return codes or None


def lt_translate_single(
    endpoint_base: str,
    text: str,
    target: str,
    source: str,
    verbose: bool = False,
) -> str:
    url = endpoint_base.rstrip("/") + "/translate"
    payload = {"q": text, "source": source, "target": target, "format": "text"}

    for attempt in range(5):
        try:
            with _lt_post(url, payload) as resp:
                data = json.load(resp)

            if isinstance(data, dict) and "translatedText" in data:
                return _to_text(data["translatedText"])

            if isinstance(data, list):
                if not data:
                    return ""
                first = data[0]
                if isinstance(first, dict) and "translatedText" in first:
                    return _to_text(first["translatedText"])
                return _to_text(first)

            return ""

        except urllib.error.HTTPError as exc:
            body = _http_error_body(exc)
            if exc.code in (429, 500, 502, 503, 504):
                if verbose:
                    print(
                        f"WARN: LT HTTP {exc.code}; retry {attempt + 1}/5: {body}",
                        file=sys.stderr,
                    )
                time.sleep(1.3 * (attempt + 1))
                continue

            print(
                f"WARN: LT HTTP {exc.code} for {source}->{target}: {body}",
                file=sys.stderr,
            )
            return ""

        except (urllib.error.URLError, TimeoutError) as exc:
            if verbose:
                print(
                    f"WARN: LT connection error; retry {attempt + 1}/5: {exc}",
                    file=sys.stderr,
                )
            time.sleep(1.3 * (attempt + 1))

    return ""


def lt_translate_batch(
    endpoint_base: str,
    texts: list[str],
    target: str,
    source: str,
    verbose: bool = False,
) -> list[str]:
    if not texts:
        return []

    url = endpoint_base.rstrip("/") + "/translate"
    payload = {"q": texts, "source": source, "target": target, "format": "text"}

    try:
        with _lt_post(url, payload) as resp:
            data = json.load(resp)
    except Exception as exc:
        if verbose:
            print(f"[{target}] batch request failed ({exc}); using per-item calls")
        return [
            lt_translate_single(endpoint_base, text, target, source, verbose)
            for text in texts
        ]

    out: list[str] = []

    # Common LibreTranslate batch response:
    # {"translatedText": ["...", "..."]}
    if isinstance(data, dict) and "translatedText" in data:
        translated = data["translatedText"]
        if isinstance(translated, list):
            out = [_to_text(v) for v in translated]
        elif len(texts) == 1:
            out = [_to_text(translated)]

    # Some deployments return a list of strings/dicts instead.
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict) and "translatedText" in item:
                out.append(_to_text(item["translatedText"]))
            else:
                out.append(_to_text(item))

    if len(out) != len(texts):
        if verbose:
            print(
                f"[{target}] batch response length mismatch "
                f"({len(out)} != {len(texts)}); using per-item calls"
            )
        return [
            lt_translate_single(endpoint_base, text, target, source, verbose)
            for text in texts
        ]

    return out


# ---------------------------------------------------------------------------
# Translation logic
# ---------------------------------------------------------------------------

def translate_catalog(
    po_path: str,
    endpoint: str,
    target_lang: str,
    source_lang: str = "en",
    batch_size: int = 50,
    verbose: bool = False,
    repair_only: bool = False,
    force: bool = False,
) -> tuple[int, int, int]:
    with open(po_path, encoding="utf-8") as f:
        catalog = read_po(f, locale=target_lang)

    repaired = repair_existing_catalog(catalog)
    translated = 0
    failed = 0

    if repair_only:
        if repaired:
            write_po_robust(po_path, catalog)
        return translated, repaired, failed

    num_plurals = max(1, int(getattr(catalog, "num_plurals", 2) or 2))

    # Each task:
    # (message, source_part, protected_source, tokens, target_indexes, overwrite)
    tasks = []

    for msg in list(catalog):
        if not msg.id:
            continue

        is_fuzzy = "fuzzy" in getattr(msg, "flags", set())

        if msg.pluralizable:
            singular_src, plural_src = msg.id
            values = _plural_values(msg, num_plurals)

            singular_needed = force or is_fuzzy or not values[0]
            plural_indexes = list(range(1, num_plurals))
            plural_needed = (
                bool(plural_indexes)
                and (force or is_fuzzy or any(not values[i] for i in plural_indexes))
            )

            if singular_needed:
                protected, tokens = protect_placeholders(singular_src)
                tasks.append(
                    (msg, "singular", protected, tokens, [0], force or is_fuzzy)
                )

            if plural_needed:
                protected, tokens = protect_placeholders(plural_src)
                tasks.append(
                    (
                        msg,
                        "plural",
                        protected,
                        tokens,
                        plural_indexes,
                        force or is_fuzzy,
                    )
                )

        else:
            current = _to_text(msg.string)
            if force or is_fuzzy or not current:
                src = msg.id if isinstance(msg.id, str) else msg.id[0]
                protected, tokens = protect_placeholders(src)
                tasks.append(
                    (msg, "simple", protected, tokens, None, force or is_fuzzy)
                )

    if verbose:
        print(f"[{target_lang}] queued translation tasks: {len(tasks)}")

    # Track per-message task success so fuzzy is removed only when all queued
    # parts of that message translated successfully.
    expected_by_msg = defaultdict(int)
    success_by_msg = defaultdict(int)
    msg_by_id = {}

    for task in tasks:
        msg = task[0]
        key = id(msg)
        expected_by_msg[key] += 1
        msg_by_id[key] = msg

    i = 0
    while i < len(tasks):
        batch = tasks[i : i + batch_size]
        texts = [task[2] for task in batch]

        if verbose:
            print(f"[{target_lang}] batch {i}..{i + len(batch) - 1}")

        outs = lt_translate_batch(
            endpoint, texts, target_lang, source_lang, verbose=verbose
        )

        for task, raw_translation in zip(batch, outs):
            msg, part, _protected, tokens, target_indexes, overwrite = task

            tr = restore_placeholders(_to_text(raw_translation), tokens).strip()
            if not tr:
                failed += 1
                if verbose:
                    print(
                        f"[{target_lang}] WARN: no translation returned for: {msg.id!r}",
                        file=sys.stderr,
                    )
                continue

            src = (
                msg.id
                if isinstance(msg.id, str)
                else (msg.id[0] if part in ("singular", "simple") else msg.id[1])
            )
            tr = repair_legacy_tokens(tr, extract_placeholders(src))

            if msg.pluralizable:
                values = _plural_values(msg, num_plurals)
                assert target_indexes is not None
                for idx in target_indexes:
                    if overwrite or not values[idx]:
                        values[idx] = tr
                        translated += 1
                msg.string = tuple(values)
            else:
                msg.string = tr
                translated += 1

            success_by_msg[id(msg)] += 1

        i += batch_size

    # Only clear fuzzy when every required source part for the message succeeded.
    for key, expected in expected_by_msg.items():
        msg = msg_by_id[key]
        if success_by_msg[key] == expected and "fuzzy" in getattr(msg, "flags", set()):
            msg.flags.remove("fuzzy")

    if translated or repaired:
        write_po_robust(po_path, catalog)

    return translated, repaired, failed


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def discover_languages(trans_dir: str) -> list[str]:
    langs = []
    for name in os.listdir(trans_dir):
        po = os.path.join(trans_dir, name, "LC_MESSAGES", "messages.po")
        if os.path.isfile(po) and name != "en":
            langs.append(name)
    return sorted(langs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--trans-dir",
        default="echorepo/translations",
        help="translations dir with <lang>/LC_MESSAGES/messages.po",
    )
    ap.add_argument(
        "--langs",
        nargs="+",
        help="target language codes; if omitted, auto-detect from --trans-dir",
    )
    ap.add_argument(
        "--endpoint",
        default=os.environ.get("LT_URL", "http://127.0.0.1:5001"),
        help="LibreTranslate base URL (no trailing /translate)",
    )
    ap.add_argument("--source", default="en")
    ap.add_argument("--batch", type=int, default=60)
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument(
        "--repair-only",
        action="store_true",
        help="repair old placeholder damage without calling LibreTranslate",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing translations too (normally only empty/fuzzy entries are translated)",
    )
    args = ap.parse_args()

    if not os.path.isdir(args.trans_dir):
        print(f"ERROR: translation directory not found: {args.trans_dir}", file=sys.stderr)
        return 2

    langs = args.langs or discover_languages(args.trans_dir)
    if not langs:
        print("ERROR: no target language catalogues found", file=sys.stderr)
        return 2

    supported = None if args.repair_only else get_supported_languages(args.endpoint)

    total_translated = 0
    total_repaired = 0
    total_failed = 0
    skipped = []

    for lang in langs:
        po = os.path.join(args.trans_dir, lang, "LC_MESSAGES", "messages.po")
        if not os.path.isfile(po):
            print(f"[{lang}] missing: {po} — skipping")
            continue

        if supported is not None and lang not in supported:
            print(
                f"[{lang}] WARNING: target is not advertised by "
                f"{args.endpoint}/languages — skipping"
            )
            skipped.append(lang)
            continue

        if args.repair_only:
            print(f"[{lang}] repairing existing placeholder tokens …")
        else:
            mode = "all" if args.force else "empty/fuzzy"
            print(f"[{lang}] translating {mode} entries via {args.endpoint} …")

        translated, repaired, failed = translate_catalog(
            po_path=po,
            endpoint=args.endpoint,
            target_lang=lang,
            source_lang=args.source,
            batch_size=args.batch,
            verbose=args.verbose,
            repair_only=args.repair_only,
            force=args.force,
        )

        print(
            f"[{lang}] translated values: {translated}; "
            f"repaired: {repaired}; failed tasks: {failed}"
        )
        total_translated += translated
        total_repaired += repaired
        total_failed += failed

    print(
        "ALL DONE — "
        f"translated values: {total_translated}; "
        f"repaired: {total_repaired}; "
        f"failed tasks: {total_failed}"
    )
    if skipped:
        print("Skipped unsupported languages: " + ", ".join(skipped))

    # We intentionally do not fail the whole pipeline merely because a few
    # individual MT requests failed. Their msgstr/fuzzy state is preserved.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
