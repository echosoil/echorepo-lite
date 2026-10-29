#!/usr/bin/env python3
"""Apply the ECHOREPO compact-marker / lazy-popup patch safely.

Run from the echorepo-lite checkout:
  python3 apply_lazy_map.py           # validate; do not modify files
  python3 apply_lazy_map.py --apply   # back up and apply

Preserves LF/CRLF line endings and leaves other source modifications in place.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import subprocess
import tempfile

FILES = ('echorepo/routes/data_api.py', 'static/js/map.js')
PATCH = Path(__file__).with_name('echorepo_lazy_map_loading.patch')

def git_apply(root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(['git', 'apply', *args, str(PATCH)], cwd=root, capture_output=True, text=True)

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--apply', action='store_true', help='Write modified files after validation')
    ap.add_argument('--root', type=Path, default=Path.cwd(), help='Repository root (default: current directory)')
    args = ap.parse_args()
    root = args.root.resolve()
    if not PATCH.exists():
        ap.error(f'Patch not found next to installer: {PATCH}')
    originals: dict[str, bytes] = {}
    endings: dict[str, bytes] = {}
    for file in FILES:
        path = root / file
        if not path.is_file():
            ap.error(f'Missing source file: {path}')
        content = path.read_bytes()
        originals[file] = content
        endings[file] = b'\r\n' if b'\r\n' in content else b'\n'

    # Use a disposable git worktree for validation. Reformat only in memory,
    # so the checked-out files keep their original line endings.
    with tempfile.TemporaryDirectory(prefix='echorepo-lazy-map-') as tmp:
        dest = Path(tmp)
        subprocess.run(['git', 'init', '-q'], cwd=dest, check=True)
        for name in FILES:
            path = dest / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(originals[name].replace(b'\r\n', b'\n'))

        check = git_apply(dest, ['--check'])
        if check.returncode:
            already = git_apply(dest, ['--reverse', '--check'])
            if already.returncode == 0:
                print('Patch is already applied; no changes necessary.')
                return
            raise SystemExit('Patch does not apply cleanly. No files were modified.\n' + check.stderr)
        print('Patch verified against local sources (including existing edits).')
        if not args.apply:
            print('Run again with --apply to install it.')
            return
        result = git_apply(dest, [])
        if result.returncode:
            raise SystemExit('Patch failed during staging. No files were modified.\n' + result.stderr)

        changes = [(name, (dest / name).read_bytes().replace(b'\n', endings[name])) for name in FILES]
        for name, _ in changes:
            p = root / name
            backup = p.with_name(p.name + '.before-lazy-map.bak')
            if backup.exists():
                raise SystemExit(f'Backup already exists, refusing to overwrite: {backup}')
        for name, content in changes:
            path = root / name
            path.with_name(path.name + '.before-lazy-map.bak').write_bytes(originals[name])
            path.write_bytes(content)
            print('Updated:', path.relative_to(root))
        print('Done. Backups saved next to both original files.')

if __name__ == '__main__':
    main()
