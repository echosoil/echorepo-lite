#!/usr/bin/env python3
"""
Repo overview generator that *respects .gitignore* and only considers files Git
sees as part of your project (tracked + untracked but not ignored).

- Directory tree is built from `git ls-files --exclude-standard --others --cached`.
- Route scan supports @*.route("/...") and @*.(get|post|put|delete|patch|options|head)("/...").
"""

import io
import os
import re
import subprocess
from collections import defaultdict
from collections.abc import Iterable

# ---------- Config ----------
MAX_DEPTH = 4

# Matches:
#   @app.route("/x")              @bp.route('/x', methods=['GET'])
#   @app.get("/x")                @bp.post('/x'), @api.delete("/x"), etc.
ROUTE_RE = re.compile(
    r"@(?:[A-Za-z_][\w\.]*)\.(?:route|get|post|put|delete|patch|options|head)\s*"
    r'\(\s*(?P<q>[\'"])(?P<path>.*?)(?P=q)',
    re.S,
)


# ---------- Helpers ----------
def repo_root() -> str:
    """Find the repo root via Git; fall back to current working directory."""
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            stderr=subprocess.DEVNULL,
        )
        return out.decode().strip()
    except Exception:
        return os.path.abspath(".")


def git_list_project_paths(root: str) -> list[str]:
    """
    Return repo-relative paths that Git considers part of the project:
    - tracked files
    - untracked files not ignored (respect .gitignore, .git/info/exclude, global excludes)
    """
    try:
        out = subprocess.check_output(
            ["git", "-C", root, "ls-files", "--exclude-standard", "--others", "--cached", "-z"],
            stderr=subprocess.DEVNULL,
        )
        # Null-separated; filter-out empty
        paths = [p for p in out.decode("utf-8", errors="ignore").split("\x00") if p]
        return sorted(paths)
    except Exception:
        # Fallback: naive full walk (won't fully respect .gitignore)
        paths = []
        for dp, dns, fns in os.walk(root):
            # skip .git directory explicitly
            dns[:] = [d for d in dns if d != ".git"]
            for f in fns:
                rp = os.path.relpath(os.path.join(dp, f), root)
                if rp.startswith(".git" + os.sep):
                    continue
                paths.append(rp)
        return sorted(paths)


def collect_dirs_for_paths(paths: Iterable[str]) -> set[str]:
    """All directories implied by the file list, including parents up to root.”"""
    dirs: set[str] = set()
    for p in paths:
        d = os.path.dirname(p)
        while True:
            if d == "" or d == ".":
                break
            dirs.add(d)
            nd = os.path.dirname(d)
            if nd == d:
                break
            d = nd
    return dirs


def render_tree(paths: list[str], maxdepth: int = MAX_DEPTH) -> str:
    """
    Render a tree limited to the provided repo-relative paths.
    Only directories/files that exist in `paths` are shown.
    """
    # Build directory -> (subdirs, files) index
    dir_index = defaultdict(lambda: {"dirs": set(), "files": set()})
    for p in paths:
        d, f = os.path.split(p)
        d = "." if d in ("", ".") else d
        if f:
            dir_index[d]["files"].add(f)
        # also record the parent chain for directories
        parent = d
        while parent not in (".", ""):
            grand = os.path.dirname(parent) or "."
            dir_index[grand]["dirs"].add(parent)
            if grand == parent:
                break
            parent = grand

    def walk(d: str, prefix: str = "", depth: int = 0, out: io.StringIO = None):
        if out is None:
            out = io.StringIO()
        if depth > maxdepth:
            return out
        # Sort dirs then files for stable rendering
        subdirs = sorted(dir_index[d]["dirs"])
        files = sorted(dir_index[d]["files"])
        entries = [(name, True) for name in subdirs] + [(name, False) for name in files]
        for i, (name, is_dir) in enumerate(entries):
            tee = "└── " if i == len(entries) - 1 else "├── "
            out.write(prefix + tee + name + ("\n"))
            if is_dir:
                next_dir = os.path.normpath(os.path.join(d, name))
                cont = "    " if i == len(entries) - 1 else "│   "
                walk(next_dir, prefix + cont, depth + 1, out)
        return out

    return walk(".", "", 0).getvalue()


def find_routes(root: str, py_paths: Iterable[str]) -> list[tuple[str, str]]:
    """Scan only the given Python file subset for route-like decorators."""
    results: list[tuple[str, str]] = []
    for rel in py_paths:
        p = os.path.join(root, rel)
        try:
            with open(p, encoding="utf-8", errors="ignore") as fh:
                txt = fh.read()
        except Exception:
            continue
        for m in ROUTE_RE.finditer(txt):
            results.append((rel, m.group("path")))
    return sorted(results)


# ---------- Main ----------
def main():
    ROOT = repo_root()
    os.chdir(ROOT)  # ensure relative opens write to repo root
    repo = os.path.basename(ROOT.rstrip(os.sep))

    # 1) Collect project paths via Git (respects .gitignore)
    paths = git_list_project_paths(ROOT)

    # 2) Derive python subset for route scan
    py_paths = [p for p in paths if p.endswith(".py")]

    # 3) Build a directory tree from those paths
    tree_md = render_tree(paths, maxdepth=MAX_DEPTH).rstrip()

    # 4) Collect “Flask markers”
    markers = []
    for name in ("app.py", "wsgi.py", "manage.py", "run.py"):
        if name in paths:
            markers.append(name)
    if any(p.startswith("templates/") or p == "templates" for p in paths):
        markers.append("templates/")
    if any(p.startswith("static/") or p == "static" for p in paths):
        markers.append("static/")
    if "config.py" in paths:
        markers.append("config.py")

    # 5) Routes
    rts = find_routes(ROOT, py_paths)

    # 6) Write Markdown
    lines = [f"# Repository overview: {repo}", ""]
    if markers:
        lines += ["## Flask markers found", ""]
        lines += ["- " + "\n- ".join(markers), ""]

    if rts:
        lines += ["## Routes (best-effort scan)", ""]
        for f, path in rts:
            lines.append(f"- `{path}`  _(in {f})_")
        lines.append("")

    lines += [f"## Directory tree (depth ≤ {MAX_DEPTH})", "", "```", tree_md, "```", ""]

    with open("REPO_OVERVIEW.md", "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))
    print(f"Wrote REPO_OVERVIEW.md at {ROOT}")


if __name__ == "__main__":
    main()
