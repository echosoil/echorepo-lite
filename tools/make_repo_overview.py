import os, re, io, sys, textwrap

ROOT = os.path.abspath(".")
IGNORE = re.compile(r'(^\.git$|^venv$|^env$|__pycache__|node_modules|\.mypy_cache|\.pytest_cache|\.DS_Store|^dist$|^build$)')
ROUTE_RE = re.compile(r'@([A-Za-z_][\w\.]*)\.route\(\s*(?P<q>[\'"])(?P<path>.*?)(?P=q)', re.S)

def tree(maxdepth=3):
  out = io.StringIO()
  def walk(root, prefix='', depth=0):
      if depth>maxdepth: return
      entries = sorted(e for e in os.listdir(root) if not IGNORE.search(e))
      for i, name in enumerate(entries):
          path = os.path.join(root, name)
          tee = '└── ' if i==len(entries)-1 else '├── '
          out.write(prefix + tee + name + ("\n"))
          if os.path.isdir(path):
              walk(path, prefix + ('    ' if i==len(entries)-1 else '│   '), depth+1)
  walk(ROOT)
  return out.getvalue()

def find_routes():
  routes = []
  for dirpath, dirnames, filenames in os.walk(ROOT):
      dirnames[:] = [d for d in dirnames if not IGNORE.search(d)]
      for f in filenames:
          if f.endswith(".py"):
              p = os.path.join(dirpath, f)
              try:
                  with open(p, "r", encoding="utf-8", errors="ignore") as fh:
                      txt = fh.read()
              except Exception:
                  continue
              for m in ROUTE_RE.finditer(txt):
                  routes.append((os.path.relpath(p, ROOT), m.group('path')))
  return sorted(routes)

def main():
  repo = os.path.basename(ROOT)
  print(f"Generating REPO_OVERVIEW.md for repo: {repo}")
  lines = [f"# Repository overview: {repo}\n"]
  # Common Flask markers
  markers = []
  for name in ("app.py","wsgi.py","manage.py","run.py"):
      if os.path.exists(os.path.join(ROOT,name)): markers.append(name)
  if os.path.isdir("templates"): markers.append("templates/")
  if os.path.isdir("static"): markers.append("static/")
  if os.path.exists("config.py"): markers.append("config.py")
  if markers:
      lines += ["## Flask markers found", "", "- " + "\n- ".join(markers), ""]
  # Routes
  rts = find_routes()
  if rts:
      lines += ["## Routes (best-effort scan)", ""]
      for f, path in rts:
          lines.append(f"- `{path}`  _(in {f})_")
      lines.append("")
  # Tree
  lines += ["## Directory tree (depth ≤ 3)", "", "```", tree(3).rstrip(), "```", ""]
  with open("REPO_OVERVIEW.md","w",encoding="utf-8") as fh:
      fh.write("\n".join(lines))
  print("Wrote REPO_OVERVIEW.md")

if __name__ == "__main__":
  main()