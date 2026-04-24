mkdir -p /data/biodiversity

python - <<'PY'
import re
import requests
from pathlib import Path

url = "http://www.stbates.org/funguild_db.php"
out = Path("/data/biodiversity/FUNGuild_db.json")

print(f"Downloading {url} ...")
r = requests.get(url, timeout=60)
r.raise_for_status()

text = r.text.strip()

# Sometimes the endpoint wraps JSON in a minimal HTML body.
text = re.sub(r"(?is)^.*?<body[^>]*>\s*", "", text)
text = re.sub(r"(?is)\s*</body>.*$", "", text)
text = text.strip()

out.write_text(text, encoding="utf-8")
print(f"Saved: {out}")
print(f"Size: {out.stat().st_size} bytes")
PY
