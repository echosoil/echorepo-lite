# ECHOrepo – On-demand translation pipeline

This document describes how to run the on-demand translation workflow that uses:

- your existing `echorepo/translations/` tree (Babel/Flask-Babel style),
- a throwaway LibreTranslate container (so it’s not running all the time),
- a helper `i18n` container that actually runs the Python script,
- your existing `tools/auto_translate.py`,
- and finally re-compiles translations inside the **real app container**.

The whole thing is wrapped in one script: `tools/translate_all.sh`.

---

## What the script does

1. **Starts** two temporary services from `docker-compose.yml`:
   - `libretranslate` – the MT engine
   - `i18n` – a tiny image with Babel + requests, mounted to your repo
2. **Waits** until LibreTranslate is up (HTTP check on `http://localhost:5001/languages`).
3. **Discovers** all languages in `./echorepo/translations/*` except `en`.
4. **Runs** your `tools/auto_translate.py` **inside the `i18n` container**, pointing it at LibreTranslate.
5. **Finds** the actual Flask/gunicorn app container (the one that has `/app/echorepo/translations`) and runs:
   ```bash
   pybabel compile -d /app/echorepo/translations
   ```
6. **Stops the temporary i18n services to save RAM/CPU.**

Net result: you get updated .po → compiled .mo without having LibreTranslate running forever.

## Prerequisites

1. **Your compose files define** these services (names can be changed, but keep in sync with the script):
    - libretranslate
    - i18n
    - the main app, e.g. echorepo-lite

2. **Your repo is mounted** into the i18n container at /work, so this works:
```bash
docker compose run --rm i18n ls /work/tools
```
3. LibreTranslate is reachable from inside the i18n container as:
`http://libretranslate:5000`

and from the host as:
`http://localhost:5001`

4. **Your app image** already has Babel and is able to run:
`pybabel compile -d /app/echorepo/translations`

5. You already have the helper script:
`tools/auto_translate.py`


## The script (`tools/translate_all.sh`)

Put this file into `tools/translate_all.sh` and `chmod +x` it.
## How to run it 
From the repo root:
```bash
./tools/translate_all.sh
```

It will:
1. start the two “translation” containers,
2. translate,
3. compile,
4. shut them down.

## Common reasons for “filled 0 entries”
* You didn’t re-extract / update catalogs, so the new English strings aren’t in the .po files yet.

Run (in your app container or locally with Babel installed):
```bash
pybabel extract -F babel.cfg -o echorepo/translations/messages.pot .
pybabel update -i echorepo/translations/messages.pot -d echorepo/translations
```
Then re-run `./tools/translate_all.sh`.

* The `.po` already has a translation for that msgid — the script only fills empty ones.

* LibreTranslate is up, but you asked it to translate into a language it doesn’t have (we list languages from your folders, not from LT). In that case, LT might return empty strings.

## Forcing a container
If the script guesses the wrong app container, do this:
```bash
APP_CONTAINER=echorepo-lite ./tools/translate_all.sh
```
## Notes
* We start LT with `docker compose ... up -d` so you don’t have to keep it running.
* We shut it down again with `docker compose ... down` so it doesn’t eat RAM.
* The script assumes your repo is mounted into the i18n container as `/work` (that’s how your compose was set up).
* If you add a new language folder under `echorepo/translations/<lang>`, it will be picked up automatically.
* To force-stop libretranslate and i18n execute in the root: `docker compose --profile devtools down`.