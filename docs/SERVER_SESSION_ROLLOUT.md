# ECHOREPO: PostgreSQL-backed Flask sessions (staged deployment)

**Scope:** Browser web sessions for `/login`, `/sso/*`, `/my`, `/i18n`, privacy acceptance, and Jupyter/JupyterHub auth helpers. API-key access and canonical data are unchanged.

## Why this is safer

Before this change, Flask's default cookie session can contain the Keycloak `access_token` and `refresh_token`. A Flask signed cookie is **readable** to the browser, even though it is tamper-evident. The new backend sends only a new 256-bit random session identifier (`echorepo_sid`) in a secure, HttpOnly cookie, and stores session contents in PostgreSQL. Only its SHA-256 hash is persisted as the database lookup key.

The database **does contain** Keycloak tokens in JSONB. PostgreSQL access, database backups, support dumps, logs, and diagnostic routes therefore require confidentiality controls. Never print or publish the `echorepo_web_sessions.data` column. Encryption at rest is an infrastructure/database responsibility; the patch does **not** provide application-level field encryption.

## Requirements and preflight

- Applies to the `develop` branch at commit `ac5a74946de115af1194f8f295d7aa5db41c2393` (2026-11-20). Later commits may require a small patch refresh.
- Uses existing `psycopg2-binary` dependency; no Redis or SQLAlchemy required.
- Requires the existing PostgreSQL service and session migration table.
- **Keep the same strong `SECRET_KEY`** during migration. Do not rotate it simultaneously. The patch refuses legacy-cookie migration when the key is shorter than 24 characters or is a known example default.
- Back up PostgreSQL and `.env` before production deployment. Check the existing cookie name using the browser Network > `Set-Cookie` response header; current Flask configuration uses the default `session` cookie name unless locally overridden.
- Always test this in **development first** with a separate dev URL and browser profile.
- `/my` and `/explore` must remain accessible in normal cases; if the database is unavailable, authentication fails closed with a service error rather than moving credentials back into a cookie.

## Stage 1 — deploy while keeping legacy cookie sessions

1. Apply the Git patch and run the tests.
2. Add to *development* `.env`:

   ```dotenv
   ECHOREPO_SESSION_STORAGE=cookie
   ECHOREPO_MIGRATE_LEGACY_SESSIONS=false
   ```

3. Rebuild/start dev with its normal Compose deployment. Schema migration is run by `scripts/run_pg_migrations.py` **before** Gunicorn starts.
4. Check the new table exists:

   ```bash
   docker compose -f docker-compose.yml -f docker-compose.dev.yml exec -T postgres \
     psql -U echorepo -d echorepo -c '\\d echorepo_web_sessions'
   ```

5. Do not remove or modify any pre-existing client-side sessions at this stage.

## Stage 2 — switch development to server sessions

Change *development* `.env` to:

```dotenv
ECHOREPO_SESSION_STORAGE=postgres
ECHOREPO_MIGRATE_LEGACY_SESSIONS=true
ECHOREPO_SESSION_COOKIE=echorepo_sid
ECHOREPO_LEGACY_SESSION_COOKIE=session
ECHOREPO_SESSION_TTL_DAYS=31
```

`SESSION_COOKIE_SECURE=true` MUST remain enabled when accessed through HTTPS. If you intentionally use bare HTTP for a localhost development environment, `SESSION_COOKIE_SECURE=false` is necessary locally only; never use that in production.

Restart/recreate the app container so environment settings take effect:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build echorepo-lite
```

### Check these flows in a fresh private browser AND a browser previously logged in

- [ ] Anonymous `/explore` works and receives no new auth cookie before session data is created.
- [ ] Existing signed cookie migrates on first request, `session` cookie is deleted, new `echorepo_sid` is set.
- [ ] Password login works, redirects to the requested `/my?lat=...` URL, and reload retains login.
- [ ] SSO redirect/callback works (state cookie is stored server-side).
- [ ] Session survives multiple HTTP requests and Gunicorn workers (cross-worker consistency).
- [ ] `/my` export, API-backed personal data, privacy-acceptance and translation language cookie work.
- [ ] Jupyter/JupyterHub trusted auth endpoints (`/internal/jupyter-auth`, `/internal/jupyterhub-auth`) still return 204 for permitted authenticated users.
- [ ] Logout destroys the database session; the old SID cannot authenticate again.
- [ ] Token refresh works without changing the session ID on every refresh.
- [ ] Browser Application > Cookies: **no Keycloak tokens or user ID** in the `echorepo_sid` value; `HttpOnly`, `Secure` (HTTPS), `SameSite=Lax`.
- [ ] Confirm the URL `?desktop` and map layer controls are unchanged.
- [ ] Simulate an unavailable PostgreSQL instance in *development*: session reads must not fall back to client-side storage.

To view session count **without disclosing tokens**:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml exec -T postgres \
  psql -U echorepo -d echorepo -c 'SELECT count(*) FROM echorepo_web_sessions;'
```

## Stage 3 — production rollout

Deploy the same code to production while `ECHOREPO_SESSION_STORAGE=cookie`, verify migration, then switch production `.env` to `postgres` and `ECHOREPO_MIGRATE_LEGACY_SESSIONS=true`. Recreate the production web container via your existing Compose commands. Do **not** enable the new backend in one Gunicorn worker while another still uses client cookies under the same URL—deploy atomically using the container replacement process.

This grace period prevents a sudden logout for visitors carrying an existing valid signed cookie. However, the old cookie already exposed its token payload to the user's browser; delete the migration compatibility after an agreed window (for example 48 hours to one week), and evaluate revoking previous identity-provider refresh tokens if there is evidence of compromise.

After the window, set:

```dotenv
ECHOREPO_MIGRATE_LEGACY_SESSIONS=false
```

Recreate the container again. Legacy cookies will then be deleted without being accepted; visitors who never migrated must log in again.

## Scheduled cleanup

The migration adds an index on `expires_at`, but old rows are retained until removed. Run periodically (for example daily):

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml exec -T echorepo-lite \
  flask --app echorepo.wsgi:app prune-web-sessions
```

For production substitute the appropriate Compose files and working directory. This command prints only a deleted-row count.

## Rollback and limits

- **Immediate rollback:** Set `ECHOREPO_SESSION_STORAGE=cookie` again and recreate the web container. The previous Flask session mechanism resumes; **users already migrated to the new opaque cookie will have to log in again**. There is intentionally no insecure shadow cookie carrying identity tokens.
- No database schema needs to be dropped when rolling back. Never drop the session table during an active server-side deployment.
- Running a stale browser request concurrently with a token refresh may hit an optimistic concurrency conflict. The stale request fails instead of overwriting a newer token. Retrying the request normally succeeds. This is deliberate safety over silent lost updates.
- This migration does not fix existing unrelated issues: Keycloak direct password grant behaviour, CSRF policies, token lifetime choices, or analytics retention.
- Keep `ENABLE_DEBUG_ROUTES=0` in production. The patch redacts `/debug/whoami` to a boolean authentication indicator and session-key names, but debug routes still should not be exposed publicly.
- PostgreSQL backups include tokens while sessions are active; audit access and retention of backups.
- This package's tests are offline; **a real dev/production integration test is required before production activation**.
