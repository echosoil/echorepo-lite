# ECHOREPO research JupyterHub — Phase 1 (pilot)

This is an **opt-in, side-by-side pilot**. It does not replace the existing
`docker-compose.jupyter.yml` JupyterLab service or its live URL.

## Architecture and security contract

- Existing ECHOREPO Flask session + Nginx `auth_request` controls admission.
- Nginx asks `/internal/jupyterhub-auth` for a validated, stable Keycloak
  `sub` and *overwrites* the `X-ECHOREPO-User` upstream header.
- JupyterHub trusts that header **only because its port must be reachable
  solely by the reverse proxy**. Allowing direct access to port 8890 lets
  clients forge an identity; this is a deployment blocker.
- `c.JupyterHub.active_server_limit=5` rejects additional starts with HTTP 429.
- DockerSpawner starts a dedicated, unprivileged notebook container on a
  separate **internal** Docker network. Research users cannot connect to the
  Postgres network directly; `research-api` permits read-only canonical API
  GET requests.
- There are **no personal notebook volumes**. Containers are removed on stop:
  unsaved work is lost. Encourage Download/Export before leaving.
- The curated shared notebook library is mounted **read-only** as
  `/home/jovyan/work/Library`. Copy a notebook outside Library to edit it.
- Only Hub configuration and small account metadata persist in a named volume.
  Never mount `/var/run/docker.sock` in notebook containers. The Hub's
  Docker socket is host-administrative access and must be protected.
- This pilot has **no enforced per-user disk quota** for Docker writable
  layers: monitor Docker disk usage. Memory and CPU limits are enforced.

## Prepare the official notebook library

The repository's `notebooks/README.md` describes three example notebooks,
but the matching `.ipynb` files may exist **only on the deployed server**.
Do not assume they are committed to Git. Review their outputs and any secrets
before putting them in the library.

```bash
cd ~/ECHO-STORE/echorepo-lite
mkdir -p "$HOME/ECHO-STORE/echorepo-notebook-library"
find notebooks -maxdepth 2 -type f -name '*.ipynb' -print
# Review and manually copy approved .ipynb files into the library directory.
# Optionally include README.md and docs/ for discoverability.
```

In the production `.env`, add:

```dotenv
ECHOREPO_LIBRARY_HOST_DIR=/home/echo/ECHO-STORE/echorepo-notebook-library
RESEARCH_MAX_SERVERS=5
RESEARCH_USER_MEM_LIMIT=2G
RESEARCH_USER_CPU_LIMIT=1.0
RESEARCH_IDLE_TIMEOUT=1800
```

The library path must be the absolute **Docker host** path. Ensure it exists
and is readable by the single-user container's UID. The `LOCAL_IP` in
`.env` must be the server's private address reachable by Nginx.

## Stage and build without touching the current service

```bash
cd ~/ECHO-STORE/echorepo-lite
git fetch origin
git switch feature/research-jupyterhub-phase1
git pull --ff-only

# Build the isolated notebook image (needed before DockerSpawner can spawn).
docker build -f jupyterhub/singleuser.Dockerfile \
    -t echorepo-research-singleuser:phase1 .

export COMPOSE_PROJECT_NAME=echorepo_prod
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
    -f docker-compose.jupyterhub.yml config --quiet

# Start only the new pilot services; do not restart production Flask or Postgres.
docker compose -f docker-compose.yml -f docker-compose.prod.yml \
    -f docker-compose.jupyterhub.yml up -d --no-deps --build \
    jupyterhub research-api
```

Ask the external proxy administrator to apply
`jupyterhub/nginx-research-hub.conf.example` at
`https://echorepo.quanta-labs.com/research/hub/`, keeping existing
`/research/workspace/` and `/research/dashboard/` locations intact.

**Before the Nginx change**, apply an IP firewall/security-group rule
allowing TCP/8890 **only from the trusted reverse proxy**. Header-based
authentication is unsafe without this restriction. If Nginx runs on another
host and private routing is unavailable, use a secure tunnel instead.
Never use `0.0.0.0:8890`.

## Acceptance test

1. Anonymous browser goes to `/research/hub/` and sees the existing
   ECHOREPO `/login` page; after login it returns to the Hub.
2. An authenticated allowlisted ECHOREPO user enters directly without a
   second password screen. Unauthorised users receive HTTP 403.
3. Two different accounts get separate writable notebooks. Neither can
   inspect the other's workspace. `Library` is read-only; copied files
   can be edited.
4. Five simultaneous users can start servers; the sixth receives an
   explicit capacity error (HTTP 429, not a sixth notebook).
5. `from lib.echorepo import EchoRepo; EchoRepo().ping()` succeeds. A
   notebook cannot resolve or contact the production `postgres` service.
6. Download a notebook; stop that user's server via the Hub; restart it:
   previous private work is gone but the shared library remains.
7. A server idle for approximately 30 minutes is stopped. Test an active
   long-running calculation before enabling this for the public.
8. Inspect `docker stats` and `docker system df`. Verify Docker layer
   cleanup and that the production services remain healthy.

## Follow-up phases

**Phase 2:** create a curated `echosoil/echorepo-notebooks` Git-backed
library; sync *reviewed* notebook versions to the host library directory
without exposing unpublished or sensitive files.

**Phase 3:** add an ECHOREPO "Contribute notebook" UI, file validation,
secret/output stripping, author attribution, review requests, version history
and moderation. Uploaded notebooks are **never executed automatically**.

**Phase 4:** monitoring, audited image updates, controlled rollback,
resource quotas and optional real-time collaboration.

## Rollback

The pilot has its own Compose services and port. Remove its Nginx location
first; then stop only `jupyterhub` and `research-api` from the combined
Compose stack. Do not use `docker compose down -v`; that could remove
production volumes. The old `jupyter` service remains untouched.
