"""ECHOREPO JupyterHub pilot: trusted-proxy SSO, temporary isolated servers."""
import os
import re
import sys

from jupyterhub.auth import Authenticator

c = get_config()  # Provided by the JupyterHub configuration loader.

class ECHOREPOProxyAuthenticator(Authenticator):
    """Only trust an identity header inserted by the authenticated edge proxy."""

    async def authenticate(self, handler, data):
        identity = handler.request.headers.get("X-ECHOREPO-User", "").strip()
        # Keycloak sub (not email) is stable even if the user changes email.
        if not re.fullmatch(r"[a-zA-Z0-9._-]{1,128}", identity):
            return None
        return identity.lower()

c.JupyterHub.authenticator_class = ECHOREPOProxyAuthenticator
c.Authenticator.allow_all = True
c.Authenticator.auto_login = True

# The upstream reverse proxy retains this prefix verbatim.
c.JupyterHub.bind_url = "http://0.0.0.0:8000/research/hub/"
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.hub_connect_ip = "jupyterhub"
c.JupyterHub.db_url = "sqlite:////srv/jupyterhub/state/jupyterhub.sqlite"
c.JupyterHub.cookie_secret_file = "/srv/jupyterhub/state/cookie_secret"

c.JupyterHub.active_server_limit = int(os.environ.get("RESEARCH_MAX_SERVERS", "5"))
c.JupyterHub.concurrent_spawn_limit = 2
c.JupyterHub.cleanup_servers = True

c.JupyterHub.spawner_class = "dockerspawner.DockerSpawner"
c.DockerSpawner.image = os.environ.get(
    "ECHOREPO_SINGLEUSER_IMAGE", "echorepo-research-singleuser:phase1"
)
c.DockerSpawner.network_name = os.environ["DOCKER_NETWORK_NAME"]
c.DockerSpawner.use_internal_ip = True
c.DockerSpawner.remove = True  # No per-user volumes; remove writable layer on stop.
c.DockerSpawner.notebook_dir = "/home/jovyan/work"
c.DockerSpawner.cmd = ["jupyterhub-singleuser"]
c.DockerSpawner.mem_limit = os.environ.get("RESEARCH_USER_MEM_LIMIT", "2G")
c.DockerSpawner.cpu_limit = float(os.environ.get("RESEARCH_USER_CPU_LIMIT", "1.0"))
c.DockerSpawner.environment = {
    "ECHOREPO_URL": "http://research-api:8080",
    "PYTHONPATH": "/opt/echorepo",
}
c.Spawner.default_url = "/lab"
c.Spawner.start_timeout = 120

library_host_dir = os.environ.get("ECHOREPO_LIBRARY_HOST_DIR", "")
if not os.path.isabs(library_host_dir):
    raise ValueError("ECHOREPO_LIBRARY_HOST_DIR must be an absolute HOST path")
# A bind mount is read-only. Users may copy notebooks into their own work dir.
c.DockerSpawner.read_only_volumes = {
    library_host_dir: "/home/jovyan/work/Library",
}

# Hub-managed culler; give it only the minimal API permissions.
c.JupyterHub.services = [{
    "name": "idle-culler",
    "command": [
        sys.executable, "-m", "jupyterhub_idle_culler",
        "--timeout=" + os.environ.get("RESEARCH_IDLE_TIMEOUT", "1800"),
        "--cull-every=300",
    ],
}]
c.JupyterHub.load_roles = [{
    "name": "idle-culler-role",
    "scopes": ["list:users", "read:users:activity", "read:servers", "delete:servers"],
    "services": ["idle-culler"],
}]
