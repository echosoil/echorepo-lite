"""Offline regression checks; no production configuration or external network."""

import shutil
import sqlite3
import subprocess
from pathlib import Path

import pytest

from echorepo.services.db import _find_main_table, _quote_sqlite_identifier

ROOT = Path(__file__).resolve().parents[1]


def test_sqlite_identifier_quoting():
    name = 'table " with spaces; --'
    identifier = _quote_sqlite_identifier(name)
    with sqlite3.connect(":memory:") as conn:
        conn.execute(f"CREATE TABLE {identifier} (sampleId TEXT, GPS_lat REAL, GPS_long REAL)")
        conn.execute(f"INSERT INTO {identifier} VALUES (?, ?, ?)", ("test", 1, 2))
        assert _find_main_table(conn) == name
        assert conn.execute(f"SELECT sampleId FROM {identifier}").fetchone() == ("test",)
    with pytest.raises(ValueError):
        _quote_sqlite_identifier("")


@pytest.mark.skipif(not shutil.which("bash") or not shutil.which("git"), reason="requires bash and git")
def test_release_preserves_production_environment(tmp_path, monkeypatch):
    # Disposable local bare remote and two worktrees. No Docker or network.
    remote = tmp_path / "remote.git"
    dev = tmp_path / "echorepo-lite-dev"
    prod = tmp_path / "echorepo-lite"
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.setenv("GIT_AUTHOR_NAME", "Release Test")
    monkeypatch.setenv("GIT_AUTHOR_EMAIL", "release@example.invalid")
    monkeypatch.setenv("GIT_COMMITTER_NAME", "Release Test")
    monkeypatch.setenv("GIT_COMMITTER_EMAIL", "release@example.invalid")
    monkeypatch.setenv("SOURCE_REMOTE", "origin")

    def git(cwd, *args):
        return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)

    git(tmp_path, "init", "--bare", str(remote))
    git(tmp_path, "clone", str(remote), str(dev))
    git(dev, "switch", "-c", "main")
    (dev / ".gitignore").write_text(".env\n")
    (dev / "README").write_text("base\n")
    script = dev / "scripts/make_release.sh"
    script.parent.mkdir()
    shutil.copyfile(ROOT / "scripts/make_release.sh", script)
    git(dev, "add", ".")
    git(dev, "commit", "-m", "base")
    git(dev, "push", "-u", "origin", "main")
    git(dev, "switch", "-c", "develop")
    (dev / "README").write_text("new source\n")
    git(dev, "commit", "-am", "update")
    git(dev, "push", "-u", "origin", "develop")
    git(tmp_path, "clone", "-b", "main", str(remote), str(prod))
    (dev / ".env").write_text("APP_ENV=dev\nDB_PASSWORD=dev-fixture\n")
    before = b"APP_ENV=prod\nDB_PASSWORD=prod-fixture\n"
    env = prod / ".env"
    env.write_bytes(before)
    env.chmod(0o600)
    initial_mode = env.stat().st_mode
    subprocess.run(["bash", "-n", str(script)], check=True)
    subprocess.run(["bash", str(script)], cwd=dev, check=True, capture_output=True, text=True)
    assert env.read_bytes() == before
    assert env.stat().st_mode == initial_mode
    assert (prod / "README").read_text() == "new source\n"
    env.unlink()
    result = subprocess.run(["bash", str(script)], cwd=dev, capture_output=True, text=True)
    assert result.returncode != 0
    assert not env.exists()
