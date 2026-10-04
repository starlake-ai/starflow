"""Detect a running Quack on Demand manager and wire the Starflow api to it.

`qod serve` (and `qod login`) record the manager URL and the Flight SQL edge in
a profile of the qod CLI config file. When that manager answers, the api is
started in QoD mode (QOD_ENABLED/QOD_URL/QOD_FLIGHT_URL): users sign in with
their QoD credentials and the QoD pools show up as connections.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

DEFAULT_MANAGER_URL = "http://localhost:20900"
DEFAULT_EDGE_PORT = 31338


@dataclass(frozen=True)
class QodTarget:
    manager_url: str
    flight_url: str
    tls_insecure: bool

    def env(self) -> dict[str, str]:
        env = {
            "QOD_ENABLED": "true",
            "QOD_URL": self.manager_url,
            "QOD_FLIGHT_URL": self.flight_url,
        }
        if self.tls_insecure:
            env["QOD_TLS_INSECURE"] = "true"
        return env


def qod_config_path() -> Path:
    """Same resolution as the qod CLI: QOD_CONFIG_FILE, else the platform config dir."""
    env_path = os.environ.get("QOD_CONFIG_FILE")
    if env_path:
        return Path(env_path).expanduser()
    import platformdirs

    return Path(platformdirs.user_config_dir("qod", appauthor=False, roaming=True)) / "config.toml"


def load_profile(path: Path, profile: str | None) -> dict:
    """The named qod profile, else QOD_PROFILE, else the file's sticky default.
    Empty when the file or the profile does not exist."""
    if not path.is_file():
        return {}
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except (OSError, tomllib.TOMLDecodeError):
        return {}
    name = profile or os.environ.get("QOD_PROFILE") or str(data.get("default_profile", "default"))
    return dict(data.get("profiles", {}).get(name, {}))


def _as_bool(value, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def flight_url(host: str, port: int, tls: bool, manager_url: str) -> str:
    """The QOD_FLIGHT_URL shape the api parses: scheme https/http encodes TLS.
    An unset or wildcard edge host means "same host as the manager"."""
    if host in ("", "0.0.0.0", "::"):
        host = urlparse(manager_url).hostname or "localhost"
    return f"{'https' if tls else 'http'}://{host}:{port}"


def resolve_target(profile: dict, edge: dict | None) -> QodTarget:
    """Combine the profile with the manager's /api/config/client answer (when it
    gave one). Env vars the qod CLI honours override the profile."""
    manager_url = (
        os.environ.get("QOD_MANAGER_URL") or profile.get("manager_url") or DEFAULT_MANAGER_URL
    ).rstrip("/")
    edge = edge or {}
    host = edge.get("flightSqlHost") or os.environ.get("QOD_HOST") or profile.get("edge_host") or ""
    port = int(
        edge.get("flightSqlPort")
        or os.environ.get("QOD_PORT")
        or profile.get("edge_port")
        or DEFAULT_EDGE_PORT
    )
    tls = _as_bool(
        edge.get("flightSqlTls", os.environ.get("QOD_TLS", profile.get("edge_tls"))), True
    )
    verify = _as_bool(os.environ.get("QOD_TLS_VERIFY", profile.get("edge_tls_verify")), False)
    return QodTarget(
        manager_url=manager_url,
        flight_url=flight_url(host, port, tls, manager_url),
        tls_insecure=tls and not verify,
    )


def is_ready(manager_url: str, timeout_s: float = 2.0) -> bool:
    try:
        return httpx.get(f"{manager_url}/ready", timeout=timeout_s).status_code == 200
    except httpx.HTTPError:
        return False


def fetch_edge(manager_url: str, token: str | None, timeout_s: float = 2.0) -> dict | None:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        resp = httpx.get(f"{manager_url}/api/config/client", headers=headers, timeout=timeout_s)
        if resp.status_code == 200:
            body = resp.json()
            return body if isinstance(body, dict) else None
    except (httpx.HTTPError, ValueError):
        pass
    return None


def detect(profile_name: str | None = None) -> QodTarget | None:
    """A reachable QoD manager, or None."""
    profile = load_profile(qod_config_path(), profile_name)
    candidate = resolve_target(profile, None)
    if not is_ready(candidate.manager_url):
        return None
    token = os.environ.get("QOD_TOKEN") or profile.get("token")
    return resolve_target(profile, fetch_edge(candidate.manager_url, token))
