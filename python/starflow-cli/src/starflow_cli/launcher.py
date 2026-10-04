"""Provision and launch a Starflow install for `starflow serve`.

The install lives in a single durable home (projects and the api's H2 database
sit next to the binaries, so it is a data dir, not a cache). It is provisioned
by the same machinery as setup.sh: the launcher script (starlake.sh or
starlake.cmd) is fetched from the release tag and its `install` runs Setup.java,
which pulls the core jar, Spark, the connector jars and the api zip (UI
included). Upgrading only re-runs `install` with the new SL_VERSION; projects
and the H2 database are left alone.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import httpx

GITHUB_REPO = "starlake-ai/starflow"

# Matches setup.sh's ensure_java for 1.5+ releases.
MIN_JAVA_MAJOR = 17

# Connectors provisioned for a `uvx starflow serve` install: DuckDB and Flight SQL
# for QoD, PostgreSQL for the common JDBC case. Setup.java treats every unset
# ENABLE_* as true, so the rest are switched off explicitly. Spark is always
# installed (the core jar runs on its classpath). A user's own ENABLE_* wins.
CONNECTOR_FLAGS = {
    "ENABLE_ALL": "false",
    "ENABLE_DUCKDB": "true",
    "ENABLE_FLIGHTSQL": "true",
    "ENABLE_POSTGRESQL": "true",
    "ENABLE_API": "true",
    "ENABLE_BIGQUERY": "false",
    "ENABLE_AZURE": "false",
    "ENABLE_SNOWFLAKE": "false",
    "ENABLE_REDSHIFT": "false",
    "ENABLE_KAFKA": "false",
    "ENABLE_MARIA": "false",
    "ENABLE_CLICKHOUSE": "false",
    "ENABLE_TRINODB": "false",
}

_RELEASE_RE = re.compile(r"^\d+\.\d+\.\d+$")
_JAVA_VERSION_RE = re.compile(r'version "(\d+)(?:\.(\d+))?')
# versions.sh: SL_VERSION=${SL_VERSION:-1.8.9}; versions.cmd: SET SL_VERSION=1.8.9
_SH_VERSION_RE = re.compile(r"^SL_VERSION=\$\{SL_VERSION:-([^}]+)\}", re.M)
_CMD_VERSION_RE = re.compile(r"^\s*SET SL_VERSION=(\S+)", re.M | re.I)


class IntegrityError(RuntimeError):
    """Downloaded artifact does not match its published sha256."""


def is_windows(os_name: str = sys.platform) -> bool:
    return os_name == "win32"


def default_home() -> Path:
    """`STARFLOW_HOME` or the platform user data dir."""
    env_home = os.environ.get("STARFLOW_HOME")
    if env_home:
        return Path(env_home).expanduser()
    import platformdirs

    return Path(platformdirs.user_data_dir("starflow", appauthor=False))


# --- version resolution ------------------------------------------------------


def is_release_version(version: str) -> bool:
    return bool(_RELEASE_RE.match(version))


def latest_release_version() -> str:
    resp = httpx.get(
        f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
        follow_redirects=True,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["tag_name"].lstrip("v")


def resolve_version(cli_version: str, override: str | None, home: Path) -> str:
    """Starflow version to run: an explicit override, else the release matching
    this launcher, else (dev launcher) the latest GitHub release, falling back to
    what is already installed when GitHub is unreachable."""
    if override:
        return override
    if is_release_version(cli_version):
        return cli_version
    try:
        return latest_release_version()
    except httpx.HTTPError:
        installed = installed_version(home)
        if installed:
            return installed
        raise


def launcher_script_ref(version: str) -> str:
    """Git ref the launcher script is fetched from. A release pins its own tag so
    the script matches the core jar; SNAPSHOTs and local builds have no tag."""
    return f"v{version}" if is_release_version(version) else "master"


# --- install state -----------------------------------------------------------


def launcher_script(home: Path, os_name: str = sys.platform) -> Path:
    return home / ("starlake.cmd" if is_windows(os_name) else "starlake.sh")


def parse_installed_version(text: str) -> str | None:
    m = _SH_VERSION_RE.search(text) or _CMD_VERSION_RE.search(text)
    return m.group(1).strip() if m else None


def installed_version(home: Path, os_name: str = sys.platform) -> str | None:
    versions = home / ("versions.cmd" if is_windows(os_name) else "versions.sh")
    if not versions.is_file():
        return None
    return parse_installed_version(versions.read_text(errors="replace"))


def is_installed(home: Path, version: str, os_name: str = sys.platform) -> bool:
    return (
        installed_version(home, os_name) == version
        and launcher_script(home, os_name).is_file()
        and (home / "bin" / "sl").is_dir()
        and (home / "bin" / "api").is_dir()
    )


# --- java --------------------------------------------------------------------


def parse_java_major(version_line: str) -> int | None:
    """Major version from `java -version` output; `1.8` style maps to 8."""
    m = _JAVA_VERSION_RE.search(version_line)
    if not m:
        return None
    major = int(m.group(1))
    if major == 1 and m.group(2):
        return int(m.group(2))
    return major


def java_major(java_path: str) -> int | None:
    try:
        proc = subprocess.run([java_path, "-version"], capture_output=True, text=True, timeout=30)
    except OSError:
        return None
    # java -version historically prints to stderr.
    return parse_java_major(proc.stderr or proc.stdout)


def _java_exe(java_home: Path) -> Path:
    return java_home / "bin" / ("java.exe" if is_windows() else "java")


def find_java_home(home: Path, min_major: int = MIN_JAVA_MAJOR) -> Path | None:
    """A JAVA_HOME recent enough to run Starflow: JAVA_HOME first, then the java
    on PATH, then the JRE a previous run embedded under <home>/jdk (the same
    location starlake.sh falls back to)."""
    candidates = []
    if os.environ.get("JAVA_HOME"):
        candidates.append(Path(os.environ["JAVA_HOME"]))
    on_path = shutil.which("java")
    if on_path:
        # PATH entries are often symlinks (/usr/bin/java); resolve to the real home.
        candidates.append(Path(on_path).resolve().parent.parent)
    candidates.append(home / "jdk")
    for java_home in candidates:
        exe = _java_exe(java_home)
        if exe.is_file():
            major = java_major(str(exe))
            if major is not None and major >= min_major:
                return java_home
    return None


_ADOPTIUM_OS = {"darwin": "mac", "linux": "linux", "win32": "windows"}
_ADOPTIUM_ARCH = {"x86_64": "x64", "amd64": "x64", "arm64": "aarch64", "aarch64": "aarch64"}


def adoptium_assets_url(os_name: str, arch: str, major: int = MIN_JAVA_MAJOR) -> str:
    return (
        f"https://api.adoptium.net/v3/assets/latest/{major}/hotspot"
        f"?architecture={_ADOPTIUM_ARCH[arch.lower()]}&image_type=jre"
        f"&os={_ADOPTIUM_OS[os_name]}&vendor=eclipse"
    )


def _download(url: str, dest: Path, label: str) -> str:
    """Stream `url` to `dest`, returning the sha256 of what was written."""
    digest = hashlib.sha256()
    with httpx.stream("GET", url, follow_redirects=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0)) or None
        with dest.open("wb") as out:
            if sys.stderr.isatty():
                from rich.progress import Progress

                with Progress() as progress:
                    task = progress.add_task(f"downloading {label}", total=total)
                    for chunk in resp.iter_bytes():
                        out.write(chunk)
                        digest.update(chunk)
                        progress.advance(task, len(chunk))
            else:
                for chunk in resp.iter_bytes():
                    out.write(chunk)
                    digest.update(chunk)
    return digest.hexdigest()


def _extracted_java_home(root: Path) -> Path | None:
    # Temurin layouts: <top>/bin/java (linux, windows) and <top>/Contents/Home/bin/java (mac).
    for top in sorted(p for p in root.iterdir() if p.is_dir()):
        for java_home in (top / "Contents" / "Home", top):
            if _java_exe(java_home).is_file():
                return java_home
    return None


def ensure_jre(home: Path, os_name: str = sys.platform, arch: str | None = None) -> Path:
    """Download a Temurin JRE into <home>/jdk (checksum-verified from the Adoptium
    metadata) and return it as a JAVA_HOME."""
    import platform
    import tarfile
    import tempfile
    import zipfile

    if os_name not in _ADOPTIUM_OS:
        raise RuntimeError(f"unsupported OS {os_name}: install Java {MIN_JAVA_MAJOR}+ and set JAVA_HOME")
    arch = arch if arch is not None else platform.machine()
    if arch.lower() not in _ADOPTIUM_ARCH:
        raise RuntimeError(f"unsupported architecture {arch}: install Java {MIN_JAVA_MAJOR}+ and set JAVA_HOME")

    resp = httpx.get(adoptium_assets_url(os_name, arch), follow_redirects=True, timeout=30)
    resp.raise_for_status()
    package = resp.json()[0]["binary"]["package"]

    home.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=home, prefix=".jre-") as tmp:
        tmp_dir = Path(tmp)
        suffix = ".zip" if package["link"].endswith(".zip") else ".tar.gz"
        archive = tmp_dir / f"temurin-jre{suffix}"
        actual = _download(package["link"], archive, f"Temurin {MIN_JAVA_MAJOR} JRE")
        if actual != package["checksum"]:
            raise IntegrityError(
                f"sha256 mismatch for the Temurin JRE: expected {package['checksum']}, got {actual}"
            )
        unpack = tmp_dir / "unpack"
        unpack.mkdir()
        if suffix == ".zip":
            with zipfile.ZipFile(archive) as z:
                z.extractall(unpack)
        else:
            with tarfile.open(archive) as tar:
                try:
                    tar.extractall(unpack, filter="data")
                except TypeError:  # filter= needs >= 3.10.12; floor is 3.10.0
                    tar.extractall(unpack)
        java_home = _extracted_java_home(unpack)
        if java_home is None:
            raise RuntimeError("no bin/java found after extracting the Temurin JRE")
        target = home / "jdk"
        shutil.rmtree(target, ignore_errors=True)
        shutil.move(str(java_home), str(target))
    return target


# --- install -----------------------------------------------------------------


def launcher_script_url(version: str, os_name: str = sys.platform) -> str:
    name = "starlake.cmd" if is_windows(os_name) else "starlake.sh"
    return (
        f"https://raw.githubusercontent.com/{GITHUB_REPO}/"
        f"{launcher_script_ref(version)}/distrib/{name}"
    )


def install_env(base_env: dict, version: str, java_home: Path) -> dict:
    env = dict(base_env)
    for key, value in CONNECTOR_FLAGS.items():
        env.setdefault(key, value)
    env["SL_VERSION"] = version
    env["JAVA_HOME"] = str(java_home)
    return env


def script_command(script: Path, *args: str, os_name: str = sys.platform) -> list[str]:
    if is_windows(os_name):
        return ["cmd.exe", "/c", str(script), *args]
    return ["bash", str(script), *args]


def install(home: Path, version: str, java_home: Path) -> None:
    """Fetch the launcher script pinned to `version` and run its `install`.
    Re-running over an older install upgrades it in place."""
    home.mkdir(parents=True, exist_ok=True)
    script = launcher_script(home)
    resp = httpx.get(launcher_script_url(version), follow_redirects=True, timeout=60)
    resp.raise_for_status()
    script.write_bytes(resp.content)
    script.chmod(0o755)
    env = install_env(dict(os.environ), version, java_home)
    rc = subprocess.call(script_command(script, "install"), cwd=home, env=env)
    if rc != 0:
        raise RuntimeError(f"Starflow {version} install failed (exit {rc}); see the output above")
    if not is_installed(home, version):
        raise RuntimeError(
            f"Starflow {version} install finished but {home} is incomplete "
            "(missing versions file, bin/sl or bin/api)"
        )
