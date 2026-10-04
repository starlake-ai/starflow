from pathlib import Path

import httpx
import pytest

from starflow_cli import launcher, main, qod


def test_parse_installed_version_unix_and_windows():
    sh = "#!/bin/bash\nset -e\n\nENABLE_DUCKDB=${ENABLE_DUCKDB:-true}\nSL_VERSION=${SL_VERSION:-1.8.9}\n"
    cmd = '@ECHO OFF\n\nif "%SL_VERSION%"=="" (\n    SET SL_VERSION=1.8.9\n)\n'
    assert launcher.parse_installed_version(sh) == "1.8.9"
    assert launcher.parse_installed_version(cmd) == "1.8.9"
    assert launcher.parse_installed_version("nothing here") is None


def _fake_install(home: Path, version: str) -> None:
    (home / "bin" / "sl").mkdir(parents=True)
    (home / "bin" / "api").mkdir(parents=True)
    (home / "starlake.sh").write_text("#!/bin/bash\n")
    (home / "versions.sh").write_text(f"SL_VERSION=${{SL_VERSION:-{version}}}\n")


def test_is_installed_requires_matching_version_and_binaries(tmp_path):
    assert not launcher.is_installed(tmp_path, "1.8.9", os_name="darwin")
    _fake_install(tmp_path, "1.8.8")
    assert not launcher.is_installed(tmp_path, "1.8.9", os_name="darwin")
    assert launcher.is_installed(tmp_path, "1.8.8", os_name="darwin")


def test_resolve_version_precedence(tmp_path, monkeypatch):
    assert launcher.resolve_version("1.8.9", "1.7.6", tmp_path) == "1.7.6"
    assert launcher.resolve_version("1.8.9", None, tmp_path) == "1.8.9"
    monkeypatch.setattr(launcher, "latest_release_version", lambda: "1.9.0")
    assert launcher.resolve_version("0.0.0.dev0", None, tmp_path) == "1.9.0"


def test_resolve_version_offline_falls_back_to_installed(tmp_path, monkeypatch):
    def offline():
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(launcher, "latest_release_version", offline)
    with pytest.raises(httpx.HTTPError):
        launcher.resolve_version("0.0.0.dev0", None, tmp_path)
    (tmp_path / "versions.sh").write_text("SL_VERSION=${SL_VERSION:-1.8.7}\n")
    if launcher.is_windows():
        pytest.skip("versions.cmd on windows")
    assert launcher.resolve_version("0.0.0.dev0", None, tmp_path) == "1.8.7"


def test_launcher_script_pinned_to_release_tag():
    assert launcher.launcher_script_url("1.8.9", "linux").endswith("/v1.8.9/distrib/starlake.sh")
    assert launcher.launcher_script_url("1.8.9", "win32").endswith("/v1.8.9/distrib/starlake.cmd")
    assert "/master/" in launcher.launcher_script_url("1.8.9-SNAPSHOT", "linux")


def test_install_env_disables_unrequested_connectors_but_user_wins(tmp_path):
    env = launcher.install_env({"ENABLE_SNOWFLAKE": "true"}, "1.8.9", tmp_path)
    assert env["ENABLE_ALL"] == "false"
    assert env["ENABLE_DUCKDB"] == env["ENABLE_FLIGHTSQL"] == "true"
    assert env["ENABLE_BIGQUERY"] == "false"
    assert env["ENABLE_SNOWFLAKE"] == "true"
    assert env["SL_VERSION"] == "1.8.9"
    assert env["JAVA_HOME"] == str(tmp_path)


def test_parse_java_major():
    assert launcher.parse_java_major('openjdk version "17.0.12" 2024-07-16') == 17
    assert launcher.parse_java_major('java version "1.8.0_402"') == 8
    assert launcher.parse_java_major("garbage") is None


def test_adoptium_url():
    url = launcher.adoptium_assets_url("darwin", "arm64")
    assert "/latest/17/" in url and "architecture=aarch64" in url and "os=mac" in url
    assert "image_type=jre" in url


# --- qod -----------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_qod_env(monkeypatch):
    for var in ("QOD_MANAGER_URL", "QOD_HOST", "QOD_PORT", "QOD_TLS", "QOD_TLS_VERIFY",
                "QOD_PROFILE", "QOD_TOKEN", "QOD_CONFIG_FILE"):
        monkeypatch.delenv(var, raising=False)


def test_load_profile_uses_sticky_default(tmp_path):
    cfg = tmp_path / "config.toml"
    cfg.write_text(
        'default_profile = "demo"\n'
        '[profiles.demo]\nmanager_url = "http://qod:20900"\n'
        '[profiles.other]\nmanager_url = "http://other:20900"\n'
    )
    assert qod.load_profile(cfg, None)["manager_url"] == "http://qod:20900"
    assert qod.load_profile(cfg, "other")["manager_url"] == "http://other:20900"
    assert qod.load_profile(tmp_path / "missing.toml", None) == {}


def test_resolve_target_defaults_to_local_tls_edge():
    t = qod.resolve_target({}, None)
    assert t.manager_url == "http://localhost:20900"
    assert t.flight_url == "https://localhost:31338"
    assert t.tls_insecure
    assert t.env() == {
        "QOD_ENABLED": "true",
        "QOD_URL": "http://localhost:20900",
        "QOD_FLIGHT_URL": "https://localhost:31338",
        "QOD_TLS_INSECURE": "true",
    }


def test_resolve_target_edge_answer_wins_and_wildcard_host_maps_to_manager():
    profile = {"manager_url": "http://box:20900/", "edge_host": "old", "edge_port": 1}
    edge = {"flightSqlHost": "0.0.0.0", "flightSqlPort": 4000, "flightSqlTls": False}
    t = qod.resolve_target(profile, edge)
    assert t.manager_url == "http://box:20900"
    assert t.flight_url == "http://box:4000"
    assert not t.tls_insecure
    assert "QOD_TLS_INSECURE" not in t.env()


def test_detect_returns_none_when_manager_down(monkeypatch, tmp_path):
    monkeypatch.setenv("QOD_CONFIG_FILE", str(tmp_path / "none.toml"))
    monkeypatch.setattr(qod, "is_ready", lambda url, timeout_s=2.0: False)
    assert qod.detect() is None


def test_detect_uses_profile_and_edge(monkeypatch, tmp_path):
    cfg = tmp_path / "config.toml"
    cfg.write_text('[profiles.default]\nmanager_url = "http://m:1"\ntoken = "tok"\n')
    monkeypatch.setenv("QOD_CONFIG_FILE", str(cfg))
    monkeypatch.setattr(qod, "is_ready", lambda url, timeout_s=2.0: url == "http://m:1")
    seen = {}

    def edge(url, token, timeout_s=2.0):
        seen["token"] = token
        return {"flightSqlHost": "m", "flightSqlPort": 31338, "flightSqlTls": True}

    monkeypatch.setattr(qod, "fetch_edge", edge)
    t = qod.detect()
    assert t.flight_url == "https://m:31338"
    assert seen["token"] == "tok"


def test_serve_env_sets_port_java_and_qod(tmp_path):
    target = qod.QodTarget("http://m:1", "https://m:2", True)
    env = main.serve_env({"PATH": "/bin"}, tmp_path, 9100, target)
    assert env["SL_API_HTTP_PORT"] == "9100"
    assert env["JAVA_HOME"] == str(tmp_path)
    assert env["QOD_URL"] == "http://m:1"
    assert "QOD_ENABLED" not in main.serve_env({}, tmp_path, 9100, None)
