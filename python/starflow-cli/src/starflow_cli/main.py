"""`starflow` command line: `starflow serve` runs the Starflow UI."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from . import __version__, launcher, qod

DEFAULT_PORT = 9900


def _echo(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def serve_env(base_env: dict, java_home: Path, port: int, target: qod.QodTarget | None) -> dict:
    env = dict(base_env)
    env["JAVA_HOME"] = str(java_home)
    env["SL_API_HTTP_PORT"] = str(port)
    if target is not None:
        env.update(target.env())
    return env


def serve(args: argparse.Namespace) -> int:
    home = Path(args.home).expanduser() if args.home else launcher.default_home()
    version = launcher.resolve_version(
        __version__, args.sl_version or os.environ.get("SL_VERSION"), home
    )

    java_home = launcher.find_java_home(home)
    if java_home is None:
        _echo(f"No Java {launcher.MIN_JAVA_MAJOR}+ found; installing a Temurin JRE into {home / 'jdk'}")
        java_home = launcher.ensure_jre(home)

    if not launcher.is_installed(home, version):
        current = launcher.installed_version(home)
        action = f"Upgrading Starflow {current} to" if current else "Installing Starflow"
        _echo(f"{action} {version} into {home} (first run downloads about 1 GB)")
        launcher.install(home, version, java_home)

    target = None
    if args.no_qod:
        pass
    elif os.environ.get("QOD_ENABLED"):
        _echo("QOD_ENABLED is set: using the QOD_* settings from the environment")
    else:
        target = qod.detect(args.qod_profile)
        if target is None:
            _echo("No Quack on Demand manager detected: starting Starflow standalone")
        else:
            _echo(
                f"Quack on Demand detected at {target.manager_url} "
                f"(Flight SQL {target.flight_url}): sign in with your QoD credentials"
            )

    _echo(f"Starflow {version} UI: http://localhost:{args.port}")
    env = serve_env(dict(os.environ), java_home, args.port, target)
    cmd = launcher.script_command(launcher.launcher_script(home), "serve")
    if launcher.is_windows():
        return subprocess.call(cmd, cwd=home, env=env)
    os.chdir(home)
    os.execvpe(cmd[0], cmd, env)
    return 0  # unreachable


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="starflow", description="Starlake Starflow launcher")
    parser.add_argument("--version", action="version", version=f"starflow {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser(
        "serve",
        help="Run the Starflow UI, installing Starflow on first use",
        description=(
            "Run the Starflow UI and api. The first run installs Starflow (and a Java "
            "runtime when none is found) into the Starflow home. A running Quack on "
            "Demand manager is detected and wired in automatically."
        ),
    )
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"HTTP port (default {DEFAULT_PORT})")
    p.add_argument(
        "--home",
        help="Install and data directory (default: STARFLOW_HOME, else the platform user data dir)",
    )
    p.add_argument(
        "--sl-version",
        help="Starflow release to run (default: the release matching this launcher, else the latest)",
    )
    p.add_argument("--no-qod", action="store_true", help="Do not look for a Quack on Demand manager")
    p.add_argument("--qod-profile", help="qod CLI profile holding the manager URL (default: qod's own default)")
    p.set_defaults(func=serve)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return 130
    except Exception as e:  # noqa: BLE001 - one-line error instead of a traceback
        _echo(f"error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
