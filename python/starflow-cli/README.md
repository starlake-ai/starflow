# starflow

Launcher for [Starlake Starflow](https://github.com/starlake-ai/starflow), the declarative data pipeline tool.

```bash
uvx starflow serve
```

The command starts the Starflow UI at http://localhost:9900. Nothing needs to be installed first.

On the first run it:

1. Finds Java 17+ (`JAVA_HOME`, then `PATH`). If there is none, it downloads a Temurin JRE into the Starflow home.
2. Installs (about 1 GB) the Starflow release that matches this launcher's version into the Starflow home. This covers the core, Spark, the DuckDB, Flight SQL and PostgreSQL connectors, and the UI. Later runs start right away. A newer launcher upgrades the install in place and keeps your projects.
3. Starts the UI and its api. The api stores its metadata in an embedded H2 database under the Starflow home.

## Quack on Demand

If a Quack on Demand manager is running, for example `uvx qod serve --demo`, Starflow detects it and runs in QoD mode. You sign in with your QoD credentials, and your QoD pools appear as connections.

Detection reads the profile that `qod serve` / `qod login` saved: `QOD_CONFIG_FILE` or the qod config dir, honouring `QOD_PROFILE`. If there is no profile, it tries `http://localhost:20900`. Set `QOD_ENABLED`/`QOD_URL`/`QOD_FLIGHT_URL` yourself to skip detection, or pass `--no-qod` to start standalone.

## Options

| Option | Default | |
|---|---|---|
| `--port` | `9900` | HTTP port of the UI |
| `--home` | `STARFLOW_HOME`, else the platform user data dir | Install, projects and H2 database |
| `--sl-version` | this launcher's version (`SL_VERSION` also works) | Starflow release to run |
| `--qod-profile` | qod's default profile | qod profile to read the manager URL from |
| `--no-qod` | | Skip Quack on Demand detection |

Connector selection follows the usual `ENABLE_*` variables (e.g. `ENABLE_SNOWFLAKE=true`). They are read on install.
