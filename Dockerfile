# Pin the distro flavor: the default python3 must stay within the range
# supported by the bundled Spark's PySpark (noble ships Python 3.12).
FROM eclipse-temurin:25-noble

ARG BUILD_DATE
ARG VCS_REF
ARG SL_VERSION

# Labels.
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.build-date=$BUILD_DATE
LABEL org.label-schema.vendor="starlakeai"
LABEL org.label-schema.name="starlakeai/starlake"
LABEL org.label-schema.version=$SL_VERSION
LABEL org.label-schema.description="A declarative text based tool that enables analysts and engineers to extract, load, transform and orchestrate their data pipelines."
LABEL org.label-schema.url="https://starlake.ai"
LABEL org.label-schema.vcs-url="https://github.com/starlake-ai/starlake"
LABEL org.label-schema.vcs-ref=$VCS_REF

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        findutils \
        jq \
        unzip \
        curl \
        bash \
        vim \
        python3 \
        python3-pip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# PythonRunner spawns this interpreter for PySpark transform tasks
ENV PYSPARK_PYTHON=python3

COPY starlake/bin /app/bin
COPY starlake/starlake.sh /app/starlake.sh
COPY starlake/versions.sh /app/versions.sh

# The archive of classes that cuts the startup of every command (AppCDS in starlake.sh). Containers
# usually run as a user who cannot write to /app, so it has to be built here. Like at install time,
# a failed build only makes startup slower.
RUN /app/starlake.sh appcds-build || true

ENTRYPOINT ["/app/starlake.sh"]
