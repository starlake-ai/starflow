
if [[ -z "$SL_HOME" ]]
then
    echo "SL_HOME is not set. Please set SL_HOME to the root of your Starlake installation."
    exit 1
fi

if [[ -f "$SL_HOME/starlake.sh" ]] || [[ -f "$SL_HOME/starlake" ]]
then
    echo "SL_HOME is set to $SL_HOME"
else
    echo "SL_HOME is set to $SL_HOME, but it does not contain the Starlake CLI."
    exit 1
fi
export SL_ROOT="$(pwd)"
export SL_METRICS_ACTIVE=true
export SL_ASSERTIONS_ACTIVE=true
export SF_TIMEZONE=TIMESTAMP_LTZ
export SL_CONNECTION=postgres

source ./postgres-env

if [[ -z "$POSTGRES_DATABASE" ]] || [[ -z "$DATABASE_USER" ]] || [[ -z "$DATABASE_PASSWORD" ]] || [[ -z "$POSTGRES_HOST" ]] || [[ -z "$POSTGRES_PORT" ]]; then
    echo "Must provide POSTGRES_DATABASE, DATABASE_USER, DATABASE_PASSWORD, POSTGRES_HOST, POSTGRES_PORT in environment" 1>&2
    exit 1
fi

