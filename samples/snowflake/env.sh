
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
export SL_CONNECTION=snowflake

source ./.snowflake-env

if [[ -z "$SNOWFLAKE_ACCOUNT" ]] || [[ -z "$SNOWFLAKE_USER" ]] || [[ -z "$SNOWFLAKE_PASSWORD" ]] || [[ -z "$SNOWFLAKE_WAREHOUSE" ]] || [[ -z "$SNOWFLAKE_DB" ]] || [[ -z "$SNOWFLAKE_SCHEMA" ]]; then
    echo "Must provide SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DB, SNOWFLAKE_SCHEMA in environment" 1>&2
    exit 1
fi

