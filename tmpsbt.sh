#!/usr/bin/env bash
set -euo pipefail

# Copy targets are named after the version being built, so derive it from
# version.sbt: the LOCAL_STARLAKE_VERSION env var goes stale in shells opened
# before a release bumped ~/.bash_profile.
SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
LOCAL_STARLAKE_VERSION="$(sed -n 's/.*"\(.*\)".*/\1/p' "$SCRIPT_DIR/version.sbt")"
echo "Building starlake-core $LOCAL_STARLAKE_VERSION"

sbt ++2.13 clean package assembly

rm $HOME/starlake/bin/sl/starlake-core_2.13-*-assembly.jar || true
#cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/starlake-app/bin/sl/
rm $HOME/git/starlake-api/lib/starlake-core_2.13-*-assembly.jar || true
rm $HOME/git/starlake-api/tmpbuild/starlake/bin/sl/starlake-core_2.13-*-assembly.jar || true
rm $HOME/starlake/bin/api/lib/starlake-core_2.13-*-assembly.jar || true

cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/starlake/bin/sl/
#cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/starlake-app/bin/sl/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/git/starlake-api/lib/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}-assembly.jar $HOME/git/starlake-api/tmpbuild/starlake/bin/sl/
cp $HOME/git/public/starlake/target/scala-2.13/starlake-core_2.13-${LOCAL_STARLAKE_VERSION}.jar $HOME/starlake/bin/api/lib/

