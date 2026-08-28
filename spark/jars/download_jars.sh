#!/usr/bin/env bash
# Downloads the JDBC drivers needed by the Spark load jobs (git-ignored --
# binaries, not source). Run once after cloning, before bringing up the stack.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

curl -sL -o postgresql-42.7.3.jar \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"

curl -sL -o clickhouse-jdbc-0.6.5-shaded.jar \
  "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.5/clickhouse-jdbc-0.6.5-shaded.jar"

echo "Downloaded: $(ls -1 *.jar)"
