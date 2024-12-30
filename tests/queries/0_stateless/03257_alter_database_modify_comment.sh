#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=alterdb1

${CLICKHOUSE_CLIENT} <<EOF
DROP DATABASE IF EXISTS $db;
CREATE DATABASE $db ENGINE=Atomic COMMENT 'COMMENT 1';
SHOW CREATE DATABASE $db;
ALTER DATABASE $db MODIFY COMMENT 'COMMENT 2';
SHOW CREATE DATABASE $db;
EOF

datadir=$(${CLICKHOUSE_CLIENT} --query "SELECT data_path FROM system.databases WHERE name = '$db'")
rootdir=$(dirname "$datadir")
dbsql="$rootdir/metadata/$db.sql"
grep COMMENT "$dbsql"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE $db;"

