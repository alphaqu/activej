#!/bin/bash

set -e

BASEDIR=$(dirname $0)
cd "${BASEDIR}"

database_name=${DATABASE_NAME:-cube}
server_name=${SERVER_NAME:-localhost}
min_revisions=${MIN_REVISIONS:-10}
cleanup_from=${CLEANUP_FROM:-"10 MINUTE"}

mysql -h $server_name $database_name -e "$(cat ../sql/cleanup.sql | sed -e "s/{min_revisions}/${min_revisions}/g; s/{cleanup_from}/${cleanup_from}/g")"