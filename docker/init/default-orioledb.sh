#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "template1" <<-EOSQL
CREATE EXTENSION orioledb;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
CREATE EXTENSION orioledb;
EOSQL

# The maintenance database predates template1's extension too.  Without it,
# default_table_access_method = orioledb leaves it unable to create any table,
# a TEMP one included.
if [ "$POSTGRES_DB" != "postgres" ]; then
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
	CREATE EXTENSION orioledb;
	EOSQL
fi
