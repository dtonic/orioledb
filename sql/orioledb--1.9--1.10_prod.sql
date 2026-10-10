/* contrib/orioledb/sql/orioledb--1.9--1.10_prod.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION orioledb UPDATE TO '1.10'" to load this file. \quit

CREATE FUNCTION orioledb_s3_compact(rel regclass, threshold float8 DEFAULT 0.3)
RETURNS bigint
AS 'MODULE_PATHNAME'
VOLATILE STRICT LANGUAGE C;
REVOKE ALL ON FUNCTION orioledb_s3_compact(regclass, float8) FROM PUBLIC;

CREATE FUNCTION orioledb_s3_status(
	OUT role text,
	OUT token bigint,
	OUT lease_remaining float8,
	OUT root_checkpoint bigint,
	OUT wal_flush_lsn pg_lsn,
	OUT wal_shipped_lsn pg_lsn,
	OUT wal_ship_lag bigint,
	OUT wal_shipper_running bool,
	OUT gc_retain bigint,
	OUT gc_folded bigint,
	OUT gc_collected bigint,
	OUT unsealed_parts bigint)
RETURNS record
AS 'MODULE_PATHNAME'
VOLATILE LANGUAGE C;
