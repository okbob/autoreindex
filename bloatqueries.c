/*
 * autoreidex @ Pavel Stehule, 2016
 *
 * Resource file - SQL queries builder
 *
 */

#include "postgres.h"

#include "lib/stringinfo.h"

#include "autoreindex.h"

char *
bloat_indexes_query(bool with_limits)
{
	char *basequery = 
		"SELECT current_database(), nspname AS schemaname, tblname, table_oid, idxname::text, indexrelid, indisprimary, bs*(relpages)::bigint AS real_size,"
		" bs*(relpages-est_pages)::bigint AS extra_size,"
		" 100 * (relpages-est_pages)::float / relpages AS extra_ratio,"
		" fillfactor, bs*(relpages-est_pages_ff) AS bloat_size,"
		" 100 * (relpages-est_pages_ff)::float / relpages AS bloat_ratio,"
		" is_na"
		" FROM ("
		" SELECT coalesce(1 +"
		" ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0"
		" ) AS est_pages,"
		" coalesce(1 +"
		" ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0"
		" ) AS est_pages_ff,"
		" bs, nspname, table_oid, tblname, idxname, indexrelid, indisprimary, relpages, fillfactor, is_na"
		" FROM ("
		" SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, table_oid, fillfactor, indexrelid, indisprimary,"
		" ( index_tuple_hdr_bm +"
		" maxalign - CASE"
		" WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign"
		" ELSE index_tuple_hdr_bm%maxalign"
		" END"
		" + nulldatawidth + maxalign - CASE"
		" WHEN nulldatawidth = 0 THEN 0"
		" WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign"
		" ELSE nulldatawidth::integer%maxalign"
		" END"
		" )::numeric AS nulldatahdrwidth, pagehdr, pageopqdata, is_na"
		" FROM ("
		" SELECT"
		" indexrelid, indisprimary, i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid AS table_oid,"
		" current_setting(\'block_size\')::numeric AS bs, fillfactor,"
		" CASE"
		" WHEN version() ~ \'mingw32\' OR version() ~ \'64-bit|x86_64|ppc64|ia64|amd64\' THEN 8"
		" ELSE 4"
		" END AS maxalign,"
		" 24 AS pagehdr,"
		" 16 AS pageopqdata,"
		" CASE WHEN max(coalesce(s.null_frac,0)) = 0"
		" THEN 2"
		" ELSE 2 + (( 32 + 8 - 1 ) / 8)"
		" END AS index_tuple_hdr_bm,"
		" sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth,"
		" max( CASE WHEN a.atttypid = \'pg_catalog.name\'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na"
		" FROM pg_attribute AS a"
		" JOIN ("
		" SELECT nspname, tbl.relname AS tblname, idx.relname AS idxname, idx.reltuples, idx.relpages, idx.relam,"
		" indrelid, indexrelid, indisprimary, indkey::smallint[] AS attnum,"
		" coalesce(substring("
		" array_to_string(idx.reloptions, \' \')"
		" from \'fillfactor=([0-9]+)\')::smallint, 90) AS fillfactor"
		" FROM pg_index"
		" JOIN pg_class idx ON idx.oid=pg_index.indexrelid"
		" JOIN pg_class tbl ON tbl.oid=pg_index.indrelid"
		" JOIN pg_namespace ON pg_namespace.oid = idx.relnamespace"
		" WHERE pg_index.indisvalid AND tbl.relkind = \'r\' AND idx.relpages > 0"
		" ) AS i ON a.attrelid = i.indexrelid"
		" JOIN pg_stats AS s ON s.schemaname = i.nspname"
		" AND ((s.tablename = i.tblname AND s.attname = pg_catalog.pg_get_indexdef(a.attrelid, a.attnum, TRUE))"
		" OR   (s.tablename = i.idxname AND s.attname = a.attname))"
		" JOIN pg_type AS t ON a.atttypid = t.oid"
		" WHERE a.attnum > 0"
		" GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11"
		" ) AS s1"
		" ) AS s2"
		" JOIN pg_am am ON s2.relam = am.oid WHERE am.amname = \'btree\'"
		") AS sub"
		" ORDER BY 2,3,4";

		if (with_limits)
		{
			StringInfoData			str;

			initStringInfo(&str);
			appendStringInfo(&str, "SELECT * FROM (%s) sub2 WHERE bloat_size >= $1 AND bloat_ratio >= $2", basequery);
			return str.data;
		}
			return basequery;
}
