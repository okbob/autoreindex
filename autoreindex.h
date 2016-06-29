/*
 * autoreidex @ Pavel Stehule, 2016
 *
 * Public declarations
 *
 */

#include "nodes/pg_list.h"

/* utils.c */
void autoreindex_execute_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
void autoreindex_execute_single_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
List *get_database_list(void);
List *get_bloated_indexes_oid(float bloat_size_limit, float bloat_ratio_limit);

/* reindexdb.c */
void reindex_current_database(int options);

/* bloatqueries.c */
char *bloat_indexes_query(bool with_limits);

