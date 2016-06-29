/*
 * autoreidex @ Pavel Stehule, 2016
 *
 * Public declarations
 *
 */

#include "nodes/pg_list.h"

typedef struct
{
	Oid			index_id;
	Oid			indrel_id;
	bool		is_primary;
	char		*indexname;
} autoreindex_index_desc;

typedef struct
{
	Oid		db_oid;
	char	dbname[NAMEDATALEN];
} DatabaseDesc;

/* utils.c */
void autoreindex_execute_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
void autoreindex_execute_single_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
List *get_database_list(void);
List *get_bloated_indexes_oid(float bloat_size_limit, float bloat_ratio_limit);
bool is_system_class(Oid reloid, bool *is_valid);
char *get_indexdef(Oid indexid);


/* reindexdb.c */
void reindex_current_database(int options);

/* bloatqueries.c */
char *bloat_indexes_query(bool with_limits);

