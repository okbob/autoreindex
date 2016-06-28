
#include "nodes/pg_list.h"

/* utils.c */
void autoreindex_execute_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
void autoreindex_execute_single_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
List *get_database_list(void);

/* reindexdb.c */
void reindex_current_database(int options);
