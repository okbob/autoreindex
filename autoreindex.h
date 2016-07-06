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
	Oid		dboid;
	char	dbname[NAMEDATALEN];
} DatabaseDesc;

/*
 * Worker state holds result of worker execution. It is reseted
 * by controller (is_done = false) before worker start, and it
 * filled by worker when worker is ending. This structure is in
 * shared memory. Workers get possition of in the array of
 * WorkerStates.
 */
#define			MAX_ERROR_MESSAGE_LEN		1024

typedef struct
{
	/* ----------------------------- */
	bool		is_active;
	Oid			dboid;
	Oid			index_oid;
	/* ---- updated under locks ---- */

	bool	is_valid;
	int		stat_processed_tables;

	int		exitcode;
	int		sqlstate;
	char	errormessage[MAX_ERROR_MESSAGE_LEN];
} WorkerState;


/* utils.c */
void autoreindex_execute_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
void autoreindex_execute_single_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt);
List *get_database_list(void);
List *get_bloated_indexes_oid(float bloat_size_limit, float bloat_ratio_limit);
bool is_system_class(Oid reloid, bool *is_valid);
char *get_indexdef(Oid indexid);


/* reindexdb.c */
void reindex_current_database(int options, slock_t *mutex, WorkerState *worker_state, WorkerState *states);

/* bloatqueries.c */
char *bloat_indexes_query(bool with_limits);

