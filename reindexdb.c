/*
 * autoreidex @ Pavel Stehule, 2016
 *
 * This function does hard work
 *
 */

#include "postgres.h"
#include "pgstat.h"
#include "storage/spin.h"
#include "utils/palloc.h"
#include "utils/resowner.h"

#include "autoreindex.h"

/*
 * This routine is executed only one time, so there are no necessary to create temporary
 * memory context. When this function finish, then the process will be terminated. 
 */
void
reindex_current_database(int options,  slock_t *mutex, WorkerState *worker_state, WorkerState *states)
{
	ResourceOwner ro = CurrentResourceOwner;
	MemoryContext mcxt = CurrentMemoryContext;
	List		*candidates;
	ListCell	*lc;

	char *cmdstr =
					"do $$\n"
					"begin\n"
					"  raise log 'from do statement on % database', current_database();\n"
					"  perform pg_sleep(4);\n"
					"  raise log 'plpgsql ended';\n"
					"end $$";

	candidates = get_bloated_indexes_oid(0.0, 0.0);

	MemoryContextSwitchTo(mcxt);
	CurrentResourceOwner = ro;

	foreach(lc, candidates)
	{
		autoreindex_index_desc *idx_desc;
		bool		is_valid;
		bool		is_system;

		idx_desc = (autoreindex_index_desc *) lfirst(lc);
		is_system = is_system_class(idx_desc->indrel_id, &is_valid);

		if (!is_valid)
			continue;			/* dropped by other process */

		if (is_system || idx_desc->is_primary)
		{
			/* It is not supported yet */
			elog(LOG, "skip system index or primary index");
			continue;
		}


		elog(LOG, "index oid: %d, name: \"%s\", def: \"%s\"", idx_desc->index_id, idx_desc->indexname, get_indexdef(idx_desc->index_id));

		SpinLockAcquire(mutex);


		SpinLockRelease(mutex);

	}


	autoreindex_execute_single_sql_command(cmdstr, true, mcxt);
}

