/*
 * autoreidex @ Pavel Stehule, 2016
 *
 * This function does hard work
 *
 */

#include "postgres.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/resowner.h"

#include "autoreindex.h"

/*
 * run statements via sql exec */
void
reindex_current_database(int options)
{
	ResourceOwner ro = CurrentResourceOwner;
	MemoryContext tmpcxt;
	MemoryContext oldcxt;

	char *cmdstr =
					"do $$\n"
					"begin\n"
					"  raise log 'from do statement on % database', current_database();\n"
					"  perform pg_sleep(4);\n"
					"  raise log 'plpgsql ended';\n"
					"end $$";

	tmpcxt =  AllocSetContextCreate(CurrentMemoryContext,
																  "Short life background worker context",
															 ALLOCSET_DEFAULT_MINSIZE,
															 ALLOCSET_DEFAULT_INITSIZE,
															 ALLOCSET_DEFAULT_MAXSIZE);

	oldcxt = MemoryContextSwitchTo(tmpcxt);

	get_bloated_indexes_oid(0.0, 0.0);

	MemoryContextSwitchTo(tmpcxt);
	CurrentResourceOwner = ro;

	autoreindex_execute_single_sql_command(cmdstr, true, tmpcxt);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(tmpcxt);

	CurrentResourceOwner = ro;
}

