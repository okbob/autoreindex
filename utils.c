/*
 * autoreidex @ Pavel Stehule, 2016
 *
 * auxilary SQL and catalogue related code
 *
 */

#include "postgres.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/elog.h"
#include "utils/snapmgr.h"

#include "autoreindex.h"

/*
 * Add context to the errors produced by autoreindex_execute_sql_command().
 *
 */
static void
execute_sql_command_error_cb(void *arg)
{
	errcontext("during execution of queued SQL statement: %s", (char *) arg);
}


/*
 * Run single sql command in transaction
 *
 */
void
autoreindex_execute_single_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt)
{
	StartTransactionCommand();

	autoreindex_execute_sql_command(cmdstr, isTopLevel, mcxt);

	CommitTransactionCommand();
}

/*
 * Execute any commands - like VACUUM, REINDEX, .. Result is dropped.
 * This code is taken from pglogical project.
 *
 */
void
autoreindex_execute_sql_command(char *cmdstr, bool isTopLevel, MemoryContext mcxt)
{
	List	   *commands;
	ListCell   *command_i;
	ErrorContextCallback errcallback;

	errcallback.callback = execute_sql_command_error_cb;
	errcallback.arg = cmdstr;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	MemoryContextSwitchTo(mcxt);

	commands = pg_parse_query(cmdstr);

	/*
	 * Do a limited amount of safety checking against CONCURRENTLY commands
	 * executed in situations where they aren't allowed. The sender side should
	 * provide protection, but better be safe than sorry.
	 */
	isTopLevel = isTopLevel && (list_length(commands) == 1);

	foreach(command_i, commands)
	{
		List	   *plantree_list;
		List	   *querytree_list;
		Node	   *command = (Node *) lfirst(command_i);
		const char *commandTag;
		Portal		portal;
		DestReceiver *receiver;

		/* temporarily push snapshot for parse analysis/planning */
		PushActiveSnapshot(GetTransactionSnapshot());

		commandTag = CreateCommandTag(command);

		querytree_list = pg_analyze_and_rewrite(
			command, cmdstr, NULL, 0);

		plantree_list = pg_plan_queries(
			querytree_list, 0, NULL);

		PopActiveSnapshot();

		portal = CreatePortal("autoreindex", true, true);
		PortalDefineQuery(portal, NULL,
						  cmdstr, commandTag,
						  plantree_list, NULL);
		PortalStart(portal, NULL, 0, InvalidSnapshot);

		receiver = CreateDestReceiver(DestNone);

		(void) PortalRun(portal, FETCH_ALL,
						 isTopLevel,
						 receiver, receiver,
						 NULL);
		(*receiver->rDestroy) (receiver);

		PortalDrop(portal, false);

		CommandCounterIncrement();

		MemoryContextSwitchTo(mcxt);
	}

	/* protect against stack resets during CONCURRENTLY processing */
	if (error_context_stack == &errcallback)
		error_context_stack = errcallback.previous;
}

/*
 * Attention - it reset resource owner - after the call of
 * this function, the resource owner should be set again.
 */
List *
get_database_list(void)
{
	Relation	rel;
	HeapScanDesc	scan;
	HeapTuple	tup;
	List		*result = NIL;

	MemoryContext top_ctx = CurrentMemoryContext;
	MemoryContext old_ctx;

	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT (tup);

		if (pgdatabase->datallowconn)
		{
			old_ctx = MemoryContextSwitchTo(top_ctx);
			result = lappend(result, pstrdup(NameStr(pgdatabase->datname)));
			MemoryContextSwitchTo(old_ctx);
		}
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	MemoryContextSwitchTo(top_ctx);

	return result;
}
