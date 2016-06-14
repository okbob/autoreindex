#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_database.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/shm_toc.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

#define AUTOREINDEX_SHM_MAGIC			0x730715fb

void _PG_init(void);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

static int			controller_max_workers = 4;
static int			max_workers_per_db = 1;

static bool 		verbose = false;

typedef struct
{
	char		dbname[64];
	bool		is_active;
	bool		is_fresh;
} WorkerProcSlot;

#define			MAX_ERROR_MESSAGE_LEN		1024

typedef struct
{
	bool		is_valid;

	int		stat_processed_tables;

	int		exitcode;
	int		sqlstate;
	char	errormessage[MAX_ERROR_MESSAGE_LEN];
} WorkerState;

typedef struct
{
	int		workers_total;
	int		workers_active;
	dsm_segment			*data;
	WorkerProcSlot				*worker_slots;
	WorkerState					*worker_states;
	BackgroundWorkerHandle		*worker_handles[FLEXIBLE_ARRAY_MEMBER];
} ControllerState;

static ControllerState		*controller;


#define			CONTROLLER_NAME			"autoreindex: controller"
#define			WORKER_NAME				"autoreindex: worker"

static ControllerState *setup_controller(dsm_segment *seg, WorkerProcSlot *slots, WorkerState *states, int nworkers);
static BackgroundWorkerHandle *launch_worker_internal(uint32 segment, int index);


/*
 * used by worker, pointers to shared memory.
 * Lock is not necessary.
 */
static WorkerProcSlot		*worker_slot = NULL;
static WorkerState			*worker_state = NULL;

/*
 * Signal processing
 */
static void
handler_sigusr1(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigusr1 = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

static void
handler_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
	errno = save_errno;
}

/*
 * Setup the shared memory segments. Creates toc and allocates nworkers and worker states.
 *
 * Note: it is initialized before start of any worker, and released when all
 * workers are terminated.
 */
static void
setup_dynamic_shared_memory(int nworkers)
{
	shm_toc_estimator	e;
	dsm_segment		*seg;
	shm_toc			*toc;

	Size			segsize;

	WorkerProcSlot		*slots;
	WorkerState			*states;

	/* Estimate how much shared memory we need */

	shm_toc_initialize_estimator(&e);

	shm_toc_estimate_chunk(&e, nworkers * sizeof(WorkerProcSlot));
	shm_toc_estimate_chunk(&e, nworkers * sizeof(WorkerState));

	shm_toc_estimate_keys(&e, 2);
	segsize = shm_toc_estimate(&e);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, CONTROLLER_NAME);

	seg = dsm_create(segsize, 0);
	memset(dsm_segment_address(seg), 0, segsize);
	toc = shm_toc_create(AUTOREINDEX_SHM_MAGIC, dsm_segment_address(seg), segsize);

	slots = shm_toc_allocate(toc, nworkers * sizeof(WorkerState));
	shm_toc_insert(toc, 0, slots);
	states = shm_toc_allocate(toc, nworkers * sizeof(WorkerProcSlot));
	shm_toc_insert(toc, 1, states);

	controller = setup_controller(seg, slots, states, nworkers);
}

/*
 * Setup controller, no workers are active.
 */
static ControllerState *
setup_controller(dsm_segment *seg, WorkerProcSlot *slots, WorkerState *states, int nworkers)
{
	int		i;
	ControllerState *result = palloc(offsetof(ControllerState, worker_handles) +
										  nworkers * sizeof(BackgroundWorkerHandle *));

	result->workers_total = nworkers;
	result->workers_active = 0;
	result->data = seg;
	result->worker_slots = slots;
	result->worker_states = states;
	for (i = 0; i < nworkers; i++)
		result->worker_handles[i] = NULL;
	return result;
}

/*
 * Attach shared memory from worker perspective
 */
static void
worker_attach_to_shared_memory(int segmentno, int index)
{
	dsm_segment			*seg;
	shm_toc				*toc;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, WORKER_NAME);
	seg = dsm_attach(segmentno);
	if (seg == NULL)
		elog(ERROR, "unable map dynamic memory segment");

	toc = shm_toc_attach(AUTOREINDEX_SHM_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		elog(ERROR, "bad magic number in dynamic memory segment");

	worker_slot = &((WorkerProcSlot *) shm_toc_lookup(toc, 0))[index];
	worker_state = &((WorkerState *) shm_toc_lookup(toc, 1))[index];

	Assert(worker_slot->is_active);
	Assert(worker_slot->is_fresh);

	worker_slot->is_fresh = false;

	worker_state->is_valid = true;
	worker_state->exitcode = 0;
	worker_state->sqlstate = 0;
	worker_state->errormessage[0] = '\0';

	if (verbose)
		elog(LOG, "%s attached shared memory with index: %d for database: %s",
						  WORKER_NAME, index, worker_slot->dbname);
}

static void
worker_report_feedback(int exitcode, int sqlstate, char *msg)
{
	worker_state->is_valid = true;
	worker_state->exitcode = exitcode;
	worker_state->sqlstate = sqlstate;

	if (msg)
		snprintf(worker_state->errormessage, MAX_ERROR_MESSAGE_LEN - 1, "%s", msg);
}

static void
cleanup_on_workers_exit()
{
	int		i;
	pid_t	pid;

	for (i = 0; i < controller->workers_total; i++)
	{
		if (controller->worker_handles[i] != NULL && GetBackgroundWorkerPid(controller->worker_handles[i], &pid) != BGWH_STARTED)
		{
			WorkerState   *state = &controller->worker_states[i];
			WorkerProcSlot *slot = &controller->worker_slots[i];

			if (state->is_valid && verbose)
			{
				elog(LOG, "worker %s pid: %d database: %s ended", WORKER_NAME, pid, slot->dbname);
				elog(LOG, "exit code: %d, sqlstate: %s, errmsg: %s",
						  state->exitcode,
						  unpack_sql_state(state->sqlstate),
						  state->errormessage);
			}

			state->is_valid = false;
			slot->is_active = false;
			controller->worker_handles[i] = NULL;

			controller->workers_active--;
		}
	}
}

static void
terminate_workers()
{
	int		i;
	pid_t	pid;

	for (i = 0; i < controller->workers_total; i++)
	{
		if (controller->worker_handles[i] != NULL && GetBackgroundWorkerPid(controller->worker_handles[i], &pid) != BGWH_STARTED)
		{
			if (verbose)
				elog(LOG, "terminating worker %s pid: %d because of the controller exit",
							  WORKER_NAME, pid);
			TerminateBackgroundWorker(controller->worker_handles[i]);
		}
	}
}

/*
 * Attention - it reset resource owner.
 */
static List *
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

static const char *
bloat_index_query(void)
{
	return
		"SELECT current_database(), nspname AS schemaname, tblname, idxname, bs*(relpages)::bigint AS real_size,"
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
		" bs, nspname, table_oid, tblname, idxname, relpages, fillfactor, is_na"
		" FROM ("
		" SELECT maxalign, bs, nspname, tblname, idxname, reltuples, relpages, relam, table_oid, fillfactor,"
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
		" i.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.relam, a.attrelid AS table_oid,"
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
		" indrelid, indexrelid, indkey::smallint[] AS attnum,"
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
		" GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9"
		" ) AS s1"
		" ) AS s2"
		" JOIN pg_am am ON s2.relam = am.oid WHERE am.amname = \'btree\'"
		") AS sub"
		" ORDER BY 2,3,4";
}

/*
 * Add context to the errors produced by pglogical_execute_sql_command().
 */
static void
execute_sql_command_error_cb(void *arg)
{
	errcontext("during execution of queued SQL statement: %s", (char *) arg);
}

static void
execute_sql_command(char *cmdstr, bool isTopLevel, MemoryContext active_context)
{
	List	   *commands;
	ListCell   *command_i;
	ErrorContextCallback errcallback;

	errcallback.callback = execute_sql_command_error_cb;
	errcallback.arg = cmdstr;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	MemoryContextSwitchTo(active_context);

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

		MemoryContextSwitchTo(active_context);
	}

	/* protect against stack resets during CONCURRENTLY processing */
	if (error_context_stack == &errcallback)
		error_context_stack = errcallback.previous;
}


static void
worker_main(Datum main_arg)
{
	int		index;
	char	buf[MAXPGPATH];
	MemoryContext oldcontext = CurrentMemoryContext;

	if (verbose)
		elog(LOG, "worker %s started", WORKER_NAME);

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	memcpy(&index, MyBgworkerEntry->bgw_extra, sizeof(int));

	worker_attach_to_shared_memory(DatumGetUInt32(main_arg), index);

	Assert(worker_slot != NULL);
	Assert(worker_state != NULL);

	if (verbose)
		elog(LOG, "worker %s is stared for database: %s",
							WORKER_NAME, worker_slot->dbname);

	snprintf(buf, MAXPGPATH, "bgworker: %s (%s)", WORKER_NAME, worker_slot->dbname);
	init_ps_display(buf, "", "", "");

	PG_TRY();
	{

		BackgroundWorkerInitializeConnection(worker_slot->dbname, NULL);

		pgstat_report_appname(MyBgworkerEntry->bgw_name);

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();

		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		SPI_execute("set enable_nestloop to off", false, 0);

		pgstat_report_activity(STATE_RUNNING, bloat_index_query());

		SPI_execute(bloat_index_query(), false, 0);

//		if (SPI_processed > 0)
//		{
//			int		i;
//
//			for (i = 0; i < SPI_processed; i++)
//			{
//				char *val = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 4);
//
//				elog(LOG, "%s", val);
//			}
//
//		}


		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();

		MemoryContextSwitchTo(oldcontext);

		if (strcmp(worker_slot->dbname, "postgres") == 0)
		{
			char *query_string = "create index concurrently if not exists stehule on public.obce(nazev)";

			StartTransactionCommand();

			execute_sql_command(query_string, true, oldcontext);

			CommitTransactionCommand();

			break;
		}

	}
	PG_CATCH();
	{
		ErrorData		*errdata;

		MemoryContextSwitchTo(oldcontext);

		errdata = CopyErrorData();
		worker_report_feedback(errdata->saved_errno, errdata->sqlerrcode, errdata->message);

		PG_RE_THROW();
	}
	PG_END_TRY();

	pgstat_report_activity(STATE_IDLE, NULL);

	proc_exit(0);
}

static void
controller_main(Datum main_arg)
{
	List		*databases;
	ListCell	*lc;

	pqsignal(SIGTERM, handler_sigterm);
	pqsignal(SIGUSR1, handler_sigusr1);
	BackgroundWorkerUnblockSignals();

	setup_dynamic_shared_memory(controller_max_workers);

	BackgroundWorkerInitializeConnection(NULL, NULL);

	pgstat_report_appname(MyBgworkerEntry->bgw_name);

	databases = get_database_list();
	lc = list_head(databases);

	while (!got_sigterm)
	{
		int			rc;

		rc = WaitLatch(&MyProc->procLatch,
						  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000L);
		ResetLatch(&MyProc->procLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		if (got_sigterm)
			break;

		if (got_sigusr1)
		{
			got_sigusr1 = false;
			pgstat_report_activity(STATE_RUNNING, "removing stopped child processes");
			cleanup_on_workers_exit();
			goto wait;
		}

		while (controller->workers_active < controller->workers_total)
		{
			int		i;
			char		*dbname = NULL;
			bool		is_validdb = false;

			while (!is_validdb)
			{
				int		nworkers = 0;

				dbname = (char *) lfirst(lc);

				for (i = 0; i < controller->workers_total; i++)
				{
					if (controller->worker_slots[i].is_active &&
								strcmp(controller->worker_slots[i].dbname, dbname) == 0)
						nworkers++;
				}

				if (nworkers >= max_workers_per_db)
				{
				
					/* Too much workers. Try to next databaze. */

					lc = lnext(lc);
					if (lc == NULL)
					{
						lc = list_head(databases);
						goto wait;
					}
				}
				else
					is_validdb = true;
			}

			for (i = 0; i < controller->workers_total; i++)
			{
				if (controller->worker_handles[i] == NULL)
				{
					WorkerProcSlot *slot = &controller->worker_slots[i];
					WorkerState		*state = &controller->worker_states[i];
					BackgroundWorkerHandle *handle;

					slot->is_active = true;
					slot->is_fresh = true;
					state->is_valid = false;

					strcpy(slot->dbname, dbname);

					handle = launch_worker_internal(dsm_segment_handle(controller->data), i);
					if (handle != NULL)
					{
						controller->worker_handles[i] = handle;
						controller->workers_active++;

						lc = lnext(lc);
						if (lc == NULL)
						{
							lc = list_head(databases);
							goto wait;
						}
						break;
					}
					else
					{
						if (verbose)
							elog(LOG, "%s cannot to start worker %s",
										CONTROLLER_NAME, WORKER_NAME);
						slot->is_active = false;
						goto wait;
					}
				}
			}
		}

wait:
		pgstat_report_activity(STATE_IDLE, NULL);
	}

	terminate_workers();
	cleanup_on_workers_exit();

	if (verbose)
		elog(LOG, "%s finished", CONTROLLER_NAME);
	proc_exit(0);
}

/* alter system set shared_preload_libraries = 'autoreindex'; */

/*
 * This routine expects free slot.
 */
static BackgroundWorkerHandle *
launch_worker_internal(uint32 segment, int index)
{
	BackgroundWorker		worker;
	BackgroundWorkerHandle		*handle;
	BgwHandleStatus			status;
	pid_t		pid;

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		  BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = worker_main;
	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", WORKER_NAME);
	worker.bgw_main_arg = UInt32GetDatum(segment);
	memcpy(worker.bgw_extra, &index, sizeof(int));
	worker.bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return NULL;

	status = WaitForBackgroundWorkerStartup(handle, &pid);
	if (status == BGWH_STOPPED)
		elog(ERROR, "Cound not start %s background process", WORKER_NAME);

	if (status == BGWH_POSTMASTER_DIED)
		elog(ERROR, "Cannot start %s background process without postmaster", WORKER_NAME);

	Assert(status == BGWH_STARTED);

	if (verbose)
		elog(LOG, "Background worker %s pid: %d successfully started by controller %s",
					  WORKER_NAME, pid, CONTROLLER_NAME);

	return handle;
}

void
_PG_init(void)
{
	BackgroundWorker	controller_worker;

	controller_worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	controller_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	controller_worker.bgw_main = controller_main;
	snprintf(controller_worker.bgw_name, BGW_MAXLEN, "%s", CONTROLLER_NAME);
	controller_worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	controller_worker.bgw_main_arg = (Datum) 0;
	controller_worker.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&controller_worker);
}