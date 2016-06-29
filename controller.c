/*
 * autoreidex @ Pavel Stehule, 2016
 *
 * Infrastructure for maintaining controller and controlled
 * bacground worker processes
 *
 */

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/shm_toc.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"

#include "autoreindex.h"

PG_MODULE_MAGIC;

#define AUTOREINDEX_SHM_MAGIC			0x730715fb

void _PG_init(void);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

/*
 * We don't expect large number of workers, so there are
 * not necessary to play with flexible arrays.
 */
#define				MAX_WORKERS				10

/* GUC */
static int			controller_max_workers = 4;
static int			max_workers_per_database = 1;
static int			controller_naptime = 10;
static bool			verbose = false;
static bool			hide_ctx = true;

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
	bool	is_valid;
	int		stat_processed_tables;

	int		exitcode;
	int		sqlstate;
	char	errormessage[MAX_ERROR_MESSAGE_LEN];
} WorkerState;

/*
 * These data will be copied to the worker with bgw_extra.
 */
typedef struct
{
	char		dbname[NAMEDATALEN];
	int			worker_state_slot_number;
	int			options;
} WorkerStartupParams;

/* The context for LOG level is overhead, should be disabled */
emit_log_hook_type previous_log_hook = NULL;


/*
 * Holds privated data about active worker - owned by controller.
 */
typedef struct
{
	char		dbname[NAMEDATALEN];

} WorkerPrivateData;

typedef struct
{
	int		workers_total;
	int		workers_active;
	List	*database_list;
	ListCell	*current_dblc;
	MemoryContext		database_list_mcxt;
	dsm_segment			*data;
	WorkerState					*worker_states;
	BackgroundWorkerHandle		*worker_handles[MAX_WORKERS];
	WorkerPrivateData			workers[MAX_WORKERS];
} ControllerState;

static WorkerState			*worker_state;
static WorkerStartupParams	*worker_startup_params;				/* Worker Startup Params */


#define			CONTROLLER_NAME			"autoreindex: controller"
#define			WORKER_NAME				"autoreindex: worker"

static void setup_controller(ControllerState *controller, dsm_segment *seg,
															  WorkerState *states, int nworkers);
static void manage_workers(ControllerState *controller, bool *loop_completed);
static bool launch_worker(ControllerState *controller, char *dbname, int options);
static BackgroundWorkerHandle *launch_worker_internal(uint32 segment, WorkerStartupParams *params);


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
 * When any message is going from SQL executed by worker, then
 * the context is intrusive. This log hook handler removes it.
 *
 */
static void
worker_logger(ErrorData *edata)
{
	/* Hide context of LOG level messages */
	if (hide_ctx && edata->elevel == LOG)
	{
		edata->hide_ctx = true;
	}
}

/*
 * Setup the shared memory segments. Creates toc and allocates nworkers and worker states.
 *
 * Note: it is initialized before start of any worker, and released when all
 * workers are terminated.
 */
static void
setup_dynamic_shared_memory(ControllerState *controller, int nworkers)
{
	shm_toc_estimator	e;
	dsm_segment		*seg;
	shm_toc			*toc;
	Size			segsize;
	WorkerState			*states;

	/* Estimate how much shared memory we need */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, nworkers * sizeof(WorkerState));
	shm_toc_estimate_keys(&e, 1);
	segsize = shm_toc_estimate(&e);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, CONTROLLER_NAME);

	seg = dsm_create(segsize, 0);
	memset(dsm_segment_address(seg), 0, segsize);
	toc = shm_toc_create(AUTOREINDEX_SHM_MAGIC, dsm_segment_address(seg), segsize);

	states = shm_toc_allocate(toc, nworkers * sizeof(WorkerState));
	shm_toc_insert(toc, 1, states);

	setup_controller(controller, seg, states, nworkers);
}

/*
 * Setup controller, no workers are active.
 */
static void
setup_controller(ControllerState *controller, dsm_segment *seg, WorkerState *states, int nworkers)
{
	int			i;

	Assert(nworkers <= MAX_WORKERS);

	controller->workers_total = nworkers;
	controller->workers_active = 0;
	controller->data = seg;
	controller->worker_states = states;
	for (i = 0; i < nworkers; i++)
	{
		controller->worker_handles[i] = NULL;
		controller->worker_states[i].is_valid = false;
	}
	controller->database_list = NIL;
	controller->current_dblc = NULL;
	controller->database_list_mcxt = AllocSetContextCreate(CurrentMemoryContext,
																  "Database list storage",
															 ALLOCSET_DEFAULT_MINSIZE,
															 ALLOCSET_DEFAULT_INITSIZE,
															 ALLOCSET_DEFAULT_MAXSIZE);
}

/*
 * Attach shared memory from worker perspective
 */
static WorkerState *
setup_worker_state(int segmentno, WorkerStartupParams *params)
{
	WorkerState		*worker_state;
	dsm_segment			*seg;
	shm_toc				*toc;

	CurrentResourceOwner = ResourceOwnerCreate(NULL, WORKER_NAME);
	seg = dsm_attach(segmentno);
	if (seg == NULL)
		elog(ERROR, "unable map dynamic memory segment");

	toc = shm_toc_attach(AUTOREINDEX_SHM_MAGIC, dsm_segment_address(seg));
	if (toc == NULL)
		elog(ERROR, "bad magic number in dynamic memory segment");

	worker_state = &((WorkerState *) shm_toc_lookup(toc, 1))[params->worker_state_slot_number];

	/* Clean result state, and the state is valid now. */
	worker_state->is_valid = true;
	worker_state->exitcode = 0;
	worker_state->sqlstate = 0;
	worker_state->errormessage[0] = '\0';

	return worker_state;
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

/*
 * Called when sigusr1 is received - some worker finished
 */
static void
cleanup_on_workers_exit(ControllerState *controller)
{
	int		i;
	pid_t	pid;

	pgstat_report_activity(STATE_RUNNING, "removing stopped child processes");

	for (i = 0; i < controller->workers_total; i++)
	{
		if (controller->worker_handles[i] != NULL && GetBackgroundWorkerPid(controller->worker_handles[i], &pid) != BGWH_STARTED)
		{
			WorkerState   *state = &controller->worker_states[i];

			if (state->is_valid && verbose)
			{
				elog(LOG, "worker %s pid: %d database: %s ended", WORKER_NAME, pid, controller->workers[i].dbname);
				elog(LOG, "exit code: %d, sqlstate: %s, errmsg: %s",
						  state->exitcode,
						  unpack_sql_state(state->sqlstate),
						  state->errormessage);
			}

			state->is_valid = false;
			controller->worker_handles[i] = NULL;
			controller->workers_active--;
		}
	}
}

/*
 * Terminate all active workers
 */
static void
terminate_workers(ControllerState *controller)
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
 * The body of controlled worker - it takes params, connect to database,
 * run the statements and store exist code.
 *
 */
static void
worker_main(Datum main_arg)
{
	WorkerStartupParams		wsp;
	char	buf[MAXPGPATH];
	MemoryContext		mcxt;

	pgstat_report_appname(MyBgworkerEntry->bgw_name);

	mcxt  = CurrentMemoryContext;

	if (verbose)
		elog(LOG, "worker %s started", WORKER_NAME);

	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* take worker startup params */
	memcpy(&wsp, MyBgworkerEntry->bgw_extra, sizeof(WorkerStartupParams));
	worker_state = setup_worker_state(DatumGetUInt32(main_arg), &wsp);
	worker_startup_params = &wsp;

	snprintf(buf, MAXPGPATH, "bgworker: %s of %s", WORKER_NAME, wsp.dbname);
	init_ps_display(buf, "", "", "");

	previous_log_hook = emit_log_hook;
	emit_log_hook = worker_logger;

	PG_TRY();
	{
		BackgroundWorkerInitializeConnection(wsp.dbname, NULL);

		if (verbose)
			elog(LOG, "worker %s (%d) is stared inside database: %s",
							WORKER_NAME,
								  wsp.worker_state_slot_number,
								  wsp.dbname);

		MemoryContextSwitchTo(mcxt);

		pgstat_report_activity(STATE_RUNNING, "reindexing database");

		reindex_current_database(wsp.options);
	}
	PG_CATCH();
	{
		ErrorData		*errdata;

		MemoryContextSwitchTo(mcxt);
		errdata = CopyErrorData();
		worker_report_feedback(errdata->saved_errno, errdata->sqlerrcode, errdata->message);

		PG_RE_THROW();
	}
	PG_END_TRY();

	if (verbose)
		elog(LOG, "worker %s (%d) is over inside database: %s",
						WORKER_NAME,
							  wsp.worker_state_slot_number,
							  wsp.dbname);

	pgstat_report_activity(STATE_IDLE, NULL);

	proc_exit(0);
}

/*
 * Main cycle of controller background worker. It is starting controlled workers
 * with respecting limits of max workers and max workers per database.
 *
 */
static void
controller_main(Datum main_arg)
{
	ControllerState		controller;
	bool				loop_completed = false;

	pgstat_report_appname(MyBgworkerEntry->bgw_name);

	pqsignal(SIGTERM, handler_sigterm);
	pqsignal(SIGUSR1, handler_sigusr1);
	BackgroundWorkerUnblockSignals();

	setup_dynamic_shared_memory(&controller, controller_max_workers);

	/* work with global catalogue only */
	BackgroundWorkerInitializeConnection(NULL, NULL);

	while (!got_sigterm)
	{
		int			rc;

		rc = WaitLatch(MyLatch,
						  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, controller_naptime * 1000L);
		ResetLatch(MyLatch);

		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/* the normal shutdown */
		if (got_sigterm)
			break;

		/* when some controlled worker is ended */
		if (got_sigusr1)
		{
			got_sigusr1 = false;
			cleanup_on_workers_exit(&controller);

			/*
			 * when is completed cycle over databases, ignore sigusr latch,
			 * and starts new cycle on timeout.
			 */
			 if (loop_completed)
				 continue;
		}

		loop_completed = false;

		pgstat_report_activity(STATE_RUNNING, "autoreindex workers are starting");

		/* Now we can do some useful */
		manage_workers(&controller, &loop_completed);

		pgstat_report_activity(STATE_IDLE, NULL);
	}

	terminate_workers(&controller);
	cleanup_on_workers_exit(&controller);

	if (verbose)
		elog(LOG, "%s finished", CONTROLLER_NAME);
	proc_exit(0);
}

/*
 * Try to run worker for any database. It respects limits:
 *  - controller_max_workers
 *  - max_workers_per_database
 *
 */
static void
manage_workers(ControllerState *controller, bool *loop_completed)
{
	ListCell		*lc;


	if (controller->database_list == NIL)
	{
		ResourceOwner	ro = CurrentResourceOwner;
		MemoryContext	oldcxt;

		oldcxt = MemoryContextSwitchTo(controller->database_list_mcxt);
		controller->database_list = get_database_list();
		CurrentResourceOwner = ro;
		MemoryContextSwitchTo(oldcxt);
		controller->current_dblc = NULL;
	}

	lc = controller->current_dblc;
	if (lc == NULL)
	{
		/* return to start */
		lc = controller->current_dblc = list_head(controller->database_list);
	}

	/* repeat to last database when active workers are less than total workers */
	while (controller->workers_active < controller->workers_total && lc != NULL)
	{
		char	*dbname = (char *) lfirst(lc);
		int		 nworkers = 0;
		int		 i;

		/* count active workers of this database */
		for (i = 0; i < controller->workers_total; i++)
		{
			if (controller->worker_handles[i] != NULL &&
					strcmp(controller->workers[i].dbname, dbname) == 0)
				nworkers++;
		}

		if (nworkers >= max_workers_per_database)
		{
			lc = lnext(lc);
			continue;
		}

		if (!launch_worker(controller, dbname, 0))
		{
			if (verbose)
					elog(LOG, "%s cannot to start worker %s",
								CONTROLLER_NAME, WORKER_NAME);
			break;
		}

		lc = lnext(lc);
	}

	*loop_completed = lc == NULL;

	/*
	 * Database list is used for only one iteration. Release it, when we
	 * are on the end. The database list is usually short and with this technique
	 * we should not to solve refreshing the content of database list.
	 */
	if (*loop_completed)
	{
		controller->database_list = NIL;
		MemoryContextReset(controller->database_list_mcxt);
	}

	controller->current_dblc = lc;
}


/* alter system set shared_preload_libraries = 'autoreindex'; */


/*
 * Prepare startup params for background worker: free state slot, dbname
 * and options. When background worker is successfully started with these
 * parameters, then handle is saved and true is returned.
 */
static bool
launch_worker(ControllerState *controller, char *dbname, int options)
{
	int			i;

	for (i = 0; i < controller->workers_total; i++)
	{
		if (controller->worker_handles[i] == NULL)
		{
			WorkerStartupParams params;
			BackgroundWorkerHandle *handle;

			strncpy(params.dbname, dbname, NAMEDATALEN);
			params.worker_state_slot_number = i;
			params.options = options;

			controller->worker_states[i].is_valid = false;

			handle = launch_worker_internal(dsm_segment_handle(controller->data), &params);
			if (handle != NULL)
			{
				controller->worker_handles[i] = handle;
				strncpy(controller->workers[i].dbname, dbname, NAMEDATALEN);

				return true;
			}
		}
	}

	return false;
}

/*
 * Initialize startup params and starts background worker
 *
 */
static BackgroundWorkerHandle *
launch_worker_internal(uint32 segment, WorkerStartupParams *params)
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
	memcpy(worker.bgw_extra, params, sizeof(WorkerStartupParams));
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

/*
 * Register controller worker.
 *
 * ToDo: Initialize GUC
 *
 */
void
_PG_init(void)
{
	BackgroundWorker	controller_worker;

	DefineCustomBoolVariable("autoreindex.diagnostics",
						    "when is true, then diagnostics messages are generates ",
						    NULL,
						    &verbose,
						    false,
						    PGC_SIGHUP, 0,
						    NULL, NULL, NULL);

	DefineCustomBoolVariable("autoreindex.hide_context_log_messages",
						    "when is true, then context of LOG level messages is hidden",
						    NULL,
						    &hide_ctx,
						    true,
						    PGC_SIGHUP, 0,
						    NULL, NULL, NULL);

	DefineCustomIntVariable("autoreindex.max_workers",
			 "Maximal numbers of active workers started by controller",
							NULL,
							&controller_max_workers,
							4,
							1, MAX_WORKERS,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("autoreindex.max_workers_per_database",
			 "Maximal numbers of active workers executed on one database",
							NULL,
							&max_workers_per_database,
							1,
							1, MAX_WORKERS,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);


	DefineCustomIntVariable("autoreindex.naptime",
			 "Duration between each check (in seconds)",
							NULL,
							&controller_naptime,
							5,
							1, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL,
							NULL,
							NULL);

	EmitWarningsOnPlaceholders("autoreindex");

	controller_worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	controller_worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	controller_worker.bgw_main = controller_main;
	snprintf(controller_worker.bgw_name, BGW_MAXLEN, "%s", CONTROLLER_NAME);
	controller_worker.bgw_restart_time = BGW_DEFAULT_RESTART_INTERVAL;
	controller_worker.bgw_main_arg = (Datum) 0;
	controller_worker.bgw_notify_pid = 0;

	RegisterBackgroundWorker(&controller_worker);
}
