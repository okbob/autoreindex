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
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

#define AUTOREINDEX_SHM_MAGIC			0x730715fb

void _PG_init(void);

static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sigusr1 = false;

static int			controller_max_workers = 4;
static int			max_workers_per_db = 2;

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

			if (state->is_valid)
			{
				elog(LOG, "worker %s pid: %d ended", WORKER_NAME, pid);
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

	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdatabase = (Form_pg_database) GETSTRUCT (tup);

		if (pgdatabase->datallowconn)
			result = lappend(result, pstrdup(NameStr(pgdatabase->datname)));
	}

	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	CommitTransactionCommand();

	return result;
}

static void
worker_main(Datum main_arg)
{
	int		index;

	elog(LOG, "worker %s started", WORKER_NAME);

	pqsignal(SIGTERM, handler_sigterm);
	BackgroundWorkerUnblockSignals();

	memcpy(&index, MyBgworkerEntry->bgw_extra, sizeof(int));

	worker_attach_to_shared_memory(DatumGetUInt32(main_arg), index);

	Assert(worker_slot != NULL);
	Assert(worker_state != NULL);

	elog(LOG, "worker %s is stared for database: %s",
						WORKER_NAME, worker_slot->dbname);

	PG_TRY();
	{

		BackgroundWorkerInitializeConnection(worker_slot->dbname, NULL);

		pgstat_report_appname(MyBgworkerEntry->bgw_name);

		SetCurrentStatementStartTimestamp();
		StartTransactionCommand();

		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());
		pgstat_report_activity(STATE_RUNNING, "select pg_sleep((random()*20)::int)");

		SPI_execute("select pg_sleep((random()*20)::int)", false, 1);

		SPI_finish();
		PopActiveSnapshot();
		CommitTransactionCommand();
	}
	PG_CATCH();
	{
		ErrorData		*errdata;

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
						  WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 5000L);
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
					}
					else
					{
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