#include "postgres.h"
#include "commands/explain.h"
#include "executor/spi.h"
#include "utils/guc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

// GUC variables 
static bool		qflash_enabled_status		= false;
static double	qflash_log_min_duration		= 0.0;	// msec 
static char		*qflash_log_hash			= "";	// egz. indentifier for queries in one session
static char		*qflash_log_rel_name		= "";
static char		*qflash_log_namespace_name	= "";
static Oid		qflash_log_namespace_oid	= InvalidOid;
static Oid		qflash_log_rel_oid			= InvalidOid;
static bool		qflash_log_nested			= false;

// Current nesting depth of ExecutorRun calls
static int  nesting_level		= 0;
static int  nesting_spi_level	= 0;

// Saved hook values in case of unload 
static ExecutorStart_hook_type prev_ExecutorStart	= NULL;
static ExecutorRun_hook_type prev_ExecutorRun		= NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish	= NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd		= NULL;

// count elements in array
#define NELEMS(x)  (sizeof(x) / sizeof((x)[0]))

// export symbols on windows 
PGDLLEXPORT void _PG_init();
PGDLLEXPORT void _PG_fini();

// symbols
void _PG_init(void);
void _PG_fini(void);

void enabled_GucAssign(bool newval, void *extra);
bool namespace_GucCheck(char **newval, void **extra, GucSource source);
bool rel_GucCheck(char **newval, void **extra, GucSource source);

static void explain_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count);
static void explain_ExecutorFinish(QueryDesc *queryDesc);
static void explain_ExecutorEnd(QueryDesc *queryDesc);

bool set_qflash_namespace_oid(const char *namespace_name);
bool set_qflash_relname_oid(const char *relname_name);
Oid get_qflash_log_rel_oid(void);

bool qflash_enabled(QueryDesc *queryDesc);
void log_InRelation(ExplainState *es, QueryDesc *queryDesc);
char* generate_insert_log_query(void);

PG_FUNCTION_INFO_V1(qflash_init);

Datum
qflash_init(PG_FUNCTION_ARGS)
{
	text  *namespace_name	= PG_GETARG_TEXT_P(0);
	text  *relname_name		= PG_GETARG_TEXT_P(1);
	StringInfoData	ddl_query;
	
	initStringInfo(&ddl_query);
	appendStringInfo(&ddl_query, "\
		CREATE TABLE %s.%s\
		(\
			id BIGSERIAL NOT NULL,\
			added TIME WITH TIME ZONE NOT NULL DEFAULT now(),\
			query TEXT,\
			plan TEXT,\
			total_time DOUBLE PRECISION,\
			hash TEXT,\
			CONSTRAINT qflash_pkey PRIMARY KEY(id)\
		)\
	", VARDATA(namespace_name), VARDATA(relname_name));

	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
		PG_RETURN_BOOL(false);
	}

	if (SPI_execute(ddl_query.data, false, 1) != SPI_OK_UTILITY)
	{
		elog(ERROR, "SPI_execute DDL failed");
	}

	if (SPI_finish() != SPI_OK_FINISH)
	{
		elog(ERROR, "SPI_finish failed");
	}

	PG_RETURN_BOOL(true);
}

void
_PG_init(void)
{
	elog(LOG, "Init started!");

	/* Define custom GUC variables. */
	DefineCustomBoolVariable("qflash.enabled",
		"Is query flash enabled.",
		"True or False",
		&qflash_enabled_status, false, PGC_USERSET, 0, NULL, enabled_GucAssign, NULL);

	DefineCustomRealVariable("qflash.log_min_duration",
		"Sets the minimum execution time above which plans will be logged.",
		"Zero prints all plans.",
		&qflash_log_min_duration, 0.0, 0.0, DBL_MAX, PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomStringVariable("qflash.log_hash",
		"Query will be saved with this hash.",
		"Egz. identifier for queries in one session.",
		&qflash_log_hash, "", PGC_USERSET, 0, NULL, NULL, NULL);

	
	DefineCustomStringVariable("qflash.log_namespace_name",
		"Schema on the query plans logs.",
		"Define only schema name.",
		&qflash_log_namespace_name, "", PGC_USERSET, 0, namespace_GucCheck, NULL, NULL);

	DefineCustomStringVariable("qflash.log_relname",
		"Table on the query plans logs.",
		"Define only table name.",
		&qflash_log_rel_name, "", PGC_USERSET, 0, rel_GucCheck, NULL, NULL);

	DefineCustomBoolVariable("qflash.log_nested",
		"Log nested statements.", NULL,
		&qflash_log_nested, false, PGC_SUSET, 0, NULL, NULL, NULL);

	/* Install hooks. */
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = explain_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = explain_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = explain_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;
}

bool
set_qflash_namespace_oid(const char *namespace_name)
{
	if(strlen(namespace_name) == 0) return false;
	qflash_log_namespace_oid = get_namespace_oid(namespace_name, true);
	if (!OidIsValid(qflash_log_namespace_oid)) return false;

	return true;
}

bool
set_qflash_relname_oid(const char *relname_name)
{
	if (!OidIsValid(qflash_log_namespace_oid) || strlen(relname_name) == 0) return false;
	qflash_log_rel_oid = get_relname_relid(relname_name, qflash_log_namespace_oid);
	if (!OidIsValid(qflash_log_rel_oid)) return false;

	return true;
}

Oid get_qflash_log_rel_oid()
{
	if (!OidIsValid(qflash_log_rel_oid))
	{
		set_qflash_namespace_oid(qflash_log_namespace_name);
		set_qflash_relname_oid(qflash_log_rel_name);
	}

	return qflash_log_rel_oid;
}

bool
qflash_enabled(QueryDesc *queryDesc)
{
	return ( 
		qflash_enabled_status
		&& (nesting_level == 0 || qflash_log_nested)
		&& get_qflash_log_rel_oid() != InvalidOid
		&& (queryDesc->operation == CMD_SELECT || queryDesc->operation == CMD_UPDATE || queryDesc->operation == CMD_INSERT || queryDesc->operation == CMD_DELETE)
		/* Note: Next line is protection against recursion to log query generated by QFlash */
		&& (
			queryDesc->plannedstmt->relationOids == NULL || (
				queryDesc->plannedstmt->relationOids->length > 0
				// insert into log
				&& queryDesc->plannedstmt->relationOids->head->data.oid_value != qflash_log_rel_oid
				// pg_type
				&& queryDesc->plannedstmt->relationOids->head->data.oid_value != TypeRelationId
				)
			)
		);
}

void enabled_GucAssign(bool newval, void *extra)
{
	if (newval) return;
	
	qflash_log_rel_oid			= InvalidOid;
	qflash_log_namespace_oid	= InvalidOid;
}

bool
namespace_GucCheck(char **newval, void **extra, GucSource source)
{
	if (qflash_enabled_status && !set_qflash_namespace_oid(*newval)) return false;

	return true;
}

bool
rel_GucCheck(char **newval, void **extra, GucSource source)
{
	if (qflash_enabled_status && !set_qflash_relname_oid(*newval)) return false;
	
	//GUC_check_errdetail("\"max_stack_depth\" must not exceed %ldkB.",
	//	(stack_rlimit - STACK_DEPTH_SLOP) / 1024L);
	//GUC_check_errhint("Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.");
	return true;
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}

static void
explain_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	elog(LOG, "Init explain_ExecutorStart");
	
	if (qflash_enabled(queryDesc))
	{
		queryDesc->instrument_options |= INSTRUMENT_ALL;
	}

	if (prev_ExecutorStart) prev_ExecutorStart(queryDesc, eflags);
	else standard_ExecutorStart(queryDesc, eflags);

	if (qflash_enabled(queryDesc) && queryDesc->totaltime == NULL)
	{
		MemoryContext oldcxt;

		oldcxt = MemoryContextSwitchTo(queryDesc->estate->es_query_cxt);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL);
		MemoryContextSwitchTo(oldcxt);
	}

	elog(DEBUG1, "End explain_ExecutorStart");
}

/*
* ExecutorRun hook: all we need do is track nesting depth
*/
static void
explain_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count);
		else
			standard_ExecutorRun(queryDesc, direction, count);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
* ExecutorFinish hook: all we need do is track nesting depth
*/
static void
explain_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}


static void
explain_ExecutorEnd(QueryDesc *queryDesc)
{
	ExplainState *es;

	elog(LOG, "Init explain_ExecutorEnd");

	if (qflash_enabled(queryDesc))
	{
		/* Make sure stats accumulation is done.  (Note: it's okay if several levels of hook all do this.) */
		InstrEndLoop(queryDesc->totaltime);

		if ((queryDesc->totaltime->total * 1000.0) <= qflash_log_min_duration) 
			return;

		es = NewExplainState();
		/* Query plan settings */
		es->analyze	= true;
		es->verbose	= true;
		es->buffers	= es->analyze;
		es->timing	= es->analyze;
		es->summary	= es->analyze;
		es->format	= EXPLAIN_FORMAT_TEXT;

		ExplainBeginOutput(es);				// Header: XML, JSON ... etc. depends es->format
		ExplainPrintPlan(es, queryDesc);	// Print query plan to es->str->data
		if (es->analyze)
			ExplainPrintTriggers(es, queryDesc);	// Add plans for triggers
		ExplainEndOutput(es);				// Footer: XML, JSON ... etc. depends es->format

		/* Remove last line break */
		if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
			es->str->data[--es->str->len] = '\0';

		/* Fix JSON to output an object */
		if (es->format == EXPLAIN_FORMAT_JSON)
		{
			es->str->data[0] = '{';
			es->str->data[es->str->len - 1] = '}';
		}

		log_InRelation(es, queryDesc);

		// Clean query plan from memory.
		pfree(es->str->data);
	}

	if (prev_ExecutorEnd) prev_ExecutorEnd(queryDesc);
	else standard_ExecutorEnd(queryDesc);
	
	elog(DEBUG1, "End explain_ExecutorEnd");
}

void
log_InRelation(ExplainState *es, QueryDesc *queryDesc)
{
	SPIPlanPtr	spi_plan;
	int			spi_res_state;
	Oid			arg_types[4]	= { TEXTOID, TEXTOID, FLOAT8OID, TEXTOID };
	char		nulls[4]		= { ' ', ' ', ' ', (strlen(qflash_log_hash) ? ' ' : 'n') };
	Datum		values[4]		= {
		CStringGetTextDatum(queryDesc->sourceText),
		CStringGetTextDatum(es->str->data),
		Float8GetDatum(queryDesc->totaltime->total * 1000.0),
		CStringGetTextDatum(qflash_log_hash)
	};

	const char* query_string = generate_insert_log_query();
	if (!query_string) return;

	if (nesting_spi_level < nesting_level)
	{
		SPI_push();
		nesting_spi_level = nesting_level;
	}
	
	if (nesting_spi_level > nesting_level)
	{
		SPI_pop();
		nesting_spi_level = nesting_level;
	} 
	else if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect failed");
		return;
	}
	
	spi_plan = SPI_prepare(query_string, NELEMS(arg_types), arg_types);

	if (spi_plan == NULL)
	{
		elog(ERROR, "SPI_execute_plan failed for \"%s\" when log query \"%s\"", query_string, queryDesc->sourceText);
	}

	spi_res_state = SPI_execute_plan(spi_plan, values, nulls, false, 1);

	if (spi_res_state <= 0)
	{
		elog(ERROR, "SPI_execute_plan failed for \"%s\" when log query \"%s\"", query_string, queryDesc->sourceText);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

char*
generate_insert_log_query()
{
	StringInfoData insert_log_query;
	initStringInfo(&insert_log_query);
	appendStringInfo(&insert_log_query, "INSERT INTO %s.%s (query, plan, total_time, hash) VALUES ($1, $2, $3, $4)", qflash_log_namespace_name, qflash_log_rel_name);

	return insert_log_query.data;
}