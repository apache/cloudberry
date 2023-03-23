#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/hash.h"
#include "executor/instrument.h"
#include "executor/execdesc.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/scanner.h"
#include "parser/gram.h"
#include "pgstat.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/spin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "pg_stat_statements_ya_parser.h"

static post_parse_analyze_hook_type prev_post_parse_analyze_hook = NULL;

#define JUMBLE_SIZE 1024 /* query serialization buffer size */

/*
 * Struct for tracking locations/lengths of constants during normalization
 */
typedef struct pgssLocationLen
{
	int location; /* start offset in query text */
	int length;	  /* length in bytes, or -1 to ignore */
} pgssLocationLen;

/*
 * Working state for computing a query jumble and producing a normalized
 * query string
 */
typedef struct pgssJumbleState
{
	/* Jumble of current query tree */
	unsigned char *jumble;

	/* Number of bytes used in jumble[] */
	Size jumble_len;

	/* Array of locations of constants that should be removed */
	pgssLocationLen *clocations;

	/* Allocated length of clocations array */
	int clocations_buf_size;

	/* Current number of valid entries in clocations array */
	int clocations_count;

	/* highest Param id we've seen, in order to start normalization correctly */
	int highest_extern_param_id;
} pgssJumbleState;

static void AppendJumble(pgssJumbleState *jstate,
						 const unsigned char *item, Size size);
static void JumbleQuery(pgssJumbleState *jstate, Query *query);
static void JumbleRangeTable(pgssJumbleState *jstate, List *rtable);
static void JumbleExpr(pgssJumbleState *jstate, Node *node);
static void RecordConstLocation(pgssJumbleState *jstate, int location);

static StringInfo gen_normplan(const char *execution_plan);

static bool need_replace(int token);

void pgss_post_parse_analyze(ParseState *pstate, Query *query);

void stat_statements_parser_init()
{
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = pgss_post_parse_analyze;
}

void stat_statements_parser_deinit()
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
}

/*
 * AppendJumble: Append a value that is substantive in a given query to
 * the current jumble.
 */
static void
AppendJumble(pgssJumbleState *jstate, const unsigned char *item, Size size)
{
	unsigned char *jumble = jstate->jumble;
	Size jumble_len = jstate->jumble_len;

	/*
	 * Whenever the jumble buffer is full, we hash the current contents and
	 * reset the buffer to contain just that hash value, thus relying on the
	 * hash to summarize everything so far.
	 */
	while (size > 0)
	{
		Size part_size;

		if (jumble_len >= JUMBLE_SIZE)
		{
			uint32 start_hash = hash_any(jumble, JUMBLE_SIZE);

			memcpy(jumble, &start_hash, sizeof(start_hash));
			jumble_len = sizeof(start_hash);
		}
		part_size = Min(size, JUMBLE_SIZE - jumble_len);
		memcpy(jumble + jumble_len, item, part_size);
		jumble_len += part_size;
		item += part_size;
		size -= part_size;
	}
	jstate->jumble_len = jumble_len;
}

/*
 * Wrappers around AppendJumble to encapsulate details of serialization
 * of individual local variable elements.
 */
#define APP_JUMB(item) \
	AppendJumble(jstate, (const unsigned char *)&(item), sizeof(item))
#define APP_JUMB_STRING(str) \
	AppendJumble(jstate, (const unsigned char *)(str), strlen(str) + 1)

/*
 * JumbleQuery: Selectively serialize the query tree, appending significant
 * data to the "query jumble" while ignoring nonsignificant data.
 *
 * Rule of thumb for what to include is that we should ignore anything not
 * semantically significant (such as alias names) as well as anything that can
 * be deduced from child nodes (else we'd just be double-hashing that piece
 * of information).
 */
void JumbleQuery(pgssJumbleState *jstate, Query *query)
{
	Assert(IsA(query, Query));
	Assert(query->utilityStmt == NULL);

	APP_JUMB(query->commandType);
	/* resultRelation is usually predictable from commandType */
	JumbleExpr(jstate, (Node *)query->cteList);
	JumbleRangeTable(jstate, query->rtable);
	JumbleExpr(jstate, (Node *)query->jointree);
	JumbleExpr(jstate, (Node *)query->targetList);
	JumbleExpr(jstate, (Node *)query->returningList);
	JumbleExpr(jstate, (Node *)query->groupClause);
	JumbleExpr(jstate, query->havingQual);
	JumbleExpr(jstate, (Node *)query->windowClause);
	JumbleExpr(jstate, (Node *)query->distinctClause);
	JumbleExpr(jstate, (Node *)query->sortClause);
	JumbleExpr(jstate, query->limitOffset);
	JumbleExpr(jstate, query->limitCount);
	/* we ignore rowMarks */
	JumbleExpr(jstate, query->setOperations);
}

/*
 * Jumble a range table
 */
static void
JumbleRangeTable(pgssJumbleState *jstate, List *rtable)
{
	ListCell *lc;

	foreach (lc, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);

		Assert(IsA(rte, RangeTblEntry));
		APP_JUMB(rte->rtekind);
		switch (rte->rtekind)
		{
		case RTE_RELATION:
			APP_JUMB(rte->relid);
			break;
		case RTE_SUBQUERY:
			JumbleQuery(jstate, rte->subquery);
			break;
		case RTE_JOIN:
			APP_JUMB(rte->jointype);
			break;
		case RTE_FUNCTION:
			JumbleExpr(jstate, (Node *)rte->functions);
			break;
		case RTE_VALUES:
			JumbleExpr(jstate, (Node *)rte->values_lists);
			break;
		case RTE_CTE:

			/*
			 * Depending on the CTE name here isn't ideal, but it's the
			 * only info we have to identify the referenced WITH item.
			 */
			APP_JUMB_STRING(rte->ctename);
			APP_JUMB(rte->ctelevelsup);
			break;
		default:
			elog(ERROR, "unrecognized RTE kind: %d", (int)rte->rtekind);
			break;
		}
	}
}

/*
 * Jumble an expression tree
 *
 * In general this function should handle all the same node types that
 * expression_tree_walker() does, and therefore it's coded to be as parallel
 * to that function as possible.  However, since we are only invoked on
 * queries immediately post-parse-analysis, we need not handle node types
 * that only appear in planning.
 *
 * Note: the reason we don't simply use expression_tree_walker() is that the
 * point of that function is to support tree walkers that don't care about
 * most tree node types, but here we care about all types.  We should complain
 * about any unrecognized node type.
 */
static void
JumbleExpr(pgssJumbleState *jstate, Node *node)
{
	ListCell *temp;

	if (node == NULL)
		return;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	/*
	 * We always emit the node's NodeTag, then any additional fields that are
	 * considered significant, and then we recurse to any child nodes.
	 */
	APP_JUMB(node->type);

	switch (nodeTag(node))
	{
	case T_Var:
	{
		Var *var = (Var *)node;

		APP_JUMB(var->varno);
		APP_JUMB(var->varattno);
		APP_JUMB(var->varlevelsup);
	}
	break;
	case T_Const:
	{
		Const *c = (Const *)node;

		/* We jumble only the constant's type, not its value */
		APP_JUMB(c->consttype);
		/* Also, record its parse location for query normalization */
		RecordConstLocation(jstate, c->location);
	}
	break;
	case T_Param:
	{
		Param *p = (Param *)node;

		APP_JUMB(p->paramkind);
		APP_JUMB(p->paramid);
		APP_JUMB(p->paramtype);
	}
	break;
	case T_Aggref:
	{
		Aggref *expr = (Aggref *)node;

		APP_JUMB(expr->aggfnoid);
		JumbleExpr(jstate, (Node *)expr->aggdirectargs);
		JumbleExpr(jstate, (Node *)expr->args);
		JumbleExpr(jstate, (Node *)expr->aggorder);
		JumbleExpr(jstate, (Node *)expr->aggdistinct);
		JumbleExpr(jstate, (Node *)expr->aggfilter);
	}
	break;
	case T_WindowFunc:
	{
		WindowFunc *expr = (WindowFunc *)node;

		APP_JUMB(expr->winfnoid);
		APP_JUMB(expr->winref);
		JumbleExpr(jstate, (Node *)expr->args);
		JumbleExpr(jstate, (Node *)expr->aggfilter);
	}
	break;
	case T_ArrayRef:
	{
		ArrayRef *aref = (ArrayRef *)node;

		JumbleExpr(jstate, (Node *)aref->refupperindexpr);
		JumbleExpr(jstate, (Node *)aref->reflowerindexpr);
		JumbleExpr(jstate, (Node *)aref->refexpr);
		JumbleExpr(jstate, (Node *)aref->refassgnexpr);
	}
	break;
	case T_FuncExpr:
	{
		FuncExpr *expr = (FuncExpr *)node;

		APP_JUMB(expr->funcid);
		JumbleExpr(jstate, (Node *)expr->args);
	}
	break;
	case T_NamedArgExpr:
	{
		NamedArgExpr *nae = (NamedArgExpr *)node;

		APP_JUMB(nae->argnumber);
		JumbleExpr(jstate, (Node *)nae->arg);
	}
	break;
	case T_OpExpr:
	case T_DistinctExpr: /* struct-equivalent to OpExpr */
	case T_NullIfExpr:	 /* struct-equivalent to OpExpr */
	{
		OpExpr *expr = (OpExpr *)node;

		APP_JUMB(expr->opno);
		JumbleExpr(jstate, (Node *)expr->args);
	}
	break;
	case T_ScalarArrayOpExpr:
	{
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *)node;

		APP_JUMB(expr->opno);
		APP_JUMB(expr->useOr);
		JumbleExpr(jstate, (Node *)expr->args);
	}
	break;
	case T_BoolExpr:
	{
		BoolExpr *expr = (BoolExpr *)node;

		APP_JUMB(expr->boolop);
		JumbleExpr(jstate, (Node *)expr->args);
	}
	break;
	case T_SubLink:
	{
		SubLink *sublink = (SubLink *)node;

		APP_JUMB(sublink->subLinkType);
		JumbleExpr(jstate, (Node *)sublink->testexpr);
		JumbleQuery(jstate, (Query *)sublink->subselect);
	}
	break;
	case T_FieldSelect:
	{
		FieldSelect *fs = (FieldSelect *)node;

		APP_JUMB(fs->fieldnum);
		JumbleExpr(jstate, (Node *)fs->arg);
	}
	break;
	case T_FieldStore:
	{
		FieldStore *fstore = (FieldStore *)node;

		JumbleExpr(jstate, (Node *)fstore->arg);
		JumbleExpr(jstate, (Node *)fstore->newvals);
	}
	break;
	case T_RelabelType:
	{
		RelabelType *rt = (RelabelType *)node;

		APP_JUMB(rt->resulttype);
		JumbleExpr(jstate, (Node *)rt->arg);
	}
	break;
	case T_CoerceViaIO:
	{
		CoerceViaIO *cio = (CoerceViaIO *)node;

		APP_JUMB(cio->resulttype);
		JumbleExpr(jstate, (Node *)cio->arg);
	}
	break;
	case T_ArrayCoerceExpr:
	{
		ArrayCoerceExpr *acexpr = (ArrayCoerceExpr *)node;

		APP_JUMB(acexpr->resulttype);
		JumbleExpr(jstate, (Node *)acexpr->arg);
	}
	break;
	case T_ConvertRowtypeExpr:
	{
		ConvertRowtypeExpr *crexpr = (ConvertRowtypeExpr *)node;

		APP_JUMB(crexpr->resulttype);
		JumbleExpr(jstate, (Node *)crexpr->arg);
	}
	break;
	case T_CollateExpr:
	{
		CollateExpr *ce = (CollateExpr *)node;

		APP_JUMB(ce->collOid);
		JumbleExpr(jstate, (Node *)ce->arg);
	}
	break;
	case T_CaseExpr:
	{
		CaseExpr *caseexpr = (CaseExpr *)node;

		JumbleExpr(jstate, (Node *)caseexpr->arg);
		foreach (temp, caseexpr->args)
		{
			CaseWhen *when = (CaseWhen *)lfirst(temp);

			Assert(IsA(when, CaseWhen));
			JumbleExpr(jstate, (Node *)when->expr);
			JumbleExpr(jstate, (Node *)when->result);
		}
		JumbleExpr(jstate, (Node *)caseexpr->defresult);
	}
	break;
	case T_CaseTestExpr:
	{
		CaseTestExpr *ct = (CaseTestExpr *)node;

		APP_JUMB(ct->typeId);
	}
	break;
	case T_ArrayExpr:
		JumbleExpr(jstate, (Node *)((ArrayExpr *)node)->elements);
		break;
	case T_RowExpr:
		JumbleExpr(jstate, (Node *)((RowExpr *)node)->args);
		break;
	case T_RowCompareExpr:
	{
		RowCompareExpr *rcexpr = (RowCompareExpr *)node;

		APP_JUMB(rcexpr->rctype);
		JumbleExpr(jstate, (Node *)rcexpr->largs);
		JumbleExpr(jstate, (Node *)rcexpr->rargs);
	}
	break;
	case T_CoalesceExpr:
		JumbleExpr(jstate, (Node *)((CoalesceExpr *)node)->args);
		break;
	case T_MinMaxExpr:
	{
		MinMaxExpr *mmexpr = (MinMaxExpr *)node;

		APP_JUMB(mmexpr->op);
		JumbleExpr(jstate, (Node *)mmexpr->args);
	}
	break;
	case T_XmlExpr:
	{
		XmlExpr *xexpr = (XmlExpr *)node;

		APP_JUMB(xexpr->op);
		JumbleExpr(jstate, (Node *)xexpr->named_args);
		JumbleExpr(jstate, (Node *)xexpr->args);
	}
	break;
	case T_NullTest:
	{
		NullTest *nt = (NullTest *)node;

		APP_JUMB(nt->nulltesttype);
		JumbleExpr(jstate, (Node *)nt->arg);
	}
	break;
	case T_BooleanTest:
	{
		BooleanTest *bt = (BooleanTest *)node;

		APP_JUMB(bt->booltesttype);
		JumbleExpr(jstate, (Node *)bt->arg);
	}
	break;
	case T_CoerceToDomain:
	{
		CoerceToDomain *cd = (CoerceToDomain *)node;

		APP_JUMB(cd->resulttype);
		JumbleExpr(jstate, (Node *)cd->arg);
	}
	break;
	case T_CoerceToDomainValue:
	{
		CoerceToDomainValue *cdv = (CoerceToDomainValue *)node;

		APP_JUMB(cdv->typeId);
	}
	break;
	case T_SetToDefault:
	{
		SetToDefault *sd = (SetToDefault *)node;

		APP_JUMB(sd->typeId);
	}
	break;
	case T_CurrentOfExpr:
	{
		CurrentOfExpr *ce = (CurrentOfExpr *)node;

		APP_JUMB(ce->cvarno);
		if (ce->cursor_name)
			APP_JUMB_STRING(ce->cursor_name);
		APP_JUMB(ce->cursor_param);
	}
	break;
	case T_TargetEntry:
	{
		TargetEntry *tle = (TargetEntry *)node;

		APP_JUMB(tle->resno);
		APP_JUMB(tle->ressortgroupref);
		JumbleExpr(jstate, (Node *)tle->expr);
	}
	break;
	case T_RangeTblRef:
	{
		RangeTblRef *rtr = (RangeTblRef *)node;

		APP_JUMB(rtr->rtindex);
	}
	break;
	case T_JoinExpr:
	{
		JoinExpr *join = (JoinExpr *)node;

		APP_JUMB(join->jointype);
		APP_JUMB(join->isNatural);
		APP_JUMB(join->rtindex);
		JumbleExpr(jstate, join->larg);
		JumbleExpr(jstate, join->rarg);
		JumbleExpr(jstate, join->quals);
	}
	break;
	case T_FromExpr:
	{
		FromExpr *from = (FromExpr *)node;

		JumbleExpr(jstate, (Node *)from->fromlist);
		JumbleExpr(jstate, from->quals);
	}
	break;
	case T_List:
		foreach (temp, (List *)node)
		{
			JumbleExpr(jstate, (Node *)lfirst(temp));
		}
		break;
	case T_SortGroupClause:
	{
		SortGroupClause *sgc = (SortGroupClause *)node;

		APP_JUMB(sgc->tleSortGroupRef);
		APP_JUMB(sgc->eqop);
		APP_JUMB(sgc->sortop);
		APP_JUMB(sgc->nulls_first);
	}
	break;
	case T_WindowClause:
	{
		WindowClause *wc = (WindowClause *)node;

		APP_JUMB(wc->winref);
		APP_JUMB(wc->frameOptions);
		JumbleExpr(jstate, (Node *)wc->partitionClause);
		JumbleExpr(jstate, (Node *)wc->orderClause);
		JumbleExpr(jstate, wc->startOffset);
		JumbleExpr(jstate, wc->endOffset);
	}
	break;
	case T_CommonTableExpr:
	{
		CommonTableExpr *cte = (CommonTableExpr *)node;

		/* we store the string name because RTE_CTE RTEs need it */
		APP_JUMB_STRING(cte->ctename);
		JumbleQuery(jstate, (Query *)cte->ctequery);
	}
	break;
	case T_SetOperationStmt:
	{
		SetOperationStmt *setop = (SetOperationStmt *)node;

		APP_JUMB(setop->op);
		APP_JUMB(setop->all);
		JumbleExpr(jstate, setop->larg);
		JumbleExpr(jstate, setop->rarg);
	}
	break;
	case T_RangeTblFunction:
	{
		RangeTblFunction *rtfunc = (RangeTblFunction *)node;

		JumbleExpr(jstate, rtfunc->funcexpr);
	}
	break;
	default:
		/* Only a warning, since we can stumble along anyway */
		elog(WARNING, "unrecognized node type: %d",
			 (int)nodeTag(node));
		break;
	}
}

/*
 * Record location of constant within query string of query tree
 * that is currently being walked.
 */
static void
RecordConstLocation(pgssJumbleState *jstate, int location)
{
	/* -1 indicates unknown or undefined location */
	if (location >= 0)
	{
		/* enlarge array if needed */
		if (jstate->clocations_count >= jstate->clocations_buf_size)
		{
			jstate->clocations_buf_size *= 2;
			jstate->clocations = (pgssLocationLen *)
				repalloc(jstate->clocations,
						 jstate->clocations_buf_size *
							 sizeof(pgssLocationLen));
		}
		jstate->clocations[jstate->clocations_count].location = location;
		/* initialize lengths to -1 to simplify fill_in_constant_lengths */
		jstate->clocations[jstate->clocations_count].length = -1;
		jstate->clocations_count++;
	}
}

/* check if token should be replaced by substitute varable */
static bool
need_replace(int token)
{
	return (token == FCONST) || (token == ICONST) || (token == SCONST) || (token == BCONST) || (token == XCONST);
}

/*
 * gen_normplan - parse execution plan using flex and replace all CONST to
 * substitute variables.
 */
static StringInfo
gen_normplan(const char *execution_plan)
{
	core_yyscan_t yyscanner;
	core_yy_extra_type yyextra;
	core_YYSTYPE yylval;
	YYLTYPE yylloc;
	int tok;
	int bind_prefix = 1;
	char *tmp_str;
	YYLTYPE last_yylloc = 0;
	int last_tok = 0;
	StringInfo plan_out = makeStringInfo();
	;

	yyscanner = scanner_init(execution_plan,
							 &yyextra,
#if PG_VERSION_NUM >= 120000
							 &ScanKeywords,
							 ScanKeywordTokens
#else
							 ScanKeywords,
							 NumScanKeywords
#endif
	);

	for (;;)
	{
		/* get the next lexem */
		tok = core_yylex(&yylval, &yylloc, yyscanner);

		/* now we store end previsous lexem in yylloc - so could prcess it */
		if (need_replace(last_tok))
		{
			/* substitute variable instead of CONST */
			int s_len = asprintf(&tmp_str, "$%i", bind_prefix++);
			if (s_len > 0)
			{
				appendStringInfoString(plan_out, tmp_str);
				free(tmp_str);
			}
			else
			{
				appendStringInfoString(plan_out, "??");
			}
		}
		else
		{
			/* do not change - just copy as-is */
			tmp_str = strndup((char *)execution_plan + last_yylloc, yylloc - last_yylloc);
			appendStringInfoString(plan_out, tmp_str);
			free(tmp_str);
		}
		/* check if further parsing not needed */
		if (tok == 0)
			break;
		last_tok = tok;
		last_yylloc = yylloc;
	}

	scanner_finish(yyscanner);

	return plan_out;
}

uint64_t get_plan_id(QueryDesc *queryDesc)
{
	if (!queryDesc->sourceText)
		return 0;
	StringInfo normalized = gen_normplan(queryDesc->sourceText);
	return hash_any((unsigned char *)normalized->data, normalized->len);
}

/*
 * Post-parse-analysis hook: mark query with a queryId
 */
void pgss_post_parse_analyze(ParseState *pstate, Query *query)
{
	pgssJumbleState jstate;

	if (prev_post_parse_analyze_hook)
		prev_post_parse_analyze_hook(pstate, query);

	/* Assert we didn't do this already */
	Assert(query->queryId == 0);

	/*
	 * Utility statements get queryId zero.  We do this even in cases where
	 * the statement contains an optimizable statement for which a queryId
	 * could be derived (such as EXPLAIN or DECLARE CURSOR).  For such cases,
	 * runtime control will first go through ProcessUtility and then the
	 * executor, and we don't want the executor hooks to do anything, since we
	 * are already measuring the statement's costs at the utility level.
	 */
	if (query->utilityStmt)
	{
		query->queryId = 0;
		return;
	}

	/* Set up workspace for query jumbling */
	jstate.jumble = (unsigned char *)palloc(JUMBLE_SIZE);
	jstate.jumble_len = 0;
	jstate.clocations_buf_size = 32;
	jstate.clocations = (pgssLocationLen *)
		palloc(jstate.clocations_buf_size * sizeof(pgssLocationLen));
	jstate.clocations_count = 0;

	/* Compute query ID and mark the Query node with it */
	JumbleQuery(&jstate, query);
	query->queryId = hash_any(jstate.jumble, jstate.jumble_len);

	/*
	 * If we are unlucky enough to get a hash of zero, use 1 instead, to
	 * prevent confusion with the utility-statement case.
	 */
	if (query->queryId == 0)
		query->queryId = 1;
}