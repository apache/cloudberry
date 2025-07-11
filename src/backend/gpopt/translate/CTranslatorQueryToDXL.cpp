//---------------------------------------------------------------------------
//  Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorQueryToDXL.cpp
//
//	@doc:
//		Implementation of the methods used to translate a query into DXL tree.
//		All translator methods allocate memory in the provided memory pool, and
//		the caller is responsible for freeing it
//
//	@test:
//
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "access/sysattr.h"
#include "catalog/heap.h"
#include "catalog/pg_class.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/walkers.h"
#include "utils/guc.h"
#include "utils/rel.h"
}

#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/gpdbwrappers.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/translate/CCTEListEntry.h"
#include "gpopt/translate/CQueryMutators.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorQueryToDXL.h"
#include "gpopt/translate/CTranslatorRelcacheToDXL.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/operators/CDXLLogicalCTAS.h"
#include "naucrates/dxl/operators/CDXLLogicalCTEAnchor.h"
#include "naucrates/dxl/operators/CDXLLogicalCTEConsumer.h"
#include "naucrates/dxl/operators/CDXLLogicalCTEProducer.h"
#include "naucrates/dxl/operators/CDXLLogicalConstTable.h"
#include "naucrates/dxl/operators/CDXLLogicalDelete.h"
#include "naucrates/dxl/operators/CDXLLogicalForeignGet.h"
#include "naucrates/dxl/operators/CDXLLogicalGet.h"
#include "naucrates/dxl/operators/CDXLLogicalGroupBy.h"
#include "naucrates/dxl/operators/CDXLLogicalInsert.h"
#include "naucrates/dxl/operators/CDXLLogicalJoin.h"
#include "naucrates/dxl/operators/CDXLLogicalLimit.h"
#include "naucrates/dxl/operators/CDXLLogicalProject.h"
#include "naucrates/dxl/operators/CDXLLogicalSelect.h"
#include "naucrates/dxl/operators/CDXLLogicalUpdate.h"
#include "naucrates/dxl/operators/CDXLLogicalWindow.h"
#include "naucrates/dxl/operators/CDXLScalarBooleanTest.h"
#include "naucrates/dxl/operators/CDXLScalarLimitCount.h"
#include "naucrates/dxl/operators/CDXLScalarLimitOffset.h"
#include "naucrates/dxl/operators/CDXLScalarProjElem.h"
#include "naucrates/dxl/operators/CDXLScalarProjList.h"
#include "naucrates/dxl/operators/CDXLScalarSortCol.h"
#include "naucrates/dxl/operators/CDXLScalarSortColList.h"
#include "naucrates/dxl/operators/CDXLScalarWindowFrameEdge.h"
#include "naucrates/dxl/operators/CDXLScalarWindowRef.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/exception.h"
#include "naucrates/md/CMDIdGPDBCtas.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpdxl;
using namespace gpos;
using namespace gpopt;
using namespace gpnaucrates;
using namespace gpmd;

extern bool optimizer_enable_ctas;
extern bool optimizer_enable_dml;
extern bool optimizer_enable_dml_constraints;
extern bool optimizer_enable_replicated_table;
extern bool optimizer_enable_multiple_distinct_aggs;

// OIDs of variants of LEAD window function
static const OID lead_func_oids[] = {
	7011, 7074, 7075, 7310, 7312, 7314, 7316, 7318, 7320, 7322, 7324, 7326,
	7328, 7330, 7332, 7334, 7336, 7338, 7340, 7342, 7344, 7346, 7348, 7350,
	7352, 7354, 7356, 7358, 7360, 7362, 7364, 7366, 7368, 7370, 7372, 7374,
	7376, 7378, 7380, 7382, 7384, 7386, 7388, 7390, 7392, 7394, 7396, 7398,
	7400, 7402, 7404, 7406, 7408, 7410, 7412, 7414, 7416, 7418, 7420, 7422,
	7424, 7426, 7428, 7430, 7432, 7434, 7436, 7438, 7440, 7442, 7444, 7446,
	7448, 7450, 7452, 7454, 7456, 7458, 7460, 7462, 7464, 7466, 7468, 7470,
	7472, 7474, 7476, 7478, 7480, 7482, 7484, 7486, 7488, 7214, 7215, 7216,
	7220, 7222, 7224, 7244, 7246, 7248, 7260, 7262, 7264};

// OIDs of variants of LAG window function
static const OID lag_func_oids[] = {
	7675, 7491, 7493, 7495, 7497, 7499, 7501, 7503, 7505, 7507, 7509, 7511,
	7513, 7515, 7517, 7519, 7521, 7523, 7525, 7527, 7529, 7531, 7533, 7535,
	7537, 7539, 7541, 7543, 7545, 7547, 7549, 7551, 7553, 7555, 7557, 7559,
	7561, 7563, 7565, 7567, 7569, 7571, 7573, 7575, 7577, 7579, 7581, 7583,
	7585, 7587, 7589, 7591, 7593, 7595, 7597, 7599, 7601, 7603, 7605, 7607,
	7609, 7611, 7613, 7615, 7617, 7619, 7621, 7623, 7625, 7627, 7629, 7631,
	7633, 7635, 7637, 7639, 7641, 7643, 7645, 7647, 7649, 7651, 7653, 7655,
	7657, 7659, 7661, 7663, 7665, 7667, 7669, 7671, 7673, 7211, 7212, 7213,
	7226, 7228, 7230, 7250, 7252, 7254, 7266, 7268, 7270};

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CTranslatorQueryToDXL
//
//	@doc:
//		Private constructor. This is used when starting on the
//		top-level Query, and also when recursing into a subquery.
//
//---------------------------------------------------------------------------
CTranslatorQueryToDXL::CTranslatorQueryToDXL(
	CContextQueryToDXL *context, CMDAccessor *md_accessor,
	const CMappingVarColId *var_colid_mapping, Query *query, ULONG query_level,
	BOOL is_top_query_dml, HMUlCTEListEntry *query_level_to_cte_map)
	: m_context(context),
	  m_mp(context->m_mp),
	  m_sysid(IMDId::EmdidGeneral, GPMD_GPDB_SYSID),
	  m_md_accessor(md_accessor),
	  m_query_level(query_level),
	  m_is_top_query_dml(is_top_query_dml),
	  m_is_ctas_query(false),
	  m_query_level_to_cte_map(nullptr),
	  m_dxl_query_output_cols(nullptr),
	  m_dxl_cte_producers(nullptr),
	  m_cteid_at_current_query_level_map(nullptr)
{
	GPOS_ASSERT(nullptr != query);
	CheckSupportedCmdType(query);

	m_query_id = m_context->GetNextQueryId();

	CheckRangeTable(query);

	// GPDB_94_MERGE_FIXME: WITH CHECK OPTION views are not supported yet.
	// I'm not sure what would be needed to support them; maybe need to
	// just pass through the withCheckOptions to the ModifyTable / DML node?
	if (query->withCheckOptions)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("View with WITH CHECK OPTION"));
	}

	// Initialize the map that stores gpdb att to optimizer col mapping.
	// If this is a subquery, make a copy of the parent's mapping, otherwise
	// initialize a new, empty, mapping.
	if (var_colid_mapping)
	{
		m_var_to_colid_map = var_colid_mapping->CopyMapColId(m_mp);
	}
	else
	{
		m_var_to_colid_map = GPOS_NEW(m_mp) CMappingVarColId(m_mp);
	}

	m_query_level_to_cte_map = GPOS_NEW(m_mp) HMUlCTEListEntry(m_mp);
	m_dxl_cte_producers = GPOS_NEW(m_mp) CDXLNodeArray(m_mp);
	m_cteid_at_current_query_level_map = GPOS_NEW(m_mp) UlongBoolHashMap(m_mp);

	if (nullptr != query_level_to_cte_map)
	{
		HMIterUlCTEListEntry cte_list_hashmap_iter(query_level_to_cte_map);

		while (cte_list_hashmap_iter.Advance())
		{
			ULONG cte_query_level = *(cte_list_hashmap_iter.Key());

			CCTEListEntry *cte_list_entry =
				const_cast<CCTEListEntry *>(cte_list_hashmap_iter.Value());

			// CTE's that have been defined before the m_query_level
			// should only be inserted into the hash map
			// For example:
			// WITH ab as (SELECT a as a, b as b from foo)
			// SELECT *
			// FROM
			// 	(WITH aEq10 as (SELECT b from ab ab1 where ab1.a = 10)
			//  	SELECT *
			//  	FROM (WITH aEq20 as (SELECT b from ab ab2 where ab2.a = 20)
			// 		      SELECT * FROM aEq10 WHERE b > (SELECT min(b) from aEq20)
			// 		      ) dtInner
			// 	) dtOuter
			// When translating the from expression containing "aEq10" in the derived table "dtInner"
			// we have already seen three CTE namely: "ab", "aEq10" and "aEq20". BUT when we expand aEq10
			// in the dt1, we should only have access of CTE's defined prior to its level namely "ab".

			if (cte_query_level < query_level && nullptr != cte_list_entry)
			{
				cte_list_entry->AddRef();
				BOOL is_res GPOS_ASSERTS_ONLY =
					m_query_level_to_cte_map->Insert(
						GPOS_NEW(m_mp) ULONG(cte_query_level), cte_list_entry);
				GPOS_ASSERT(is_res);
			}
		}
	}

	// check if the query has any unsupported node types
	CheckUnsupportedNodeTypes(query);

	// check if the query has SIRV functions in the targetlist without a FROM clause
	CheckSirvFuncsWithoutFromClause(query);

	// first normalize the query
	m_query =
		CQueryMutators::NormalizeQuery(m_mp, m_md_accessor, query, query_level);

	if (nullptr != m_query->cteList)
	{
		ConstructCTEProducerList(m_query->cteList, query_level);
	}

	m_scalar_translator = GPOS_NEW(m_mp)
		CTranslatorScalarToDXL(m_context, m_md_accessor, m_query_level,
							   m_query_level_to_cte_map, m_dxl_cte_producers);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::QueryToDXLInstance
//
//	@doc:
//		Factory function. Creates a new CTranslatorQueryToDXL object
//		for translating the given top-level query.
//
//---------------------------------------------------------------------------
CTranslatorQueryToDXL *
CTranslatorQueryToDXL::QueryToDXLInstance(CMemoryPool *mp,
										  CMDAccessor *md_accessor,
										  Query *query)
{
	CContextQueryToDXL *context = GPOS_NEW(mp) CContextQueryToDXL(mp);

	return GPOS_NEW(context->m_mp)
		CTranslatorQueryToDXL(context, md_accessor,
							  nullptr,	// var_colid_mapping,
							  query,
							  0,	   // query_level
							  false,   // is_top_query_dml
							  nullptr  // query_level_to_cte_map
		);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::~CTranslatorQueryToDXL
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CTranslatorQueryToDXL::~CTranslatorQueryToDXL()
{
	GPOS_DELETE(m_scalar_translator);
	GPOS_DELETE(m_var_to_colid_map);
	gpdb::GPDBFree(m_query);
	m_query_level_to_cte_map->Release();
	m_dxl_cte_producers->Release();
	m_cteid_at_current_query_level_map->Release();
	CRefCount::SafeRelease(m_dxl_query_output_cols);

	if (m_query_level == 0)
	{
		GPOS_DELETE(m_context);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CheckUnsupportedNodeTypes
//
//	@doc:
//		Check for unsupported node types, and throws an exception when found
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::CheckUnsupportedNodeTypes(Query *query)
{
	static const SUnsupportedFeature unsupported_features[] = {
		{T_RowExpr, GPOS_WSZ_LIT("ROW EXPRESSION")},
		{T_RowCompareExpr, GPOS_WSZ_LIT("ROW COMPARE")},
		{T_FieldStore, GPOS_WSZ_LIT("FIELDSTORE")},
		{T_CoerceToDomainValue, GPOS_WSZ_LIT("COERCETODOMAINVALUE")},
		{T_GroupId, GPOS_WSZ_LIT("GROUPID")},
		{T_CurrentOfExpr, GPOS_WSZ_LIT("CURRENT OF")},
	};

	List *unsupported_list = NIL;
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(unsupported_features); ul++)
	{
		unsupported_list = gpdb::LAppendInt(unsupported_list,
											unsupported_features[ul].node_tag);
	}

	INT unsupported_node = gpdb::FindNodes((Node *) query, unsupported_list);
	gpdb::GPDBFree(unsupported_list);

	if (0 <= unsupported_node)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   unsupported_features[unsupported_node].m_feature_name);
	}

	// GPDB_91_MERGE_FIXME: collation
	INT non_default_collation = gpdb::CheckCollation((Node *) query);

	if (0 < non_default_collation)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Non-default collation"));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CheckSirvFuncsWithoutFromClause
//
//	@doc:
//		Check for SIRV functions in the target list without a FROM clause, and
//		throw an exception when found
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::CheckSirvFuncsWithoutFromClause(Query *query)
{
	// if there is a FROM clause or if target list is empty, look no further
	if ((nullptr != query->jointree &&
		 0 < gpdb::ListLength(query->jointree->fromlist)) ||
		NIL == query->targetList)
	{
		return;
	}

	// see if we have SIRV functions in the target list
	if (HasSirvFunctions((Node *) query->targetList))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("SIRV functions"));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::HasSirvFunctions
//
//	@doc:
//		Check for SIRV functions in the tree rooted at the given node
//
//---------------------------------------------------------------------------
BOOL
CTranslatorQueryToDXL::HasSirvFunctions(Node *node) const
{
	GPOS_ASSERT(nullptr != node);

	List *function_list = gpdb::ExtractNodesExpression(
		node, T_FuncExpr, true /*descendIntoSubqueries*/);
	ListCell *lc = nullptr;

	BOOL has_sirv = false;
	ForEach(lc, function_list)
	{
		FuncExpr *func_expr = (FuncExpr *) lfirst(lc);
		if (CTranslatorUtils::IsSirvFunc(m_mp, m_md_accessor,
										 func_expr->funcid))
		{
			has_sirv = true;
			break;
		}
	}
	gpdb::ListFree(function_list);

	return has_sirv;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CheckSupportedCmdType
//
//	@doc:
//		Check for supported command types, throws an exception when command
//		type not yet supported
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::CheckSupportedCmdType(Query *query)
{
	if (nullptr != query->utilityStmt)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("UTILITY command"));
	}

	if (CMD_SELECT == query->commandType)
	{
		// GPDB_92_MERGE_FIXME: CTAS is a UTILITY statement after upstream
		// refactoring commit 9dbf2b7d . We are temporarily *always* falling
		// back. Detect CTAS harder when we get back to it.

		if (!optimizer_enable_ctas &&
			query->parentStmtType == PARENTSTMTTYPE_CTAS)
		{
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT(
					"CTAS. Set optimizer_enable_ctas to on to enable CTAS with GPORCA"));
		}
		if (query->parentStmtType == PARENTSTMTTYPE_COPY)
		{
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT(
					"COPY. Copy select statement to file on segment is not supported with GPORCA"));
		}
		if (query->parentStmtType == PARENTSTMTTYPE_REFRESH_MATVIEW)
		{
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT("Refresh matview is not supported with GPORCA"));
		}

		// supported: regular select or CTAS when it is enabled
		return;
	}

	static const SCmdNameElem unsupported_commands[] = {
		{CMD_UTILITY, GPOS_WSZ_LIT("UTILITY command")}};

	const ULONG length = GPOS_ARRAY_SIZE(unsupported_commands);
	for (ULONG ul = 0; ul < length; ul++)
	{
		SCmdNameElem mapelem = unsupported_commands[ul];
		if (mapelem.m_cmd_type == query->commandType)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   mapelem.m_cmd_name);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CheckRangeTable
//
//	@doc:
//		Check for supported stuff in range table, throws an exception
//		if there is something that is not yet supported
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::CheckRangeTable(Query *query)
{
	ListCell *lc;
	ForEach(lc, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->security_barrier)
		{
			GPOS_ASSERT_FIXME(RTE_SUBQUERY == rte->rtekind);
			// otherwise ORCA most likely pushes potentially leaky filters down
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("views with security_barrier ON"));
		}

		// In a rewritten parse tree
		//
		// [1] When hasRowSecurity=false and security_quals are not
		// present in an rte, means that the relations present in a
		// query don't have row level security enabled.
		//
		// [2] When hasRowSecurity=true and security_quals are present
		// in an rte, means that the relations present in a query have
		// row level security enabled.
		//
		// [3] When hasRowSecurity=true and security_quals are not
		// present in an rte, means that the relations present in
		// a query have row level security enabled but the query is
		// executed by the owner of the relation.
		//
		// [4] When hasRowSecurity=false and security_quals are
		// present in an rte example: A view with security barrier
		// enabled and the view contains a relation with rules.
		// Example query is below
		//
		// ```SQL
		// CREATE TABLE foo(id int PRIMARY KEY, data text, deleted boolean);
		// CREATE RULE foo_del_rule AS ON DELETE TO foo DO INSTEAD UPDATE foo SET deleted = true WHERE id = old.id;
		// CREATE VIEW rw_view1 WITH (security_barrier=true) AS SELECT id, data FROM foo WHERE NOT deleted;
		// DELETE FROM rw_view1 WHERE id = 1;
		// ```
		// ORCA will fallback to planner for this case [4].
		if (!query->hasRowSecurity && nullptr != rte->securityQuals)
		{
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT(
					"Security quals present in RTE without row level security enabled"));
		}

		// ORCA will fallback to planner if row level security is
		// enabled for a relation and the security quals contain
		// sublinks.
		if (query->hasRowSecurity && query->hasSubLinks &&
			0 < gpdb::ListLength(rte->securityQuals) &&
			CheckSublinkInSecurityQuals((Node *) rte->securityQuals, nullptr))
		{
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT(
					"Query has row level security enabled and security quals contain sublinks"));
		}

		if (rte->tablesample)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("TABLESAMPLE in the FROM clause"));
		}

		if (rte->relkind == RELKIND_PARTITIONED_TABLE && query->hasRowSecurity && GPOS_FTRACE(EopttraceDisableDynamicTableScan)) {
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				GPOS_WSZ_LIT("ORCA not support row-level security if dynamic table scan is disabled."));
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CheckSublinkInSecurityQuals
//
//	@doc:
//		When row level security is enabled we add the security quals
//		while translating the table scans from DXL To Planned Statement.
//		If the security quals consists of SUBLINKS then those queries
//		will not have been planned as we add them at the end during
//		translation. So falling back to planner for such cases. This walker
// 		is used to find if we have any sublinks present in the security quals.
//
//---------------------------------------------------------------------------

BOOL
CTranslatorQueryToDXL::CheckSublinkInSecurityQuals(Node *node, void *context)
{
	if (nullptr == node)
	{
		return false;
	}

	if (IsA(node, SubLink))
	{
		return true;
	}

	return gpdb::WalkExpressionTree(
		node, (bool (*)(Node *, void *)) CTranslatorQueryToDXL::CheckSublinkInSecurityQuals,
		context);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::GetQueryOutputCols
//
//	@doc:
//		Return the list of query output columns
//
//---------------------------------------------------------------------------
CDXLNodeArray *
CTranslatorQueryToDXL::GetQueryOutputCols() const
{
	return m_dxl_query_output_cols;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::GetCTEs
//
//	@doc:
//		Return the list of CTEs
//
//---------------------------------------------------------------------------
CDXLNodeArray *
CTranslatorQueryToDXL::GetCTEs() const
{
	return m_dxl_cte_producers;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateSelectQueryToDXL
//
//	@doc:
//		Translates a Query into a DXL tree. The function allocates memory in
//		the translator memory pool, and caller is responsible for freeing it.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateSelectQueryToDXL()
{
	// The parsed query contains an RTE for the view, which is maintained all the way through planned statement.
	// This entries is annotated as requiring SELECT permissions for the current user.
	// In Orca, we only keep range table entries for the base tables in the planned statement, but not for the view itself.
	// Since permissions are only checked during ExecutorStart, we lose track of the permissions required for the view and the select goes through successfully.
	// We therefore need to check permissions before we go into optimization for all RTEs, including the ones not explicitly referred in the query, e.g. views.
	CTranslatorUtils::CheckRTEPermissions(m_query->rtable);

	if (m_query->hasForUpdate)
	{
		int rt_len = gpdb::ListLength(m_query->rtable);
		for (int i = 0; i < rt_len; i++)
		{
			const RangeTblEntry *rte =
				(RangeTblEntry *) gpdb::ListNth(m_query->rtable, i);

			if (rte->relkind == 'f' && rte->rellockmode == ExclusiveLock)
			{
				GPOS_RAISE(gpdxl::ExmaDXL,
						   gpdxl::ExmiQuery2DXLUnsupportedFeature,
						   GPOS_WSZ_LIT("Locking clause on foreign table"));
			}
		}
	}

	// RETURNING is not supported yet.
	if (m_query->returningList)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("RETURNING clause"));
	}

	// ON CONFLICT is not supported yet.
	if (m_query->onConflict)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("ON CONFLICT clause"));
	}

	if (m_query->limitOption == LIMIT_OPTION_WITH_TIES)
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("LIMIT WITH TIES clause"));

	CDXLNode *child_dxlnode = nullptr;
	IntToUlongMap *sort_group_attno_to_colid_mapping =
		GPOS_NEW(m_mp) IntToUlongMap(m_mp);
	IntToUlongMap *output_attno_to_colid_mapping =
		GPOS_NEW(m_mp) IntToUlongMap(m_mp);

	// construct CTEAnchor operators for the CTEs defined at the top level
	CDXLNode *dxl_cte_anchor_top = nullptr;
	CDXLNode *dxl_cte_anchor_bottom = nullptr;
	ConstructCTEAnchors(m_dxl_cte_producers, &dxl_cte_anchor_top,
						&dxl_cte_anchor_bottom);
	GPOS_ASSERT_IMP(
		m_dxl_cte_producers == nullptr || 0 < m_dxl_cte_producers->Size(),
		nullptr != dxl_cte_anchor_top && nullptr != dxl_cte_anchor_bottom);

	GPOS_ASSERT_IMP(nullptr != m_query->setOperations,
					0 == gpdb::ListLength(m_query->windowClause));
	if (nullptr != m_query->setOperations)
	{
		List *target_list = m_query->targetList;
		// translate set operations
		child_dxlnode = TranslateSetOpToDXL(m_query->setOperations, target_list,
											output_attno_to_colid_mapping);

		CDXLLogicalSetOp *dxlop =
			CDXLLogicalSetOp::Cast(child_dxlnode->GetOperator());
		const CDXLColDescrArray *dxl_col_descr_array =
			dxlop->GetDXLColumnDescrArray();
		ListCell *lc = nullptr;
		ULONG resno = 1;
		ForEach(lc, target_list)
		{
			TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
			if (0 < target_entry->ressortgroupref)
			{
				ULONG colid = ((*dxl_col_descr_array)[resno - 1])->Id();
				AddSortingGroupingColumn(
					target_entry, sort_group_attno_to_colid_mapping, colid);
			}
			resno++;
		}
	}
	else if (0 != gpdb::ListLength(
					  m_query->windowClause))  // translate window clauses
	{
		CDXLNode *dxlnode = TranslateFromExprToDXL(m_query->jointree);
		GPOS_ASSERT(nullptr == m_query->groupClause);
		GPOS_ASSERT(nullptr == m_query->groupingSets);
		child_dxlnode = TranslateWindowToDXL(
			dxlnode, m_query->targetList, m_query->windowClause,
			m_query->sortClause, sort_group_attno_to_colid_mapping,
			output_attno_to_colid_mapping);
	}
	else
	{
		child_dxlnode = TranslateGroupingSets(
			m_query->jointree, m_query->targetList, m_query->groupClause,
			m_query->groupingSets, m_query->groupDistinct, m_query->hasAggs,
			sort_group_attno_to_colid_mapping, output_attno_to_colid_mapping);
	}

	// translate limit clause
	CDXLNode *limit_dxlnode = TranslateLimitToDXLGroupBy(
		m_query->sortClause, m_query->limitCount, m_query->limitOffset,
		child_dxlnode, sort_group_attno_to_colid_mapping);


	if (nullptr == m_query->targetList)
	{
		m_dxl_query_output_cols = GPOS_NEW(m_mp) CDXLNodeArray(m_mp);
	}
	else
	{
		m_dxl_query_output_cols = CreateDXLOutputCols(
			m_query->targetList, output_attno_to_colid_mapping);
	}

	// cleanup
	CRefCount::SafeRelease(sort_group_attno_to_colid_mapping);

	output_attno_to_colid_mapping->Release();

	// add CTE anchors if needed
	CDXLNode *result_dxlnode = limit_dxlnode;

	if (nullptr != dxl_cte_anchor_top)
	{
		GPOS_ASSERT(nullptr != dxl_cte_anchor_bottom);
		dxl_cte_anchor_bottom->AddChild(result_dxlnode);
		result_dxlnode = dxl_cte_anchor_top;
	}

	return result_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateSelectProjectJoinToDXL
//
//	@doc:
//		Construct a DXL SPJ tree from the given query parts
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateSelectProjectJoinToDXL(
	List *target_list, FromExpr *from_expr,
	IntToUlongMap *sort_group_attno_to_colid_mapping,
	IntToUlongMap *output_attno_to_colid_mapping, List *group_clause)
{
	CDXLNode *join_tree_dxlnode = TranslateFromExprToDXL(from_expr);

	// translate target list entries into a logical project
	return TranslateTargetListToDXLProject(
		target_list, join_tree_dxlnode, sort_group_attno_to_colid_mapping,
		output_attno_to_colid_mapping, group_clause);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateSelectProjectJoinForGrpSetsToDXL
//
//	@doc:
//		Construct a DXL SPJ tree from the given query parts, and keep variables
//		appearing in aggregates in the project list
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateSelectProjectJoinForGrpSetsToDXL(
	List *target_list, FromExpr *from_expr,
	IntToUlongMap *sort_group_attno_to_colid_mapping,
	IntToUlongMap *output_attno_to_colid_mapping, List *group_clause)
{
	CDXLNode *join_tree_dxlnode = TranslateFromExprToDXL(from_expr);

	// translate target list entries into a logical project
	return TranslateTargetListToDXLProject(
		target_list, join_tree_dxlnode, sort_group_attno_to_colid_mapping,
		output_attno_to_colid_mapping, group_clause,
		true /*is_expand_aggref_expr*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateQueryToDXL
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateQueryToDXL()
{
	CAutoTimer at("\n[OPT]: Query To DXL Translation Time",
				  GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	switch (m_query->commandType)
	{
		case CMD_SELECT:
			if (m_query->parentStmtType == PARENTSTMTTYPE_NONE)
			{
				return TranslateSelectQueryToDXL();
			}
			else
			{
				return TranslateCTASToDXL();
			}

		case CMD_INSERT:
			return TranslateInsertQueryToDXL();

		case CMD_DELETE:
			return TranslateDeleteQueryToDXL();

		case CMD_UPDATE:
			return TranslateUpdateQueryToDXL();

		default:
			GPOS_ASSERT(!"Statement type not supported");
			return nullptr;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateInsertQueryToDXL
//
//	@doc:
//		Translate an insert stmt
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateInsertQueryToDXL()
{
	GPOS_ASSERT(CMD_INSERT == m_query->commandType);
	GPOS_ASSERT(0 < m_query->resultRelation);

	if (!optimizer_enable_dml)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("DML not enabled"));
	}

	if (gp_random_insert_segments > 0)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("limited insert segments not supported"));
	}

	CDXLNode *query_dxlnode = TranslateSelectQueryToDXL();
	const RangeTblEntry *rte = (RangeTblEntry *) gpdb::ListNth(
		m_query->rtable, m_query->resultRelation - 1);
	if (rte->relkind == 'f')
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Inserts with foreign tables"));
	}
	CDXLTableDescr *table_descr = CTranslatorUtils::GetTableDescr(
		m_mp, m_md_accessor, m_context->m_colid_counter, rte, m_query_id,
		&m_context->m_has_distributed_tables);

	const IMDRelation *md_rel = m_md_accessor->RetrieveRel(table_descr->MDId());

	BOOL rel_has_constraints = CTranslatorUtils::RelHasConstraints(md_rel);
	if (!optimizer_enable_dml_constraints && rel_has_constraints)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("INSERT with constraints"));
	}

	BOOL contains_foreign_parts =
		CTranslatorUtils::RelContainsForeignPartitions(md_rel, m_md_accessor);
	if (contains_foreign_parts)
	{
		// Partitioned tables with external/foreign partitions
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT(
					   "Insert with External/foreign partition storage types"));
	}

	// make note of the operator classes used in the distribution key
	NoteDistributionPolicyOpclasses(rte);

	const ULONG num_table_columns =
		CTranslatorUtils::GetNumNonSystemColumns(md_rel);
	const ULONG target_list_length = gpdb::ListLength(m_query->targetList);
	GPOS_ASSERT(num_table_columns >= target_list_length);
	GPOS_ASSERT(target_list_length == m_dxl_query_output_cols->Size());

	CDXLNode *project_list_dxlnode = nullptr;

	const ULONG num_system_cols = md_rel->ColumnCount() - num_table_columns;
	const ULONG num_non_dropped_cols =
		md_rel->NonDroppedColsCount() - num_system_cols;
	if (num_non_dropped_cols > target_list_length)
	{
		// missing target list entries
		project_list_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));
	}

	ULongPtrArray *source_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

	ULONG target_list_pos = 0;
	for (ULONG ul = 0; ul < num_table_columns; ul++)
	{
		const IMDColumn *mdcol = md_rel->GetMdCol(ul);
		GPOS_ASSERT(!mdcol->IsSystemColumn());

		if (mdcol->IsDropped())
		{
			continue;
		}

		if (target_list_pos < target_list_length)
		{
			INT attno = mdcol->AttrNum();

			TargetEntry *target_entry = (TargetEntry *) gpdb::ListNth(
				m_query->targetList, target_list_pos);
			AttrNumber resno = target_entry->resno;

			if (attno == resno)
			{
				CDXLNode *dxl_column =
					(*m_dxl_query_output_cols)[target_list_pos];
				CDXLScalarIdent *dxl_ident =
					CDXLScalarIdent::Cast(dxl_column->GetOperator());
				source_array->Append(
					GPOS_NEW(m_mp) ULONG(dxl_ident->GetDXLColRef()->Id()));
				target_list_pos++;
				continue;
			}
		}

		// target entry corresponding to the tables column not found, therefore
		// add a project element with null value scalar child
		CDXLNode *project_elem_dxlnode =
			CTranslatorUtils::CreateDXLProjElemConstNULL(
				m_mp, m_md_accessor, m_context->m_colid_counter, mdcol);
		ULONG colid =
			CDXLScalarProjElem::Cast(project_elem_dxlnode->GetOperator())->Id();
		project_list_dxlnode->AddChild(project_elem_dxlnode);
		source_array->Append(GPOS_NEW(m_mp) ULONG(colid));
	}

	CDXLLogicalInsert *insert_dxlnode =
		GPOS_NEW(m_mp) CDXLLogicalInsert(m_mp, table_descr, source_array);

	if (nullptr != project_list_dxlnode)
	{
		GPOS_ASSERT(0 < project_list_dxlnode->Arity());

		CDXLNode *project_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp));
		project_dxlnode->AddChild(project_list_dxlnode);
		project_dxlnode->AddChild(query_dxlnode);
		query_dxlnode = project_dxlnode;
	}

	return GPOS_NEW(m_mp) CDXLNode(m_mp, insert_dxlnode, query_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateCTASToDXL
//
//	@doc:
//		Translate a CTAS
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateCTASToDXL()
{
	GPOS_ASSERT(CMD_SELECT == m_query->commandType);
	const char *const relname = "FAKE_CTAS_RELNAME";

	m_is_ctas_query = true;
	CDXLNode *query_dxlnode = TranslateSelectQueryToDXL();
	CMDName *md_relname = CDXLUtils::CreateMDNameFromCharArray(m_mp, relname);

	CDXLColDescrArray *dxl_col_descr_array =
		GPOS_NEW(m_mp) CDXLColDescrArray(m_mp);

	const ULONG num_columns = gpdb::ListLength(m_query->targetList);

	ULongPtrArray *source_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);
	IntPtrArray *var_typmods = GPOS_NEW(m_mp) IntPtrArray(m_mp);

	List *col_names = NIL;
	for (ULONG ul = 0; ul < num_columns; ul++)
	{
		TargetEntry *target_entry =
			(TargetEntry *) gpdb::ListNth(m_query->targetList, ul);
		if (target_entry->resjunk)
		{
			continue;
		}
		AttrNumber resno = target_entry->resno;
		int var_typmod = gpdb::ExprTypeMod((Node *) target_entry->expr);
		var_typmods->Append(GPOS_NEW(m_mp) INT(var_typmod));

		CDXLNode *dxl_column = (*m_dxl_query_output_cols)[ul];
		CDXLScalarIdent *dxl_ident =
			CDXLScalarIdent::Cast(dxl_column->GetOperator());
		source_array->Append(GPOS_NEW(m_mp)
								 ULONG(dxl_ident->GetDXLColRef()->Id()));

		CMDName *md_colname = nullptr;
		if (nullptr != col_names && ul < gpdb::ListLength(col_names))
		{
			ColumnDef *col_def = (ColumnDef *) gpdb::ListNth(col_names, ul);
			md_colname =
				CDXLUtils::CreateMDNameFromCharArray(m_mp, col_def->colname);
		}
		else
		{
			md_colname = GPOS_NEW(m_mp)
				CMDName(m_mp, dxl_ident->GetDXLColRef()->MdName()->GetMDName());
		}

		GPOS_ASSERT(nullptr != md_colname);
		IMDId *mdid = dxl_ident->MdidType();
		mdid->AddRef();
		CDXLColDescr *dxl_col_descr = GPOS_NEW(m_mp)
			CDXLColDescr(md_colname, m_context->m_colid_counter->next_id(),
						 resno /* attno */, mdid, dxl_ident->TypeModifier(),
						 false /* is_dropped */
			);
		dxl_col_descr_array->Append(dxl_col_descr);
	}

	IMDRelation::Ereldistrpolicy rel_distr_policy =
		IMDRelation::EreldistrRandom;
	ULongPtrArray *distribution_colids = nullptr;

	IMdIdArray *distr_opfamilies = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	IMdIdArray *distr_opclasses = GPOS_NEW(m_mp) IMdIdArray(m_mp);

	if (nullptr != m_query->intoPolicy)
	{
		rel_distr_policy =
			CTranslatorRelcacheToDXL::GetRelDistribution(m_query->intoPolicy);

		if (IMDRelation::EreldistrHash == rel_distr_policy)
		{
			distribution_colids = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

			for (ULONG ul = 0; ul < (ULONG) m_query->intoPolicy->nattrs; ul++)
			{
				AttrNumber attno = m_query->intoPolicy->attrs[ul];
				GPOS_ASSERT(0 < attno);
				distribution_colids->Append(GPOS_NEW(m_mp) ULONG(attno - 1));

				Oid opfamily =
					gpdb::GetOpclassFamily(m_query->intoPolicy->opclasses[ul]);
				GPOS_ASSERT(InvalidOid != opfamily);
				// We use the opfamily to populate the
				// distribution spec within ORCA, but also need
				// the opclass to populate the distribution
				// policy of the created table in the catalog
				distr_opfamilies->Append(
					GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, opfamily));
				distr_opclasses->Append(GPOS_NEW(m_mp) CMDIdGPDB(
					IMDId::EmdidGeneral, m_query->intoPolicy->opclasses[ul]));
			}
		}
	}
	else
	{
		GpdbEreport(
			ERRCODE_SUCCESSFUL_COMPLETION, NOTICE,
			"Table doesn't have 'DISTRIBUTED BY' clause. Creating a NULL policy entry.",
			nullptr);
	}

	GPOS_ASSERT(IMDRelation::EreldistrMasterOnly != rel_distr_policy);
	m_context->m_has_distributed_tables = true;

	OID oid = 1;
	CMDIdGPDB *mdid = GPOS_NEW(m_mp) CMDIdGPDBCtas(oid);

	// Used to create a `CMDRelationCtasGPDB` in `PexprLogicalCTAS`
	// In the end, the "fake" relation will be generated as CPhysicalDML(Result node)
	// So the empty option/storagetype/relname/oid is fine. Cause we won't use it 
	// in physical plan.
	CDXLLogicalCTAS *ctas_dxlop = GPOS_NEW(m_mp) CDXLLogicalCTAS(
		m_mp, mdid, nullptr, md_relname, dxl_col_descr_array,
		GPOS_NEW(m_mp) CDXLCtasStorageOptions(), // empty
		rel_distr_policy, distribution_colids, distr_opfamilies,
		distr_opclasses, true /*fTempTable*/, IMDRelation::ErelstorageHeap, // heap by defualt
		source_array, var_typmods);

	return GPOS_NEW(m_mp) CDXLNode(m_mp, ctas_dxlop, query_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::ExtractStorageOptionStr
//
//	@doc:
//		Extract value for storage option
//
//---------------------------------------------------------------------------
CWStringDynamic *
CTranslatorQueryToDXL::ExtractStorageOptionStr(DefElem *def_elem)
{
	GPOS_ASSERT(nullptr != def_elem);

	CHAR *value = gpdb::DefGetString(def_elem);

	CWStringDynamic *result_str =
		CDXLUtils::CreateDynamicStringFromCharArray(m_mp, value);

	return result_str;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::GetCtidAndSegmentId
//
//	@doc:
//		Obtains the ids of the ctid and segmentid columns for the target
//		table of a DML query
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::GetCtidAndSegmentId(ULONG *ctid, ULONG *segment_id)
{
	const FormData_pg_attribute *att_tup_tupid =
		SystemAttributeDefinition(SelfItemPointerAttributeNumber);
	const FormData_pg_attribute *att_tup_segid =
		SystemAttributeDefinition(GpSegmentIdAttributeNumber);


	// ctid column id
	IMDId *mdid =
		GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, att_tup_tupid->atttypid);
	*ctid = CTranslatorUtils::GetColId(m_query_level, m_query->resultRelation,
									   SelfItemPointerAttributeNumber, mdid,
									   m_var_to_colid_map);
	mdid->Release();

	// segmentid column id
	mdid =
		GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, att_tup_segid->atttypid);
	*segment_id = CTranslatorUtils::GetColId(
		m_query_level, m_query->resultRelation, GpSegmentIdAttributeNumber,
		mdid, m_var_to_colid_map);
	mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateDeleteQueryToDXL
//
//	@doc:
//		Translate a delete stmt
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateDeleteQueryToDXL()
{
	GPOS_ASSERT(CMD_DELETE == m_query->commandType);
	GPOS_ASSERT(0 < m_query->resultRelation);

	if (!optimizer_enable_dml)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("DML not enabled"));
	}

	CDXLNode *query_dxlnode = TranslateSelectQueryToDXL();
	const RangeTblEntry *rte = (RangeTblEntry *) gpdb::ListNth(
		m_query->rtable, m_query->resultRelation - 1);
	if (rte->relkind == 'f')
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Deletes with foreign tables"));
	}
	CDXLTableDescr *table_descr = CTranslatorUtils::GetTableDescr(
		m_mp, m_md_accessor, m_context->m_colid_counter, rte, m_query_id,
		&m_context->m_has_distributed_tables);

	const IMDRelation *md_rel = m_md_accessor->RetrieveRel(table_descr->MDId());

	// CBDB_MERGE_FIXME: Support DML operations on partitioned tables
	if (md_rel->IsPartitioned())
	{
		// GPDB_12_MERGE_FIXME: Support DML operations on partitioned tables
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("DML(delete) on partitioned tables"));
	}

	BOOL contains_foreign_parts =
		CTranslatorUtils::RelContainsForeignPartitions(md_rel, m_md_accessor);
	if (contains_foreign_parts)
	{
		// Partitioned tables with external/foreign partitions
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT(
					   "Delete with External/foreign partition storage types"));
	}
	// make note of the operator classes used in the distribution key
	NoteDistributionPolicyOpclasses(rte);

	ULONG ctid_colid = 0;
	ULONG segid_colid = 0;
	GetCtidAndSegmentId(&ctid_colid, &segid_colid);

	ULongPtrArray *delete_colid_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

	const ULONG num_of_non_sys_cols = md_rel->ColumnCount();
	for (ULONG ul = 0; ul < num_of_non_sys_cols; ul++)
	{
		const IMDColumn *mdcol = md_rel->GetMdCol(ul);
		if (mdcol->IsSystemColumn() || mdcol->IsDropped())
		{
			continue;
		}

		ULONG colid = CTranslatorUtils::GetColId(
			m_query_level, m_query->resultRelation, mdcol->AttrNum(),
			mdcol->MdidType(), m_var_to_colid_map);
		delete_colid_array->Append(GPOS_NEW(m_mp) ULONG(colid));
	}

	CDXLLogicalDelete *delete_dxlop = GPOS_NEW(m_mp) CDXLLogicalDelete(
		m_mp, table_descr, ctid_colid, segid_colid, delete_colid_array);

	return GPOS_NEW(m_mp) CDXLNode(m_mp, delete_dxlop, query_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateUpdateQueryToDXL
//
//	@doc:
//		Translate an update stmt
//
//---------------------------------------------------------------------------

CDXLNode *
CTranslatorQueryToDXL::TranslateUpdateQueryToDXL()
{
	GPOS_ASSERT(CMD_UPDATE == m_query->commandType);
	GPOS_ASSERT(0 < m_query->resultRelation);

	if (!optimizer_enable_dml)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("DML not enabled"));
	}

	CDXLNode *query_dxlnode = TranslateSelectQueryToDXL();
	const RangeTblEntry *rte = (RangeTblEntry *) gpdb::ListNth(
		m_query->rtable, m_query->resultRelation - 1);

	if (rte->relkind == 'f')
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Updates with foreign tables"));
	}
	CDXLTableDescr *table_descr = CTranslatorUtils::GetTableDescr(
		m_mp, m_md_accessor, m_context->m_colid_counter, rte, m_query_id,
		&m_context->m_has_distributed_tables);

	const IMDRelation *md_rel = m_md_accessor->RetrieveRel(table_descr->MDId());

	if (!optimizer_enable_dml_constraints &&
		CTranslatorUtils::RelHasConstraints(md_rel))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("UPDATE with constraints"));
	}

	// CBDB_MERGE_FIXME: Support DML operations on partitioned tables
	if (md_rel->IsPartitioned())
	{
		// GPDB_12_MERGE_FIXME: Support DML operations on partitioned tables
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("DML(update) on partitioned tables"));
	}

	BOOL contains_foreign_parts =
		CTranslatorUtils::RelContainsForeignPartitions(md_rel, m_md_accessor);
	if (contains_foreign_parts)
	{
		// Partitioned tables with external/foreign partitions
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT(
					   "Update with External/foreign partition storage types"));
	}
	// make note of the operator classes used in the distribution key
	NoteDistributionPolicyOpclasses(rte);

	ULONG ctid_colid = 0;
	ULONG segmentid_colid = 0;
	GetCtidAndSegmentId(&ctid_colid, &segmentid_colid);

	// get (resno -> colId) mapping of columns to be updated
	IntToUlongMap *update_column_map = UpdatedColumnMapping();

	const ULONG num_of_non_sys_cols = md_rel->ColumnCount();
	ULongPtrArray *insert_colid_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);
	ULongPtrArray *delete_colid_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

	for (ULONG ul = 0; ul < num_of_non_sys_cols; ul++)
	{
		const IMDColumn *mdcol = md_rel->GetMdCol(ul);
		if (mdcol->IsSystemColumn() || mdcol->IsDropped())
		{
			continue;
		}

		INT attno = mdcol->AttrNum();
		ULONG *updated_colid = update_column_map->Find(&attno);

		ULONG colid = CTranslatorUtils::GetColId(
			m_query_level, m_query->resultRelation, attno, mdcol->MdidType(),
			m_var_to_colid_map);

		// if the column is in the query outputs then use it
		// otherwise get the column id created by the child query
		if (nullptr != updated_colid)
		{
			insert_colid_array->Append(GPOS_NEW(m_mp) ULONG(*updated_colid));
		}
		else
		{
			insert_colid_array->Append(GPOS_NEW(m_mp) ULONG(colid));
		}

		delete_colid_array->Append(GPOS_NEW(m_mp) ULONG(colid));
	}

	update_column_map->Release();
	CDXLLogicalUpdate *pdxlopupdate = GPOS_NEW(m_mp)
		CDXLLogicalUpdate(m_mp, table_descr, ctid_colid, segmentid_colid,
						  delete_colid_array, insert_colid_array);

	return GPOS_NEW(m_mp) CDXLNode(m_mp, pdxlopupdate, query_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::UpdatedColumnMapping
//
//	@doc:
// 		Return resno -> colId mapping of columns to be updated
//
//---------------------------------------------------------------------------
IntToUlongMap *
CTranslatorQueryToDXL::UpdatedColumnMapping()
{
	IntToUlongMap *update_column_map = GPOS_NEW(m_mp) IntToUlongMap(m_mp);

	ListCell *lc = nullptr;
	ULONG ul = 0;
	ULONG output_columns GPOS_ASSERTS_ONLY = 0;
	ForEach(lc, m_query->targetList)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));
		ULONG resno = target_entry->resno;
		GPOS_ASSERT(0 < resno);

		// resjunk true columns may be now existing in the query tree, for instance
		// ctid column in case of relations, see rewriteTargetListUD in GPDB.
		// In ORCA, resjunk true columns (ex ctid) required to identify the tuple
		// are included later, so, its safe to not include them here in the output query list.
		// In planner, a MODIFYTABLE node is created on top of the plan instead of DML node,
		// once we plan generating MODIFYTABLE node from ORCA, we may revisit it.
		if (!target_entry->resjunk)
		{
			CDXLNode *dxl_column = (*m_dxl_query_output_cols)[ul];
			CDXLScalarIdent *dxl_ident =
				CDXLScalarIdent::Cast(dxl_column->GetOperator());
			ULONG colid = dxl_ident->GetDXLColRef()->Id();

			StoreAttnoColIdMapping(update_column_map, resno, colid);
			output_columns++;
		}
		ul++;
	}

	GPOS_ASSERT(output_columns == m_dxl_query_output_cols->Size());
	return update_column_map;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::OIDFound
//
//	@doc:
// 		Helper to check if OID is included in given array of OIDs
//
//---------------------------------------------------------------------------
BOOL
CTranslatorQueryToDXL::OIDFound(OID oid, const OID oids[], ULONG size)
{
	BOOL found = false;
	for (ULONG ul = 0; !found && ul < size; ul++)
	{
		found = (oids[ul] == oid);
	}

	return found;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::IsLeadWindowFunc
//
//	@doc:
// 		Check if given operator is LEAD window function
//
//---------------------------------------------------------------------------
BOOL
CTranslatorQueryToDXL::IsLeadWindowFunc(CDXLOperator *dxlop)
{
	BOOL is_lead_func = false;
	if (EdxlopScalarWindowRef == dxlop->GetDXLOperator())
	{
		CDXLScalarWindowRef *winref_dxlop = CDXLScalarWindowRef::Cast(dxlop);
		const CMDIdGPDB *mdid_gpdb =
			CMDIdGPDB::CastMdid(winref_dxlop->FuncMdId());
		OID oid = mdid_gpdb->Oid();
		is_lead_func =
			OIDFound(oid, lead_func_oids, GPOS_ARRAY_SIZE(lead_func_oids));
	}

	return is_lead_func;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::IsLagWindowFunc
//
//	@doc:
// 		Check if given operator is LAG window function
//
//---------------------------------------------------------------------------
BOOL
CTranslatorQueryToDXL::IsLagWindowFunc(CDXLOperator *dxlop)
{
	BOOL is_lag = false;
	if (EdxlopScalarWindowRef == dxlop->GetDXLOperator())
	{
		CDXLScalarWindowRef *winref_dxlop = CDXLScalarWindowRef::Cast(dxlop);
		const CMDIdGPDB *mdid_gpdb =
			CMDIdGPDB::CastMdid(winref_dxlop->FuncMdId());
		OID oid = mdid_gpdb->Oid();
		is_lag = OIDFound(oid, lag_func_oids, GPOS_ARRAY_SIZE(lag_func_oids));
	}

	return is_lag;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CreateWindowFramForLeadLag
//
//	@doc:
// 		Manufacture window frame for lead/lag functions
//
//---------------------------------------------------------------------------
CDXLWindowFrame *
CTranslatorQueryToDXL::CreateWindowFramForLeadLag(BOOL is_lead_func,
												  CDXLNode *dxl_offset) const
{
	EdxlFrameBoundary dxl_frame_lead = EdxlfbBoundedFollowing;
	EdxlFrameBoundary dxl_frame_trail = EdxlfbBoundedFollowing;
	if (!is_lead_func)
	{
		dxl_frame_lead = EdxlfbBoundedPreceding;
		dxl_frame_trail = EdxlfbBoundedPreceding;
	}

	CDXLNode *dxl_lead_edge = nullptr;
	CDXLNode *dxl_trail_edge = nullptr;
	if (nullptr == dxl_offset)
	{
		dxl_lead_edge = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarWindowFrameEdge(
							   m_mp, true /* fLeading */, dxl_frame_lead));
		dxl_trail_edge = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarWindowFrameEdge(
							   m_mp, false /* fLeading */, dxl_frame_trail));

		dxl_lead_edge->AddChild(
			CTranslatorUtils::CreateDXLProjElemFromInt8Const(
				m_mp, m_md_accessor, 1 /*iVal*/));
		dxl_trail_edge->AddChild(
			CTranslatorUtils::CreateDXLProjElemFromInt8Const(
				m_mp, m_md_accessor, 1 /*iVal*/));
	}
	else
	{
		// overwrite frame edge types based on specified offset type
		if (EdxlopScalarConstValue !=
			dxl_offset->GetOperator()->GetDXLOperator())
		{
			if (is_lead_func)
			{
				dxl_frame_lead = EdxlfbDelayedBoundedFollowing;
				dxl_frame_trail = EdxlfbDelayedBoundedFollowing;
			}
			else
			{
				dxl_frame_lead = EdxlfbDelayedBoundedPreceding;
				dxl_frame_trail = EdxlfbDelayedBoundedPreceding;
			}
		}
		dxl_lead_edge = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarWindowFrameEdge(
							   m_mp, true /* fLeading */, dxl_frame_lead));
		dxl_trail_edge = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarWindowFrameEdge(
							   m_mp, false /* fLeading */, dxl_frame_trail));

		dxl_offset->AddRef();
		dxl_lead_edge->AddChild(dxl_offset);
		dxl_offset->AddRef();
		dxl_trail_edge->AddChild(dxl_offset);
	}

	// manufacture a frame for LEAD/LAG function
	return GPOS_NEW(m_mp) CDXLWindowFrame(
		EdxlfsRow,	   // frame specification
		EdxlfesNulls,  // frame exclusion strategy is set to exclude NULLs in GPDB
		dxl_lead_edge, dxl_trail_edge, InvalidOid, InvalidOid, InvalidOid,
		false, false);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::UpdateLeadLagWinSpecPos
//
//	@doc:
// 		LEAD/LAG window functions need special frames to get executed correctly;
//		these frames are system-generated and cannot be specified in query text;
//		this function adds new entries to the list of window specs holding these
//		manufactured frames, and updates window spec references of LEAD/LAG
//		functions accordingly
//
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::UpdateLeadLagWinSpecPos(
	CDXLNode *project_list_dxlnode,			// project list holding WinRef nodes
	CDXLWindowSpecArray *window_spec_array	// original list of window spec
) const
{
	GPOS_ASSERT(nullptr != project_list_dxlnode);
	GPOS_ASSERT(nullptr != window_spec_array);

	const ULONG arity = project_list_dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *child_dxlnode = (*(*project_list_dxlnode)[ul])[0];
		CDXLOperator *dxlop = child_dxlnode->GetOperator();
		BOOL is_lead_func = IsLeadWindowFunc(dxlop);
		BOOL is_lag = IsLagWindowFunc(dxlop);
		if (is_lead_func || is_lag)
		{
			CDXLScalarWindowRef *winref_dxlop =
				CDXLScalarWindowRef::Cast(dxlop);
			CDXLWindowSpec *window_spec_dxlnode =
				(*window_spec_array)[winref_dxlop->GetWindSpecPos()];
			CMDName *mdname = nullptr;
			if (nullptr != window_spec_dxlnode->MdName())
			{
				mdname = GPOS_NEW(m_mp)
					CMDName(m_mp, window_spec_dxlnode->MdName()->GetMDName());
			}

			// find if an offset is specified
			CDXLNode *dxl_offset = nullptr;
			if (1 < child_dxlnode->Arity())
			{
				dxl_offset = (*child_dxlnode)[1];
			}

			// create LEAD/LAG frame
			CDXLWindowFrame *window_frame =
				CreateWindowFramForLeadLag(is_lead_func, dxl_offset);

			// create new window spec object
			window_spec_dxlnode->GetPartitionByColIdArray()->AddRef();
			window_spec_dxlnode->GetSortColListDXL()->AddRef();
			CDXLWindowSpec *pdxlwsNew = GPOS_NEW(m_mp) CDXLWindowSpec(
				m_mp, window_spec_dxlnode->GetPartitionByColIdArray(), mdname,
				window_spec_dxlnode->GetSortColListDXL(), window_frame);
			window_spec_array->Append(pdxlwsNew);

			// update win spec pos of LEAD/LAG function
			winref_dxlop->SetWinSpecPos(window_spec_array->Size() - 1);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateWindowSpecToDXL
//
//	@doc:
//		Translate window specs
//
//---------------------------------------------------------------------------
CDXLWindowSpecArray *
CTranslatorQueryToDXL::TranslateWindowSpecToDXL(
	List *window_clause, IntToUlongMap *sort_col_attno_to_colid_mapping,
	CDXLNode *project_list_dxlnode_node)
{
	GPOS_ASSERT(nullptr != window_clause);
	GPOS_ASSERT(nullptr != sort_col_attno_to_colid_mapping);
	GPOS_ASSERT(nullptr != project_list_dxlnode_node);

	CDXLWindowSpecArray *window_spec_array =
		GPOS_NEW(m_mp) CDXLWindowSpecArray(m_mp);

	// translate window specification
	ListCell *lc;
	ForEach(lc, window_clause)
	{
		WindowClause *wc = (WindowClause *) lfirst(lc);
		ULongPtrArray *part_columns = TranslatePartColumns(
			wc->partitionClause, sort_col_attno_to_colid_mapping);

		CDXLNode *sort_col_list_dxl = nullptr;
		CMDName *mdname = nullptr;
		CDXLWindowFrame *window_frame = nullptr;

		if (nullptr != wc->name)
		{
			CWStringDynamic *alias_str =
				CDXLUtils::CreateDynamicStringFromCharArray(m_mp, wc->name);
			mdname = GPOS_NEW(m_mp) CMDName(m_mp, alias_str);
			GPOS_DELETE(alias_str);
		}

		if (0 < gpdb::ListLength(wc->orderClause))
		{
			// create a sorting col list
			sort_col_list_dxl = GPOS_NEW(m_mp)
				CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarSortColList(m_mp));

			CDXLNodeArray *dxl_sort_cols = TranslateSortColumsToDXL(
				wc->orderClause, sort_col_attno_to_colid_mapping);
			const ULONG size = dxl_sort_cols->Size();
			for (ULONG ul = 0; ul < size; ul++)
			{
				CDXLNode *dxl_sort_clause = (*dxl_sort_cols)[ul];
				dxl_sort_clause->AddRef();
				sort_col_list_dxl->AddChild(dxl_sort_clause);
			}
			dxl_sort_cols->Release();
		}

		window_frame = m_scalar_translator->TranslateWindowFrameToDXL(
			wc->frameOptions, wc->startOffset, wc->endOffset,
			wc->startInRangeFunc, wc->endInRangeFunc, wc->inRangeColl,
			wc->inRangeAsc, wc->inRangeNullsFirst, m_var_to_colid_map,
			project_list_dxlnode_node);

		CDXLWindowSpec *window_spec_dxlnode = GPOS_NEW(m_mp) CDXLWindowSpec(
			m_mp, part_columns, mdname, sort_col_list_dxl, window_frame);
		window_spec_array->Append(window_spec_dxlnode);
	}

	return window_spec_array;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateWindowToDXL
//
//	@doc:
//		Translate a window operator
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateWindowToDXL(
	CDXLNode *child_dxlnode, List *target_list, List *window_clause,
	List * /*sort_clause*/, IntToUlongMap *sort_col_attno_to_colid_mapping,
	IntToUlongMap *output_attno_to_colid_mapping)
{
	if (0 == gpdb::ListLength(window_clause))
	{
		return child_dxlnode;
	}

	// translate target list entries
	CDXLNode *project_list_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

	CDXLNode *new_child_project_list_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));
	ListCell *lc = nullptr;
	ULONG resno = 1;

	// target entries that are result of flattening join alias and
	// are equivalent to a defined Window specs target entry
	List *omitted_target_entries = NIL;
	List *resno_list = NIL;

	ForEach(lc, target_list)
	{
		BOOL insert_sort_info = true;
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));

		// create the DXL node holding the target list entry
		CDXLNode *project_elem_dxlnode = TranslateExprToDXLProject(
			target_entry->expr, target_entry->resname);
		ULONG colid =
			CDXLScalarProjElem::Cast(project_elem_dxlnode->GetOperator())->Id();

		if (!target_entry->resjunk)
		{
			if (IsA(target_entry->expr, Var) ||
				IsA(target_entry->expr, WindowFunc))
			{
				// add window functions and non-computed columns to the project list of the window operator
				project_list_dxlnode->AddChild(project_elem_dxlnode);

				StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno,
									   colid);
			}
			else if (CTranslatorUtils::IsReferencedInWindowSpec(target_entry,
																window_clause))
			{
				// add computed column used in window specification needed in the output columns
				// to the child's project list
				new_child_project_list_dxlnode->AddChild(project_elem_dxlnode);

				// construct a scalar identifier that points to the computed column and
				// add it to the project list of the window operator
				CMDName *mdname_alias = GPOS_NEW(m_mp)
					CMDName(m_mp, CDXLScalarProjElem::Cast(
									  project_elem_dxlnode->GetOperator())
									  ->GetMdNameAlias()
									  ->GetMDName());
				CDXLNode *new_project_elem_dxlnode = GPOS_NEW(m_mp)
					CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjElem(
									   m_mp, colid, mdname_alias));
				CDXLNode *project_elem_new_child_dxlnode =
					GPOS_NEW(m_mp) CDXLNode(
						m_mp,
						GPOS_NEW(m_mp) CDXLScalarIdent(
							m_mp, GPOS_NEW(m_mp) CDXLColRef(
									  GPOS_NEW(m_mp) CMDName(
										  m_mp, mdname_alias->GetMDName()),
									  colid,
									  GPOS_NEW(m_mp) CMDIdGPDB(
										  IMDId::EmdidGeneral,
										  gpdb::ExprType(
											  (Node *) target_entry->expr)),
									  gpdb::ExprTypeMod(
										  (Node *) target_entry->expr))));
				new_project_elem_dxlnode->AddChild(
					project_elem_new_child_dxlnode);
				project_list_dxlnode->AddChild(new_project_elem_dxlnode);

				StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno,
									   colid);
			}
			else
			{
				insert_sort_info = false;
				omitted_target_entries =
					gpdb::LAppend(omitted_target_entries, target_entry);
				resno_list = gpdb::LAppendInt(resno_list, resno);

				project_elem_dxlnode->Release();
			}
		}
		else if (IsA(target_entry->expr, WindowFunc))
		{
			// computed columns used in the order by clause
			project_list_dxlnode->AddChild(project_elem_dxlnode);
		}
		else if (!IsA(target_entry->expr, Var))
		{
			GPOS_ASSERT(CTranslatorUtils::IsReferencedInWindowSpec(
				target_entry, window_clause));
			// computed columns used in the window specification
			new_child_project_list_dxlnode->AddChild(project_elem_dxlnode);
		}
		else
		{
			project_elem_dxlnode->Release();
		}

		if (insert_sort_info)
		{
			AddSortingGroupingColumn(target_entry,
									 sort_col_attno_to_colid_mapping, colid);
		}

		resno++;
	}

	lc = nullptr;

	// process target entries that are a result of flattening join alias
	ListCell *lc_resno = nullptr;
	ForBoth(lc, omitted_target_entries, lc_resno, resno_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		INT resno = (INT) lfirst_int(lc_resno);

		TargetEntry *te_window_spec =
			CTranslatorUtils::GetWindowSpecTargetEntry(
				(Node *) target_entry->expr, window_clause, target_list);
		if (nullptr != te_window_spec)
		{
			const ULONG colid = CTranslatorUtils::GetColId(
				(INT) te_window_spec->ressortgroupref,
				sort_col_attno_to_colid_mapping);
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
			AddSortingGroupingColumn(target_entry,
									 sort_col_attno_to_colid_mapping, colid);
		}
	}
	if (NIL != omitted_target_entries)
	{
		gpdb::GPDBFree(omitted_target_entries);
	}

	// translate window spec
	CDXLWindowSpecArray *window_spec_array =
		TranslateWindowSpecToDXL(window_clause, sort_col_attno_to_colid_mapping,
								 new_child_project_list_dxlnode);

	CDXLNode *new_child_dxlnode = nullptr;

	if (0 < new_child_project_list_dxlnode->Arity())
	{
		// create a project list for the computed columns used in the window specification
		new_child_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp));
		new_child_dxlnode->AddChild(new_child_project_list_dxlnode);
		new_child_dxlnode->AddChild(child_dxlnode);
		child_dxlnode = new_child_dxlnode;
	}
	else
	{
		// clean up
		new_child_project_list_dxlnode->Release();
	}

	if (!CTranslatorUtils::HasProjElem(project_list_dxlnode,
									   EdxlopScalarWindowRef))
	{
		project_list_dxlnode->Release();
		window_spec_array->Release();

		return child_dxlnode;
	}

	// update window spec positions of LEAD/LAG functions
	UpdateLeadLagWinSpecPos(project_list_dxlnode, window_spec_array);

	CDXLLogicalWindow *window_dxlop =
		GPOS_NEW(m_mp) CDXLLogicalWindow(m_mp, window_spec_array);
	CDXLNode *dxlnode = GPOS_NEW(m_mp) CDXLNode(m_mp, window_dxlop);

	dxlnode->AddChild(project_list_dxlnode);
	dxlnode->AddChild(child_dxlnode);

	return dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslatePartColumns
//
//	@doc:
//		Translate the list of partition-by column identifiers
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorQueryToDXL::TranslatePartColumns(
	List *partition_by_clause, IntToUlongMap *col_attno_colid_mapping) const
{
	ULongPtrArray *part_cols = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

	ListCell *lc = nullptr;
	ForEach(lc, partition_by_clause)
	{
		Node *partition_clause = (Node *) lfirst(lc);
		GPOS_ASSERT(nullptr != partition_clause);

		GPOS_ASSERT(IsA(partition_clause, SortGroupClause));
		SortGroupClause *sort_group_clause =
			(SortGroupClause *) partition_clause;

		// get the colid of the partition-by column
		ULONG colid = CTranslatorUtils::GetColId(
			(INT) sort_group_clause->tleSortGroupRef, col_attno_colid_mapping);

		part_cols->Append(GPOS_NEW(m_mp) ULONG(colid));
	}

	return part_cols;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateSortColumsToDXL
//
//	@doc:
//		Translate the list of sorting columns
//
//---------------------------------------------------------------------------
CDXLNodeArray *
CTranslatorQueryToDXL::TranslateSortColumsToDXL(
	List *sort_clause, IntToUlongMap *col_attno_colid_mapping) const
{
	CDXLNodeArray *dxlnodes = GPOS_NEW(m_mp) CDXLNodeArray(m_mp);

	ListCell *lc = nullptr;
	ForEach(lc, sort_clause)
	{
		Node *node_sort_clause = (Node *) lfirst(lc);
		GPOS_ASSERT(nullptr != node_sort_clause);

		GPOS_ASSERT(IsA(node_sort_clause, SortGroupClause));

		SortGroupClause *sort_group_clause =
			(SortGroupClause *) node_sort_clause;

		// get the colid of the sorting column
		const ULONG colid = CTranslatorUtils::GetColId(
			(INT) sort_group_clause->tleSortGroupRef, col_attno_colid_mapping);

		OID oid = sort_group_clause->sortop;

		// get operator name
		CMDIdGPDB *op_mdid = GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, oid);
		const IMDScalarOp *md_scalar_op = m_md_accessor->RetrieveScOp(op_mdid);

		const CWStringConst *str = md_scalar_op->Mdname().GetMDName();
		GPOS_ASSERT(nullptr != str);

		CDXLScalarSortCol *sc_sort_col_dxlop = GPOS_NEW(m_mp)
			CDXLScalarSortCol(m_mp, colid, op_mdid,
							  GPOS_NEW(m_mp) CWStringConst(str->GetBuffer()),
							  sort_group_clause->nulls_first);

		// create the DXL node holding the sorting col
		CDXLNode *sort_col_dxlnode =
			GPOS_NEW(m_mp) CDXLNode(m_mp, sc_sort_col_dxlop);

		dxlnodes->Append(sort_col_dxlnode);
	}

	return dxlnodes;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateLimitToDXLGroupBy
//
//	@doc:
//		Translate the list of sorting columns, limit offset and limit count
//		into a CDXLLogicalGroupBy node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateLimitToDXLGroupBy(
	List *sort_clause, Node *limit_count, Node *limit_offset_node,
	CDXLNode *child_dxlnode, IntToUlongMap *grpcols_to_colid_mapping)
{
	if (0 == gpdb::ListLength(sort_clause) && nullptr == limit_count &&
		nullptr == limit_offset_node)
	{
		return child_dxlnode;
	}

	// do not remove limit if it is immediately under a DML (JIRA: GPSQL-2669)
	// otherwise we may increase the storage size because there are less opportunities for compression
	BOOL is_limit_top_level = (m_is_top_query_dml && 1 == m_query_level) ||
							  (m_is_ctas_query && 0 == m_query_level);
	CDXLNode *limit_dxlnode = GPOS_NEW(m_mp) CDXLNode(
		m_mp, GPOS_NEW(m_mp) CDXLLogicalLimit(m_mp, is_limit_top_level));

	// create a sorting col list
	CDXLNode *sort_col_list_dxl = GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarSortColList(m_mp));

	CDXLNodeArray *dxl_sort_cols =
		TranslateSortColumsToDXL(sort_clause, grpcols_to_colid_mapping);
	const ULONG size = dxl_sort_cols->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CDXLNode *sort_col_dxlnode = (*dxl_sort_cols)[ul];
		sort_col_dxlnode->AddRef();
		sort_col_list_dxl->AddChild(sort_col_dxlnode);
	}
	dxl_sort_cols->Release();

	// create limit count
	CDXLNode *limit_count_dxlnode = GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarLimitCount(m_mp));

	if (nullptr != limit_count)
	{
		limit_count_dxlnode->AddChild(TranslateExprToDXL((Expr *) limit_count));
	}

	// create limit offset
	CDXLNode *limit_offset_dxlnode = GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarLimitOffset(m_mp));

	if (nullptr != limit_offset_node)
	{
		limit_offset_dxlnode->AddChild(
			TranslateExprToDXL((Expr *) limit_offset_node));
	}

	limit_dxlnode->AddChild(sort_col_list_dxl);
	limit_dxlnode->AddChild(limit_count_dxlnode);
	limit_dxlnode->AddChild(limit_offset_dxlnode);
	limit_dxlnode->AddChild(child_dxlnode);

	return limit_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::AddSortingGroupingColumn
//
//	@doc:
//		Add sorting and grouping column into the hash map
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::AddSortingGroupingColumn(
	TargetEntry *target_entry, IntToUlongMap *sort_grpref_to_colid_mapping,
	ULONG colid) const
{
	if (0 < target_entry->ressortgroupref)
	{
		INT *key = GPOS_NEW(m_mp) INT(target_entry->ressortgroupref);
		ULONG *value = GPOS_NEW(m_mp) ULONG(colid);

		// insert idx-colid mapping in the hash map
		BOOL is_res GPOS_ASSERTS_ONLY =
			sort_grpref_to_colid_mapping->Insert(key, value);

		GPOS_ASSERT(is_res);
	}
}

static BOOL
ExpressionContainsMissingVars(const Expr *expr, CBitSet *grpby_cols_bitset)
{
	if (IsA(expr, Var) && !grpby_cols_bitset->Get(((Var *) expr)->varattno))
	{
		return true;
	}
	if (IsA(expr, SubLink) && IsA(((SubLink *) expr)->subselect, Query))
	{
		ListCell *lc = nullptr;
		ForEach(lc, ((Query *) ((SubLink *) expr)->subselect)->targetList)
		{
			if (ExpressionContainsMissingVars(
					((TargetEntry *) lfirst(lc))->expr, grpby_cols_bitset))
			{
				return true;
			}
		}
	}
	else if (IsA(expr, OpExpr))
	{
		ListCell *lc = nullptr;
		ForEach(lc, ((OpExpr *) expr)->args)
		{
			if (ExpressionContainsMissingVars((Expr *) lfirst(lc),
											  grpby_cols_bitset))
			{
				return true;
			}
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CreateSimpleGroupBy
//
//	@doc:
//		Translate a query with grouping clause into a CDXLLogicalGroupBy node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::CreateSimpleGroupBy(
	List *target_list, List *group_clause, CBitSet *grpby_cols_bitset,
	BOOL has_aggs, BOOL has_grouping_sets, CDXLNode *child_dxlnode,
	IntToUlongMap *sort_grpref_to_colid_mapping,
	IntToUlongMap *child_attno_colid_mapping,
	IntToUlongMap *output_attno_to_colid_mapping)
{
	if (nullptr == grpby_cols_bitset)
	{
		GPOS_ASSERT(!has_aggs);
		if (!has_grouping_sets)
		{
			// no group by needed and not part of a grouping sets query:
			// propagate child columns to output columns
			IntUlongHashmapIter mi(child_attno_colid_mapping);
			while (mi.Advance())
			{
#ifdef GPOS_DEBUG
				BOOL result =
#endif	// GPOS_DEBUG
					output_attno_to_colid_mapping->Insert(
						GPOS_NEW(m_mp) INT(*(mi.Key())),
						GPOS_NEW(m_mp) ULONG(*(mi.Value())));
				GPOS_ASSERT(result);
			}
		}
		// else:
		// in queries with grouping sets we may generate a branch corresponding to GB grouping sets ();
		// in that case do not propagate the child columns to the output hash map, as later
		// processing may introduce NULLs for those

		return child_dxlnode;
	}

	List *dqa_list = NIL;
	// construct the project list of the group-by operator
	CDXLNode *project_list_grpby_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

	ListCell *lc = nullptr;
	ULONG num_dqa = 0;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));
		GPOS_ASSERT(0 < target_entry->resno);
		ULONG resno = target_entry->resno;

		TargetEntry *te_equivalent =
			CTranslatorUtils::GetGroupingColumnTargetEntry(
				(Node *) target_entry->expr, group_clause, target_list);

		BOOL is_grouping_col =
			grpby_cols_bitset->Get(target_entry->ressortgroupref) ||
			(nullptr != te_equivalent &&
			 grpby_cols_bitset->Get(te_equivalent->ressortgroupref));
		ULONG colid = 0;

		if (is_grouping_col)
		{
			// find colid for grouping column
			colid =
				CTranslatorUtils::GetColId(resno, child_attno_colid_mapping);
		}
		else if (IsA(target_entry->expr, Aggref))
		{
			if (IsA(target_entry->expr, Aggref) &&
				((Aggref *) target_entry->expr)->aggdistinct &&
				!IsDuplicateDqaArg(dqa_list, (Aggref *) target_entry->expr))
			{
				dqa_list = gpdb::LAppend(dqa_list,
										 gpdb::CopyObject(target_entry->expr));
				num_dqa++;
			}

			if (has_grouping_sets)
			{
				// If the grouping set is an ordered aggregate with direct
				// args, then we need to ensure that every direct arg exists in
				// the group by columns bitset. This is important when a ROLLUP
				// uses direct args. For example, consider the followinng
				// query:
				//
				// ```
				// SELECT a, rank(a) WITHIN GROUP (order by b nulls last)
				// FROM (values (1,1),(1,4),(1,5),(3,1),(3,2)) v(a,b)
				// GROUP BY ROLLUP (a) ORDER BY a;
				// ```
				//
				// ROLLUP (a) on values produces sets: (1), (3), ().
				//
				// In this case we need to ensure that () set will fetch direct
				// arg "a" as NULL. Whereas (1) and (3) will fetch "a" off of
				// any tuple in their respective sets.
				ListCell *ilc = nullptr;
				ForEach(ilc, ((Aggref *) target_entry->expr)->aggdirectargs)
				{
					if (ExpressionContainsMissingVars((Expr *) lfirst(ilc),
													  grpby_cols_bitset))
					{
						((Aggref *) target_entry->expr)->aggdirectargs = NIL;
						break;
					}
				}
			}

			// create a project element for aggregate
			CDXLNode *project_elem_dxlnode = TranslateExprToDXLProject(
				target_entry->expr, target_entry->resname);
			project_list_grpby_dxlnode->AddChild(project_elem_dxlnode);
			colid =
				CDXLScalarProjElem::Cast(project_elem_dxlnode->GetOperator())
					->Id();
			AddSortingGroupingColumn(target_entry, sort_grpref_to_colid_mapping,
									 colid);
		}

		if (is_grouping_col || IsA(target_entry->expr, Aggref))
		{
			// add to the list of output columns
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
		}
		else if (0 == grpby_cols_bitset->Size() && !has_grouping_sets &&
				 !has_aggs)
		{
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
		}
	}

	if (1 < num_dqa && !optimizer_enable_multiple_distinct_aggs)
	{
		GPOS_RAISE(
			gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
			GPOS_WSZ_LIT(
				"Multiple Distinct Qualified Aggregates are disabled in the optimizer"));
	}

	// initialize the array of grouping columns
	ULongPtrArray *grouping_cols = CTranslatorUtils::GetGroupingColidArray(
		m_mp, grpby_cols_bitset, sort_grpref_to_colid_mapping);

	// clean up
	if (NIL != dqa_list)
	{
		gpdb::ListFree(dqa_list);
	}

	return GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalGroupBy(m_mp, grouping_cols),
				 project_list_grpby_dxlnode, child_dxlnode);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::IsDuplicateDqaArg
//
//	@doc:
//		Check if the argument of a DQA has already being used by another DQA
//---------------------------------------------------------------------------
BOOL
CTranslatorQueryToDXL::IsDuplicateDqaArg(List *dqa_list, Aggref *aggref)
{
	GPOS_ASSERT(nullptr != aggref);

	if (NIL == dqa_list || 0 == gpdb::ListLength(dqa_list))
	{
		return false;
	}

	ListCell *lc = nullptr;
	ForEach(lc, dqa_list)
	{
		Node *node = (Node *) lfirst(lc);
		GPOS_ASSERT(IsA(node, Aggref));

		if (gpdb::Equals(aggref->args, ((Aggref *) node)->args))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		GroupingSetContainsValue
//
//	@doc:
//		Check if value is a member of the GroupingSet content. Content for
//		SIMPLE nodes is an integer list of ressortgroupref values. Content
//		CUBE, ROLLUP, and SET nodes are either SIMPLE nodes or other ROLLUP or
//		CUBE nodes. See details in parsenodes.h GroupingSet for more details.
//---------------------------------------------------------------------------
static BOOL
GroupingSetContainsValue(GroupingSet *group, INT value)
{
	ListCell *lc = nullptr;
	if (group->kind == GROUPING_SET_SIMPLE)
	{
		ForEach(lc, group->content)
		{
			if (lfirst_int(lc) == value)
			{
				return true;
			}
		}
	}
	if (group->kind == GROUPING_SET_CUBE ||
		group->kind == GROUPING_SET_ROLLUP || group->kind == GROUPING_SET_SETS)
	{
		ForEach(lc, group->content)
		{
			if (GroupingSetContainsValue((GroupingSet *) lfirst(lc), value))
			{
				return true;
			}
		}
	}
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CheckNoDuplicateAliasGroupingColumn
//
//	@doc:
//		Check if there are multiple grouping set specs that reference
//		duplicate alias columns that may produce NULL values. This can lead to
//		a known wrong results scenario even in Postgres. Punt until a proper
//		solution is found in Postgres. See following threads [1][2] for more
//		details.
//
//		[1] https://www.postgresql.org/message-id/flat/CAHnPFjSdFx_TtNpQturPMkRSJMYaD5rGP2=8iFH9V24-OjHGiQ@mail.gmail.com
//		[2] https://www.postgresql.org/message-id/flat/830269.1656693747@sss.pgh.pa.us
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::CheckNoDuplicateAliasGroupingColumn(List *target_list,
														   List *group_clause,
														   List *grouping_set)
{
	if (gpdb::ListLength(grouping_set) < 2)
	{
		// no duplicates in different grouping specs if only 1 grouping set
		return;
	}

	if (gpdb::ListLength(group_clause) < 2)
	{
		// no duplicates referenced from grouping set if only 1 group clause
		return;
	}

	// Find if there are duplicate aliases in the target list
	ListCell *lc1 = nullptr;
	ListCell *lc2 = nullptr;

	CBitSet *bitset = GPOS_NEW(m_mp) CBitSet(m_mp);

	List *processed_list = NIL;
	ForEach(lc1, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc1);

		ForEach(lc2, processed_list)
		{
			TargetEntry *target_entry_inner = (TargetEntry *) lfirst(lc2);
			if (gpdb::Equals(target_entry->expr, target_entry_inner->expr))
			{
				// ressortgroupref's point to alias'd columns
				bitset->ExchangeSet(target_entry->ressortgroupref);
				bitset->ExchangeSet(target_entry_inner->ressortgroupref);
			}
		}

		processed_list = gpdb::LAppend(processed_list, target_entry);
	}

	if (gpdb::ListLength(processed_list) < 1)
	{
		// no duplicates if no duplicates found in target list
		bitset->Release();
		return;
	}

	int countSimple = 0;
	int countNonSimple = 0;
	ForEach(lc1, grouping_set)
	{
		GroupingSet *group = (GroupingSet *) lfirst(lc1);
		CBitSetIter bsiter(*bitset);

		while (bsiter.Advance())
		{
			if (GroupingSetContainsValue(group, bsiter.Bit()))
			{
				if (group->kind == GROUPING_SET_CUBE ||
					group->kind == GROUPING_SET_ROLLUP ||
					group->kind == GROUPING_SET_SETS)
				{
					countNonSimple += 1;
				}
				else if (group->kind == GROUPING_SET_SIMPLE)
				{
					countSimple += 1;
				}

				if (countNonSimple > 1 ||
					(countNonSimple > 0 && countSimple > 0))
				{
					GPOS_RAISE(
						gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
						GPOS_WSZ_LIT(
							"Multiple grouping sets specifications with duplicate aliased columns"));
				}
			}
		}
	}
	bitset->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateGroupingSets
//
//	@doc:
//		Translate a query with grouping sets
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateGroupingSets(
	FromExpr *from_expr, List *target_list, List *group_clause,
	List *grouping_set, bool grouping_distinct, BOOL has_aggs,
	IntToUlongMap *sort_grpref_to_colid_mapping,
	IntToUlongMap *output_attno_to_colid_mapping)
{
	const ULONG num_of_cols = gpdb::ListLength(target_list) + 1;

	if (nullptr == group_clause && nullptr == grouping_set)
	{
		IntToUlongMap *child_attno_colid_mapping =
			GPOS_NEW(m_mp) IntToUlongMap(m_mp);

		CDXLNode *select_project_join_dxlnode = TranslateSelectProjectJoinToDXL(
			target_list, from_expr, sort_grpref_to_colid_mapping,
			child_attno_colid_mapping, group_clause);

		CBitSet *bitset = nullptr;
		if (has_aggs)
		{
			bitset = GPOS_NEW(m_mp) CBitSet(m_mp);
		}

		// in case of aggregates, construct a group by operator
		CDXLNode *result_dxlnode = CreateSimpleGroupBy(
			target_list, group_clause, bitset, has_aggs,
			false,	// has_grouping_sets
			select_project_join_dxlnode, sort_grpref_to_colid_mapping,
			child_attno_colid_mapping, output_attno_to_colid_mapping);

		// cleanup
		child_attno_colid_mapping->Release();
		CRefCount::SafeRelease(bitset);
		return result_dxlnode;
	}

	CheckNoDuplicateAliasGroupingColumn(target_list, group_clause,
										grouping_set);

	// grouping functions refer to grouping col positions, so construct a map pos->grouping column
	// while processing the grouping clause
	UlongToUlongMap *grpcol_index_to_colid_mapping =
		GPOS_NEW(m_mp) UlongToUlongMap(m_mp);
	CBitSet *unique_grp_cols_bitset = GPOS_NEW(m_mp) CBitSet(m_mp, num_of_cols);
	CBitSetArray *bitset_array = CTranslatorUtils::GetColumnAttnosForGroupBy(
		m_mp, group_clause, grouping_set, grouping_distinct, num_of_cols,
		grpcol_index_to_colid_mapping, unique_grp_cols_bitset);

	const ULONG num_of_grouping_sets = bitset_array->Size();

	if (1 == num_of_grouping_sets)
	{
		// simple group by
		IntToUlongMap *child_attno_colid_mapping =
			GPOS_NEW(m_mp) IntToUlongMap(m_mp);
		CDXLNode *select_project_join_dxlnode = TranslateSelectProjectJoinToDXL(
			target_list, from_expr, sort_grpref_to_colid_mapping,
			child_attno_colid_mapping, group_clause);

		// translate the groupby clauses into a logical group by operator
		CBitSet *bitset = (*bitset_array)[0];


		CDXLNode *groupby_dxlnode = CreateSimpleGroupBy(
			target_list, group_clause, bitset, has_aggs,
			false,	// has_grouping_sets
			select_project_join_dxlnode, sort_grpref_to_colid_mapping,
			child_attno_colid_mapping, output_attno_to_colid_mapping);

		CDXLNode *result_dxlnode = CreateDXLProjectGroupingFuncs(
			target_list, groupby_dxlnode, bitset, output_attno_to_colid_mapping,
			grpcol_index_to_colid_mapping, sort_grpref_to_colid_mapping);

		child_attno_colid_mapping->Release();
		bitset_array->Release();
		unique_grp_cols_bitset->Release();
		grpcol_index_to_colid_mapping->Release();

		return result_dxlnode;
	}

	CDXLNode *result_dxlnode = CreateDXLUnionAllForGroupingSets(
		from_expr, target_list, group_clause, has_aggs, bitset_array,
		sort_grpref_to_colid_mapping, output_attno_to_colid_mapping,
		grpcol_index_to_colid_mapping);

	unique_grp_cols_bitset->Release();
	grpcol_index_to_colid_mapping->Release();

	return result_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CreateDXLUnionAllForGroupingSets
//
//	@doc:
//		Construct a union all for the given grouping sets
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::CreateDXLUnionAllForGroupingSets(
	FromExpr *from_expr, List *target_list, List *group_clause, BOOL has_aggs,
	CBitSetArray *bitset_array, IntToUlongMap *sort_grpref_to_colid_mapping,
	IntToUlongMap *output_attno_to_colid_mapping,
	UlongToUlongMap *
		grpcol_index_to_colid_mapping  // mapping pos->unique grouping columns for grouping func arguments
)
{
	GPOS_ASSERT(nullptr != bitset_array);
	GPOS_ASSERT(1 < bitset_array->Size());

	const ULONG num_of_grouping_sets = bitset_array->Size();
	CDXLNode *unionall_dxlnode = nullptr;
	ULongPtrArray *colid_array_inner = nullptr;

	const ULONG cte_id = m_context->m_cte_id_counter->next_id();

	// construct a CTE producer on top of the SPJ query
	IntToUlongMap *spj_output_attno_to_colid_mapping =
		GPOS_NEW(m_mp) IntToUlongMap(m_mp);
	IntToUlongMap *sort_groupref_to_colid_producer_mapping =
		GPOS_NEW(m_mp) IntToUlongMap(m_mp);
	CDXLNode *select_project_join_dxlnode =
		TranslateSelectProjectJoinForGrpSetsToDXL(
			target_list, from_expr, sort_groupref_to_colid_producer_mapping,
			spj_output_attno_to_colid_mapping, group_clause);

	// construct output colids
	ULongPtrArray *op_colid_array_cte_producer =
		ExtractColIds(m_mp, spj_output_attno_to_colid_mapping);

	GPOS_ASSERT(nullptr != m_dxl_cte_producers);

	CDXLLogicalCTEProducer *cte_prod_dxlop = GPOS_NEW(m_mp)
		CDXLLogicalCTEProducer(m_mp, cte_id, op_colid_array_cte_producer,
			false /*could_be_pruned*/);
	CDXLNode *cte_producer_dxlnode = GPOS_NEW(m_mp)
		CDXLNode(m_mp, cte_prod_dxlop, select_project_join_dxlnode);
	m_dxl_cte_producers->Append(cte_producer_dxlnode);

	CMappingVarColId *var_colid_orig_mapping =
		m_var_to_colid_map->CopyMapColId(m_mp);

	for (ULONG ul = 0; ul < num_of_grouping_sets; ul++)
	{
		CBitSet *grouping_set_bitset = (*bitset_array)[ul];

		// remap columns
		ULongPtrArray *colid_array_cte_consumer =
			GenerateColIds(m_mp, op_colid_array_cte_producer->Size());

		// reset col mapping with new consumer columns
		GPOS_DELETE(m_var_to_colid_map);
		m_var_to_colid_map = var_colid_orig_mapping->CopyRemapColId(
			m_mp, op_colid_array_cte_producer, colid_array_cte_consumer);

		IntToUlongMap *spj_consumer_output_attno_to_colid_mapping =
			RemapColIds(m_mp, spj_output_attno_to_colid_mapping,
						op_colid_array_cte_producer, colid_array_cte_consumer);
		IntToUlongMap *phmiulSortgrouprefColIdConsumer =
			RemapColIds(m_mp, sort_groupref_to_colid_producer_mapping,
						op_colid_array_cte_producer, colid_array_cte_consumer);

		// construct a CTE consumer
		CDXLNode *cte_consumer_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalCTEConsumer(
							   m_mp, cte_id, colid_array_cte_consumer));

		List *target_list_copy = (List *) gpdb::CopyObject(target_list);

		IntToUlongMap *groupby_attno_to_colid_mapping =
			GPOS_NEW(m_mp) IntToUlongMap(m_mp);
		CDXLNode *groupby_dxlnode = CreateSimpleGroupBy(
			target_list_copy, group_clause, grouping_set_bitset, has_aggs,
			true,  // has_grouping_sets
			cte_consumer_dxlnode, phmiulSortgrouprefColIdConsumer,
			spj_consumer_output_attno_to_colid_mapping,
			groupby_attno_to_colid_mapping);

		// add a project list for the NULL values
		CDXLNode *project_dxlnode = CreateDXLProjectNullsForGroupingSets(
			target_list_copy, groupby_dxlnode, grouping_set_bitset,
			phmiulSortgrouprefColIdConsumer, groupby_attno_to_colid_mapping,
			grpcol_index_to_colid_mapping);

		ULongPtrArray *colids_outer_array =
			CTranslatorUtils::GetOutputColIdsArray(
				m_mp, target_list_copy, groupby_attno_to_colid_mapping);
		if (nullptr != unionall_dxlnode)
		{
			GPOS_ASSERT(nullptr != colid_array_inner);
			CDXLColDescrArray *dxl_col_descr_array =
				CTranslatorUtils::GetDXLColumnDescrArray(
					m_mp, target_list_copy, colids_outer_array,
					true /* keep_res_junked */);

			colids_outer_array->AddRef();

			ULongPtr2dArray *input_colids =
				GPOS_NEW(m_mp) ULongPtr2dArray(m_mp);
			input_colids->Append(colids_outer_array);
			input_colids->Append(colid_array_inner);

			CDXLLogicalSetOp *dxl_setop = GPOS_NEW(m_mp)
				CDXLLogicalSetOp(m_mp, EdxlsetopUnionAll, dxl_col_descr_array,
								 input_colids, false);
			unionall_dxlnode = GPOS_NEW(m_mp)
				CDXLNode(m_mp, dxl_setop, project_dxlnode, unionall_dxlnode);
		}
		else
		{
			unionall_dxlnode = project_dxlnode;
		}

		colid_array_inner = colids_outer_array;

		if (ul == num_of_grouping_sets - 1)
		{
			// add the sortgroup columns to output map of the last column
			ULONG te_pos = 0;
			ListCell *lc = nullptr;
			ForEach(lc, target_list_copy)
			{
				TargetEntry *target_entry = (TargetEntry *) lfirst(lc);

				INT sortgroupref = INT(target_entry->ressortgroupref);
				if (0 < sortgroupref &&
					nullptr !=
						phmiulSortgrouprefColIdConsumer->Find(&sortgroupref))
				{
					// add the mapping information for sorting columns
					AddSortingGroupingColumn(target_entry,
											 sort_grpref_to_colid_mapping,
											 *(*colid_array_inner)[te_pos]);
				}

				te_pos++;
			}
		}

		// cleanup
		groupby_attno_to_colid_mapping->Release();
		spj_consumer_output_attno_to_colid_mapping->Release();
		phmiulSortgrouprefColIdConsumer->Release();
	}

	// cleanup
	spj_output_attno_to_colid_mapping->Release();
	sort_groupref_to_colid_producer_mapping->Release();
	GPOS_DELETE(var_colid_orig_mapping);
	colid_array_inner->Release();

	// compute output columns
	CDXLLogicalSetOp *union_dxlop =
		CDXLLogicalSetOp::Cast(unionall_dxlnode->GetOperator());

	ListCell *lc = nullptr;
	ULONG output_col_idx = 0;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));
		GPOS_ASSERT(0 < target_entry->resno);
		ULONG resno = target_entry->resno;

		// note that all target list entries are kept in union all's output column
		// this is achieved by the keep_res_junked flag in CTranslatorUtils::GetDXLColumnDescrArray
		const CDXLColDescr *dxl_col_descr =
			union_dxlop->GetColumnDescrAt(output_col_idx);
		const ULONG colid = dxl_col_descr->Id();
		output_col_idx++;

		if (!target_entry->resjunk)
		{
			// add non-resjunk columns to the hash map that maintains the output columns
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
		}
	}

	// cleanup
	bitset_array->Release();

	// construct a CTE anchor operator on top of the union all
	return GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalCTEAnchor(m_mp, cte_id),
				 unionall_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::DXLDummyConstTableGet
//
//	@doc:
//		Create a dummy constant table get (CTG) with a boolean true value
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::DXLDummyConstTableGet() const
{
	// construct the schema of the const table
	CDXLColDescrArray *dxl_col_descr_array =
		GPOS_NEW(m_mp) CDXLColDescrArray(m_mp);

	const CMDTypeBoolGPDB *md_type_bool = dynamic_cast<const CMDTypeBoolGPDB *>(
		m_md_accessor->PtMDType<IMDTypeBool>(m_sysid));
	const CMDIdGPDB *mdid = CMDIdGPDB::CastMdid(md_type_bool->MDId());

	// empty column name
	CWStringConst str_unnamed_col(GPOS_WSZ_LIT(""));
	CMDName *mdname = GPOS_NEW(m_mp) CMDName(m_mp, &str_unnamed_col);
	CDXLColDescr *dxl_col_descr = GPOS_NEW(m_mp) CDXLColDescr(
		mdname, m_context->m_colid_counter->next_id(), 1 /* attno */,
		GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, mdid->Oid()),
		default_type_modifier, false /* is_dropped */
	);
	dxl_col_descr_array->Append(dxl_col_descr);

	// create the array of datum arrays
	CDXLDatum2dArray *dispatch_identifier_datum_arrays =
		GPOS_NEW(m_mp) CDXLDatum2dArray(m_mp);

	// create a datum array
	CDXLDatumArray *dxl_datum_array = GPOS_NEW(m_mp) CDXLDatumArray(m_mp);

	Const *const_expr =
		(Const *) gpdb::MakeBoolConst(true /*value*/, false /*isnull*/);
	CDXLDatum *datum_dxl = m_scalar_translator->TranslateConstToDXL(const_expr);
	gpdb::GPDBFree(const_expr);

	dxl_datum_array->Append(datum_dxl);
	dispatch_identifier_datum_arrays->Append(dxl_datum_array);

	CDXLLogicalConstTable *dxlop = GPOS_NEW(m_mp) CDXLLogicalConstTable(
		m_mp, dxl_col_descr_array, dispatch_identifier_datum_arrays);

	return GPOS_NEW(m_mp) CDXLNode(m_mp, dxlop);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateSetOpToDXL
//
//	@doc:
//		Translate a set operation
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateSetOpToDXL(
	Node *setop_node, List *target_list,
	IntToUlongMap *output_attno_to_colid_mapping)
{
	GPOS_ASSERT(IsA(setop_node, SetOperationStmt));
	SetOperationStmt *psetopstmt = (SetOperationStmt *) setop_node;
	GPOS_ASSERT(SETOP_NONE != psetopstmt->op);

	EdxlSetOpType setop_type =
		CTranslatorUtils::GetSetOpType(psetopstmt->op, psetopstmt->all);

	// translate the left and right child
	ULongPtrArray *leftchild_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);
	ULongPtrArray *rightchild_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);
	IMdIdArray *mdid_array_leftchild = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	IMdIdArray *mdid_array_rightchild = GPOS_NEW(m_mp) IMdIdArray(m_mp);

	CDXLNode *left_child_dxlnode = TranslateSetOpChild(
		psetopstmt->larg, leftchild_array, mdid_array_leftchild, target_list);
	CDXLNode *right_child_dxlnode = TranslateSetOpChild(
		psetopstmt->rarg, rightchild_array, mdid_array_rightchild, target_list);

	// mark outer references in input columns from left child
	ULONG *colid = GPOS_NEW_ARRAY(m_mp, ULONG, leftchild_array->Size());
	BOOL *outer_ref_array = GPOS_NEW_ARRAY(m_mp, BOOL, leftchild_array->Size());
	const ULONG size = leftchild_array->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		colid[ul] = *(*leftchild_array)[ul];
		outer_ref_array[ul] = true;
	}
	CTranslatorUtils::MarkOuterRefs(colid, outer_ref_array, size,
									left_child_dxlnode);

	ULongPtr2dArray *input_colids = GPOS_NEW(m_mp) ULongPtr2dArray(m_mp);
	input_colids->Append(leftchild_array);
	input_colids->Append(rightchild_array);

	ULongPtrArray *output_colids = CTranslatorUtils::GenerateColIds(
		m_mp, target_list, mdid_array_leftchild, leftchild_array,
		outer_ref_array, m_context->m_colid_counter);
	GPOS_ASSERT(output_colids->Size() == leftchild_array->Size());

	GPOS_DELETE_ARRAY(colid);
	GPOS_DELETE_ARRAY(outer_ref_array);

	BOOL is_cast_across_input =
		SetOpNeedsCast(target_list, mdid_array_leftchild) ||
		SetOpNeedsCast(target_list, mdid_array_rightchild);

	CDXLNodeArray *children_dxlnodes = GPOS_NEW(m_mp) CDXLNodeArray(m_mp);
	children_dxlnodes->Append(left_child_dxlnode);
	children_dxlnodes->Append(right_child_dxlnode);

	CDXLNode *dxlnode = CreateDXLSetOpFromColumns(
		setop_type, target_list, output_colids, input_colids, children_dxlnodes,
		is_cast_across_input, false /* keep_res_junked */
	);

	CDXLLogicalSetOp *dxlop = CDXLLogicalSetOp::Cast(dxlnode->GetOperator());
	const CDXLColDescrArray *dxl_col_descr_array =
		dxlop->GetDXLColumnDescrArray();

	ULONG output_col_idx = 0;
	ListCell *lc = nullptr;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));
		GPOS_ASSERT(0 < target_entry->resno);
		ULONG resno = target_entry->resno;

		if (!target_entry->resjunk)
		{
			const CDXLColDescr *dxl_col_descr_new =
				(*dxl_col_descr_array)[output_col_idx];
			ULONG colid = dxl_col_descr_new->Id();
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
			output_col_idx++;
		}
	}

	// clean up
	output_colids->Release();
	mdid_array_leftchild->Release();
	mdid_array_rightchild->Release();

	return dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::PdxlSetOp
//
//	@doc:
//		Create a set op after adding dummy cast on input columns where needed
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::CreateDXLSetOpFromColumns(
	EdxlSetOpType setop_type, List *output_target_list,
	ULongPtrArray *output_colids, ULongPtr2dArray *input_colids,
	CDXLNodeArray *children_dxlnodes, BOOL is_cast_across_input,
	BOOL keep_res_junked) const
{
	GPOS_ASSERT(nullptr != output_colids);
	GPOS_ASSERT(nullptr != input_colids);
	GPOS_ASSERT(nullptr != children_dxlnodes);
	GPOS_ASSERT(1 < input_colids->Size());
	GPOS_ASSERT(1 < children_dxlnodes->Size());

	// positions of output columns in the target list
	ULongPtrArray *output_col_pos = CTranslatorUtils::GetPosInTargetList(
		m_mp, output_target_list, keep_res_junked);

	const ULONG num_of_cols = output_colids->Size();
	ULongPtrArray *input_first_child_array = (*input_colids)[0];
	GPOS_ASSERT(num_of_cols == input_first_child_array->Size());
	GPOS_ASSERT(num_of_cols == output_colids->Size());

	CBitSet *bitset = GPOS_NEW(m_mp) CBitSet(m_mp);

	// project list to maintain the casting of the duplicate input columns
	CDXLNode *new_child_project_list_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

	ULongPtrArray *input_first_child_new_array =
		GPOS_NEW(m_mp) ULongPtrArray(m_mp);
	CDXLColDescrArray *output_col_descrs =
		GPOS_NEW(m_mp) CDXLColDescrArray(m_mp);
	for (ULONG ul = 0; ul < num_of_cols; ul++)
	{
		ULONG colid_output = *(*output_colids)[ul];
		ULONG colid_input = *(*input_first_child_array)[ul];

		BOOL is_col_exists = bitset->Get(colid_input);
		BOOL is_casted_col = (colid_output != colid_input);

		ULONG target_list_pos = *(*output_col_pos)[ul];
		TargetEntry *target_entry =
			(TargetEntry *) gpdb::ListNth(output_target_list, target_list_pos);
		GPOS_ASSERT(nullptr != target_entry);

		CDXLColDescr *output_col_descr = nullptr;
		if (!is_col_exists)
		{
			bitset->ExchangeSet(colid_input);
			input_first_child_new_array->Append(GPOS_NEW(m_mp)
													ULONG(colid_input));

			output_col_descr = CTranslatorUtils::GetColumnDescrAt(
				m_mp, target_entry, colid_output, ul + 1);
		}
		else
		{
			// we add a dummy-cast to distinguish between the output columns of the union
			ULONG colid_new = m_context->m_colid_counter->next_id();
			input_first_child_new_array->Append(GPOS_NEW(m_mp)
													ULONG(colid_new));

			ULONG colid_union_output = colid_new;
			if (is_casted_col)
			{
				// create new output column id since current colid denotes its duplicate
				colid_union_output = m_context->m_colid_counter->next_id();
			}

			output_col_descr = CTranslatorUtils::GetColumnDescrAt(
				m_mp, target_entry, colid_union_output, ul + 1);
			CDXLNode *project_elem_dxlnode =
				CTranslatorUtils::CreateDummyProjectElem(
					m_mp, colid_input, colid_new, output_col_descr);

			new_child_project_list_dxlnode->AddChild(project_elem_dxlnode);
		}

		output_col_descrs->Append(output_col_descr);
	}

	input_colids->Replace(0, input_first_child_new_array);

	if (0 < new_child_project_list_dxlnode->Arity())
	{
		// create a project node for the dummy casted columns
		CDXLNode *first_child_dxlnode = (*children_dxlnodes)[0];
		first_child_dxlnode->AddRef();
		CDXLNode *new_child_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp));
		new_child_dxlnode->AddChild(new_child_project_list_dxlnode);
		new_child_dxlnode->AddChild(first_child_dxlnode);

		children_dxlnodes->Replace(0, new_child_dxlnode);
	}
	else
	{
		new_child_project_list_dxlnode->Release();
	}

	CDXLLogicalSetOp *dxlop =
		GPOS_NEW(m_mp) CDXLLogicalSetOp(m_mp, setop_type, output_col_descrs,
										input_colids, is_cast_across_input);
	CDXLNode *dxlnode = GPOS_NEW(m_mp) CDXLNode(dxlop, children_dxlnodes);

	bitset->Release();
	output_col_pos->Release();

	return dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::SetOpNeedsCast
//
//	@doc:
//		Check if the set operation need to cast any of its input columns
//
//---------------------------------------------------------------------------
BOOL
CTranslatorQueryToDXL::SetOpNeedsCast(List *target_list,
									  IMdIdArray *input_col_mdids)
{
	GPOS_ASSERT(nullptr != input_col_mdids);
	GPOS_ASSERT(
		input_col_mdids->Size() <=
		gpdb::ListLength(target_list));	 // there may be resjunked columns

	ULONG col_pos_idx = 0;
	ListCell *lc = nullptr;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		OID expr_type_oid = gpdb::ExprType((Node *) target_entry->expr);
		if (!target_entry->resjunk)
		{
			IMDId *mdid = (*input_col_mdids)[col_pos_idx];
			if (CMDIdGPDB::CastMdid(mdid)->Oid() != expr_type_oid)
			{
				return true;
			}
			col_pos_idx++;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateSetOpChild
//
//	@doc:
//		Translate the child of a set operation
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateSetOpChild(Node *child_node,
										   ULongPtrArray *colids,
										   IMdIdArray *input_col_mdids,
										   List *target_list)
{
	GPOS_ASSERT(nullptr != colids);
	GPOS_ASSERT(nullptr != input_col_mdids);

	if (IsA(child_node, RangeTblRef))
	{
		RangeTblRef *range_tbl_ref = (RangeTblRef *) child_node;
		const ULONG rt_index = range_tbl_ref->rtindex;
		const RangeTblEntry *rte =
			(RangeTblEntry *) gpdb::ListNth(m_query->rtable, rt_index - 1);

		if (RTE_SUBQUERY == rte->rtekind)
		{
			Query *query_derived_tbl = CTranslatorUtils::FixUnknownTypeConstant(
				rte->subquery, target_list);
			GPOS_ASSERT(nullptr != query_derived_tbl);

			CTranslatorQueryToDXL query_to_dxl_translator(
				m_context, m_md_accessor, m_var_to_colid_map, query_derived_tbl,
				m_query_level + 1, IsDMLQuery(), m_query_level_to_cte_map);

			// translate query representing the derived table to its DXL representation
			CDXLNode *query_dxlnode =
				query_to_dxl_translator.TranslateSelectQueryToDXL();
			GPOS_ASSERT(nullptr != query_dxlnode);

			CDXLNodeArray *cte_dxlnode_array =
				query_to_dxl_translator.GetCTEs();
			CUtils::AddRefAppend(m_dxl_cte_producers, cte_dxlnode_array);

			// get the output columns of the derived table
			CDXLNodeArray *dxlnodes =
				query_to_dxl_translator.GetQueryOutputCols();
			GPOS_ASSERT(dxlnodes != nullptr);
			const ULONG length = dxlnodes->Size();
			for (ULONG ul = 0; ul < length; ul++)
			{
				CDXLNode *current_dxlnode = (*dxlnodes)[ul];
				CDXLScalarIdent *dxl_scalar_ident =
					CDXLScalarIdent::Cast(current_dxlnode->GetOperator());
				ULONG *colid = GPOS_NEW(m_mp)
					ULONG(dxl_scalar_ident->GetDXLColRef()->Id());
				colids->Append(colid);

				IMDId *mdid_col = dxl_scalar_ident->MdidType();
				GPOS_ASSERT(nullptr != mdid_col);
				mdid_col->AddRef();
				input_col_mdids->Append(mdid_col);
			}

			return query_dxlnode;
		}
	}
	else if (IsA(child_node, SetOperationStmt))
	{
		IntToUlongMap *output_attno_to_colid_mapping =
			GPOS_NEW(m_mp) IntToUlongMap(m_mp);
		CDXLNode *dxlnode = TranslateSetOpToDXL(child_node, target_list,
												output_attno_to_colid_mapping);

		// cleanup
		output_attno_to_colid_mapping->Release();

		const CDXLColDescrArray *dxl_col_descr_array =
			CDXLLogicalSetOp::Cast(dxlnode->GetOperator())
				->GetDXLColumnDescrArray();
		GPOS_ASSERT(nullptr != dxl_col_descr_array);
		const ULONG length = dxl_col_descr_array->Size();
		for (ULONG ul = 0; ul < length; ul++)
		{
			const CDXLColDescr *dxl_col_descr = (*dxl_col_descr_array)[ul];
			ULONG *colid = GPOS_NEW(m_mp) ULONG(dxl_col_descr->Id());
			colids->Append(colid);

			IMDId *mdid_col = dxl_col_descr->MdidType();
			GPOS_ASSERT(nullptr != mdid_col);
			mdid_col->AddRef();
			input_col_mdids->Append(mdid_col);
		}

		return dxlnode;
	}

	CHAR *temp_str = (CHAR *) gpdb::NodeToString(child_node);
	CWStringDynamic *str =
		CDXLUtils::CreateDynamicStringFromCharArray(m_mp, temp_str);

	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
			   str->GetBuffer());
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateFromExprToDXL
//
//	@doc:
//		Translate the FromExpr on a GPDB query into either a CDXLLogicalJoin
//		or a CDXLLogicalGet
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateFromExprToDXL(FromExpr *from_expr)
{
	CDXLNode *dxlnode = nullptr;

	if (0 == gpdb::ListLength(from_expr->fromlist))
	{
		dxlnode = DXLDummyConstTableGet();
	}
	else
	{
		if (1 == gpdb::ListLength(from_expr->fromlist))
		{
			Node *node = (Node *) gpdb::ListNth(from_expr->fromlist, 0);
			GPOS_ASSERT(nullptr != node);
			dxlnode = TranslateFromClauseToDXL(node);
		}
		else
		{
			// In DXL, we represent an n-ary join (where n>2) by an inner join with condition true.
			// The join conditions represented in the FromExpr->quals is translated
			// into a CDXLLogicalSelect on top of the CDXLLogicalJoin

			dxlnode = GPOS_NEW(m_mp) CDXLNode(
				m_mp, GPOS_NEW(m_mp) CDXLLogicalJoin(m_mp, EdxljtInner));

			ListCell *lc = nullptr;
			ForEach(lc, from_expr->fromlist)
			{
				Node *node = (Node *) lfirst(lc);
				CDXLNode *child_dxlnode = TranslateFromClauseToDXL(node);
				dxlnode->AddChild(child_dxlnode);
			}
		}
	}

	// translate the quals
	Node *qual_node = from_expr->quals;
	CDXLNode *condition_dxlnode = nullptr;
	if (nullptr != qual_node)
	{
		condition_dxlnode = TranslateExprToDXL((Expr *) qual_node);
	}

	if (1 >= gpdb::ListLength(from_expr->fromlist))
	{
		if (nullptr != condition_dxlnode)
		{
			CDXLNode *select_dxlnode = GPOS_NEW(m_mp)
				CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalSelect(m_mp));
			select_dxlnode->AddChild(condition_dxlnode);
			select_dxlnode->AddChild(dxlnode);

			dxlnode = select_dxlnode;
		}
	}
	else  //n-ary joins
	{
		if (nullptr == condition_dxlnode)
		{
			// A cross join (the scalar condition is true)
			condition_dxlnode = CreateDXLConstValueTrue();
		}

		dxlnode->AddChild(condition_dxlnode);
	}

	return dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateFromClauseToDXL
//
//	@doc:
//		Returns a CDXLNode representing a from clause entry which can either be
//		(1) a fromlist entry in the FromExpr or (2) left/right child of a JoinExpr
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateFromClauseToDXL(Node *node)
{
	GPOS_ASSERT(nullptr != node);

	if (IsA(node, RangeTblRef))
	{
		RangeTblRef *range_tbl_ref = (RangeTblRef *) node;
		ULONG rt_index = range_tbl_ref->rtindex;
		const RangeTblEntry *rte =
			(RangeTblEntry *) gpdb::ListNth(m_query->rtable, rt_index - 1);
		GPOS_ASSERT(nullptr != rte);

		if (rte->forceDistRandom)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("gp_dist_random"));
		}

		if (rte->lateral)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("LATERAL"));
		}

		if (rte->funcordinality)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("WITH ORDINALITY"));
		}

		switch (rte->rtekind)
		{
			default:
			{
				UnsupportedRTEKind(rte->rtekind);

				return nullptr;
			}
			case RTE_RELATION:
			{
				return TranslateRTEToDXLLogicalGet(rte, rt_index,
												   m_query_level);
			}
			case RTE_VALUES:
			{
				return TranslateValueScanRTEToDXL(rte, rt_index, m_query_level);
			}
			case RTE_CTE:
			{
				return TranslateCTEToDXL(rte, rt_index, m_query_level);
			}
			case RTE_SUBQUERY:
			{
				return TranslateDerivedTablesToDXL(rte, rt_index,
												   m_query_level);
			}
			case RTE_FUNCTION:
			{
				return TranslateTVFToDXL(rte, rt_index, m_query_level);
			}
		}
	}

	if (IsA(node, JoinExpr))
	{
		return TranslateJoinExprInFromToDXL((JoinExpr *) node);
	}

	CHAR *sz = (CHAR *) gpdb::NodeToString(node);
	CWStringDynamic *str =
		CDXLUtils::CreateDynamicStringFromCharArray(m_mp, sz);

	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
			   str->GetBuffer());

	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::UnsupportedRTEKind
//
//	@doc:
//		Raise exception for unsupported RangeTblEntries of a particular kind
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::UnsupportedRTEKind(RTEKind rtekind)
{
	GPOS_ASSERT(!(RTE_RELATION == rtekind || RTE_CTE == rtekind ||
				  RTE_FUNCTION == rtekind || RTE_SUBQUERY == rtekind ||
				  RTE_VALUES == rtekind));

	switch (rtekind)
	{
		default:
		{
			GPOS_RTL_ASSERT(!"Unrecognized RTE kind");
			__builtin_unreachable();
		}
		case RTE_JOIN:
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("RangeTableEntry of type Join"));
		}
		case RTE_VOID:
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("RangeTableEntry of type Void"));
		}
		case RTE_TABLEFUNCTION:
		case RTE_TABLEFUNC:
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("RangeTableEntry of type Table Function"));
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateRTEToDXLLogicalGet
//
//	@doc:
//		Returns a CDXLNode representing a from relation range table entry
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateRTEToDXLLogicalGet(const RangeTblEntry *rte,
												   ULONG rt_index,
												   ULONG  //current_query_level
)
{
	if (false == rte->inh)
	{
		GPOS_ASSERT(RTE_RELATION == rte->rtekind);
		// RangeTblEntry::inh is set to false iff there is ONLY in the FROM
		// clause. c.f. transformTableEntry, called from transformFromClauseItem
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("ONLY in the FROM clause"));
	}


	BOOL rteHasSecurityQuals = gpdb::ListLength(rte->securityQuals) > 0;

	// query_id_for_target_rel is used to tag table descriptors assigned to target
	// (result) relations one. In case of possible nested DML subqueries it's
	// field points to target relation of corresponding Query structure of subquery.
	ULONG query_id_for_target_rel = UNASSIGNED_QUERYID;
	if (m_query->resultRelation > 0 &&
		ULONG(m_query->resultRelation) == rt_index)
	{
		query_id_for_target_rel = m_query_id;
	}

	// construct table descriptor for the scan node from the range table entry
	CDXLTableDescr *dxl_table_descr = CTranslatorUtils::GetTableDescr(
		m_mp, m_md_accessor, m_context->m_colid_counter, rte,
		query_id_for_target_rel, &m_context->m_has_distributed_tables);

	CDXLLogicalGet *dxl_op = nullptr;
	const IMDRelation *md_rel =
		m_md_accessor->RetrieveRel(dxl_table_descr->MDId());
	if (IMDRelation::ErelstorageForeign == md_rel->RetrieveRelStorageType())
	{
		dxl_op = GPOS_NEW(m_mp) CDXLLogicalForeignGet(m_mp, dxl_table_descr);
	}
	else
	{
		dxl_op = GPOS_NEW(m_mp)
			CDXLLogicalGet(m_mp, dxl_table_descr, rteHasSecurityQuals);
	}

	CDXLNode *dxl_node = GPOS_NEW(m_mp) CDXLNode(m_mp, dxl_op);

	// make note of new columns from base relation
	m_var_to_colid_map->LoadTblColumns(m_query_level, rt_index,
									   dxl_table_descr);

	// make note of the operator classes used in the distribution key
	NoteDistributionPolicyOpclasses(rte);

	return dxl_node;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::NoteDistributionPolicyOpclasses
//
//	@doc:
//		Observe what operator classes are used in the distribution
//		keys of the given RTE's relation.
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::NoteDistributionPolicyOpclasses(const RangeTblEntry *rte)
{
	// What opclasses are being used in the distribution policy?
	// We categorize them into three categories:
	//
	// 1. Default opclasses for the datatype
	// 2. Legacy cdbhash opclasses for the datatype
	// 3. Any other opclasses
	//
	// ORCA doesn't know about hash opclasses attached to distribution
	// keys. So if a query involves two tables, with e.g. integer
	// datatype as distribution key, but with different opclasses,
	// ORCA doesn'thinks they're nevertheless compatible, and will
	// merrily create a join between them without a Redistribute
	// Motion. To avoid incorrect plans like that, we keep track of the
	// opclasses used in the distribution keys of all the tables
	// being referenced in the plan.  As long the all use the default
	// opclasses, or the legacy ones, ORCA will produce a valid plan.
	// But if we see mixed use, or non-default opclasses, throw an error.
	//
	// This conservative, there are many cases that we bail out on,
	// for which the ORCA-generated plan would in fact be OK, but
	// we have to play it safe. When converting the DXL plan to
	// a Plan tree, we will use the default opclasses, or the legacy
	// ones, for all hashing within the query.
	if (rte->rtekind == RTE_RELATION)
	{
		gpdb::RelationWrapper rel = gpdb::GetRelation(rte->relid);
		GpPolicy *policy = rel->rd_cdbpolicy;

		// master-only tables
		if (nullptr == policy)
		{
			return;
		}

		if (!optimizer_enable_replicated_table &&
			policy->ptype == POLICYTYPE_REPLICATED)
		{
			GPOS_RAISE(
				gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
				GPOS_WSZ_LIT(
					"Use optimizer_enable_replicated_table to enable replicated tables"));
		}

		int policy_nattrs = policy->nattrs;
		TupleDesc desc = rel->rd_att;
		bool contains_default_hashops = false;
		bool contains_legacy_hashops = false;
		bool contains_nondefault_hashops = false;
		Oid *opclasses = policy->opclasses;

		for (int i = 0; i < policy_nattrs; i++)
		{
			AttrNumber attnum = policy->attrs[i];
			Oid typeoid = desc->attrs[attnum - 1].atttypid;
			Oid opfamily;
			Oid hashfunc;

			opfamily = gpdb::GetOpclassFamily(opclasses[i]);
			hashfunc = gpdb::GetHashProcInOpfamily(opfamily, typeoid);

			if (gpdb::IsLegacyCdbHashFunction(hashfunc))
			{
				contains_legacy_hashops = true;
			}
			else
			{
				Oid default_opclass =
					gpdb::GetDefaultDistributionOpclassForType(typeoid);

				if (opclasses[i] == default_opclass)
				{
					contains_default_hashops = true;
				}
				else
				{
					contains_nondefault_hashops = true;
				}
			}
		}

		if (contains_nondefault_hashops)
		{
			/* have to fall back */
			GPOS_RAISE(
				gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
				GPOS_WSZ_LIT(
					"Query contains relations with non-default hash opclasses"));
		}
		if (contains_default_hashops &&
			m_context->m_distribution_hashops != DistrUseDefaultHashOps)
		{
			if (m_context->m_distribution_hashops !=
				DistrHashOpsNotDeterminedYet)
			{
				GPOS_RAISE(
					gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
					GPOS_WSZ_LIT(
						"Query contains relations with a mix of default and legacy hash opclasses"));
			}
			m_context->m_distribution_hashops = DistrUseDefaultHashOps;
		}
		if (contains_legacy_hashops &&
			m_context->m_distribution_hashops != DistrUseLegacyHashOps)
		{
			if (m_context->m_distribution_hashops !=
				DistrHashOpsNotDeterminedYet)
			{
				GPOS_RAISE(
					gpdxl::ExmaMD, gpdxl::ExmiMDObjUnsupported,
					GPOS_WSZ_LIT(
						"Query contains relations with a mix of default and legacy hash opclasses"));
			}
			m_context->m_distribution_hashops = DistrUseLegacyHashOps;
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateValueScanRTEToDXL
//
//	@doc:
//		Returns a CDXLNode representing a range table entry of values
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateValueScanRTEToDXL(const RangeTblEntry *rte,
												  ULONG rt_index,
												  ULONG /*current_query_level*/)
{
	List *tuples_list = rte->values_lists;
	GPOS_ASSERT(nullptr != tuples_list);

	const ULONG num_of_tuples = gpdb::ListLength(tuples_list);
	GPOS_ASSERT(0 < num_of_tuples);

	// children of the UNION ALL
	CDXLNodeArray *dxlnodes = GPOS_NEW(m_mp) CDXLNodeArray(m_mp);

	// array of datum arrays for Values
	CDXLDatum2dArray *dxl_values_datum_array =
		GPOS_NEW(m_mp) CDXLDatum2dArray(m_mp);

	// array of input colid arrays
	ULongPtr2dArray *input_colids = GPOS_NEW(m_mp) ULongPtr2dArray(m_mp);

	// array of column descriptor for the UNION ALL operator
	CDXLColDescrArray *dxl_col_descr_array =
		GPOS_NEW(m_mp) CDXLColDescrArray(m_mp);

	// translate the tuples in the value scan
	ULONG tuple_pos = 0;
	ListCell *lc_tuple = nullptr;
	GPOS_ASSERT(nullptr != rte->eref);

	// flag for checking value list has only constants. For all constants --> VALUESCAN operator else retain UnionAll
	BOOL fAllConstant = true;
	ForEach(lc_tuple, tuples_list)
	{
		List *tuple_list = (List *) lfirst(lc_tuple);
		GPOS_ASSERT(IsA(tuple_list, List));

		// array of column colids
		ULongPtrArray *colid_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

		// array of project elements (for expression elements)
		CDXLNodeArray *project_elem_dxlnode_array =
			GPOS_NEW(m_mp) CDXLNodeArray(m_mp);

		// array of datum (for datum constant values)
		CDXLDatumArray *dxl_datum_array = GPOS_NEW(m_mp) CDXLDatumArray(m_mp);

		// array of column descriptors for the CTG containing the datum array
		CDXLColDescrArray *dxl_column_descriptors =
			GPOS_NEW(m_mp) CDXLColDescrArray(m_mp);

		List *col_names = rte->eref->colnames;
		GPOS_ASSERT(nullptr != col_names);
		GPOS_ASSERT(gpdb::ListLength(tuple_list) ==
					gpdb::ListLength(col_names));

		// translate the columns
		ULONG col_pos_idx = 0;
		ListCell *lc_column = nullptr;
		ForEach(lc_column, tuple_list)
		{
			Expr *expr = (Expr *) lfirst(lc_column);

			CHAR *col_name_char_array =
				(CHAR *) strVal(gpdb::ListNth(col_names, col_pos_idx));
			ULONG colid = gpos::ulong_max;
			if (IsA(expr, Const))
			{
				// extract the datum
				Const *const_expr = (Const *) expr;
				CDXLDatum *datum_dxl =
					m_scalar_translator->TranslateConstToDXL(const_expr);
				dxl_datum_array->Append(datum_dxl);

				colid = m_context->m_colid_counter->next_id();

				CWStringDynamic *alias_str =
					CDXLUtils::CreateDynamicStringFromCharArray(
						m_mp, col_name_char_array);
				CMDName *mdname = GPOS_NEW(m_mp) CMDName(m_mp, alias_str);
				GPOS_DELETE(alias_str);

				CDXLColDescr *dxl_col_descr = GPOS_NEW(m_mp) CDXLColDescr(
					mdname, colid, col_pos_idx + 1 /* attno */,
					GPOS_NEW(m_mp)
						CMDIdGPDB(IMDId::EmdidGeneral, const_expr->consttype),
					const_expr->consttypmod, false /* is_dropped */
				);

				if (0 == tuple_pos)
				{
					dxl_col_descr->AddRef();
					dxl_col_descr_array->Append(dxl_col_descr);
				}
				dxl_column_descriptors->Append(dxl_col_descr);
			}
			else
			{
				fAllConstant = false;
				// translate the scalar expression into a project element
				CDXLNode *project_elem_dxlnode = TranslateExprToDXLProject(
					expr, col_name_char_array, true /* insist_new_colids */);
				project_elem_dxlnode_array->Append(project_elem_dxlnode);
				colid = CDXLScalarProjElem::Cast(
							project_elem_dxlnode->GetOperator())
							->Id();

				if (0 == tuple_pos)
				{
					CWStringDynamic *alias_str =
						CDXLUtils::CreateDynamicStringFromCharArray(
							m_mp, col_name_char_array);
					CMDName *mdname = GPOS_NEW(m_mp) CMDName(m_mp, alias_str);
					GPOS_DELETE(alias_str);

					CDXLColDescr *dxl_col_descr = GPOS_NEW(m_mp) CDXLColDescr(
						mdname, colid, col_pos_idx + 1 /* attno */,
						GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral,
												 gpdb::ExprType((Node *) expr)),
						gpdb::ExprTypeMod((Node *) expr), false /* is_dropped */
					);
					dxl_col_descr_array->Append(dxl_col_descr);
				}
			}

			GPOS_ASSERT(gpos::ulong_max != colid);

			colid_array->Append(GPOS_NEW(m_mp) ULONG(colid));
			col_pos_idx++;
		}

		dxlnodes->Append(
			TranslateColumnValuesToDXL(dxl_datum_array, dxl_column_descriptors,
									   project_elem_dxlnode_array));
		if (fAllConstant)
		{
			dxl_datum_array->AddRef();
			dxl_values_datum_array->Append(dxl_datum_array);
		}

		input_colids->Append(colid_array);
		tuple_pos++;

		// cleanup
		dxl_datum_array->Release();
		project_elem_dxlnode_array->Release();
		dxl_column_descriptors->Release();
	}

	GPOS_ASSERT(nullptr != dxl_col_descr_array);

	if (fAllConstant)
	{
		// create Const Table DXL Node
		CDXLLogicalConstTable *dxlop = GPOS_NEW(m_mp) CDXLLogicalConstTable(
			m_mp, dxl_col_descr_array, dxl_values_datum_array);
		CDXLNode *dxlnode = GPOS_NEW(m_mp) CDXLNode(m_mp, dxlop);

		// make note of new columns from Value Scan
		m_var_to_colid_map->LoadColumns(m_query_level, rt_index,
										dxlop->GetDXLColumnDescrArray());

		// cleanup
		dxlnodes->Release();
		input_colids->Release();

		return dxlnode;
	}
	else if (1 < num_of_tuples)
	{
		// create a UNION ALL operator
		CDXLLogicalSetOp *dxlop = GPOS_NEW(m_mp) CDXLLogicalSetOp(
			m_mp, EdxlsetopUnionAll, dxl_col_descr_array, input_colids, false);
		CDXLNode *dxlnode = GPOS_NEW(m_mp) CDXLNode(dxlop, dxlnodes);

		// make note of new columns from UNION ALL
		m_var_to_colid_map->LoadColumns(m_query_level, rt_index,
										dxlop->GetDXLColumnDescrArray());
		dxl_values_datum_array->Release();

		return dxlnode;
	}

	GPOS_ASSERT(1 == dxlnodes->Size());

	CDXLNode *dxlnode = (*dxlnodes)[0];
	dxlnode->AddRef();

	// make note of new columns
	m_var_to_colid_map->LoadColumns(m_query_level, rt_index,
									dxl_col_descr_array);

	//cleanup
	dxl_values_datum_array->Release();
	dxlnodes->Release();
	input_colids->Release();
	dxl_col_descr_array->Release();

	return dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateColumnValuesToDXL
//
//	@doc:
//		Generate a DXL node from column values, where each column value is
//		either a datum or scalar expression represented as project element.
//		Each datum is associated with a column descriptors used by the CTG
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateColumnValuesToDXL(
	CDXLDatumArray *dxl_datum_array_const_tbl_get,
	CDXLColDescrArray *dxl_column_descriptors,
	CDXLNodeArray *project_elem_dxlnode_array) const
{
	GPOS_ASSERT(nullptr != dxl_datum_array_const_tbl_get);
	GPOS_ASSERT(nullptr != project_elem_dxlnode_array);

	CDXLNode *const_tbl_get_dxlnode = nullptr;
	if (0 == dxl_datum_array_const_tbl_get->Size())
	{
		// add a dummy CTG
		const_tbl_get_dxlnode = DXLDummyConstTableGet();
	}
	else
	{
		// create the array of datum arrays
		CDXLDatum2dArray *dxl_datum_arrays_const_tbl_get =
			GPOS_NEW(m_mp) CDXLDatum2dArray(m_mp);

		dxl_datum_array_const_tbl_get->AddRef();
		dxl_datum_arrays_const_tbl_get->Append(dxl_datum_array_const_tbl_get);

		dxl_column_descriptors->AddRef();
		CDXLLogicalConstTable *dxlop = GPOS_NEW(m_mp) CDXLLogicalConstTable(
			m_mp, dxl_column_descriptors, dxl_datum_arrays_const_tbl_get);

		const_tbl_get_dxlnode = GPOS_NEW(m_mp) CDXLNode(m_mp, dxlop);
	}

	if (0 == project_elem_dxlnode_array->Size())
	{
		return const_tbl_get_dxlnode;
	}

	// create a project node for the list of project elements
	project_elem_dxlnode_array->AddRef();
	CDXLNode *project_list_dxlnode = GPOS_NEW(m_mp) CDXLNode(
		GPOS_NEW(m_mp) CDXLScalarProjList(m_mp), project_elem_dxlnode_array);

	CDXLNode *project_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp),
								project_list_dxlnode, const_tbl_get_dxlnode);

	return project_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateTVFToDXL
//
//	@doc:
//		Returns a CDXLNode representing a from relation range table entry
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateTVFToDXL(const RangeTblEntry *rte,
										 ULONG rt_index,
										 ULONG	//current_query_level
)
{
	/*
	 * GPDB_94_MERGE_FIXME: RangeTblEntry for functions can now contain multiple function calls.
	 * ORCA isn't prepared for that yet. See upstream commit 784e762e88.
	 */
	if (list_length(rte->functions) != 1)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Multi-argument UNNEST() or TABLE()"));
	}
	RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(rte->functions);

	BOOL is_composite_const =
		CTranslatorUtils::IsCompositeConst(m_mp, m_md_accessor, rtfunc);

	// if this is a folded function expression, generate a project over a CTG
	if (!IsA(rtfunc->funcexpr, FuncExpr) && !is_composite_const)
	{
		CDXLNode *const_tbl_get_dxlnode = DXLDummyConstTableGet();

		CDXLNode *project_list_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

		CDXLNode *project_elem_dxlnode = TranslateExprToDXLProject(
			(Expr *) rtfunc->funcexpr, rte->eref->aliasname,
			true /* insist_new_colids */);
		project_list_dxlnode->AddChild(project_elem_dxlnode);

		CDXLNode *project_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp));
		project_dxlnode->AddChild(project_list_dxlnode);
		project_dxlnode->AddChild(const_tbl_get_dxlnode);

		m_var_to_colid_map->LoadProjectElements(m_query_level, rt_index,
												project_list_dxlnode);

		return project_dxlnode;
	}

	CDXLLogicalTVF *tvf_dxlop = CTranslatorUtils::ConvertToCDXLLogicalTVF(
		m_mp, m_md_accessor, m_context->m_colid_counter, rte);
	CDXLNode *tvf_dxlnode = GPOS_NEW(m_mp) CDXLNode(m_mp, tvf_dxlop);

	// make note of new columns from function
	m_var_to_colid_map->LoadColumns(m_query_level, rt_index,
									tvf_dxlop->GetDXLColumnDescrArray());

	BOOL is_subquery_in_args = false;

	// funcexpr evaluates to const and returns composite type
	if (IsA(rtfunc->funcexpr, Const))
	{
		// If the const is NULL, the const value cannot be populated
		// Raise exception
		// This happens to PostGIS functions, which aren't supported
		const Const *constant = (Const *) rtfunc->funcexpr;
		if (constant->constisnull)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("Row-type variable"));
		}

		CDXLNode *constValue = m_scalar_translator->TranslateScalarToDXL(
			(Expr *) (rtfunc->funcexpr), m_var_to_colid_map);
		tvf_dxlnode->AddChild(constValue);
		return tvf_dxlnode;
	}

	GPOS_ASSERT(IsA(rtfunc->funcexpr, FuncExpr));

	FuncExpr *funcexpr = (FuncExpr *) rtfunc->funcexpr;

	// check if arguments contain SIRV functions
	if (NIL != funcexpr->args && HasSirvFunctions((Node *) funcexpr->args))
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("SIRV functions"));
	}

	ListCell *lc = nullptr;
	ForEach(lc, funcexpr->args)
	{
		Node *arg_node = (Node *) lfirst(lc);
		is_subquery_in_args =
			is_subquery_in_args || CTranslatorUtils::HasSubquery(arg_node);
		CDXLNode *func_expr_arg_dxlnode =
			m_scalar_translator->TranslateScalarToDXL((Expr *) arg_node,
													  m_var_to_colid_map);
		GPOS_ASSERT(nullptr != func_expr_arg_dxlnode);
		tvf_dxlnode->AddChild(func_expr_arg_dxlnode);
	}

	CMDIdGPDB *mdid_func =
		GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, funcexpr->funcid);
	const IMDFunction *pmdfunc = m_md_accessor->RetrieveFunc(mdid_func);
	if (is_subquery_in_args &&
		IMDFunction::EfsVolatile == pmdfunc->GetFuncStability())
	{
		GPOS_RAISE(
			gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
			GPOS_WSZ_LIT("Volatile functions with subqueries in arguments"));
	}
	mdid_func->Release();

	return tvf_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateCTEToDXL
//
//	@doc:
//		Translate a common table expression into CDXLNode
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateCTEToDXL(const RangeTblEntry *rte,
										 ULONG rt_index,
										 ULONG current_query_level)
{
	const ULONG cte_query_level = current_query_level - rte->ctelevelsup;
	const CCTEListEntry *cte_list_entry =
		m_query_level_to_cte_map->Find(&cte_query_level);
	if (nullptr == cte_list_entry)
	{
		// TODO: Sept 09 2013, remove temporary fix  (revert exception to assert) to avoid crash during algebrization
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
				   GPOS_WSZ_LIT("No CTE"));
	}

	const CDXLNode *cte_producer_dxlnode =
		cte_list_entry->GetCTEProducer(rte->ctename);
	const List *cte_producer_target_list =
		cte_list_entry->GetCTEProducerTargetList(rte->ctename);

	// fallback to Postgres optimizer in case of empty target list
	if (NIL == cte_producer_target_list)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
				   GPOS_WSZ_LIT("Empty target list"));
	}

	GPOS_ASSERT(nullptr != cte_producer_dxlnode);

	CDXLLogicalCTEProducer *cte_producer_dxlop =
		CDXLLogicalCTEProducer::Cast(cte_producer_dxlnode->GetOperator());
	ULONG cte_id = cte_producer_dxlop->Id();
	ULongPtrArray *op_colid_array_cte_producer =
		cte_producer_dxlop->GetOutputColIdsArray();

	// construct output column array
	ULongPtrArray *colid_array_cte_consumer =
		GenerateColIds(m_mp, op_colid_array_cte_producer->Size());

	// load the new columns from the CTE
	m_var_to_colid_map->LoadCTEColumns(
		current_query_level, rt_index, colid_array_cte_consumer,
		const_cast<List *>(cte_producer_target_list));

	CDXLLogicalCTEConsumer *cte_consumer_dxlop = GPOS_NEW(m_mp)
		CDXLLogicalCTEConsumer(m_mp, cte_id, colid_array_cte_consumer);
	CDXLNode *cte_dxlnode = GPOS_NEW(m_mp) CDXLNode(m_mp, cte_consumer_dxlop);

	return cte_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateDerivedTablesToDXL
//
//	@doc:
//		Translate a derived table into CDXLNode
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateDerivedTablesToDXL(const RangeTblEntry *rte,
												   ULONG rt_index,
												   ULONG current_query_level)
{
	Query *query_derived_tbl = rte->subquery;
	GPOS_ASSERT(nullptr != query_derived_tbl);

	CTranslatorQueryToDXL query_to_dxl_translator(
		m_context, m_md_accessor, m_var_to_colid_map, query_derived_tbl,
		m_query_level + 1, IsDMLQuery(), m_query_level_to_cte_map);

	// translate query representing the derived table to its DXL representation
	CDXLNode *derived_tbl_dxlnode =
		query_to_dxl_translator.TranslateSelectQueryToDXL();

	// get the output columns of the derived table
	CDXLNodeArray *query_output_cols_dxlnode_array =
		query_to_dxl_translator.GetQueryOutputCols();
	CDXLNodeArray *cte_dxlnode_array = query_to_dxl_translator.GetCTEs();
	GPOS_ASSERT(nullptr != derived_tbl_dxlnode &&
				query_output_cols_dxlnode_array != nullptr);

	CUtils::AddRefAppend(m_dxl_cte_producers, cte_dxlnode_array);

	// make note of new columns from derived table
	m_var_to_colid_map->LoadDerivedTblColumns(
		current_query_level, rt_index, query_output_cols_dxlnode_array,
		query_to_dxl_translator.Pquery()->targetList);

	return derived_tbl_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateExprToDXL
//
//	@doc:
//		Translate the Expr into a CDXLScalar node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateExprToDXL(Expr *expr)
{
	CDXLNode *scalar_dxlnode =
		m_scalar_translator->TranslateScalarToDXL(expr, m_var_to_colid_map);
	GPOS_ASSERT(nullptr != scalar_dxlnode);

	return scalar_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateJoinExprInFromToDXL
//
//	@doc:
//		Translate the JoinExpr on a GPDB query into a CDXLLogicalJoin node
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateJoinExprInFromToDXL(JoinExpr *join_expr)
{
	GPOS_ASSERT(nullptr != join_expr);

	CDXLNode *left_child_dxlnode = TranslateFromClauseToDXL(join_expr->larg);
	CDXLNode *right_child_dxlnode = TranslateFromClauseToDXL(join_expr->rarg);
	EdxlJoinType join_type =
		CTranslatorUtils::ConvertToDXLJoinType(join_expr->jointype);
	CDXLNode *join_dxlnode = GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalJoin(m_mp, join_type));

	GPOS_ASSERT(nullptr != left_child_dxlnode &&
				nullptr != right_child_dxlnode);

	join_dxlnode->AddChild(left_child_dxlnode);
	join_dxlnode->AddChild(right_child_dxlnode);

	Node *node = join_expr->quals;

	// translate the join condition
	if (nullptr != node)
	{
		join_dxlnode->AddChild(TranslateExprToDXL((Expr *) node));
	}
	else
	{
		// a cross join therefore add a CDXLScalarConstValue representing the value "true"
		join_dxlnode->AddChild(CreateDXLConstValueTrue());
	}

	// extract the range table entry for the join expr to:
	// 1. Process the alias names of the columns
	// 2. Generate a project list for the join expr and maintain it in our hash map

	const ULONG rtindex = join_expr->rtindex;
	RangeTblEntry *rte =
		(RangeTblEntry *) gpdb::ListNth(m_query->rtable, rtindex - 1);
	GPOS_ASSERT(nullptr != rte);

	Alias *alias = rte->eref;
	GPOS_ASSERT(nullptr != alias);
	GPOS_ASSERT(nullptr != alias->colnames &&
				0 < gpdb::ListLength(alias->colnames));
	GPOS_ASSERT(gpdb::ListLength(rte->joinaliasvars) ==
				gpdb::ListLength(alias->colnames));

	CDXLNode *project_list_computed_cols_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));
	CDXLNode *project_list_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

	// construct a proj element node for each entry in the joinaliasvars
	ListCell *lc_node = nullptr;
	ListCell *lc_col_name = nullptr;
	ForBoth(lc_node, rte->joinaliasvars, lc_col_name, alias->colnames)
	{
		Node *join_alias_node = (Node *) lfirst(lc_node);
		// rte->joinaliasvars may contain NULL ptrs which indicates dropped columns
		if (!join_alias_node)
		{
			continue;
		}
		GPOS_ASSERT(IsA(join_alias_node, Var) ||
					IsA(join_alias_node, FuncExpr) ||
					IsA(join_alias_node, CoalesceExpr));
		Value *value = (Value *) lfirst(lc_col_name);
		CHAR *col_name_char_array = strVal(value);

		// create the DXL node holding the target list entry and add it to proj list
		CDXLNode *project_elem_dxlnode = TranslateExprToDXLProject(
			(Expr *) join_alias_node, col_name_char_array);
		project_list_dxlnode->AddChild(project_elem_dxlnode);

		if (IsA(join_alias_node, CoalesceExpr))
		{
			// add coalesce expression to the computed columns
			project_elem_dxlnode->AddRef();
			project_list_computed_cols_dxlnode->AddChild(project_elem_dxlnode);
		}
	}
	m_var_to_colid_map->LoadProjectElements(m_query_level, rtindex,
											project_list_dxlnode);
	project_list_dxlnode->Release();

	if (0 == project_list_computed_cols_dxlnode->Arity())
	{
		project_list_computed_cols_dxlnode->Release();
		return join_dxlnode;
	}

	CDXLNode *project_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp));
	project_dxlnode->AddChild(project_list_computed_cols_dxlnode);
	project_dxlnode->AddChild(join_dxlnode);

	return project_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateTargetListToDXLProject
//
//	@doc:
//		Create a DXL project list from the target list. The function allocates
//		memory in the translator memory pool and caller responsible for freeing it.
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateTargetListToDXLProject(
	List *target_list, CDXLNode *child_dxlnode,
	IntToUlongMap *sort_grpref_to_colid_mapping,
	IntToUlongMap *output_attno_to_colid_mapping, List *plgrpcl,
	BOOL is_expand_aggref_expr)
{
	BOOL is_groupby =
		(0 != gpdb::ListLength(m_query->groupClause) ||
		 0 != gpdb::ListLength(m_query->groupingSets) || m_query->hasAggs);

	CDXLNode *project_list_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

	// construct a proj element node for each entry in the target list
	ListCell *lc = nullptr;

	// target entries that are result of flattening join alias
	// and are equivalent to a defined grouping column target entry
	List *omitted_te_list = NIL;

	// list for all vars used in aggref expressions
	List *vars_list = nullptr;
	ULONG resno = 0;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));
		GPOS_ASSERT(0 < target_entry->resno);
		resno = target_entry->resno;

		BOOL is_grouping_col =
			CTranslatorUtils::IsGroupingColumn(target_entry, plgrpcl);
		if (IsA(target_entry->expr, GroupingFunc))
		{
			GroupingFunc *grouping_func = (GroupingFunc *) target_entry->expr;

			if (1 != gpdb::ListLength(grouping_func->refs))
			{
				GPOS_RAISE(
					gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					GPOS_WSZ_LIT("Grouping function with multiple arguments"));
			}

			if (0 != grouping_func->agglevelsup)
			{
				GPOS_RAISE(
					gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					GPOS_WSZ_LIT("Grouping function with outer references"));
			}
		}
		else if (!is_groupby || is_grouping_col)
		{
			// Insist projection for any outer refs to ensure any decorelation of a
			// subquery results in a correct plan using the projected reference,
			// instead of the outer ref directly.
			// TODO: Remove is_grouping_col from this check once const projections in
			// subqueries no longer prevent decorrelation
			BOOL is_orderby_col = CTranslatorUtils::IsSortingColumn(
				target_entry, m_query->sortClause);
			BOOL insist_proj =
				IsA(target_entry->expr, Var) &&
				((Var *) (target_entry->expr))->varlevelsup > 0 &&
				!is_orderby_col && !is_grouping_col;
			CDXLNode *project_elem_dxlnode = TranslateExprToDXLProject(
				target_entry->expr, target_entry->resname,
				insist_proj /* insist_new_colids */);
			ULONG colid =
				CDXLScalarProjElem::Cast(project_elem_dxlnode->GetOperator())
					->Id();

			AddSortingGroupingColumn(target_entry, sort_grpref_to_colid_mapping,
									 colid);

			// add column to the list of output columns of the query
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);

			if (!IsA(target_entry->expr, Var) || insist_proj)
			{
				// only add computed columns to the project list or if it's an outerref
				project_list_dxlnode->AddChild(project_elem_dxlnode);
			}
			else
			{
				project_elem_dxlnode->Release();
			}
		}
		else if (is_expand_aggref_expr && IsA(target_entry->expr, Aggref))
		{
			vars_list = gpdb::ListConcat(
				vars_list,
				gpdb::ExtractNodesExpression((Node *) target_entry->expr, T_Var,
											 false /*descendIntoSubqueries*/));
		}
		else if (!IsA(target_entry->expr, Aggref))
		{
			omitted_te_list = gpdb::LAppend(omitted_te_list, target_entry);
		}
	}

	// process target entries that are a result of flattening join alias
	lc = nullptr;
	ForEach(lc, omitted_te_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		INT sort_group_ref = (INT) target_entry->ressortgroupref;

		TargetEntry *te_grouping_col =
			CTranslatorUtils::GetGroupingColumnTargetEntry(
				(Node *) target_entry->expr, plgrpcl, target_list);
		if (nullptr != te_grouping_col)
		{
			const ULONG colid = CTranslatorUtils::GetColId(
				(INT) te_grouping_col->ressortgroupref,
				sort_grpref_to_colid_mapping);
			StoreAttnoColIdMapping(output_attno_to_colid_mapping,
								   target_entry->resno, colid);
			if (0 < sort_group_ref && 0 < colid &&
				nullptr == sort_grpref_to_colid_mapping->Find(&sort_group_ref))
			{
				AddSortingGroupingColumn(target_entry,
										 sort_grpref_to_colid_mapping, colid);
			}
		}
	}
	if (NIL != omitted_te_list)
	{
		gpdb::GPDBFree(omitted_te_list);
	}

	GPOS_ASSERT_IMP(!is_expand_aggref_expr, nullptr == vars_list);

	// process all additional vars in aggref expressions
	ListCell *lc_var = nullptr;
	ForEach(lc_var, vars_list)
	{
		resno++;
		Var *var = (Var *) lfirst(lc_var);

		// TODO: Dec 28, 2012; figure out column's name
		CDXLNode *project_elem_dxlnode =
			TranslateExprToDXLProject((Expr *) var, "?col?");

		ULONG colid =
			CDXLScalarProjElem::Cast(project_elem_dxlnode->GetOperator())->Id();

		// add column to the list of output columns of the query
		StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);

		project_elem_dxlnode->Release();
	}

	if (0 < project_list_dxlnode->Arity())
	{
		// create a node with the CDXLLogicalProject operator and add as its children:
		// the CDXLProjectList node and the node representing the input to the project node
		CDXLNode *project_dxlnode = GPOS_NEW(m_mp)
			CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp));
		project_dxlnode->AddChild(project_list_dxlnode);
		project_dxlnode->AddChild(child_dxlnode);
		GPOS_ASSERT(nullptr != project_dxlnode);
		return project_dxlnode;
	}

	project_list_dxlnode->Release();
	return child_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CreateDXLProjectNullsForGroupingSets
//
//	@doc:
//		Construct a DXL project node projecting NULL values for the columns in the
//		given bitset
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::CreateDXLProjectNullsForGroupingSets(
	List *target_list, CDXLNode *child_dxlnode,
	CBitSet *bitset,  // group by columns
	IntToUlongMap
		*sort_grouping_col_mapping,	 // mapping of sorting and grouping columns
	IntToUlongMap *output_attno_to_colid_mapping,  // mapping of output columns
	UlongToUlongMap *
		grpcol_index_to_colid_mapping  // mapping of unique grouping col positions
) const
{
	CDXLNode *project_list_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

	// construct a proj element node for those non-aggregate entries in the target list which
	// are not included in the grouping set
	ListCell *lc = nullptr;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));

		BOOL is_grouping_col = bitset->Get(target_entry->ressortgroupref);
		ULONG resno = target_entry->resno;

		ULONG colid = 0;

		if (IsA(target_entry->expr, GroupingFunc))
		{
			colid = m_context->m_colid_counter->next_id();
			CDXLNode *grouping_func_dxlnode = TranslateGroupingFuncToDXL(
				target_entry->expr, bitset, grpcol_index_to_colid_mapping);

			CWStringDynamic *alias_str =
				CDXLUtils::CreateDynamicStringFromCharArray(
					m_mp, target_entry->resname);
			CMDName *mdname_alias = GPOS_NEW(m_mp) CMDName(m_mp, alias_str);
			GPOS_DELETE(alias_str);

			CDXLNode *project_elem_dxlnode = GPOS_NEW(m_mp) CDXLNode(
				m_mp,
				GPOS_NEW(m_mp) CDXLScalarProjElem(m_mp, colid, mdname_alias),
				grouping_func_dxlnode);
			project_list_dxlnode->AddChild(project_elem_dxlnode);
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
		}
		else if (!is_grouping_col && !IsA(target_entry->expr, Aggref))
		{
			OID oid_type = gpdb::ExprType((Node *) target_entry->expr);

			colid = m_context->m_colid_counter->next_id();

			CMDIdGPDB *mdid =
				GPOS_NEW(m_mp) CMDIdGPDB(IMDId::EmdidGeneral, oid_type);
			CDXLNode *project_elem_dxlnode =
				CTranslatorUtils::CreateDXLProjElemConstNULL(
					m_mp, m_md_accessor, mdid, colid, target_entry->resname);
			mdid->Release();

			project_list_dxlnode->AddChild(project_elem_dxlnode);
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
		}

		INT sort_group_ref = INT(target_entry->ressortgroupref);

#if 0
		// FIXME: The following assert is wrong for its semantics that may
		// call a member function on a null pointer.
		// The assert expression is highly relative to the if condition below.
		// We should figure out what the assert state really is and add it back.
		GPOS_ASSERT_IMP(
			nullptr == sort_grouping_col_mapping,
			nullptr != sort_grouping_col_mapping->Find(&sort_group_ref) &&
				"Grouping column with no mapping");
#endif

		if (0 < sort_group_ref && 0 < colid &&
			nullptr == sort_grouping_col_mapping->Find(&sort_group_ref))
		{
			AddSortingGroupingColumn(target_entry, sort_grouping_col_mapping,
									 colid);
		}
	}

	if (0 == project_list_dxlnode->Arity())
	{
		// no project necessary
		project_list_dxlnode->Release();
		return child_dxlnode;
	}

	return GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp),
				 project_list_dxlnode, child_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CreateDXLProjectGroupingFuncs
//
//	@doc:
//		Construct a DXL project node projecting values for the grouping funcs in
//		the target list
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::CreateDXLProjectGroupingFuncs(
	List *target_list, CDXLNode *child_dxlnode, CBitSet *bitset,
	IntToUlongMap *output_attno_to_colid_mapping,
	UlongToUlongMap *grpcol_index_to_colid_mapping,
	IntToUlongMap *sort_grpref_to_colid_mapping) const
{
	CDXLNode *project_list_dxlnode =
		GPOS_NEW(m_mp) CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLScalarProjList(m_mp));

	// construct a proj element node for those non-aggregate entries in the target list which
	// are not included in the grouping set
	ListCell *lc = nullptr;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(IsA(target_entry, TargetEntry));

		ULONG resno = target_entry->resno;

		if (IsA(target_entry->expr, GroupingFunc))
		{
			ULONG colid = m_context->m_colid_counter->next_id();
			CDXLNode *grouping_func_dxlnode = TranslateGroupingFuncToDXL(
				target_entry->expr, bitset, grpcol_index_to_colid_mapping);

			CWStringDynamic *alias_str =
				CDXLUtils::CreateDynamicStringFromCharArray(
					m_mp, target_entry->resname);
			CMDName *mdname_alias = GPOS_NEW(m_mp) CMDName(m_mp, alias_str);
			GPOS_DELETE(alias_str);

			CDXLNode *project_elem_dxlnode = GPOS_NEW(m_mp) CDXLNode(
				m_mp,
				GPOS_NEW(m_mp) CDXLScalarProjElem(m_mp, colid, mdname_alias),
				grouping_func_dxlnode);
			project_list_dxlnode->AddChild(project_elem_dxlnode);
			StoreAttnoColIdMapping(output_attno_to_colid_mapping, resno, colid);
			AddSortingGroupingColumn(target_entry, sort_grpref_to_colid_mapping,
									 colid);
		}
	}

	if (0 == project_list_dxlnode->Arity())
	{
		// no project necessary
		project_list_dxlnode->Release();
		return child_dxlnode;
	}

	return GPOS_NEW(m_mp)
		CDXLNode(m_mp, GPOS_NEW(m_mp) CDXLLogicalProject(m_mp),
				 project_list_dxlnode, child_dxlnode);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::StoreAttnoColIdMapping
//
//	@doc:
//		Store mapping between attno and generate colid
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::StoreAttnoColIdMapping(
	IntToUlongMap *attno_to_colid_mapping, INT attno, ULONG colid) const
{
	GPOS_ASSERT(nullptr != attno_to_colid_mapping);

	INT *key = GPOS_NEW(m_mp) INT(attno);
	ULONG *value = GPOS_NEW(m_mp) ULONG(colid);
	BOOL result = attno_to_colid_mapping->Insert(key, value);

	if (!result)
	{
		GPOS_DELETE(key);
		GPOS_DELETE(value);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CreateDXLOutputCols
//
//	@doc:
//		Construct an array of DXL nodes representing the query output
//
//---------------------------------------------------------------------------
CDXLNodeArray *
CTranslatorQueryToDXL::CreateDXLOutputCols(
	List *target_list, IntToUlongMap *attno_to_colid_mapping) const
{
	GPOS_ASSERT(nullptr != target_list);
	GPOS_ASSERT(nullptr != attno_to_colid_mapping);

	CDXLNodeArray *dxlnodes = GPOS_NEW(m_mp) CDXLNodeArray(m_mp);

	ListCell *lc = nullptr;
	ForEach(lc, target_list)
	{
		TargetEntry *target_entry = (TargetEntry *) lfirst(lc);
		GPOS_ASSERT(0 < target_entry->resno);
		ULONG resno = target_entry->resno;

		if (target_entry->resjunk)
		{
			continue;
		}

		GPOS_ASSERT(nullptr != target_entry);
		CMDName *mdname = nullptr;
		if (nullptr == target_entry->resname)
		{
			CWStringConst str_unnamed_col(GPOS_WSZ_LIT("?column?"));
			mdname = GPOS_NEW(m_mp) CMDName(m_mp, &str_unnamed_col);
		}
		else
		{
			CWStringDynamic *alias_str =
				CDXLUtils::CreateDynamicStringFromCharArray(
					m_mp, target_entry->resname);
			mdname = GPOS_NEW(m_mp) CMDName(m_mp, alias_str);
			// CName constructor copies string
			GPOS_DELETE(alias_str);
		}

		const ULONG colid =
			CTranslatorUtils::GetColId(resno, attno_to_colid_mapping);

		// create a column reference
		IMDId *mdid_type = GPOS_NEW(m_mp) CMDIdGPDB(
			IMDId::EmdidGeneral, gpdb::ExprType((Node *) target_entry->expr));
		INT type_modifier = gpdb::ExprTypeMod((Node *) target_entry->expr);
		CDXLColRef *dxl_colref =
			GPOS_NEW(m_mp) CDXLColRef(mdname, colid, mdid_type, type_modifier);
		CDXLScalarIdent *dxl_ident =
			GPOS_NEW(m_mp) CDXLScalarIdent(m_mp, dxl_colref);

		// create the DXL node holding the scalar ident operator
		CDXLNode *dxlnode = GPOS_NEW(m_mp) CDXLNode(m_mp, dxl_ident);

		dxlnodes->Append(dxlnode);
	}

	return dxlnodes;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateExprToDXLProject
//
//	@doc:
//		Create a DXL project element node from the target list entry or var.
//		The function allocates memory in the translator memory pool, and the caller
//		is responsible for freeing it.
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateExprToDXLProject(Expr *expr,
												 const CHAR *alias_name,
												 BOOL insist_new_colids)
{
	GPOS_ASSERT(nullptr != expr);

	// construct a scalar operator
	CDXLNode *child_dxlnode = TranslateExprToDXL(expr);

	// get the id and alias for the proj elem
	ULONG project_elem_id;
	CMDName *mdname_alias = nullptr;

	if (nullptr == alias_name)
	{
		CWStringConst str_unnamed_col(GPOS_WSZ_LIT("?column?"));
		mdname_alias = GPOS_NEW(m_mp) CMDName(m_mp, &str_unnamed_col);
	}
	else
	{
		CWStringDynamic *alias_str =
			CDXLUtils::CreateDynamicStringFromCharArray(m_mp, alias_name);
		mdname_alias = GPOS_NEW(m_mp) CMDName(m_mp, alias_str);
		GPOS_DELETE(alias_str);
	}

	if (IsA(expr, Var) && !insist_new_colids)
	{
		// project elem is a reference to a column - use the colref id
		GPOS_ASSERT(EdxlopScalarIdent ==
					child_dxlnode->GetOperator()->GetDXLOperator());
		CDXLScalarIdent *dxl_ident =
			(CDXLScalarIdent *) child_dxlnode->GetOperator();
		project_elem_id = dxl_ident->GetDXLColRef()->Id();
	}
	else
	{
		// project elem is a defined column - get a new id
		project_elem_id = m_context->m_colid_counter->next_id();
	}

	CDXLNode *project_elem_dxlnode = GPOS_NEW(m_mp) CDXLNode(
		m_mp,
		GPOS_NEW(m_mp) CDXLScalarProjElem(m_mp, project_elem_id, mdname_alias));
	project_elem_dxlnode->AddChild(child_dxlnode);

	return project_elem_dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::CreateDXLConstValueTrue
//
//	@doc:
//		Returns a CDXLNode representing scalar condition "true"
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::CreateDXLConstValueTrue()
{
	Const *const_expr =
		(Const *) gpdb::MakeBoolConst(true /*value*/, false /*isnull*/);
	CDXLNode *dxlnode = TranslateExprToDXL((Expr *) const_expr);
	gpdb::GPDBFree(const_expr);

	return dxlnode;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::TranslateGroupingFuncToDXL
//
//	@doc:
//		Translate grouping func
//
//---------------------------------------------------------------------------
CDXLNode *
CTranslatorQueryToDXL::TranslateGroupingFuncToDXL(
	const Expr *expr, CBitSet *bitset,
	UlongToUlongMap *grpcol_index_to_colid_mapping) const
{
	GPOS_ASSERT(IsA(expr, GroupingFunc));
	GPOS_ASSERT(nullptr != grpcol_index_to_colid_mapping);

	const GroupingFunc *grouping_func = (GroupingFunc *) expr;
	GPOS_ASSERT(1 == gpdb::ListLength(grouping_func->refs));
	GPOS_ASSERT(0 == grouping_func->agglevelsup);

	// generate a constant value for the result of the grouping function as follows:
	// if the grouping function argument is a group-by column, result is 0
	// otherwise, the result is 1
	LINT l_value = 0;

	ULONG sort_group_ref = gpdb::ListNthInt(grouping_func->refs, 0);
	BOOL is_grouping_col = bitset->Get(sort_group_ref);
	if (!is_grouping_col)
	{
		// not a grouping column
		l_value = 1;
	}

	const IMDType *md_type = m_md_accessor->PtMDType<IMDTypeInt4>(m_sysid);
	CMDIdGPDB *mdid_cast = CMDIdGPDB::CastMdid(md_type->MDId());
	CMDIdGPDB *mdid = GPOS_NEW(m_mp) CMDIdGPDB(*mdid_cast);

	CDXLDatum *datum_dxl =
		GPOS_NEW(m_mp) CDXLDatumInt4(m_mp, mdid, false /* is_null */, l_value);
	CDXLScalarConstValue *dxlop =
		GPOS_NEW(m_mp) CDXLScalarConstValue(m_mp, datum_dxl);
	return GPOS_NEW(m_mp) CDXLNode(m_mp, dxlop);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::ConstructCTEProducerList
//
//	@doc:
//		Construct a list of CTE producers from the query's CTE list
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::ConstructCTEProducerList(List *cte_list,
												ULONG cte_query_level)
{
	GPOS_ASSERT(nullptr != m_dxl_cte_producers &&
				"CTE Producer list not initialized");

	if (nullptr == cte_list)
	{
		return;
	}

	ListCell *lc = nullptr;

	ForEach(lc, cte_list)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
		GPOS_ASSERT(IsA(cte->ctequery, Query));

		if (cte->cterecursive)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLUnsupportedFeature,
					   GPOS_WSZ_LIT("WITH RECURSIVE"));
		}

		Query *cte_query = CQueryMutators::NormalizeQuery(
			m_mp, m_md_accessor, (Query *) cte->ctequery, cte_query_level + 1);

		// the query representing the cte can only access variables defined in the current level as well as
		// those defined at prior query levels

		CTranslatorQueryToDXL query_to_dxl_translator(
			m_context, m_md_accessor, m_var_to_colid_map, cte_query,
			cte_query_level + 1, IsDMLQuery(), m_query_level_to_cte_map);

		// translate query representing the cte table to its DXL representation
		CDXLNode *cte_child_dxlnode =
			query_to_dxl_translator.TranslateSelectQueryToDXL();

		// get the output columns of the cte table
		CDXLNodeArray *cte_query_output_colds_dxlnode_array =
			query_to_dxl_translator.GetQueryOutputCols();
		CDXLNodeArray *cte_dxlnode_array = query_to_dxl_translator.GetCTEs();

		GPOS_ASSERT(nullptr != cte_child_dxlnode &&
					nullptr != cte_query_output_colds_dxlnode_array &&
					nullptr != cte_dxlnode_array);

		// append any nested CTE
		CUtils::AddRefAppend(m_dxl_cte_producers, cte_dxlnode_array);

		ULongPtrArray *colid_array = GPOS_NEW(m_mp) ULongPtrArray(m_mp);

		const ULONG output_columns =
			cte_query_output_colds_dxlnode_array->Size();
		for (ULONG ul = 0; ul < output_columns; ul++)
		{
			CDXLNode *output_col_dxlnode =
				(*cte_query_output_colds_dxlnode_array)[ul];
			CDXLScalarIdent *dxl_scalar_ident =
				CDXLScalarIdent::Cast(output_col_dxlnode->GetOperator());
			colid_array->Append(
				GPOS_NEW(m_mp) ULONG(dxl_scalar_ident->GetDXLColRef()->Id()));
		}

		CDXLLogicalCTEProducer *lg_cte_prod_dxlop =
			GPOS_NEW(m_mp) CDXLLogicalCTEProducer(
				m_mp, m_context->m_cte_id_counter->next_id(), colid_array,
				!GPOS_FTRACE(EopttraceEnableCTEInlining) /*can_be_pruned*/);
		CDXLNode *cte_producer_dxlnode =
			GPOS_NEW(m_mp) CDXLNode(m_mp, lg_cte_prod_dxlop, cte_child_dxlnode);

		m_dxl_cte_producers->Append(cte_producer_dxlnode);
		BOOL result GPOS_ASSERTS_ONLY =
			m_cteid_at_current_query_level_map->Insert(
				GPOS_NEW(m_mp) ULONG(lg_cte_prod_dxlop->Id()),
				GPOS_NEW(m_mp) BOOL(true));
		GPOS_ASSERT(result);

		// update CTE producer mappings
		CCTEListEntry *cte_list_entry =
			m_query_level_to_cte_map->Find(&cte_query_level);
		if (nullptr == cte_list_entry)
		{
			cte_list_entry = GPOS_NEW(m_mp)
				CCTEListEntry(m_mp, cte_query_level, cte, cte_producer_dxlnode);
			BOOL is_res GPOS_ASSERTS_ONLY = m_query_level_to_cte_map->Insert(
				GPOS_NEW(m_mp) ULONG(cte_query_level), cte_list_entry);
			GPOS_ASSERT(is_res);
		}
		else
		{
			cte_list_entry->AddCTEProducer(m_mp, cte, cte_producer_dxlnode);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::ConstructCTEAnchors
//
//	@doc:
//		Construct a stack of CTE anchors for each CTE producer in the given array
//
//---------------------------------------------------------------------------
void
CTranslatorQueryToDXL::ConstructCTEAnchors(CDXLNodeArray *dxlnodes,
										   CDXLNode **dxl_cte_anchor_top,
										   CDXLNode **dxl_cte_anchor_bottom)
{
	GPOS_ASSERT(nullptr == *dxl_cte_anchor_top);
	GPOS_ASSERT(nullptr == *dxl_cte_anchor_bottom);

	if (nullptr == dxlnodes || 0 == dxlnodes->Size())
	{
		return;
	}

	const ULONG num_of_ctes = dxlnodes->Size();

	for (ULONG ul = num_of_ctes; ul > 0; ul--)
	{
		// construct a new CTE anchor on top of the previous one
		CDXLNode *cte_producer_dxlnode = (*dxlnodes)[ul - 1];
		CDXLLogicalCTEProducer *cte_prod_dxlop =
			CDXLLogicalCTEProducer::Cast(cte_producer_dxlnode->GetOperator());
		ULONG cte_producer_id = cte_prod_dxlop->Id();

		if (nullptr ==
			m_cteid_at_current_query_level_map->Find(&cte_producer_id))
		{
			// cte not defined at this level: CTE anchor was already added
			continue;
		}

		CDXLNode *cte_anchor_new_dxlnode = GPOS_NEW(m_mp) CDXLNode(
			m_mp, GPOS_NEW(m_mp) CDXLLogicalCTEAnchor(m_mp, cte_producer_id));

		if (nullptr == *dxl_cte_anchor_bottom)
		{
			*dxl_cte_anchor_bottom = cte_anchor_new_dxlnode;
		}

		if (nullptr != *dxl_cte_anchor_top)
		{
			cte_anchor_new_dxlnode->AddChild(*dxl_cte_anchor_top);
		}
		*dxl_cte_anchor_top = cte_anchor_new_dxlnode;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::GenerateColIds
//
//	@doc:
//		Generate an array of new column ids of the given size
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorQueryToDXL::GenerateColIds(CMemoryPool *mp, ULONG size) const
{
	ULongPtrArray *colid_array = GPOS_NEW(mp) ULongPtrArray(mp);

	for (ULONG ul = 0; ul < size; ul++)
	{
		colid_array->Append(GPOS_NEW(mp)
								ULONG(m_context->m_colid_counter->next_id()));
	}

	return colid_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::ExtractColIds
//
//	@doc:
//		Extract column ids from the given mapping
//
//---------------------------------------------------------------------------
ULongPtrArray *
CTranslatorQueryToDXL::ExtractColIds(
	CMemoryPool *mp, IntToUlongMap *attno_to_colid_mapping) const
{
	UlongToUlongMap *old_new_col_mapping = GPOS_NEW(mp) UlongToUlongMap(mp);

	ULongPtrArray *colid_array = GPOS_NEW(mp) ULongPtrArray(mp);

	IntUlongHashmapIter att_iter(attno_to_colid_mapping);
	while (att_iter.Advance())
	{
		ULONG colid = *(att_iter.Value());

		// do not insert colid if already inserted
		if (nullptr == old_new_col_mapping->Find(&colid))
		{
			colid_array->Append(GPOS_NEW(m_mp) ULONG(colid));
			old_new_col_mapping->Insert(GPOS_NEW(m_mp) ULONG(colid),
										GPOS_NEW(m_mp) ULONG(colid));
		}
	}

	old_new_col_mapping->Release();
	return colid_array;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::RemapColIds
//
//	@doc:
//		Construct a new hashmap which replaces the values in the From array
//		with the corresponding value in the To array
//
//---------------------------------------------------------------------------
IntToUlongMap *
CTranslatorQueryToDXL::RemapColIds(CMemoryPool *mp,
								   IntToUlongMap *attno_to_colid_mapping,
								   ULongPtrArray *from_list_colids,
								   ULongPtrArray *to_list_colids)
{
	GPOS_ASSERT(nullptr != attno_to_colid_mapping);
	GPOS_ASSERT(nullptr != from_list_colids && nullptr != to_list_colids);
	GPOS_ASSERT(from_list_colids->Size() == to_list_colids->Size());

	// compute a map of the positions in the from array
	UlongToUlongMap *old_new_col_mapping = GPOS_NEW(mp) UlongToUlongMap(mp);
	const ULONG size = from_list_colids->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		BOOL result GPOS_ASSERTS_ONLY = old_new_col_mapping->Insert(
			GPOS_NEW(mp) ULONG(*((*from_list_colids)[ul])),
			GPOS_NEW(mp) ULONG(*((*to_list_colids)[ul])));
		GPOS_ASSERT(result);
	}

	IntToUlongMap *result_attno_to_colid_mapping =
		GPOS_NEW(mp) IntToUlongMap(mp);
	IntUlongHashmapIter mi(attno_to_colid_mapping);
	while (mi.Advance())
	{
		INT *key = GPOS_NEW(mp) INT(*(mi.Key()));
		const ULONG *value = mi.Value();
		GPOS_ASSERT(nullptr != value);

		ULONG *remapped_value =
			GPOS_NEW(mp) ULONG(*(old_new_col_mapping->Find(value)));
		result_attno_to_colid_mapping->Insert(key, remapped_value);
	}

	old_new_col_mapping->Release();

	return result_attno_to_colid_mapping;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorQueryToDXL::RemapColIds
//
//	@doc:
//		True iff this query or one of its ancestors is a DML query
//
//---------------------------------------------------------------------------
BOOL
CTranslatorQueryToDXL::IsDMLQuery()
{
	return (m_is_top_query_dml || m_query->resultRelation != 0);
}

// EOF
