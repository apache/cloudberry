//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC Corp.
//
//	@filename:
//		CMemo.cpp
//
//	@doc:
//		Implementation of Memo structure
//---------------------------------------------------------------------------

#include "gpopt/search/CMemo.h"

#include "gpos/base.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CSyncHashtableAccessByIter.h"
#include "gpos/common/CSyncHashtableAccessByKey.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/COptimizationContext.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/exception.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"
#include "gpopt/search/CGroupProxy.h"

using namespace gpopt;

#define GPOPT_MEMO_HT_BUCKETS 50000

//---------------------------------------------------------------------------
//	@function:
//		CMemo::CMemo
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMemo::CMemo(CMemoryPool *mp)
	: m_mp(mp),
	  m_aul(0),
	  m_pgroupRoot(nullptr),
	  m_ulpGrps(0),
	  m_pmemotmap(nullptr)
{
	GPOS_ASSERT(nullptr != mp);

	m_sht.Init(
		mp, GPOPT_MEMO_HT_BUCKETS, GPOS_OFFSET(CGroupExpression, m_linkMemo),
		0, /*cKeyOffset (0 because we use CGroupExpression class as key)*/
		&(CGroupExpression::m_gexprInvalid), CGroupExpression::HashValue,
		CGroupExpression::Equals);

	m_listGroups.Init(GPOS_OFFSET(CGroup, m_link));
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::~CMemo
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMemo::~CMemo()
{
	CGroup *pgroup = m_listGroups.PtFirst();
	while (nullptr != pgroup)
	{
		CGroup *pgroupNext = m_listGroups.Next(pgroup);
		pgroup->Release();

		pgroup = pgroupNext;
	}

	GPOS_DELETE(m_pmemotmap);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::SetRoot
//
//	@doc:
//		Set root group
//
//---------------------------------------------------------------------------
void
CMemo::SetRoot(CGroup *pgroup)
{
	GPOS_ASSERT(nullptr == m_pgroupRoot);
	GPOS_ASSERT(nullptr != pgroup);

	m_pgroupRoot = pgroup;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::Add
//
//	@doc:
//		Add new group to list
//
//---------------------------------------------------------------------------
void
CMemo::Add(
	CGroup *pgroup,
	CExpression *pexprOrigin  // origin expression that produced the group
)
{
	GPOS_ASSERT(nullptr != pgroup);
	GPOS_ASSERT(nullptr != pexprOrigin);
	GPOS_ASSERT(!pexprOrigin->Pop()->FPhysical() &&
				"Physical operators do not create new groups");

	// extract expression props
	CDrvdProp *pdp = nullptr;
	if (pexprOrigin->Pop()->FScalar())
	{
		pdp = pexprOrigin->GetDrvdPropScalar();
	}
	else
	{
		pdp = pexprOrigin->GetDrvdPropRelational();
	}
	GPOS_ASSERT(nullptr != pdp);

	ULONG id = m_aul++;
	pdp->AddRef();
	{
		CGroupProxy gp(pgroup);
		GPOS_ASSERT(nullptr != gp.PgexprFirst());

		gp.SetId(id);
		gp.InitProperties(pdp);
	}

	m_listGroups.Push(pgroup);
	m_ulpGrps++;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::PgroupInsert
//
//	@doc:
//		Helper for inserting group expression in target group
//
//
//---------------------------------------------------------------------------
CGroup *
CMemo::PgroupInsert(CGroup *pgroupTarget, CGroupExpression *pgexpr,
					CExpression *pexprOrigin, BOOL fNewGroup)
{
	GPOS_ASSERT(nullptr != pgroupTarget);
	GPOS_ASSERT(nullptr != pgexpr);

	ShtAcc shta(m_sht, *pgexpr);

	// we do a lookup since group expression may have been already inserted
	CGroupExpression *pgexprFound = shta.Find();
	if (nullptr == pgexprFound)
	{
		shta.Insert(pgexpr);

		// group proxy scope
		{
			CGroupProxy gp(pgroupTarget);
			gp.Insert(pgexpr);
		}

		if (fNewGroup)
		{
			Add(pgroupTarget, pexprOrigin);
		}

		return pgexpr->Pgroup();
	}

	return pgexprFound->Pgroup();
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::PgroupInsert
//
//	@doc:
//		Helper to check if a new group needs to be created
//
//---------------------------------------------------------------------------
BOOL
CMemo::FNewGroup(CGroup **ppgroupTarget, CGroupExpression *pgexpr, BOOL fScalar)
{
	GPOS_ASSERT(nullptr != ppgroupTarget);

	if (nullptr == *ppgroupTarget && nullptr == pgexpr)
	{
		*ppgroupTarget = GPOS_NEW(m_mp) CGroup(m_mp, fScalar);

		return true;
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::PgroupInsert
//
//	@doc:
//		Attempt inserting a group expression in a target group;
//		if group expression is not in the hash table, insertion
//		succeeds and the function returns the input target group;
//		otherwise insertion fails and the function returns the
//		group containing the existing group expression
//
//---------------------------------------------------------------------------
CGroup *
CMemo::PgroupInsert(CGroup *pgroupTarget, CExpression *pexprOrigin,
					CGroupExpression *pgexpr)
{
	GPOS_ASSERT(nullptr != pgexpr);
	GPOS_CHECK_ABORT;
	GPOS_ASSERT(nullptr != pexprOrigin);
	GPOS_ASSERT(pgexpr->Pop() == pexprOrigin->Pop());
	GPOS_ASSERT(pgexpr->Arity() == pexprOrigin->Arity());

	CGroup *pgroupContainer = nullptr;
	CGroupExpression *pgexprFound = nullptr;
	// hash table accessor's scope
	{
		ShtAcc shta(m_sht, *pgexpr);
		pgexprFound = shta.Find();
	}

	// check if we may need to create a new group
	BOOL fNewGroup =
		FNewGroup(&pgroupTarget, pgexprFound, pgexpr->Pop()->FScalar());
	if (fNewGroup)
	{
		// we may add a new group to Memo, so we derive props here
		(void) pexprOrigin->PdpDerive();
	}

	if (nullptr != pgexprFound)
	{
		pgroupContainer = pgexprFound->Pgroup();
	}
	else
	{
		pgroupContainer =
			PgroupInsert(pgroupTarget, pgexpr, pexprOrigin, fNewGroup);
	}

	// if insertion failed, release group as needed
	if (nullptr == pgexpr->Pgroup() && fNewGroup)
	{
		fNewGroup = false;
		pgroupTarget->Release();
	}

	// if a new scalar group is added, we materialize a scalar expression
	// for statistics derivation purposes
	if (fNewGroup && pgroupTarget->FScalar())
	{
		pgroupTarget->CreateScalarExpression();
		pgroupTarget->CreateDummyCostContext();
	}

	return pgroupContainer;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::PexprExtractPlan
//
//	@doc:
//		Extract a plan that delivers the given required properties
//
//---------------------------------------------------------------------------
CExpression *
CMemo::PexprExtractPlan(CMemoryPool *mp, CGroup *pgroupRoot,
						CReqdPropPlan *prppInput, ULONG ulSearchStages)
{
	// check stack size
	GPOS_CHECK_STACK_SIZE;
	GPOS_CHECK_ABORT;

	CGroupExpression *pgexprBest = nullptr;
	COptimizationContext *poc = nullptr;
	CCost cost = GPOPT_INVALID_COST;
	IStatistics *stats = nullptr;
	if (pgroupRoot->FScalar())
	{
		// If the group has scalar expression, this group is called scalar group.
		// It has one and only one group expression, so the expression is also picked
		// up as the best expression for that group.
		// The group expression is a scalar expression, which may have 0 or multiple
		// scalar or non-scalar groups as its children.
		CGroupProxy gp(pgroupRoot);
		pgexprBest = gp.PgexprFirst();
	}
	else
	{
		// If the group does not have scalar expression, which means it has only logical
		// or physical expressions. In this case, we lookup the best optimization context
		// for the given required plan properties, and then retrieve the best group
		// expression under the optimization context.
		poc = pgroupRoot->PocLookupBest(mp, ulSearchStages, prppInput);
		GPOS_ASSERT(nullptr != poc);

		pgexprBest = pgroupRoot->PgexprBest(poc);
		if (nullptr != pgexprBest)
		{
			cost = poc->PccBest()->Cost();
			stats = poc->PccBest()->Pstats();
		}
	}

	if (nullptr == pgexprBest)
	{
		// no plan found
		return nullptr;
	}

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	// Get the length of groups for the best group expression
	// i.e. given the best expression is
	// 0: CScalarCmp (>=) [ 1 7 ]
	// the arity is 2, which means the pgexprBest has 2 children:
	// Group 1 and Group 7. Every single child is a CGroup.
	ULONG arity = pgexprBest->Arity();
	for (ULONG i = 0; i < arity; i++)
	{
		CGroup *pgroupChild = (*pgexprBest)[i];
		CReqdPropPlan *prppChild = nullptr;

		// If the child group doesn't have scalar expression, we get the optimization
		// context for that child group as well as the required plan properties.
		//
		// But if the child group has scalar expression, which means it has one and
		// only one best scalar group expression, which does not need optimization,
		// because CJobGroupExpressionOptimization does not create optimization context
		// for that group. Besides, the scalar expression doesn't have plan properties.
		// In this case, the prppChild is left to be NULL.
		if (!pgroupChild->FScalar())
		{
			if (pgroupRoot->FScalar())
			{
				// In very rare case, Orca may generate the plan that a group is a scalar
				// group, but it has non-scalar sub groups. i.e.:
				// Group 7 ():  --> pgroupRoot->FScalar() == true
				//   0: CScalarSubquery["?column?" (19)] [ 6 ]
				// Group 6 (#GExprs: 2): --> pgroupChild->FScalar() == false
				//   0: CLogicalProject [ 2 5 ]
				//   1: CPhysicalComputeScalar [ 2 5 ]
				// In the above case, because group 7 has scalar expression, Orca skipped
				// generating optimization context for group 7 and its subgroup group 6,
				// even the group 6 doesn't have scalar expression and it needs optimization.
				// Orca doesn't support this feature yet, so falls back to planner.
				GPOS_RAISE(gpopt::ExmaGPOPT,
						   gpopt::ExmiUnsatisfiedRequiredProperties);
			}

			COptimizationContext *pocChild = (*poc->PccBest()->Pdrgpoc())[i];
			GPOS_ASSERT(nullptr != pocChild);

			prppChild = pocChild->Prpp();
		}

		CExpression *pexprChild =
			PexprExtractPlan(mp, pgroupChild, prppChild, ulSearchStages);
		pdrgpexpr->Append(pexprChild);
	}

	pgexprBest->Pop()->AddRef();

	CReqdPropPlan *prpp = nullptr;
	if (poc != nullptr)
	{
		GPOS_ASSERT(!pgroupRoot->FScalar());
		prpp = poc->Prpp();
		prpp->AddRef();
	}
	CExpression *pexpr = GPOS_NEW(mp) CExpression(
		mp, pgexprBest->Pop(), pgexprBest, pdrgpexpr, prpp, stats, cost);

	if (pexpr->Pop()->FPhysical() && !poc->PccBest()->IsValid(mp))
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsatisfiedRequiredProperties);
	}

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::Pgroup
//
//	@doc:
//		Get group by id;
//
//---------------------------------------------------------------------------
CGroup *
CMemo::Pgroup(ULONG id)
{
	CGroup *pgroup = m_listGroups.PtFirst();

	while (nullptr != pgroup)
	{
		if (id == pgroup->Id())
		{
			return pgroup;
		}
		pgroup = m_listGroups.Next(pgroup);
	}

	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::MarkDuplicates
//
//	@doc:
//		Mark groups as duplicates
//
//---------------------------------------------------------------------------
void
CMemo::MarkDuplicates(CGroup *pgroupFst, CGroup *pgroupSnd)
{
	GPOS_ASSERT(nullptr != pgroupFst);
	GPOS_ASSERT(nullptr != pgroupSnd);

	pgroupFst->AddDuplicateGrp(pgroupSnd);
	pgroupFst->ResolveDuplicateMaster();
	pgroupSnd->ResolveDuplicateMaster();
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::FRehash
//
//	@doc:
//		Delete then re-insert all group expressions in memo hash table;
//		we do this at the end of exploration phase since identified
//		duplicate groups during exploration may cause changing hash values
//		of current group expressions,
//		rehashing group expressions using the new hash values allows
//		identifying duplicate group expressions that can be skipped from
//		further processing;
//
//		the function returns TRUE if rehashing resulted in discovering
//		new duplicate groups;
//
//		this function is NOT thread safe, and must not be called while
//		exploration/implementation/optimization is undergoing
//
//
//---------------------------------------------------------------------------
BOOL
CMemo::FRehash()
{
	GPOS_ASSERT(m_pgroupRoot->FExplored());
	GPOS_ASSERT(!m_pgroupRoot->FImplemented());

	// dump memo hash table into a local list
	CList<CGroupExpression> listGExprs;
	listGExprs.Init(GPOS_OFFSET(CGroupExpression, m_linkMemo));

	ShtIter shtit(m_sht);
	CGroupExpression *pgexpr = nullptr;
	while (nullptr != pgexpr || shtit.Advance())
	{
		{
			ShtAccIter shtitacc(shtit);
			pgexpr = shtitacc.Value();
			if (nullptr != pgexpr)
			{
				shtitacc.Remove(pgexpr);
				listGExprs.Append(pgexpr);
			}
		}
		GPOS_CHECK_ABORT;
	}

	// iterate on list and insert non-duplicate group expressions
	// back to memo hash table
	BOOL fNewDupGroups = false;
	while (!listGExprs.IsEmpty())
	{
		CGroupExpression *pgexpr = listGExprs.RemoveHead();
		CGroupExpression *pgexprFound = nullptr;

		{
			// hash table accessor scope
			ShtAcc shta(m_sht, *pgexpr);
			pgexprFound = shta.Find();

			if (nullptr == pgexprFound)
			{
				// group expression has no duplicates, insert back to memo hash table
				shta.Insert(pgexpr);
				continue;
			}
		}

		GPOS_ASSERT(pgexprFound != pgexpr);

		// mark duplicate group expression
		pgexpr->SetDuplicate(pgexprFound);
		CGroup *pgroup = pgexpr->Pgroup();

		// move group expression to duplicates list in owner group
		{
			// group proxy scope
			CGroupProxy gp(pgroup);
			gp.MoveDuplicateGExpr(pgexpr);
		}

		// check if we need also to mark duplicate groups
		CGroup *pgroupFound = pgexprFound->Pgroup();
		if (pgroupFound != pgroup)
		{
			CGroup *pgroupDup = pgroup->PgroupDuplicate();
			CGroup *pgroupFoundDup = pgroupFound->PgroupDuplicate();
			if ((nullptr == pgroupDup && nullptr == pgroupFoundDup) ||
				(pgroupDup != pgroupFoundDup))
			{
				MarkDuplicates(pgroup, pgroupFound);
				fNewDupGroups = true;
			}
		}

		GPOS_CHECK_ABORT;
	}

	return fNewDupGroups;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::GroupMerge
//
//	@doc:
//		Merge duplicate groups
//
//---------------------------------------------------------------------------
void
CMemo::GroupMerge()
{
	GPOS_ASSERT(m_pgroupRoot->FExplored());
	GPOS_ASSERT(!m_pgroupRoot->FImplemented());

	CAutoTimer at("\n[OPT]: Group Merge Time",
				  GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	// keep merging groups until we have no new duplicates
	BOOL fNewDupGroups = true;
	while (fNewDupGroups)
	{
		CGroup *pgroup = m_listGroups.PtFirst();
		while (nullptr != pgroup)
		{
			pgroup->MergeGroup();
			pgroup = m_listGroups.Next(pgroup);

			GPOS_CHECK_ABORT;
		}

		// check if root has been merged
		if (m_pgroupRoot->FDuplicateGroup())
		{
			m_pgroupRoot = m_pgroupRoot->PgroupDuplicate();
		}

		fNewDupGroups = FRehash();
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::Trace
//
//	@doc:
//		Print memo to output logger
//
//---------------------------------------------------------------------------
void
CMemo::Trace()
{
	CWStringDynamic str(m_mp);
	COstreamString oss(&str);

	OsPrint(oss);

	GPOS_TRACE(str.GetBuffer());
}


FORCE_GENERATE_DBGSTR(gpopt::CMemo);

//---------------------------------------------------------------------------
//	@function:
//		CMemo::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CMemo::OsPrint(IOstream &os) const
{
	CGroup *pgroup = m_listGroups.PtFirst();

	while (nullptr != pgroup)
	{
		if (m_pgroupRoot == pgroup)
		{
			os << std::endl << "ROOT ";
		}

		pgroup->OsPrint(os);
		pgroup = m_listGroups.Next(pgroup);

		GPOS_CHECK_ABORT;
	}

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CMemo::DeriveStatsIfAbsent
//
//	@doc:
//		Derive stats when no stats not present for the group
//
//---------------------------------------------------------------------------
void
CMemo::DeriveStatsIfAbsent(CMemoryPool *pmpLocal)
{
	CGroup *pgroup = m_listGroups.PtFirst();

	while (nullptr != pgroup)
	{
		GPOS_ASSERT(!pgroup->FImplemented());
		if (nullptr == pgroup->Pstats())
		{
			CGroupExpression *pgexprFirst = CEngine::PgexprFirst(pgroup);

			CExpressionHandle exprhdl(m_mp);
			exprhdl.Attach(pgexprFirst);
			exprhdl.DeriveStats(pmpLocal, m_mp, nullptr, nullptr);
		}

		pgroup = m_listGroups.Next(pgroup);

		GPOS_CHECK_ABORT;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::ResetGroupStates
//
//	@doc:
//		Reset states and job queues of memo groups
//
//---------------------------------------------------------------------------
void
CMemo::ResetGroupStates()
{
	CGroup *pgroup = m_listGroups.PtFirst();

	while (nullptr != pgroup)
	{
		pgroup->ResetGroupState();
		pgroup->ResetGroupJobQueues();
		pgroup->ResetHasNewLogicalOperators();

		pgroup = m_listGroups.Next(pgroup);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::ResetStats
//
//	@doc:
//		Reset statistics by recursively descending from root group;
//		we call this function before stats derivation to reset stale stats
//		computed during previous search stages
//
//---------------------------------------------------------------------------
void
CMemo::ResetStats()
{
	(void) m_pgroupRoot->FResetStats();
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::BuildTreeMap
//
//	@doc:
//		Build tree map of member group expressions
//
//---------------------------------------------------------------------------
void
CMemo::BuildTreeMap(COptimizationContext *poc)
{
	GPOS_ASSERT(nullptr != poc);
	GPOS_ASSERT(nullptr == m_pmemotmap && "tree map is already built");

	m_pmemotmap = GPOS_NEW(m_mp) MemoTreeMap(m_mp, CExpression::PexprRehydrate);
	m_pgroupRoot->BuildTreeMap(m_mp, poc, nullptr /*pccParent*/,
							   gpos::ulong_max /*child_index*/, m_pmemotmap);
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::ResetTreeMap
//
//	@doc:
//		Reset tree map
//
//---------------------------------------------------------------------------
void
CMemo::ResetTreeMap()
{
	if (nullptr != m_pmemotmap)
	{
		GPOS_DELETE(m_pmemotmap);
		m_pmemotmap = nullptr;
	}

	// reset link map of all groups
	CGroup *pgroup = m_listGroups.PtFirst();
	while (nullptr != pgroup)
	{
		pgroup->ResetLinkMap();
		pgroup = m_listGroups.Next(pgroup);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::UlDuplicateGroups
//
//	@doc:
//		Return number of duplicate groups
//
//---------------------------------------------------------------------------
ULONG
CMemo::UlDuplicateGroups()
{
	ULONG ulDuplicates = 0;
	CGroup *pgroup = m_listGroups.PtFirst();
	while (nullptr != pgroup)
	{
		if (pgroup->FDuplicateGroup())
		{
			ulDuplicates++;
		}
		pgroup = m_listGroups.Next(pgroup);
	}

	return ulDuplicates;
}


//---------------------------------------------------------------------------
//	@function:
//		CMemo::UlGrpExprs
//
//	@doc:
//		Return total number of group expressions
//
//---------------------------------------------------------------------------
ULONG
CMemo::UlGrpExprs()
{
	ULONG ulGExprs = 0;
	CGroup *pgroup = m_listGroups.PtFirst();
	while (nullptr != pgroup)
	{
		ulGExprs += pgroup->UlGExprs();
		pgroup = m_listGroups.Next(pgroup);
	}

	return ulGExprs;
}
