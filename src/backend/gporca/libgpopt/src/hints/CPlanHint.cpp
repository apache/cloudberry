//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2023 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CPlanHint.cpp
//
//	@doc:
//		Container of plan hint objects
//---------------------------------------------------------------------------

#include "gpopt/hints/CPlanHint.h"

#include "gpos/base.h"

#include "gpopt/hints/CHintUtils.h"

using namespace gpopt;

FORCE_GENERATE_DBGSTR(CPlanHint);

//---------------------------------------------------------------------------
//	@function:
//		CPlanHint::CPlanHint
//---------------------------------------------------------------------------
CPlanHint::CPlanHint(CMemoryPool *mp)
	: m_mp(mp),
	  m_scan_hints(GPOS_NEW(mp) ScanHintList(mp)),
	  m_row_hints(GPOS_NEW(mp) RowHintList(mp)),
	  m_join_hints(GPOS_NEW(mp) JoinHintList(mp)),
	  m_join_type_hints(GPOS_NEW(mp) JoinTypeHintList(mp))
{
}

CPlanHint::~CPlanHint()
{
	m_scan_hints->Release();
	m_row_hints->Release();
	m_join_hints->Release();
	m_join_type_hints->Release();
}

void
CPlanHint::AddHint(CScanHint *hint)
{
	m_scan_hints->Append(hint);
}

void
CPlanHint::AddHint(CRowHint *hint)
{
	m_row_hints->Append(hint);
}

void
CPlanHint::AddHint(CJoinHint *hint)
{
	m_join_hints->Append(hint);
}

void
CPlanHint::AddHint(CJoinTypeHint *hint)
{
	m_join_type_hints->Append(hint);
}

CScanHint *
CPlanHint::GetScanHint(const char *relname)
{
	CWStringConst *name = GPOS_NEW(m_mp) CWStringConst(m_mp, relname);
	CScanHint *hint = GetScanHint(name);
	GPOS_DELETE(name);
	return hint;
}

CScanHint *
CPlanHint::GetScanHint(const CWStringBase *name)
{
	for (ULONG ul = 0; ul < m_scan_hints->Size(); ul++)
	{
		CScanHint *hint = (*m_scan_hints)[ul];
		if (name->Equals(hint->GetName()))
		{
			return hint;
		}
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPlanHint::GetRowHint
//
//	@doc:
//		Given a set of table descriptors, find a matching CRowHint.  A match
//		means that the table alias names used to describe the CRowHint equals
//		the set of table alias names provided by the given set of table
//		descriptors.
//
//---------------------------------------------------------------------------
CRowHint *
CPlanHint::GetRowHint(CTableDescriptorHashSet *ptabdescs)
{
	if (ptabdescs->Size() == 0)
	{
		return nullptr;
	}

	StringPtrArray *aliases = GPOS_NEW(m_mp) StringPtrArray(m_mp);
	CTableDescriptorHashSetIter hsiter(ptabdescs);

	while (hsiter.Advance())
	{
		const CTableDescriptor *ptabdesc = hsiter.Get();
		aliases->Append(GPOS_NEW(m_mp) CWStringConst(
			m_mp, ptabdesc->Name().Pstr()->GetBuffer()));
	}
	// Row hint aliases are sorted because the hint is order agnostic.
	aliases->Sort(CWStringBase::Compare);

	CRowHint *matching_hint = nullptr;
	for (ULONG ul = 0; ul < m_row_hints->Size(); ul++)
	{
		CRowHint *hint = (*m_row_hints)[ul];
		if (aliases->Equals(hint->GetAliasNames()))
		{
			matching_hint = hint;
			matching_hint->SetHintStatus(IHint::HINT_STATE_USED);
			break;
		}
	}
	aliases->Release();
	return matching_hint;
}

//---------------------------------------------------------------------------
//	@function:
//		GetHintSubtrees
//
//	@doc:
//		Given a JoinNode, append into "subtrees" a list of all possible
//		subtrees and return the current subtree.
//
//		For example, if JoinNode is ((T1 T2) T3)
//
//		Then the subtrees will be [["T1"], ["T2"], ["T1", "T2"], ["T3"],
//		["T1", "T2", "T3"]]
//
//---------------------------------------------------------------------------
static StringPtrArray *
GetHintSubtrees(CMemoryPool *mp, const CJoinHint::JoinNode *node,
				StringPtr2dArray *subtrees)
{
	if (node->GetName())
	{
		StringPtrArray *subtree = GPOS_NEW(mp) StringPtrArray(mp);
		subtree->Append(GPOS_NEW(mp)
							CWStringConst(mp, node->GetName()->GetBuffer()));
		subtrees->Append(subtree);
		return subtree;
	}

	StringPtrArray *subtree = GPOS_NEW(mp) StringPtrArray(mp);
	StringPtrArray *outer = GetHintSubtrees(mp, node->GetOuter(), subtrees);

	// Note that "subtree" array conains hard-copies of elements because
	// CWStringConst is not a refcounted object. The elements are destroyed
	// with the array; therefore references cannot be shared between arrays.
	for (ULONG ul = 0; ul < outer->Size(); ul++)
	{
		subtree->Append(GPOS_NEW(mp)
							CWStringConst(mp, (*outer)[ul]->GetBuffer()));
	}
	StringPtrArray *inner = GetHintSubtrees(mp, node->GetInner(), subtrees);
	for (ULONG ul = 0; ul < inner->Size(); ul++)
	{
		subtree->Append(GPOS_NEW(mp)
							CWStringConst(mp, (*inner)[ul]->GetBuffer()));
	}
	subtrees->Append(subtree);
	return subtree;
}

//---------------------------------------------------------------------------
//	@function:
//		IsAliasHintSubtreeOrDisjoint
//
//	@doc:
//		Given a list of all possible hint subtrees and a list of child aliases
//		return whether the child aliases set either matches one of the subtrees
//		or is disjoint from all the subtrees.
//
//		For example, hint is: ((T1 T2) T3)
//
//		And subtrees is [["T1"], ["T2"], ["T1", "T2"], ["T3"], ["T1", "T2", "T3"]]
//
//		Then return true if childaliases is:
//
//		"T1", "T2", "T1 T2", "T3", or "T1 T2 T3" or for example: "U1", "U1 U2"
//
//		Return false if child alias is, for example: "T1 U1" or "T1 T2 U1"
//
//---------------------------------------------------------------------------
static bool
IsAliasHintSubtreeOrDisjoint(StringPtr2dArray *subtrees,
							 StringPtrArray *childaliases)
{
	if (0 == childaliases->Size())
	{
		return true;
	}

	bool is_hint_in_child = false;

	for (ULONG i = 0; i < subtrees->Size(); i++)
	{
		StringPtrArray *subtree = (*subtrees)[i];

		ULONG contained = 0;
		for (ULONG j = 0; j < subtree->Size(); j++)
		{
			if (childaliases->Find((*subtree)[j]))
			{
				contained += 1;
				is_hint_in_child = true;
			}
		}
		if ((contained == childaliases->Size() && subtree->Size() == contained))
		{
			return true;
		}
	}

	return !is_hint_in_child;
}

//---------------------------------------------------------------------------
//	@function:
//		CPlanHint::GetJoinHint
//
//	@doc:
//		Given a join expression, find a matching hint. A match means that every
//		alias in the hint is covered in the expression and at least one alias
//		is a direct child of the join. A direct child means the child has only
//		one relation.
//
//		For example, let's say the query/hint is:
//
//		  /*+
//		  Leading((t1 (t2 t3)))
//		  */
//		  EXPLAIN (costs off) SELECT * FROM t1, (SELECT * FROM t2, t3 LIMIT 42) AS q;
//
//		And the preprocessed expression is:
//
//		Algebrized preprocessed query:
//		+--CLogicalNAryJoin
//		   |--CLogicalGet "t1" ("t1"), Columns: ["a" (0), "b" (1),...
//		   |--CLogicalLimit <empty> global
//		   |  |--CLogicalNAryJoin
//		   |  |  |--CLogicalGet "t2" ("t2"), Columns: ["a" (9), "b" (10),...
//		   |  |  |--CLogicalGet "t3" ("t3"), Columns: ["a" (18), "b" (19),..
//		   |  |  ...
//		   |  ...
//		   ...
//
//		In this case, we want to apply the hint on the top level NAry join and
//		recursively ask the lower level Nary join to apply the hint
//		Leading((t2 t3)).
//
//		However, if the query/hint were instead:
//
//		  /*+
//		  Leading((t2 t3))
//		  */
//		  EXPLAIN (costs off) SELECT * FROM t1, (SELECT * FROM t2, t3 LIMIT 42) AS q;
//
//		Then we would not want to apply the hint on the top level Nary join
//		because neither "t2" nor "t3" is a direct child of the top level Nary
//		join.
//
//---------------------------------------------------------------------------
CJoinHint *
CPlanHint::GetJoinHint(CExpression *pexpr)
{
	if (COperator::EopLogicalNAryJoin != pexpr->Pop()->Eopid() &&
		COperator::EopLogicalInnerJoin != pexpr->Pop()->Eopid())
	{
		return nullptr;
	}

	CTableDescriptorHashSet *ptabdesc = pexpr->DeriveTableDescriptor();
	StringPtrArray *pexprAliases =
		CHintUtils::GetAliasesFromTableDescriptors(m_mp, ptabdesc);

	// If every hint alias is contained in the expression's table descriptor
	// set, and at least one hint alias is a direct child  in the expression
	// then the hint is returned.
	for (ULONG ul = 0; ul < m_join_hints->Size(); ul++)
	{
		CJoinHint *hint = (*m_join_hints)[ul];

		StringPtrArray *hintAliases =
			CHintUtils::GetAliasesFromHint(m_mp, hint->GetJoinNode());

		bool is_contained = true;
		for (ULONG j = 0; j < hintAliases->Size(); j++)
		{
			if (nullptr == pexprAliases->Find((*hintAliases)[j]))
			{
				is_contained = false;
				break;
			}
		}
		if (!is_contained)
		{
			hintAliases->Release();
			continue;
		}

		StringPtr2dArray *hint_subtrees = GPOS_NEW(m_mp) StringPtr2dArray(m_mp);
		GetHintSubtrees(m_mp, hint->GetJoinNode(), hint_subtrees);

		bool has_direct_child = false;
		bool is_hint_valid_on_grandchildren = true;
		for (ULONG i = 0; i < pexpr->Arity(); i++)
		{
			CTableDescriptorHashSet *childtabs =
				(*pexpr)[i]->DeriveTableDescriptor();

			// is a direct child and a hint
			if (childtabs->Size() == 1 &&
				nullptr != hintAliases->Find(childtabs->First()->Name().Pstr()))
			{
				has_direct_child = true;
				continue;
			}

			// Hint must align with valid nary join constraints.
			//
			// For example,
			//
			//		+--CLogicalNAryJoin
			//		   |--CLogicalGet "t1" ("t1"), Columns: ["a" (0), "b" (1),...
			//		   |--CLogicalLimit <empty> global
			//		   |  |--CLogicalNAryJoin
			//		   |  |  |--CLogicalGet "t2" ("t2"), Columns: ["a" (9), "b" (10),...
			//		   |  |  |--CLogicalGet "t3" ("t3"), Columns: ["a" (18), "b" (19),..
			//		   |  |  ...
			//		   |  ...
			//		   ...
			//
			// If the hint is (t1 t2), then it is invalid because no valid join
			// combination directly joins t1 and t2. Likewise, any hint that
			// builds on an invalid hint, for example ((t1 t2) t3), is also
			// invalid for same reason.
			StringPtrArray *childaliases =
				CHintUtils::GetAliasesFromTableDescriptors(m_mp, childtabs);
			if (childtabs->Size() > 1 &&
				!IsAliasHintSubtreeOrDisjoint(hint_subtrees, childaliases))
			{
				childaliases->Release();
				is_hint_valid_on_grandchildren = false;
				break;
			}
			childaliases->Release();
		}

		hintAliases->Release();
		hint_subtrees->Release();

		if (has_direct_child && is_hint_valid_on_grandchildren)
		{
			// We found a matching hint, return it
			hint->SetHintStatus(IHint::HINT_STATE_USED);
			pexprAliases->Release();
			return hint;
		}
	}
	pexprAliases->Release();
	return nullptr;
}


//---------------------------------------------------------------------------
//	@function:
//		CPlanHint::GetJoinHint
//
//	@doc:
//		Given a list of aliases, find a matching CJoinHint. A match means that
//		the hint contains every alias in the aliases list.
//
//---------------------------------------------------------------------------
CJoinTypeHint *
CPlanHint::GetJoinTypeHint(StringPtrArray *aliases)
{
	aliases->Sort(CWStringBase::Compare);

	for (ULONG i = 0; i < m_join_type_hints->Size(); i++)
	{
		CJoinTypeHint *hint = (*m_join_type_hints)[i];

		if (aliases->Equals(hint->GetAliasNames()))
		{
			return hint;
		}
	}
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPlanHint::WasCreatedViaDirectedHint
//
//	@doc:
//		Check whether an expression was created using a directed hint.
//
//---------------------------------------------------------------------------
bool
CPlanHint::WasCreatedViaDirectedHint(CExpression *pexpr)
{
	GPOS_ASSERT(COperator::EopLogicalInnerJoin == pexpr->Pop()->Eopid() ||
				COperator::EopLogicalLeftOuterJoin == pexpr->Pop()->Eopid() ||
				COperator::EopLogicalRightOuterJoin == pexpr->Pop()->Eopid());

	CTableDescriptorHashSet *ptabdesc = pexpr->DeriveTableDescriptor();
	StringPtrArray *pexprAliases =
		CHintUtils::GetAliasesFromTableDescriptors(m_mp, ptabdesc);

	bool has_join_with_direction = false;

	for (ULONG ul = 0; ul < m_join_hints->Size(); ul++)
	{
		CJoinHint *hint = (*m_join_hints)[ul];

		// skip directed-less hints
		if (!hint->GetJoinNode()->IsDirected())
		{
			continue;
		}

		// skip hints not used
		if (IHint::HINT_STATE_USED != hint->GetHintStatus())
		{
			continue;
		}

		StringPtrArray *hintAliases =
			CHintUtils::GetAliasesFromHint(m_mp, hint->GetJoinNode());

		// If every alias in pexpr exists in the hint, then the hint is
		// contained in pexpr.
		bool is_contained = true;
		for (ULONG j = 0; j < pexprAliases->Size(); j++)
		{
			if (nullptr == hintAliases->Find((*pexprAliases)[j]))
			{
				is_contained = false;
				break;
			}
		}
		hintAliases->Release();

		if (is_contained)
		{
			has_join_with_direction = true;
			break;
		}
	}

	pexprAliases->Release();
	return has_join_with_direction;
}

IOstream &
CPlanHint::OsPrint(IOstream &os) const
{
	os << "PlanHint: [";
	if (nullptr == m_scan_hints)
	{
		os << "]";
		return os;
	}

	os << "\n";
	os << "used hint:";
	os << "\n";
	for (ULONG ul = 0; ul < m_scan_hints->Size(); ul++)
	{
		if ((*m_scan_hints)[ul]->GetHintStatus() == IHint::HINT_STATE_USED)
		{
			(*m_scan_hints)[ul]->OsPrint(os) << "\n";
		}
	}

	for (ULONG ul = 0; ul < m_row_hints->Size(); ul++)
	{
		if ((*m_row_hints)[ul]->GetHintStatus() == IHint::HINT_STATE_USED)
		{
			(*m_row_hints)[ul]->OsPrint(os) << "\n";
		}
	}

	for (ULONG ul = 0; ul < m_join_hints->Size(); ul++)
	{
		if ((*m_join_hints)[ul]->GetHintStatus() == IHint::HINT_STATE_USED)
		{
			(*m_join_hints)[ul]->OsPrint(os) << "\n";
		}
	}

	for (ULONG ul = 0; ul < m_join_type_hints->Size(); ul++)
	{
		if ((*m_join_type_hints)[ul]->GetHintStatus() == IHint::HINT_STATE_USED)
		{
			(*m_join_type_hints)[ul]->OsPrint(os) << "\n";
		}
	}

	os << "not used hint:";
	os << "\n";
	for (ULONG ul = 0; ul < m_scan_hints->Size(); ul++)
	{
		if ((*m_scan_hints)[ul]->GetHintStatus() == IHint::HINT_STATE_NOTUSED)
		{
			(*m_scan_hints)[ul]->OsPrint(os) << "\n";
		}
	}

	for (ULONG ul = 0; ul < m_row_hints->Size(); ul++)
	{
		if ((*m_row_hints)[ul]->GetHintStatus() == IHint::HINT_STATE_NOTUSED)
		{
			(*m_row_hints)[ul]->OsPrint(os) << "\n";
		}
	}

	for (ULONG ul = 0; ul < m_join_hints->Size(); ul++)
	{
		if ((*m_join_hints)[ul]->GetHintStatus() == IHint::HINT_STATE_NOTUSED)
		{
			(*m_join_hints)[ul]->OsPrint(os) << "\n";
		}
	}

	for (ULONG ul = 0; ul < m_join_type_hints->Size(); ul++)
	{
		if ((*m_join_type_hints)[ul]->GetHintStatus() ==
			IHint::HINT_STATE_NOTUSED)
		{
			(*m_join_type_hints)[ul]->OsPrint(os) << "\n";
		}
	}
	os << "]";
	return os;
}

void
CPlanHint::Serialize(CXMLSerializer *xml_serializer) const
{
	for (ULONG ul = 0; ul < m_scan_hints->Size(); ul++)
	{
		(*m_scan_hints)[ul]->Serialize(xml_serializer);
	}

	for (ULONG ul = 0; ul < m_row_hints->Size(); ul++)
	{
		(*m_row_hints)[ul]->Serialize(xml_serializer);
	}

	for (ULONG ul = 0; ul < m_join_hints->Size(); ul++)
	{
		(*m_join_hints)[ul]->Serialize(xml_serializer);
	}

	for (ULONG ul = 0; ul < m_join_type_hints->Size(); ul++)
	{
		(*m_join_type_hints)[ul]->Serialize(xml_serializer);
	}
}


// EOF
