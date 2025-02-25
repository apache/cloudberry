//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2024 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		CJoinTypeHint.h
//
//	@doc:
//		CJoinTypeHint represents an ORCA optimizer hint that specifies a type
//		to join on a set of table(s) and/or alias(es).
//
//	Example
//		/*+ HashJoin(t1 t2) */ SELECT * FROM t1, t2;
//
//		DXL format:
//			<dxl:JoinTypeHint Alias="t1,t2" JoinType="HashJoin"/>
//---------------------------------------------------------------------------
#ifndef GPOS_CJoinTypeHint_H
#define GPOS_CJoinTypeHint_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/exception.h"
#include "gpopt/hints/IHint.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/COperator.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

namespace gpopt
{
class CJoinTypeHint : public IHint, public DbgPrintMixin<CJoinTypeHint>
{
public:
	enum JoinType
	{
		HINT_KEYWORD_NESTLOOP,
		HINT_KEYWORD_MERGEJOIN,
		HINT_KEYWORD_HASHJOIN,
		HINT_KEYWORD_NONESTLOOP,
		HINT_KEYWORD_NOMERGEJOIN,
		HINT_KEYWORD_NOHASHJOIN,

		SENTINEL
	};

private:
	CMemoryPool *m_mp;

	JoinType m_type{SENTINEL};

	// sorted list of alias names.
	StringPtrArray *m_aliases{nullptr};

public:
	CJoinTypeHint(CMemoryPool *mp, enum JoinType type, StringPtrArray *aliases)
		: m_mp(mp), m_type(type), m_aliases(aliases)
	{
		m_aliases->Sort(CWStringBase::Compare);
	}

	~CJoinTypeHint() override
	{
		CRefCount::SafeRelease(m_aliases);
	}

	const StringPtrArray *GetAliasNames() const;

	BOOL SatisfiesOperator(COperator *op);

	IOstream &OsPrint(IOstream &os) const;

	void Serialize(CXMLSerializer *xml_serializer);
};

using JoinTypeHintList = CDynamicPtrArray<CJoinTypeHint, CleanupRelease>;

}  // namespace gpopt

#endif	// !GPOS_CJoinTypeHint_H

// EOF
