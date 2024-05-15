//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (c) 2023 VMware, Inc. or its affiliates. All Rights Reserved.
//
//	@filename:
//		IHint.h
//---------------------------------------------------------------------------
#ifndef GPOS_IHint_H
#define GPOS_IHint_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

namespace gpopt
{
using namespace gpos;

class IHint : public CRefCount
{
public:
	enum HintStatus
	{
		HINT_STATE_NOTUSED = 0, /* specified relation not used in query */
		HINT_STATE_USED,		/* hint is used */
	};

private:
	HintStatus hint_status{HINT_STATE_NOTUSED};

public:
	void
	SetHintStatus(HintStatus status)
	{
		hint_status = status;
	}

	HintStatus
	GetHintStatus()
	{
		return hint_status;
	}
};


}  // namespace gpopt

#endif	// !GPOS_IHint_H

// EOF
