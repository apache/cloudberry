//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CFSimulatorTestExt.h
//
//	@doc:
//      Extended FS tests
//---------------------------------------------------------------------------
#ifndef GPOS_CFSimulatorTestExt_H
#define GPOS_CFSimulatorTestExt_H

#include "gpos/error/CFSimulator.h"

#ifdef GPOS_FPSIMULATOR

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CFSimulatorTestExt
//
//	@doc:
//		Extended unittests for f-simulator
//
//---------------------------------------------------------------------------
class CFSimulatorTestExt
{
public:
	// unittests
	static GPOS_RESULT EresUnittest();
	static GPOS_RESULT EresUnittest_OOM();
	static GPOS_RESULT EresUnittest_Abort();
	static GPOS_RESULT EresUnittest_IOError();
	static GPOS_RESULT EresUnittest_NetError();

	// simulate exceptions of given type
	static GPOS_RESULT EresUnittest_SimulateException(ULONG major, ULONG minor);

};	// CFSimulatorTestExt
}  // namespace gpos

#endif	// GPOS_FPSIMULATOR

#endif	// !GPOS_CFSimulatorTestExt_H

// EOF
