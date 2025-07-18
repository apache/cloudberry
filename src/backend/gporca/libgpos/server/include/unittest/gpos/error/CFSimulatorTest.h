//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CFSimulatorTest.h
//
//	@doc:
//      Unit tests for CFSimulator
//---------------------------------------------------------------------------
#ifndef GPOS_CFSimulatorTest_H
#define GPOS_CFSimulatorTest_H

#include "gpos/error/CFSimulator.h"

#ifdef GPOS_FPSIMULATOR

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CFSimulatorTest
//
//	@doc:
//		Unittests for f-simulator
//
//---------------------------------------------------------------------------
class CFSimulatorTest
{
public:
	// unittests
	static GPOS_RESULT EresUnittest();
	static GPOS_RESULT EresUnittest_BasicTracking();

};	// CFSimulatorTest
}  // namespace gpos

#endif	// GPOS_FPSIMULATOR

#endif	// !GPOS_CFSimulatorTest_H

// EOF
