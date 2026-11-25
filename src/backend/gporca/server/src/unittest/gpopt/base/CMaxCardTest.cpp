//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2012 EMC Corp.
//
//	@filename:
//		CMaxCardTest.cpp
//
//	@doc:
//		Tests for max card computation
//---------------------------------------------------------------------------

#include "unittest/gpopt/base/CMaxCardTest.h"

#include "gpos/base.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/base/CMaxCard.h"
#include "gpopt/operators/CLogicalLimit.h"

#include "unittest/gpopt/CTestUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMaxCardTest::EresUnittest
//
//	@doc:
//		Unittest for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMaxCardTest::EresUnittest()
{
	CUnittest rgut[] = {
		GPOS_UNITTEST_FUNC(CMaxCardTest::EresUnittest_Basics),
		GPOS_UNITTEST_FUNC(CMaxCardTest::EresUnittest_LimitCardinality),
		GPOS_UNITTEST_FUNC(CMaxCardTest::EresUnittest_RunMinidumpTests)};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CMaxCardTest::EresUnittest_Basics
//
//	@doc:
//		Basic test for key collections
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMaxCardTest::EresUnittest_Basics()
{
#ifdef GPOS_DEBUG

	CMaxCard mcOne(1);
	CMaxCard mcTwo(1);
	GPOS_UNITTEST_ASSERT(mcOne == mcTwo);

	CMaxCard mcThree;
	GPOS_UNITTEST_ASSERT(!(mcOne == mcThree));

	CMaxCard mcFour(0);
	mcFour *= mcThree;
	GPOS_UNITTEST_ASSERT(0 == mcFour);

	mcFour += mcOne;
	GPOS_UNITTEST_ASSERT(1 == mcFour);

	mcFour *= mcThree;
	GPOS_UNITTEST_ASSERT(GPOPT_MAX_CARD == mcFour);

	mcFour += mcThree;
	GPOS_UNITTEST_ASSERT(GPOPT_MAX_CARD == mcFour);

#endif

	return GPOS_OK;
}

GPOS_RESULT
CMaxCardTest::EresUnittest_LimitCardinality()
{
	CTestUtils::CTestSetup test_setup;
	CMemoryPool *mp = test_setup.Pmp();

	CExpression *pexprInnerLimit = CTestUtils::PexprLogicalLimit(
		mp, CTestUtils::PexprLogicalGet(mp), 0, 7, true, true);
	CExpression *pexprOuterLimit = CTestUtils::PexprLogicalLimit(
		mp, pexprInnerLimit, 0, 4294967296LL, true, true);

	GPOS_UNITTEST_ASSERT(7 == pexprOuterLimit->DeriveMaxCard().Ull());
	pexprOuterLimit->Release();

	CExpression *pexprInnerLimitWithoutCount = CTestUtils::PexprLogicalLimit(
		mp, CTestUtils::PexprLogicalGet(mp), 0, 7, true, true);
	CExpression *pexprLimitWithoutCount = CTestUtils::PexprLogicalLimit(
		mp, pexprInnerLimitWithoutCount, 0, 0, true, false);

	GPOS_UNITTEST_ASSERT(7 == pexprLimitWithoutCount->DeriveMaxCard().Ull());
	pexprLimitWithoutCount->Release();

	CExpression *pexprInnerLimitNegative = CTestUtils::PexprLogicalLimit(
		mp, CTestUtils::PexprLogicalGet(mp), 0, 7, true, true);
	COrderSpec *pos = GPOS_NEW(mp) COrderSpec(mp);
	CExpression *pexprNegativeLimit = GPOS_NEW(mp) CExpression(
		mp,
		GPOS_NEW(mp) CLogicalLimit(mp, pos, true, true, false),
		pexprInnerLimitNegative, CUtils::PexprScalarConstInt8(mp, 0),
		CUtils::PexprScalarConstInt8(mp, -1));

	GPOS_UNITTEST_ASSERT(7 == pexprNegativeLimit->DeriveMaxCard().Ull());
	pexprNegativeLimit->Release();

	CExpression *pexprInnerLimitNull = CTestUtils::PexprLogicalLimit(
		mp, CTestUtils::PexprLogicalGet(mp), 0, 7, true, true);
	COrderSpec *posNull = GPOS_NEW(mp) COrderSpec(mp);
	CExpression *pexprNullLimit = GPOS_NEW(mp) CExpression(
		mp, GPOS_NEW(mp) CLogicalLimit(mp, posNull, true, true, false),
		pexprInnerLimitNull, CUtils::PexprScalarConstInt8(mp, 0),
		CUtils::PexprScalarConstInt8(mp, 0, true));

	GPOS_UNITTEST_ASSERT(7 == pexprNullLimit->DeriveMaxCard().Ull());
	pexprNullLimit->Release();

	return GPOS_OK;
}

GPOS_RESULT
CMaxCardTest::EresUnittest_RunMinidumpTests()
{
	ULONG ulTestCounter = 0;
	// minidump files
	const CHAR *rgszFileNames[] = {
		"../data/dxl/minidump/FullOuterJoinMaxCardRightChild.mdp",
		"../data/dxl/minidump/FullOuterJoinMaxCardLeftChild.mdp",
		"../data/dxl/minidump/FullOuterJoinZeroMaxCard.mdp",
		"../data/dxl/minidump/FullOuterJoinLeftMultiplyRightMaxCard.mdp",
	};

	return CTestUtils::EresUnittest_RunTests(rgszFileNames, &ulTestCounter,
											 GPOS_ARRAY_SIZE(rgszFileNames));
}

// EOF
