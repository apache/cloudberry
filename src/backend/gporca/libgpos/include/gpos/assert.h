//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		assert.h
//
//	@doc:
//		Macros for assertions in gpos
//
//		Use '&& "explanation"' in assert condition to provide additional
//		information about the failed condition.
//
//			GPOS_ASSERT(!FSpinlockHeld() && "Must not hold spinlock during allcoation");
//
//		There is no GPOS_ASSERT_STOP macro, instead use
//
//			GPOS_ASSERT(!"Should not get here because of xyz");
//
//		Avoid using GPOS_ASSERT(false);
//---------------------------------------------------------------------------
#ifndef GPOS_assert_H
#define GPOS_assert_H

#ifndef USE_CMAKE
#include "pg_config.h"
#endif

#ifdef UNITTEST_debug_gpos_assert
#define debug_gpos_assert(file, line, msg) ((void)0)
#else
extern void debug_gpos_assert(const char *file, long line, const char *m);
#endif
#define GPOS_C_LIT(x) #x
// retail assert; available in all builds
#define GPOS_RTL_ASSERT(x)                                                 \
	((x) ? ((void) 0)                                                      \
		 : gpos::CException::Raise(__FILE__, __LINE__,                     \
								   gpos::CException::ExmaSystem,           \
								   gpos::CException::ExmiAssert, __FILE__, \
								   __LINE__, GPOS_WSZ_LIT(#x)))

#define GPOS_RTL_ASSERT_DEV(x)                                             \
	((x) ? ((void) 0)                                                      \
		 : (debug_gpos_assert(__FILE__, __LINE__, GPOS_C_LIT(x))           \
		 , gpos::CException::Raise(__FILE__, __LINE__,                     \
								   gpos::CException::ExmaSystem,           \
								   gpos::CException::ExmiAssert, __FILE__, \
								   __LINE__, GPOS_WSZ_LIT(#x))))

#ifdef GPOS_DEBUG
// standard debug assert; maps to retail assert in debug builds only
#define GPOS_ASSERT(x) GPOS_RTL_ASSERT_DEV(x)
#else
#define GPOS_ASSERT(x) ;
#endif	// !GPOS_DEBUG
// Assertion failure in GPOS_ASSERT() has different behavior between debug
// and release version. Some assertion failure in release will continue to
// run unexpectly. We don't know why these assertions fail, so replace these
// GPOS_ASSERT() by GPOS_ASSERT_FIXME(). GPOS_ASSERT_FIXME has the same
// semantics, but helpful to find the failed asserts for us.
#define GPOS_ASSERT_FIXME(x)  GPOS_RTL_ASSERT(x)

#define GPOS_UNITTEST_ASSERT GPOS_RTL_ASSERT

// implication assert
#define GPOS_ASSERT_IMP(x, y) GPOS_ASSERT(!(x) || (y))

#define GPOS_UNITTEST_ASSERT_IMP(x, y) GPOS_RTL_ASSERT(!(x) || (y))

// if-and-only-if assert
#define GPOS_ASSERT_IFF(x, y) GPOS_ASSERT((!(x) || (y)) && (!(y) || (x)))

#define GPOS_UNITTEST_ASSERT_IFF(x, y) \
	GPOS_RTL_ASSERT((!(x) || (y)) && (!(y) || (x)))

// compile assert
#define GPOS_CPL_ASSERT static_assert

// debug assert, with message
#define GPOS_ASSERT_MSG(x, msg) GPOS_ASSERT((x) && (msg))

// retail assert, with message
#define GPOS_RTL_ASSERT_MSG(x, msg) GPOS_RTL_ASSERT((x) && (msg))

// unittest assert, with message
#define GPOS_UNITTEST_ASSERT_MSG(x, msg) GPOS_RTL_ASSERT((x) && (msg))


#endif	// !GPOS_assert_H

// EOF
