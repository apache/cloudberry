#ifndef GPOS_attributes_H
#define GPOS_attributes_H

#ifdef USE_CMAKE
#define GPOS_FORMAT_ARCHETYPE printf
#else
#include "pg_config.h"
#define GPOS_FORMAT_ARCHETYPE PG_PRINTF_ATTRIBUTE
#endif

#define GPOS_ATTRIBUTE_PRINTF(f, a) \
	__attribute__((format(GPOS_FORMAT_ARCHETYPE, f, a)))

#ifdef __GNUC__
#define GPOS_UNUSED __attribute__((unused))
#else
#define GPOS_UNUSED
#endif

#ifndef GPOS_DEBUG
#define GPOS_ASSERTS_ONLY GPOS_UNUSED
#else
#define GPOS_ASSERTS_ONLY
#endif

#ifdef GPOS_DEBUG
// mark that the function can throw an exception
#define GPOS_NOEXCEPT noexcept(false)
#else
#define GPOS_NOEXCEPT noexcept
#endif

#endif	// !GPOS_attributes_H
