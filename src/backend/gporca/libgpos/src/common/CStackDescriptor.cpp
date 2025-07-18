//---------------------------------------------------------------------------
//	Greengage Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CStackDescriptor.cpp
//
//	@doc:
//		Implementation of interface class for execution stack tracing.
//---------------------------------------------------------------------------

#include "gpos/common/CStackDescriptor.h"

#include <execinfo.h>

#include "gpos/string/CWString.h"
#include "gpos/task/IWorker.h"
#include "gpos/utils.h"

#define GPOS_STACK_DESCR_TRACE_BUF (4096)

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::BackTrace
//
//	@doc:
//		Store current stack
//
//---------------------------------------------------------------------------
void
CStackDescriptor::BackTrace(ULONG top_frames_to_skip)
{
	ULONG gpos_stack_trace_depth_actual;
	void *raddrs[GPOS_STACK_TRACE_DEPTH];

	// get the backtrace in platform-independent way
	gpos_stack_trace_depth_actual = backtrace(raddrs, GPOS_STACK_TRACE_DEPTH);

	// reset stack depth
	Reset();

	// skip the first top_frames_to_skip
	for (ULONG i = top_frames_to_skip; i < gpos_stack_trace_depth_actual; i++)
	{
		// backtrace() produces pure return addresses, so just copy them
		m_array_of_addresses[m_depth++] = raddrs[i];
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendSymbolInfo
//
//	@doc:
//		Append formatted symbol description
//
//---------------------------------------------------------------------------
void
CStackDescriptor::AppendSymbolInfo(CWString *ws, CHAR *demangling_symbol_buffer,
								   SIZE_T size, const DL_INFO &symbol_info,
								   ULONG index) const
{
	const CHAR *symbol_name = demangling_symbol_buffer;

	// resolve symbol name
	if (symbol_info.dli_sname)
	{
		INT status = 0;
		symbol_name = symbol_info.dli_sname;

		// demangle C++ symbol
		CHAR *demangled_symbol = clib::Demangle(
			symbol_name, demangling_symbol_buffer, &size, &status);
		GPOS_ASSERT(size <= GPOS_STACK_SYMBOL_SIZE);

		if (0 == status)
		{
			// skip args and template symbol_info
			for (ULONG ul = 0; ul < size; ul++)
			{
				if ('(' == demangling_symbol_buffer[ul] ||
					'<' == demangling_symbol_buffer[ul])
				{
					demangling_symbol_buffer[ul] = '\0';
					break;
				}
			}

			symbol_name = demangled_symbol;
		}
	}
	else
	{
		symbol_name = "<symbol not found>";
	}

	// format symbol symbol_info
	ws->AppendFormat(
		GPOS_WSZ_LIT("%-4d 0x%016lx %s + %lu\n"),
		index + 1,	// frame no.
		(long unsigned int)
			m_array_of_addresses[index],  // current address in frame
		symbol_name,					  // symbol name
		(long unsigned int) m_array_of_addresses[index] -
			(ULONG_PTR) symbol_info.dli_saddr
		// offset from frame start
	);
}


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendTrace
//
//	@doc:
//		Append trace of stored stack to string
//
//---------------------------------------------------------------------------
void
CStackDescriptor::AppendTrace(CWString *ws, ULONG depth) const
{
	GPOS_ASSERT(GPOS_STACK_TRACE_DEPTH >= m_depth &&
				"Stack exceeds maximum depth");

	// symbol symbol_info
	Dl_info symbol_info;


	// buffer for symbol demangling
	CHAR demangling_symbol_buffer[GPOS_STACK_SYMBOL_SIZE];

	// print symbol_info for frames in stack
	for (ULONG i = 0; i < m_depth && i < depth; i++)
	{
		// resolve address
		clib::Dladdr(m_array_of_addresses[i], &symbol_info);

		// get symbol description
		AppendSymbolInfo(ws, demangling_symbol_buffer, GPOS_STACK_SYMBOL_SIZE,
						 symbol_info, i);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::AppendTrace
//
//	@doc:
//		Append trace of stored stack to stream
//
//---------------------------------------------------------------------------
void
CStackDescriptor::AppendTrace(IOstream &os, ULONG depth) const
{
	WCHAR wsz[GPOS_STACK_DESCR_TRACE_BUF];
	CWStringStatic str(wsz, GPOS_ARRAY_SIZE(wsz));

	AppendTrace(&str, depth);
	os << str.GetBuffer();
}


//---------------------------------------------------------------------------
//	@function:
//		CStackDescriptor::HashValue
//
//	@doc:
//		Get hash value for stored stack
//
//---------------------------------------------------------------------------
ULONG
CStackDescriptor::HashValue() const
{
	GPOS_ASSERT(0 < m_depth && "No stack to hash");
	GPOS_ASSERT(GPOS_STACK_TRACE_DEPTH >= m_depth &&
				"Stack exceeds maximum depth");

	return gpos::HashByteArray((BYTE *) m_array_of_addresses,
							   m_depth * GPOS_SIZEOF(m_array_of_addresses[0]));
}

// EOF
