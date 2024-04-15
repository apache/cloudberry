//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (c) 2024 VMware, Inc. or its affiliates
//---------------------------------------------------------------------------
#ifndef GPOS_set_H
#define GPOS_set_H

#include <set>

#include "gpos/memory/MemoryPoolAllocator.h"

namespace gpos
{
template <class Key, class Compare = std::less<Key>>
using set = std::set<Key, Compare, MemoryPoolAllocator<Key>>;
}
#endif	//GPOS_set_H
