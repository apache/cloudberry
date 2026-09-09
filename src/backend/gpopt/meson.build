# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership.  The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

#
# The translator layer between the GPDB planner and ORCA. Its sources include
# backend headers inside extern "C", including generated ones (fmgroids.h,
# catalog/pg_*), so the generated headers must be an explicit dependency.

gpopt_sources = files(
  'CGPOptimizer.cpp',
  'config/CConfigParamMapping.cpp',
  'gpdbwrappers.cpp',
  'relcache/CMDProviderRelcache.cpp',
  'translate/CCTEListEntry.cpp',
  'translate/CContextDXLToPlStmt.cpp',
  'translate/CContextQueryToDXL.cpp',
  'translate/CDXLTranslateContext.cpp',
  'translate/CDXLTranslateContextBaseTable.cpp',
  'translate/CMappingColIdVar.cpp',
  'translate/CMappingColIdVarPlStmt.cpp',
  'translate/CMappingElementColIdParamId.cpp',
  'translate/CMappingVarColId.cpp',
  'translate/CPartPruneStepsBuilder.cpp',
  'translate/CQueryMutators.cpp',
  'translate/CTranslatorDXLToPlStmt.cpp',
  'translate/CTranslatorDXLToScalar.cpp',
  'translate/CTranslatorQueryToDXL.cpp',
  'translate/CTranslatorRelcacheToDXL.cpp',
  'translate/CTranslatorScalarToDXL.cpp',
  'translate/CTranslatorUtils.cpp',
  'utils/CConstExprEvaluatorProxy.cpp',
  'utils/CMemoryPoolPalloc.cpp',
  'utils/CMemoryPoolPallocManager.cpp',
  'utils/COptTasks.cpp',
  'utils/RelationWrapper.cpp',
  'utils/funcs.cpp',
)

gpopt = static_library('gpopt',
  gpopt_sources,
  include_directories: [orca_inc, postgres_inc],
  dependencies: [backend_code, xerces],
  cpp_args: ['-fno-omit-frame-pointer'],
  override_options: ['cpp_std=c++14'],
  kwargs: internal_lib_args,
)
backend_link_with += gpopt
