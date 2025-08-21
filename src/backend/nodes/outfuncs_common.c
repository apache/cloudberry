/*-------------------------------------------------------------------------
 *
 * outfuncs_common.c
 *	  Common serialization functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 *
 *
 * NOTES
 *    This file contains all common serialization functions for both
 *    binary and text representations, and serialization functions
 *    that are Cloudberry-specific for text representation.
 *
 *    All text-representation only functions are guarded by the macro
 *    `COMPILING_BINARY_FUNCS`, they appear first in this file.
 *
 *    This C source file SHOULD not be compiled alone, it MUST be
 *    only be included by outfuncs.c, so we intended to not complete
 *    the header files.
 *
 *    When you consider adding new serialization functions, you should
 *    folow these rules:
 *    1. Do not add any functions(Cloudberry-specific) to outfuncs.c
 *    2. Add functions to outfuncs_common.c that can be used for both
 *       binary and text representations.
 *    3. Add functions to outfast.c if these functions can only be used
 *       for binary representation.
 *    4. Add functions to outfuncs_common.c if these functions can only
 *       be used for text representation.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMPILING_BINARY_FUNCS
/* 'flow' is only needed during planning. */
static void
_outFlow(StringInfo str, const Flow *node)
{
	WRITE_NODE_TYPE("FLOW");

	WRITE_ENUM_FIELD(flotype, FlowType);
	WRITE_ENUM_FIELD(locustype, CdbLocusType);
	WRITE_INT_FIELD(segindex);
	WRITE_INT_FIELD(numsegments);
}

static void
_outCdbPathLocus(StringInfo str, const CdbPathLocus *node)
{
	WRITE_ENUM_FIELD(locustype, CdbLocusType);
	WRITE_NODE_FIELD(distkey);
	WRITE_INT_FIELD(numsegments);
}

static void
_outTableFunctionScanPath(StringInfo str, const TableFunctionScanPath *node)
{
	WRITE_NODE_TYPE("TABLEFUNCTIONSCANPATH");

	_outPathInfo(str, (const Path *) node);

	WRITE_NODE_FIELD(subpath);
}

static void
_outAppendOnlyPath(StringInfo str, const AppendOnlyPath *node)
{
	WRITE_NODE_TYPE("APPENDONLYPATH");

	_outPathInfo(str, (Path *) node);
}

static void
_outAOCSPath(StringInfo str, const AOCSPath *node)
{
	WRITE_NODE_TYPE("APPENDONLYPATH");

	_outPathInfo(str, (Path *) node);
}

static void
_outCdbMotionPath(StringInfo str, const CdbMotionPath *node)
{
    WRITE_NODE_TYPE("MOTIONPATH");

    _outPathInfo(str, &node->path);

    WRITE_NODE_FIELD(subpath);
}

static void
_outDistributionKey(StringInfo str, const DistributionKey *node)
{
	WRITE_NODE_TYPE("DISTRIBUTIONKEY");

	WRITE_NODE_FIELD(dk_eclasses);
	WRITE_OID_FIELD(dk_opfamily);
}

static void
_outNull(StringInfo str, const Node *n pg_attribute_unused())
{
	WRITE_NODE_TYPE("NULL");
}

#endif /* COMPILING_BINARY_FUNCS */

static void
_outGpPartDefElem(StringInfo str, const GpPartDefElem *node)
{
	WRITE_NODE_TYPE("GPPARTDEFELEM");

	WRITE_STRING_FIELD(partName);
	WRITE_NODE_FIELD(boundSpec);
	WRITE_NODE_FIELD(subSpec);
	WRITE_BOOL_FIELD(isDefault);
	WRITE_NODE_FIELD(options);
	WRITE_STRING_FIELD(accessMethod);
	WRITE_STRING_FIELD(tablespacename);
	WRITE_NODE_FIELD(colencs);
}

static void
_outGpPartitionRangeSpec(StringInfo str, const GpPartitionRangeSpec *node)
{
	WRITE_NODE_TYPE("GPPARTITIONRANGESPEC");

	WRITE_NODE_FIELD(partStart);
	WRITE_NODE_FIELD(partEnd);
	WRITE_NODE_FIELD(partEvery);
}

static void
_outGpPartitionRangeItem(StringInfo str, const GpPartitionRangeItem *node)
{
	WRITE_NODE_TYPE("GPPARTITIONRANGEITEM");

	WRITE_NODE_FIELD(val);
	WRITE_ENUM_FIELD(edge, GpPartitionEdgeBounding);
}

static void
_outGpPartitionListSpec(StringInfo str, const GpPartitionListSpec *node)
{
	WRITE_NODE_TYPE("GPPARTITIONLISTSPEC");

	WRITE_NODE_FIELD(partValues);
}

static void
_outGpPartitionDefinition(StringInfo str, const GpPartitionDefinition *node)
{
	WRITE_NODE_TYPE("GPPARTITIONDEFINITION");

	WRITE_NODE_FIELD(partDefElems);
	WRITE_NODE_FIELD(encClauses);
	WRITE_BOOL_FIELD(isTemplate);
}

static void
_outAlterQueueStmt(StringInfo str, const AlterQueueStmt *node)
{
	WRITE_NODE_TYPE("ALTERQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outAlterResourceGroupStmt(StringInfo str, const AlterResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("ALTERRESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outCreateDomainStmt(StringInfo str, const CreateDomainStmt *node)
{
	WRITE_NODE_TYPE("CREATEDOMAINSTMT");
	WRITE_NODE_FIELD(domainname);
	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(collClause);
	WRITE_NODE_FIELD(constraints);
}

static void
_outAlterDomainStmt(StringInfo str, const AlterDomainStmt *node)
{
	WRITE_NODE_TYPE("ALTERDOMAINSTMT");
	WRITE_CHAR_FIELD(subtype);
	WRITE_NODE_FIELD(typeName);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(def);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outCopyStmt(StringInfo str, const CopyStmt *node)
{
	WRITE_NODE_TYPE("COPYSTMT");
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(attlist);
	WRITE_BOOL_FIELD(is_from);
	WRITE_BOOL_FIELD(is_program);
	WRITE_STRING_FIELD(filename);
	WRITE_STRING_FIELD(dirfilename);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(sreh);
}

static void
_outCreateQueueStmt(StringInfo str, const CreateQueueStmt *node)
{
	WRITE_NODE_TYPE("CREATEQUEUESTMT");

	WRITE_STRING_FIELD(queue);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outCreateResourceGroupStmt(StringInfo str, const CreateResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("CREATERESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(options); /* List of DefElem nodes */
}

static void
_outQueryDispatchDesc(StringInfo str, const QueryDispatchDesc *node)
{
	WRITE_NODE_TYPE("QUERYDISPATCHDESC");

	WRITE_NODE_FIELD(intoCreateStmt);
	WRITE_NODE_FIELD(paramInfo);
	WRITE_NODE_FIELD(oidAssignments);
	WRITE_NODE_FIELD(sliceTable);
	WRITE_NODE_FIELD(cursorPositions);
	WRITE_STRING_FIELD(parallelCursorName);
	WRITE_BOOL_FIELD(useChangedAOOpts);
	WRITE_INT_FIELD(secContext);
	WRITE_NODE_FIELD(namedRelList);
	WRITE_OID_FIELD(matviewOid);
	WRITE_OID_FIELD(tableid);
	WRITE_INT_FIELD(snaplen);
	WRITE_STRING_FIELD(snapname);
}

static void
_outTupleDescNode(StringInfo str, const TupleDescNode *node)
{
	int			i;

	Assert(node->tuple->tdtypeid == RECORDOID);

	WRITE_NODE_TYPE("TUPLEDESCNODE");
	WRITE_INT_FIELD(natts);
	WRITE_INT_FIELD(tuple->natts);

	for (i = 0; i < node->tuple->natts; i++)
		appendBinaryStringInfo(str, (char *) &node->tuple->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);

	Assert(node->tuple->constr == NULL);

	WRITE_OID_FIELD(tuple->tdtypeid);
	WRITE_INT_FIELD(tuple->tdtypmod);
	WRITE_INT_FIELD(tuple->tdrefcount);
}

static void
_outSerializedParams(StringInfo str, const SerializedParams *node)
{
	WRITE_NODE_TYPE("SERIALIZEDPARAMS");

	WRITE_INT_FIELD(nExternParams);
	for (int i = 0; i < node->nExternParams; i++)
	{
		WRITE_BOOL_FIELD(externParams[i].isnull);
		WRITE_INT_FIELD(externParams[i].pflags);
		WRITE_OID_FIELD(externParams[i].ptype);
		WRITE_INT_FIELD(externParams[i].plen);
		WRITE_BOOL_FIELD(externParams[i].pbyval);

		if (!node->externParams[i].isnull)
			outDatum(str,
					 node->externParams[i].value,
					 node->externParams[i].plen,
					 node->externParams[i].pbyval);
	}

	WRITE_INT_FIELD(nExecParams);
	for (int i = 0; i < node->nExecParams; i++)
	{
		WRITE_BOOL_FIELD(execParams[i].isnull);
		WRITE_BOOL_FIELD(execParams[i].isvalid);
		WRITE_INT_FIELD(execParams[i].plen);
		WRITE_BOOL_FIELD(execParams[i].pbyval);

		if (node->execParams[i].isvalid && !node->execParams[i].isnull)
			outDatum(str,
					 node->execParams[i].value,
					 node->execParams[i].plen,
					 node->execParams[i].pbyval);
		WRITE_BOOL_FIELD(execParams[i].pbyval);
	}

	/*
	 * No text output function for TupleDescNodes. But that's OK, we
	 * only support text output for debugging purposes.
	 */
#ifdef COMPILING_BINARY_FUNCS
	WRITE_NODE_FIELD(transientTypes);
#endif
}

static void
_outOidAssignment(StringInfo str, const OidAssignment *node)
{
	WRITE_NODE_TYPE("OIDASSIGNMENT");

	WRITE_OID_FIELD(catalog);
	WRITE_STRING_FIELD(objname);
	WRITE_OID_FIELD(namespaceOid);
	WRITE_OID_FIELD(keyOid1);
	WRITE_OID_FIELD(keyOid2);
	WRITE_OID_FIELD(oid);
}

static void
_outSequence(StringInfo str, const Sequence *node)
{
	WRITE_NODE_TYPE("SEQUENCE");
	_outPlanInfo(str, (Plan *)node);
	WRITE_NODE_FIELD(subplans);
}

static void
_outExternalScanInfo(StringInfo str, const ExternalScanInfo *node)
{
	WRITE_NODE_TYPE("EXTERNALSCANINFO");

	WRITE_NODE_FIELD(uriList);
	WRITE_CHAR_FIELD(fmtType);
	WRITE_BOOL_FIELD(isMasterOnly);
	WRITE_INT_FIELD(rejLimit);
	WRITE_BOOL_FIELD(rejLimitInRows);
	WRITE_CHAR_FIELD(logErrors);
	WRITE_INT_FIELD(encoding);
	WRITE_INT_FIELD(scancounter);
	WRITE_NODE_FIELD(extOptions);
}

static void
_outDQAExpr(StringInfo str, const DQAExpr *node)
{
    WRITE_NODE_TYPE("DQAExpr");

    WRITE_INT_FIELD(agg_expr_id);
    WRITE_BITMAPSET_FIELD(agg_args_id_bms);
    WRITE_NODE_FIELD(agg_filter);
}

static void
_outTupleSplit(StringInfo str, const TupleSplit *node)
{
	WRITE_NODE_TYPE("TupleSplit");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numCols);
	WRITE_ATTRNUMBER_ARRAY(grpColIdx, node->numCols);
	WRITE_NODE_FIELD(dqa_expr_lst);
}

static void
_outTableFunctionScan(StringInfo str, const TableFunctionScan *node)
{
	WRITE_NODE_TYPE("TABLEFUNCTIONSCAN");

	_outScanInfo(str, (Scan *) node);

	WRITE_NODE_FIELD(function);
}

static void
_outShareInputScan(StringInfo str, const ShareInputScan *node)
{
	WRITE_NODE_TYPE("SHAREINPUTSCAN");

	WRITE_BOOL_FIELD(cross_slice);
	WRITE_INT_FIELD(share_id);
	WRITE_INT_FIELD(producer_slice_id);
	WRITE_INT_FIELD(this_slice_id);
	WRITE_INT_FIELD(nconsumers);
	WRITE_BOOL_FIELD(discard_output);

	_outPlanInfo(str, (Plan *) node);
}

static void
_outMotion(StringInfo str, const Motion *node)
{
	WRITE_NODE_TYPE("MOTION");

	WRITE_INT_FIELD(motionID);
	WRITE_ENUM_FIELD(motionType, MotionType);

	WRITE_BOOL_FIELD(sendSorted);

	WRITE_NODE_FIELD(hashExprs);
	WRITE_OID_ARRAY(hashFuncs, list_length(node->hashExprs));
	WRITE_INT_FIELD(numSortCols);
	WRITE_ATTRNUMBER_ARRAY(sortColIdx, node->numSortCols);
	WRITE_INT_ARRAY(sortOperators, node->numSortCols);
	WRITE_INT_ARRAY(collations, node->numSortCols);
	WRITE_BOOL_ARRAY(nullsFirst, node->numSortCols);
	WRITE_INT_FIELD(segidColIdx);

	WRITE_INT_FIELD(numHashSegments);

	/* senderSliceInfo is intentionally omitted. It's only used during planning */

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outSplitUpdate
 */
static void
_outSplitUpdate(StringInfo str, const SplitUpdate *node)
{
	WRITE_NODE_TYPE("SplitUpdate");

	WRITE_INT_FIELD(actionColIdx);
	WRITE_NODE_FIELD(insertColIdx);
	WRITE_NODE_FIELD(deleteColIdx);

	WRITE_INT_FIELD(numHashSegments);
	WRITE_INT_FIELD(numHashAttrs);
	WRITE_ATTRNUMBER_ARRAY(hashAttnos, node->numHashAttrs);
	WRITE_OID_ARRAY(hashFuncs, node->numHashAttrs);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outSplitMerge
 */
static void
_outSplitMerge(StringInfo str, const SplitMerge *node)
{
	WRITE_NODE_TYPE("SplitMerge");

	WRITE_INT_FIELD(numHashSegments);
	WRITE_INT_FIELD(numHashAttrs);
	WRITE_ATTRNUMBER_ARRAY(hashAttnos, node->numHashAttrs);
	WRITE_OID_ARRAY(hashFuncs, node->numHashAttrs);
	WRITE_NODE_FIELD(resultRelations);
	WRITE_NODE_FIELD(mergeActionLists);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outAssertOp
 */
static void
_outAssertOp(StringInfo str, const AssertOp *node)
{
	WRITE_NODE_TYPE("AssertOp");

	WRITE_NODE_FIELD(errmessage);
	WRITE_INT_FIELD(errcode);

	_outPlanInfo(str, (Plan *) node);
}

/*
 * _outPartitionSelector
 */
static void
_outPartitionSelector(StringInfo str, const PartitionSelector *node)
{
	WRITE_NODE_TYPE("PartitionSelector");

	WRITE_INT_FIELD(paramid);
	WRITE_NODE_FIELD(part_prune_info);

	_outPlanInfo(str, (Plan *) node);
}

/*****************************************************************************
 *
 *	Stuff from primnodes.h.
 *
 *****************************************************************************/

static void
_outCopyIntoClause(StringInfo str, const CopyIntoClause *node)
{
	WRITE_NODE_TYPE("COPYINTOCLAUSE");

	WRITE_NODE_FIELD(attlist);
	WRITE_BOOL_FIELD(is_program);
	WRITE_STRING_FIELD(filename);
	WRITE_NODE_FIELD(options);
}

static void
_outRefreshClause(StringInfo str, const RefreshClause *node)
{
	WRITE_NODE_TYPE("REFRESHCLAUSE");

	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(skipData);
	WRITE_NODE_FIELD(relation);
}

static void
_outGroupId(StringInfo str, const GroupId *node)
{
	WRITE_NODE_TYPE("GROUPID");

	WRITE_INT_FIELD(agglevelsup);
	WRITE_LOCATION_FIELD(location);
}

static void
_outGroupingSetId(StringInfo str, const GroupingSetId *node)
{
	WRITE_NODE_TYPE("GROUPINGSETID");

	WRITE_LOCATION_FIELD(location);
}

static void
_outDistributionKeyElem(StringInfo str, const DistributionKeyElem *node)
{
	WRITE_NODE_TYPE("DISTRIBUTIONKEYELEM");

	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(opclass);
	WRITE_LOCATION_FIELD(location);
}

static void
_outColumnReferenceStorageDirective(StringInfo str, const ColumnReferenceStorageDirective *node)
{
	WRITE_NODE_TYPE("COLUMNREFERENCESTORAGEDIRECTIVE");

	WRITE_STRING_FIELD(column);
	WRITE_BOOL_FIELD(deflt);
	WRITE_NODE_FIELD(encoding);
}

static void
_outExtTableTypeDesc(StringInfo str, const ExtTableTypeDesc *node)
{
	WRITE_NODE_TYPE("EXTTABLETYPEDESC");

	WRITE_ENUM_FIELD(exttabletype, ExtTableType);
	WRITE_NODE_FIELD(location_list);
	WRITE_NODE_FIELD(on_clause);
	WRITE_STRING_FIELD(command_string);
}

static void
_outCreateExternalStmt(StringInfo str, const CreateExternalStmt *node)
{
	WRITE_NODE_TYPE("CREATEEXTERNALSTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(tableElts);
	WRITE_NODE_FIELD(exttypedesc);
	WRITE_STRING_FIELD(format);
	WRITE_NODE_FIELD(formatOpts);
	WRITE_BOOL_FIELD(isweb);
	WRITE_BOOL_FIELD(iswritable);
	WRITE_NODE_FIELD(sreh);
	WRITE_NODE_FIELD(extOptions);
	WRITE_NODE_FIELD(encoding);
	WRITE_NODE_FIELD(distributedBy);
	WRITE_NODE_FIELD(tags);
}

static void
_outDistributedBy(StringInfo str, const DistributedBy *node)
{
	WRITE_NODE_TYPE("DISTRIBUTEDBY");

	WRITE_ENUM_FIELD(ptype, GpPolicyType);
	WRITE_INT_FIELD(numsegments);
	WRITE_NODE_FIELD(keyCols);
}


static void
_outReindexStmt(StringInfo str, const ReindexStmt *node)
{
	WRITE_NODE_TYPE("REINDEXSTMT");

	WRITE_ENUM_FIELD(kind,ReindexObjectType);
	WRITE_NODE_FIELD(relation);
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(params);
	WRITE_OID_FIELD(relid);
	WRITE_ENUM_FIELD(concurrentlyPhase,ReindexConcurrentlyPhase);
	WRITE_NODE_FIELD(newIndexInfo);
	WRITE_NODE_FIELD(oldIndexInfo);
}

static void
_outReindexIndexInfo(StringInfo str, const ReindexIndexInfo *node)
{
	WRITE_NODE_TYPE("REINDEXINDEXINFO");

	WRITE_OID_FIELD(indexId);
	WRITE_OID_FIELD(tableId);
	WRITE_OID_FIELD(amId);
	WRITE_BOOL_FIELD(safe);
	WRITE_STRING_FIELD(ccNewName);
	WRITE_STRING_FIELD(ccOldName);
}

static void
_outViewStmt(StringInfo str, const ViewStmt *node)
{
	WRITE_NODE_TYPE("VIEWSTMT");

	WRITE_NODE_FIELD(view);
	WRITE_NODE_FIELD(aliases);
	WRITE_NODE_FIELD(query);
	WRITE_BOOL_FIELD(replace);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(tags);
}

static void
_outRuleStmt(StringInfo str, const RuleStmt *node)
{
	WRITE_NODE_TYPE("RULESTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_STRING_FIELD(rulename);
	WRITE_NODE_FIELD(whereClause);
	WRITE_ENUM_FIELD(event, CmdType);
	WRITE_BOOL_FIELD(instead);
	WRITE_NODE_FIELD(actions);
	WRITE_BOOL_FIELD(replace);
}

static void
_outDropStmt(StringInfo str, const DropStmt *node)
{
	WRITE_NODE_TYPE("DROPSTMT");

	WRITE_NODE_FIELD(objects);
	WRITE_ENUM_FIELD(removeType, ObjectType);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(isdynamic);
}

static void
_outDropOwnedStmt(StringInfo str, const DropOwnedStmt *node)
{
	WRITE_NODE_TYPE("DROPOWNEDSTMT");

	WRITE_NODE_FIELD(roles);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outReassignOwnedStmt(StringInfo str, const ReassignOwnedStmt *node)
{
	WRITE_NODE_TYPE("REASSIGNOWNEDSTMT");

	WRITE_NODE_FIELD(roles);
	WRITE_NODE_FIELD(newrole);
}

static void
_outTruncateStmt(StringInfo str, const TruncateStmt *node)
{
	WRITE_NODE_TYPE("TRUNCATESTMT");

	WRITE_NODE_FIELD(relations);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outReplicaIdentityStmt(StringInfo str, const ReplicaIdentityStmt *node)
{
	WRITE_NODE_TYPE("REPLICAIDENTITYSTMT");

	WRITE_CHAR_FIELD(identity_type);
	WRITE_STRING_FIELD(name);
}

static void
_outAlterTableStmt(StringInfo str, const AlterTableStmt *node)
{
	WRITE_NODE_TYPE("ALTERTABLESTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(cmds);
	WRITE_ENUM_FIELD(objtype, ObjectType);

	WRITE_INT_FIELD(lockmode);
	WRITE_NODE_FIELD(wqueue);
}

static void
_outAlterTableCmd(StringInfo str, const AlterTableCmd *node)
{
	WRITE_NODE_TYPE("ALTERTABLECMD");

	WRITE_ENUM_FIELD(subtype, AlterTableType);
	WRITE_STRING_FIELD(name);
	WRITE_INT_FIELD(num);
	WRITE_NODE_FIELD(newowner);
	WRITE_NODE_FIELD(def);
	WRITE_NODE_FIELD(transform);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_BOOL_FIELD(recurse);

	WRITE_INT_FIELD(backendId);
	WRITE_NODE_FIELD(policy);
	WRITE_NODE_FIELD(tags);
	WRITE_BOOL_FIELD(unsettag);
}

static void
wrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		char	   *str = (char *) lfirst(lc);

		lfirst(lc) = makeString(str);
	}
}

static void
unwrapStringList(List *list)
{
	ListCell *lc;

	foreach(lc, list)
	{
		String	   *val = lfirst(lc);

		lfirst(lc) = strVal(val);
		pfree(val);
	}
}

static void
_outAlteredTableInfo(StringInfo str, const AlteredTableInfo *node)
{
	ListCell   *lc;

	WRITE_NODE_TYPE("ALTEREDTABLEINFO");

	WRITE_OID_FIELD(relid);
	WRITE_CHAR_FIELD(relkind);
	/* oldDesc is omitted */

	for (int i = 0; i < AT_NUM_PASSES; i++)
    {
		WRITE_NODE_FIELD(subcmds[i]);
    }

	/*
	 * These aren't Nodes in upstream, so make sure the node tags
	 * are set correctly before trying to serialize them.
	 */
	foreach(lc, node->constraints)
	{
		NewConstraint *e = (NewConstraint *) lfirst(lc);
		e->type = T_NewConstraint;
	}
	foreach(lc, node->newvals)
	{
		NewColumnValue *e = (NewColumnValue *) lfirst(lc);
		e->type = T_NewColumnValue;
	}

	WRITE_NODE_FIELD(constraints);
	WRITE_NODE_FIELD(newvals);
	WRITE_NODE_FIELD(afterStmts);
	WRITE_BOOL_FIELD(verify_new_notnull);
	WRITE_INT_FIELD(rewrite);
	WRITE_OID_FIELD(newAccessMethod);
	WRITE_BOOL_FIELD(dist_opfamily_changed);
	WRITE_OID_FIELD(new_opclass);
	/*
	 * NB: newTableSpace is excluded, it will be assigned in phase 1 of AlterTable.
	 * If newTableSpace is required, refer to the name in its corresponding cmd.
	 * If newTableSpace is strongly required in serialization, please add it
	 * and update `ATPrepSetTableSpace()` to avoid error.
	 */
	WRITE_BOOL_FIELD(chgPersistence);
	WRITE_CHAR_FIELD(newrelpersistence);
	WRITE_NODE_FIELD(partition_constraint);
	WRITE_BOOL_FIELD(validate_default);
	WRITE_NODE_FIELD(changedConstraintOids);

	/* node->changedConstraintDefs is a list of naked strings, so
	 * we can't use WRITE_NODE_FIELD on it. Temporarily wrap them in Values.
	 */
	wrapStringList(node->changedConstraintDefs);
	WRITE_NODE_FIELD(changedConstraintDefs);
	/* unwrap them again */
	unwrapStringList(node->changedConstraintDefs);

	WRITE_NODE_FIELD(changedIndexOids);
	wrapStringList(node->changedIndexDefs);
	WRITE_NODE_FIELD(changedIndexDefs);
	unwrapStringList(node->changedIndexDefs);
	WRITE_NODE_FIELD(beforeStmtLists);
	WRITE_NODE_FIELD(constraintLists);
}

static void
_outNewConstraint(StringInfo str, const NewConstraint *node)
{
	WRITE_NODE_TYPE("NEWCONSTRAINT");

	WRITE_STRING_FIELD(name);
	WRITE_ENUM_FIELD(contype, ConstrType);
	WRITE_OID_FIELD(refrelid);
	WRITE_OID_FIELD(refindid);
	WRITE_OID_FIELD(conid);
	WRITE_NODE_FIELD(qual);
	/* can't serialize qualstate */
}

static void
_outNewColumnValue(StringInfo str, const NewColumnValue *node)
{
	WRITE_NODE_TYPE("NEWCOLUMNVALUE");

	WRITE_INT_FIELD(attnum);
	WRITE_NODE_FIELD(expr);
	/* can't serialize exprstate */
	WRITE_BOOL_FIELD(is_generated);
}

static void
_outCreateRoleStmt(StringInfo str, const CreateRoleStmt *node)
{
	WRITE_NODE_TYPE("CREATEROLESTMT");

	WRITE_ENUM_FIELD(stmt_type, RoleStmtType);
	WRITE_STRING_FIELD(role);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(tags);
}

static void
_outCreateProfileStmt(StringInfo str, const CreateProfileStmt *node)
{
	WRITE_NODE_TYPE("CREATEPROFILESTMT");

	WRITE_STRING_FIELD(profile_name);
	WRITE_NODE_FIELD(options);
}

static void
_outDenyLoginInterval(StringInfo str, const DenyLoginInterval *node)
{
	WRITE_NODE_TYPE("DENYLOGININTERVAL");

	WRITE_NODE_FIELD(start);
	WRITE_NODE_FIELD(end);
}

static void
_outDenyLoginPoint(StringInfo str, const DenyLoginPoint *node)
{
	WRITE_NODE_TYPE("DENYLOGINPOINT");

	WRITE_NODE_FIELD(day);
	WRITE_NODE_FIELD(time);
}

static  void
_outDropRoleStmt(StringInfo str, const DropRoleStmt *node)
{
	WRITE_NODE_TYPE("DROPROLESTMT");

	WRITE_NODE_FIELD(roles);
	WRITE_BOOL_FIELD(missing_ok);
}

static	void
_outDropProfileStmt(StringInfo str, const DropProfileStmt *node)
{
	WRITE_NODE_TYPE("DROPPROFILESTMT");

	WRITE_NODE_FIELD(profiles);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterObjectSchemaStmt(StringInfo str, const AlterObjectSchemaStmt *node)
{
	WRITE_NODE_TYPE("ALTEROBJECTSCHEMASTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(object);
	WRITE_STRING_FIELD(newschema);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_ENUM_FIELD(objectType,ObjectType);
}

static  void
_outAlterOwnerStmt(StringInfo str, const AlterOwnerStmt *node)
{
	WRITE_NODE_TYPE("ALTEROWNERSTMT");

	WRITE_ENUM_FIELD(objectType,ObjectType);
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(object);
	WRITE_NODE_FIELD(newowner);
}

static  void
_outAlterRoleSetStmt(StringInfo str, const AlterRoleSetStmt *node)
{
	WRITE_NODE_TYPE("ALTERROLESETSTMT");

	WRITE_NODE_FIELD(role);
	WRITE_NODE_FIELD(setstmt);
}

static  void
_outAlterRoleStmt(StringInfo str, const AlterRoleStmt *node)
{
	WRITE_NODE_TYPE("ALTERROLESTMT");

	WRITE_NODE_FIELD(role);
	WRITE_NODE_FIELD(options);
	WRITE_INT_FIELD(action);
	WRITE_NODE_FIELD(tags);
	WRITE_BOOL_FIELD(unsettag);
}

static	void
_outAlterProfileStmt(StringInfo str, const AlterProfileStmt *node)
{
	WRITE_NODE_TYPE("ALTERPROFILESTMT");

	WRITE_STRING_FIELD(profile_name);
	WRITE_NODE_FIELD(options);
}

static  void
_outAlterSystemStmt(StringInfo str, const AlterSystemStmt *node)
{
	WRITE_NODE_TYPE("ALTERSYSTEMSTMT");

	WRITE_NODE_FIELD(setstmt);
}


static void
_outRenameStmt(StringInfo str, const RenameStmt *node)
{
	WRITE_NODE_TYPE("RENAMESTMT");

	WRITE_ENUM_FIELD(renameType, ObjectType);
	WRITE_ENUM_FIELD(relationType, ObjectType);
	WRITE_NODE_FIELD(relation);
	WRITE_OID_FIELD(objid);
	WRITE_NODE_FIELD(object);
	WRITE_STRING_FIELD(subname);
	WRITE_STRING_FIELD(newname);
	WRITE_ENUM_FIELD(behavior,DropBehavior);

	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outCreateSeqStmt(StringInfo str, const CreateSeqStmt *node)
{
	WRITE_NODE_TYPE("CREATESEQSTMT");
	WRITE_NODE_FIELD(sequence);
	WRITE_NODE_FIELD(options);
	WRITE_OID_FIELD(ownerId);
	WRITE_BOOL_FIELD(for_identity);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_NODE_FIELD(tags);
}

static void
_outAlterSeqStmt(StringInfo str, const AlterSeqStmt *node)
{
	WRITE_NODE_TYPE("ALTERSEQSTMT");
	WRITE_NODE_FIELD(sequence);
	WRITE_NODE_FIELD(options);
	WRITE_BOOL_FIELD(for_identity);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outClusterStmt(StringInfo str, const ClusterStmt *node)
{
	WRITE_NODE_TYPE("CLUSTERSTMT");

	WRITE_NODE_FIELD(relation);
	WRITE_STRING_FIELD(indexname);
}

static void
_outCreatedbStmt(StringInfo str, const CreatedbStmt *node)
{
	WRITE_NODE_TYPE("CREATEDBSTMT");
	WRITE_STRING_FIELD(dbname);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(tags);
}

static void
_outDropdbStmt(StringInfo str, const DropdbStmt *node)
{
	WRITE_NODE_TYPE("DROPDBSTMT");
	WRITE_STRING_FIELD(dbname);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outCreateFunctionStmt(StringInfo str, const CreateFunctionStmt *node)
{
	WRITE_NODE_TYPE("CREATEFUNCSTMT");
	WRITE_BOOL_FIELD(is_procedure);
	WRITE_BOOL_FIELD(replace);
	WRITE_NODE_FIELD(funcname);
	WRITE_NODE_FIELD(parameters);
	WRITE_NODE_FIELD(returnType);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(sql_body);
}

static void
_outFunctionParameter(StringInfo str, const FunctionParameter *node)
{
	WRITE_NODE_TYPE("FUNCTIONPARAMETER");
	WRITE_STRING_FIELD(name);
	WRITE_NODE_FIELD(argType);
	WRITE_ENUM_FIELD(mode, FunctionParameterMode);
	WRITE_NODE_FIELD(defexpr);
}

static void
_outAlterFunctionStmt(StringInfo str, const AlterFunctionStmt *node)
{
	WRITE_NODE_TYPE("ALTERFUNCTIONSTMT");
	WRITE_ENUM_FIELD(objtype,ObjectType);
	WRITE_NODE_FIELD(func);
	WRITE_NODE_FIELD(actions);
}

static void
_outSegfileMapNode(StringInfo str, const SegfileMapNode *node)
{
	WRITE_NODE_TYPE("SEGFILEMAPNODE");

	WRITE_OID_FIELD(relid);
	WRITE_INT_FIELD(segno);
}


static void
_outDefineStmt(StringInfo str, const DefineStmt *node)
{
	WRITE_NODE_TYPE("DEFINESTMT");
	WRITE_ENUM_FIELD(kind, ObjectType);
	WRITE_BOOL_FIELD(oldstyle);
	WRITE_NODE_FIELD(defnames);
	WRITE_NODE_FIELD(args);
	WRITE_NODE_FIELD(definition);
	WRITE_BOOL_FIELD(if_not_exists);
	WRITE_BOOL_FIELD(replace);
	WRITE_BOOL_FIELD(trusted);  /* CDB */
}

static void
_outCompositeTypeStmt(StringInfo str, const CompositeTypeStmt *node)
{
	WRITE_NODE_TYPE("COMPTYPESTMT");

	WRITE_NODE_FIELD(typevar);
	WRITE_NODE_FIELD(coldeflist);
}

static void
_outCreateEnumStmt(StringInfo str, const CreateEnumStmt *node)
{
	WRITE_NODE_TYPE("CREATEENUMSTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(vals);
}

static void
_outCreateRangeStmt(StringInfo str, const CreateRangeStmt *node)
{
	WRITE_NODE_TYPE("CREATERANGESTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(params);
}

static void
_outCreateCastStmt(StringInfo str, const CreateCastStmt *node)
{
	WRITE_NODE_TYPE("CREATECAST");
	WRITE_NODE_FIELD(sourcetype);
	WRITE_NODE_FIELD(targettype);
	WRITE_NODE_FIELD(func);
	WRITE_ENUM_FIELD(context, CoercionContext);
	WRITE_BOOL_FIELD(inout);
}

static void
_outCreateOpClassStmt(StringInfo str, const CreateOpClassStmt *node)
{
	WRITE_NODE_TYPE("CREATEOPCLASS");
	WRITE_NODE_FIELD(opclassname);
	WRITE_NODE_FIELD(opfamilyname);
	WRITE_STRING_FIELD(amname);
	WRITE_NODE_FIELD(datatype);
	WRITE_NODE_FIELD(items);
	WRITE_BOOL_FIELD(isDefault);
}

static void
_outCreateOpClassItem(StringInfo str, const CreateOpClassItem *node)
{
	WRITE_NODE_TYPE("CREATEOPCLASSITEM");
	WRITE_INT_FIELD(itemtype);
	WRITE_NODE_FIELD(name);
	WRITE_INT_FIELD(number);
	WRITE_NODE_FIELD(order_family);
	WRITE_NODE_FIELD(class_args);
	WRITE_NODE_FIELD(storedtype);
}

static void
_outCreateOpFamilyStmt(StringInfo str, const CreateOpFamilyStmt *node)
{
	WRITE_NODE_TYPE("CREATEOPFAMILY");
	WRITE_NODE_FIELD(opfamilyname);
	WRITE_STRING_FIELD(amname);
}

static void
_outAlterOpFamilyStmt(StringInfo str, const AlterOpFamilyStmt *node)
{
	WRITE_NODE_TYPE("ALTEROPFAMILY");
	WRITE_NODE_FIELD(opfamilyname);
	WRITE_STRING_FIELD(amname);
	WRITE_BOOL_FIELD(isDrop);
	WRITE_NODE_FIELD(items);
}

static void
_outCreatePolicyStmt(StringInfo str, const CreatePolicyStmt *node)
{
	WRITE_NODE_TYPE("CREATEPOLICYSTMT");

	WRITE_STRING_FIELD(policy_name);
	WRITE_NODE_FIELD(table);
	WRITE_STRING_FIELD(cmd_name);
	WRITE_BOOL_FIELD(permissive);
	WRITE_NODE_FIELD(roles);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(with_check);
}

static void
_outAlterPolicyStmt(StringInfo str, const AlterPolicyStmt *node)
{
	WRITE_NODE_TYPE("ALTERPOLICYSTMT");

	WRITE_STRING_FIELD(policy_name);
	WRITE_NODE_FIELD(table);
	WRITE_NODE_FIELD(roles);
	WRITE_NODE_FIELD(qual);
	WRITE_NODE_FIELD(with_check);
}

static void
_outCreateTransformStmt(StringInfo str, const CreateTransformStmt *node)
{
	WRITE_NODE_TYPE("CREATETRANSFORMSTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_NODE_FIELD(type_name);
	WRITE_STRING_FIELD(lang);
	WRITE_NODE_FIELD(fromsql);
	WRITE_NODE_FIELD(tosql);
}

static void
_outCreateConversionStmt(StringInfo str, const CreateConversionStmt *node)
{
	WRITE_NODE_TYPE("CREATECONVERSION");
	WRITE_NODE_FIELD(conversion_name);
	WRITE_STRING_FIELD(for_encoding_name);
	WRITE_STRING_FIELD(to_encoding_name);
	WRITE_NODE_FIELD(func_name);
	WRITE_BOOL_FIELD(def);
}

static void
_outTransactionStmt(StringInfo str, const TransactionStmt *node)
{
	WRITE_NODE_TYPE("TRANSACTIONSTMT");

	WRITE_ENUM_FIELD(kind, TransactionStmtKind);
	WRITE_NODE_FIELD(options);
}

static void
_outSingleRowErrorDesc(StringInfo str, const SingleRowErrorDesc *node)
{
	WRITE_NODE_TYPE("SINGLEROWERRORDESC");
	WRITE_INT_FIELD(rejectlimit);
	WRITE_BOOL_FIELD(is_limit_in_rows);
	WRITE_CHAR_FIELD(log_error_type);
}


static void
_outGrantStmt(StringInfo str, const GrantStmt *node)
{
	WRITE_NODE_TYPE("GRANTSTMT");
	WRITE_BOOL_FIELD(is_grant);
	WRITE_ENUM_FIELD(targtype,GrantTargetType);
	WRITE_ENUM_FIELD(objtype,ObjectType);
	WRITE_NODE_FIELD(objects);
	WRITE_NODE_FIELD(privileges);
	WRITE_NODE_FIELD(grantees);
	WRITE_BOOL_FIELD(grant_option);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outObjectWithArgs(StringInfo str, const ObjectWithArgs *node)
{
	WRITE_NODE_TYPE("OBJECTWITHARGS");
	WRITE_NODE_FIELD(objname);
	WRITE_NODE_FIELD(objargs);
	WRITE_BOOL_FIELD(args_unspecified);
}

static void
_outGrantRoleStmt(StringInfo str, const GrantRoleStmt *node)
{
	WRITE_NODE_TYPE("GRANTROLESTMT");
	WRITE_NODE_FIELD(granted_roles);
	WRITE_NODE_FIELD(grantee_roles);
	WRITE_BOOL_FIELD(is_grant);
	WRITE_NODE_FIELD(opt);
	WRITE_NODE_FIELD(grantor);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outLockStmt(StringInfo str, const LockStmt *node)
{
	WRITE_NODE_TYPE("LOCKSTMT");
	WRITE_NODE_FIELD(relations);
	WRITE_INT_FIELD(mode);
	WRITE_BOOL_FIELD(nowait);
}

static void
_outConstraintsSetStmt(StringInfo str, const ConstraintsSetStmt *node)
{
	WRITE_NODE_TYPE("CONSTRAINTSSETSTMT");
	WRITE_NODE_FIELD(constraints);
	WRITE_BOOL_FIELD(deferred);
}

static void
_outInsertStmt(StringInfo str, const InsertStmt *node)
{
	WRITE_NODE_TYPE("INSERT");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(cols);
	WRITE_NODE_FIELD(selectStmt);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(withClause);
}

static void
_outDeleteStmt(StringInfo str, const DeleteStmt *node)
{
	WRITE_NODE_TYPE("DELETE");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(usingClause);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(withClause);
}

static void
_outUpdateStmt(StringInfo str, const UpdateStmt *node)
{
	WRITE_NODE_TYPE("UPDATE");

	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(whereClause);
	WRITE_NODE_FIELD(fromClause);
	WRITE_NODE_FIELD(returningList);
	WRITE_NODE_FIELD(withClause);
}

static void
_outDMLActionExpr(StringInfo str, const DMLActionExpr *node)
{
	WRITE_NODE_TYPE("DMLACTIONEXPR");
}

static void
_outVariableSetStmt(StringInfo str, const VariableSetStmt *node)
{
	WRITE_NODE_TYPE("VARIABLESETSTMT");

	WRITE_STRING_FIELD(name);
	WRITE_ENUM_FIELD(kind, VariableSetKind);
	WRITE_NODE_FIELD(args);
	WRITE_BOOL_FIELD(is_local);
}

static void
_outPartitionCmd(StringInfo str, const PartitionCmd *node)
{
	WRITE_NODE_TYPE("PARTITIONCMD");

	WRITE_NODE_FIELD(name);
	WRITE_NODE_FIELD(bound);
}

static void
_outGpAlterPartitionId(StringInfo str, const GpAlterPartitionId *node)
{
	WRITE_NODE_TYPE("GPALTERPARTITIONID");

	WRITE_ENUM_FIELD(idtype, GpAlterPartitionIdType);
	WRITE_NODE_FIELD(partiddef);
}

static void
_outGpDropPartitionCmd(StringInfo str, const GpDropPartitionCmd *node)
{
	WRITE_NODE_TYPE("GPDROPPARTITIONCMD");

	WRITE_NODE_FIELD(partid);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outGpAlterPartitionCmd(StringInfo str, const GpAlterPartitionCmd *node)
{
	WRITE_NODE_TYPE("GPALTERPARTITIONCMD");

	WRITE_NODE_FIELD(partid);
	WRITE_NODE_FIELD(arg);
}

static void
_outCreateSchemaStmt(StringInfo str, const CreateSchemaStmt *node)
{
	WRITE_NODE_TYPE("CREATESCHEMASTMT");

	WRITE_STRING_FIELD(schemaname);
	WRITE_NODE_FIELD(authrole);
	WRITE_BOOL_FIELD(istemp);
	WRITE_BOOL_FIELD(pop_search_path);
	WRITE_NODE_FIELD(tags);
}

static void
_outAlterSchemaStmt(StringInfo str, const AlterSchemaStmt *node)
{
	WRITE_NODE_TYPE("ALTERSCHEMASTMT");
	
	WRITE_STRING_FIELD(schemaname);
	WRITE_NODE_FIELD(tags);
	WRITE_BOOL_FIELD(unsettag);
}

static void
_outCreateTagStmt(StringInfo str, const CreateTagStmt *node)
{
	WRITE_NODE_TYPE("CREATETAGSTMT");
	
	WRITE_STRING_FIELD(tag_name);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_NODE_FIELD(allowed_values);
}

static void
_outAlterTagStmt(StringInfo str, const AlterTagStmt *node)
{
	WRITE_NODE_TYPE("ALTERTAGSTMT");
	
	WRITE_STRING_FIELD(tag_name);
	WRITE_INT_FIELD(action);
	WRITE_NODE_FIELD(tag_values);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_BOOL_FIELD(unset);
}

static void
_outDropTagStmt(StringInfo str, const DropTagStmt *node)
{
	WRITE_NODE_TYPE("DROPTAGSTMT");
	
	WRITE_NODE_FIELD(tags);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outCreatePLangStmt(StringInfo str, const CreatePLangStmt *node)
{
	WRITE_NODE_TYPE("CREATEPLANGSTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_STRING_FIELD(plname);
	WRITE_NODE_FIELD(plhandler);
	WRITE_NODE_FIELD(plinline);
	WRITE_NODE_FIELD(plvalidator);
	WRITE_BOOL_FIELD(pltrusted);
}

static void
_outVacuumStmt(StringInfo str, const VacuumStmt *node)
{
	WRITE_NODE_TYPE("VACUUMSTMT");

	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(rels);
	WRITE_BOOL_FIELD(is_vacuumcmd);
}

static void
_outVacuumRelation(StringInfo str, const VacuumRelation *node)
{
	WRITE_NODE_TYPE("VACUUMRELATION");

	WRITE_NODE_FIELD(relation);
	WRITE_OID_FIELD(oid);
	WRITE_NODE_FIELD(va_cols);
}

static void
_outCdbProcess(StringInfo str, const CdbProcess *node)
{
	WRITE_NODE_TYPE("CDBPROCESS");
	WRITE_STRING_FIELD(listenerAddr);
	WRITE_INT_FIELD(listenerPort);
	WRITE_INT_FIELD(pid);
	WRITE_INT_FIELD(contentid);
	WRITE_INT_FIELD(dbid);
}

static void
_outSliceTable(StringInfo str, const SliceTable *node)
{
	WRITE_NODE_TYPE("SLICETABLE");

	WRITE_INT_FIELD(localSlice);
	WRITE_INT_FIELD(numSlices);
	for (int i = 0; i < node->numSlices; i++)
	{
		WRITE_INT_FIELD(slices[i].sliceIndex);
		WRITE_INT_FIELD(slices[i].rootIndex);
		WRITE_INT_FIELD(slices[i].parentIndex);
		WRITE_INT_FIELD(slices[i].planNumSegments);
		WRITE_NODE_FIELD(slices[i].children); /* List of int index */
		WRITE_ENUM_FIELD(slices[i].gangType, GangType);
		WRITE_NODE_FIELD(slices[i].segments); /* List of int */
		WRITE_BOOL_FIELD(slices[i].useMppParallelMode);
		WRITE_INT_FIELD(slices[i].parallel_workers);
		WRITE_DUMMY_FIELD(slices[i].primaryGang);
		WRITE_NODE_FIELD(slices[i].primaryProcesses); /* List of (CDBProcess *) */
		WRITE_BITMAPSET_FIELD(slices[i].processesMap);
	}
	WRITE_BOOL_FIELD(hasMotions);
	WRITE_INT_FIELD(instrument_options);
	WRITE_INT_FIELD(ic_instance_id);
}

static void
_outCursorPosInfo(StringInfo str, const CursorPosInfo *node)
{
	WRITE_NODE_TYPE("CURSORPOSINFO");

	WRITE_STRING_FIELD(cursor_name);
	WRITE_INT_FIELD(gp_segment_id);
	WRITE_UINT_FIELD(ctid.ip_blkid.bi_hi);
	WRITE_UINT_FIELD(ctid.ip_blkid.bi_lo);
	WRITE_UINT_FIELD(ctid.ip_posid);
	WRITE_OID_FIELD(table_oid);
}

static void
_outCreateTrigStmt(StringInfo str, const CreateTrigStmt *node)
{
	WRITE_NODE_TYPE("CREATETRIGSTMT");

	WRITE_BOOL_FIELD(replace);
	WRITE_STRING_FIELD(trigname);
	WRITE_NODE_FIELD(relation);
	WRITE_NODE_FIELD(funcname);
	WRITE_NODE_FIELD(args);
	WRITE_BOOL_FIELD(row);
	WRITE_INT_FIELD(timing);
	WRITE_INT_FIELD(events);
	WRITE_NODE_FIELD(columns);
	WRITE_NODE_FIELD(whenClause);
	WRITE_BOOL_FIELD(isconstraint);
	WRITE_NODE_FIELD(transitionRels);
	WRITE_BOOL_FIELD(deferrable);
	WRITE_BOOL_FIELD(initdeferred);
	WRITE_NODE_FIELD(constrrel);
	WRITE_OID_FIELD(matviewId);
}

static void
_outCreateTableSpaceStmt(StringInfo str, const CreateTableSpaceStmt *node)
{
	WRITE_NODE_TYPE("CREATETABLESPACESTMT");

	WRITE_STRING_FIELD(tablespacename);
	WRITE_NODE_FIELD(owner);
	WRITE_STRING_FIELD(location);
	WRITE_NODE_FIELD(options);
	WRITE_STRING_FIELD(filehandler);
	WRITE_NODE_FIELD(tags);
}

static void
_outDropTableSpaceStmt(StringInfo str, const DropTableSpaceStmt *node)
{
	WRITE_NODE_TYPE("DROPTABLESPACESTMT");

	WRITE_STRING_FIELD(tablespacename);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outDropQueueStmt(StringInfo str, const DropQueueStmt *node)
{
	WRITE_NODE_TYPE("DROPQUEUESTMT");

	WRITE_STRING_FIELD(queue);
}

static void
_outDropResourceGroupStmt(StringInfo str, const DropResourceGroupStmt *node)
{
	WRITE_NODE_TYPE("DROPRESOURCEGROUPSTMT");

	WRITE_STRING_FIELD(name);
}


static void
_outCommentStmt(StringInfo str, const CommentStmt *node)
{
	WRITE_NODE_TYPE("COMMENTSTMT");

	WRITE_ENUM_FIELD(objtype, ObjectType);
	WRITE_NODE_FIELD(object);
	WRITE_STRING_FIELD(comment);
}

static void
_outTableValueExpr(StringInfo str, const TableValueExpr *node)
{
	WRITE_NODE_TYPE("TABLEVALUEEXPR");

	WRITE_NODE_FIELD(subquery);
}

static void
_outAlterTypeStmt(StringInfo str, const AlterTypeStmt *node)
{
	WRITE_NODE_TYPE("ALTERTYPESTMT");

	WRITE_NODE_FIELD(typeName);
	WRITE_NODE_FIELD(options);
}

static void
_outAlterExtensionStmt(StringInfo str, const AlterExtensionStmt *node)
{
	WRITE_NODE_TYPE("ALTEREXTENSIONSTMT");

	WRITE_STRING_FIELD(extname);
	WRITE_NODE_FIELD(options);
	WRITE_ENUM_FIELD(update_ext_state, UpdateExtensionState);
}

static void
_outAlterExtensionContentsStmt(StringInfo str, const AlterExtensionContentsStmt *node)
{
	WRITE_NODE_TYPE("ALTEREXTENSIONCONTENTSSTMT");

	WRITE_STRING_FIELD(extname);
	WRITE_INT_FIELD(action);
	WRITE_ENUM_FIELD(objtype, ObjectType);
	WRITE_NODE_FIELD(object);
}

static void
_outAlterTSConfigurationStmt(StringInfo str, const AlterTSConfigurationStmt *node)
{
	WRITE_NODE_TYPE("ALTERTSCONFIGURATIONSTMT");

	WRITE_NODE_FIELD(cfgname);
	WRITE_NODE_FIELD(tokentype);
	WRITE_NODE_FIELD(dicts);
	WRITE_BOOL_FIELD(override);
	WRITE_BOOL_FIELD(replace);
	WRITE_BOOL_FIELD(missing_ok);
}

static void
_outAlterTSDictionaryStmt(StringInfo str, const AlterTSDictionaryStmt *node)
{
	WRITE_NODE_TYPE("ALTERTSDICTIONARYSTMT");

	WRITE_NODE_FIELD(dictname);
	WRITE_NODE_FIELD(options);
}

static void
_outCreatePublicationStmt(StringInfo str, const CreatePublicationStmt *node)
{
	WRITE_NODE_TYPE("CREATEPUBLICATIONSTMT");

	WRITE_STRING_FIELD(pubname);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(pubobjects);
	WRITE_BOOL_FIELD(for_all_tables);
}

static void
_outAlterPublicationStmt(StringInfo str, const AlterPublicationStmt *node)
{
	WRITE_NODE_TYPE("ALTERPUBLICATIONSTMT");

	WRITE_STRING_FIELD(pubname);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(pubobjects);
	WRITE_BOOL_FIELD(for_all_tables);
	WRITE_ENUM_FIELD(action, AlterPublicationAction);
}

static void
_outCreateSubscriptionStmt(StringInfo str, const CreateSubscriptionStmt *node)
{
	WRITE_NODE_TYPE("CREATESUBSCRIPTIONSTMT");

	WRITE_STRING_FIELD(subname);
	WRITE_STRING_FIELD(conninfo);
	WRITE_NODE_FIELD(publication);
	WRITE_NODE_FIELD(options);
}

static void
_outDropSubscriptionStmt(StringInfo str, const DropSubscriptionStmt *node)
{
	WRITE_NODE_TYPE("DROPSUBSCRIPTIONSTMT");

	WRITE_STRING_FIELD(subname);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
}

static void
_outAlterSubscriptionStmt(StringInfo str, const AlterSubscriptionStmt *node)
{
	WRITE_NODE_TYPE("ALTERSUBSCRIPTIONSTMT");

	WRITE_ENUM_FIELD(kind, AlterSubscriptionType);
	WRITE_STRING_FIELD(subname);
	WRITE_STRING_FIELD(conninfo);
	WRITE_NODE_FIELD(publication);
	WRITE_NODE_FIELD(options);
}

static void
_outCTECycleClause(StringInfo str, const CTECycleClause *node)
{
	WRITE_NODE_TYPE("CTECYCLECLAUSE");

	WRITE_NODE_FIELD(cycle_col_list);
	WRITE_STRING_FIELD(cycle_mark_column);
	WRITE_NODE_FIELD(cycle_mark_value);
	WRITE_NODE_FIELD(cycle_mark_default);
	WRITE_STRING_FIELD(cycle_path_column);
	WRITE_LOCATION_FIELD(location);
	WRITE_OID_FIELD(cycle_mark_type);
	WRITE_INT_FIELD(cycle_mark_typmod);
	WRITE_OID_FIELD(cycle_mark_collation);
	WRITE_OID_FIELD(cycle_mark_neop);
}

static void
_outCTESearchClause(StringInfo str, const CTESearchClause *node)
{
	WRITE_NODE_TYPE("CTESEARCHCLAUSE");

	WRITE_NODE_FIELD(search_col_list);
	WRITE_BOOL_FIELD(search_breadth_first);
	WRITE_STRING_FIELD(search_seq_column);
	WRITE_LOCATION_FIELD(location);
}

static void
_outMemoize(StringInfo str, const Memoize *node)
{
	WRITE_NODE_TYPE("MEMOIZE");

	_outPlanInfo(str, (const Plan *) node);

	WRITE_INT_FIELD(numKeys);
	WRITE_OID_ARRAY(hashOperators, node->numKeys);
	WRITE_OID_ARRAY(collations, node->numKeys);
	WRITE_NODE_FIELD(param_exprs);
	WRITE_BOOL_FIELD(singlerow);
	WRITE_BOOL_FIELD(binary_mode);
	WRITE_UINT_FIELD(est_entries);
	WRITE_BITMAPSET_FIELD(keyparamids);
}

static void
_outTidRangeScan(StringInfo str, const TidRangeScan *node)
{
	WRITE_NODE_TYPE("TIDRANGESCAN");

	_outScanInfo(str, (const Scan *) node);

	WRITE_NODE_FIELD(tidrangequals);
}

static void
_outEphemeralNamedRelationInfo(StringInfo str, const EphemeralNamedRelationInfo *node)
{
	int			i;

	WRITE_NODE_TYPE("EphemeralNamedRelationInfo");
	WRITE_STRING_FIELD(name);
	WRITE_OID_FIELD(reliddesc);
	WRITE_INT_FIELD(natts);
	WRITE_INT_FIELD(tuple->natts);

	for (i = 0; i < node->tuple->natts; i++)
		appendBinaryStringInfo(str, (char *) &node->tuple->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);

	WRITE_OID_FIELD(tuple->tdtypeid);
	WRITE_INT_FIELD(tuple->tdtypmod);
	WRITE_INT_FIELD(tuple->tdrefcount);
	WRITE_ENUM_FIELD(enrtype, EphemeralNameRelationType);
	WRITE_FLOAT_FIELD(enrtuples);
}

static void
_outAlterDatabaseStmt(StringInfo str, const AlterDatabaseStmt *node)
{
	WRITE_NODE_TYPE("ALTERDATABASESTMT");
	WRITE_STRING_FIELD(dbname);
	WRITE_NODE_FIELD(options);
	WRITE_NODE_FIELD(tags);
	WRITE_BOOL_FIELD(unsettag);
}

static void
_outDropStmtInfo(StringInfo str, const DropStmt *node)
{
	WRITE_NODE_FIELD(objects);
	WRITE_ENUM_FIELD(removeType, ObjectType);
	WRITE_ENUM_FIELD(behavior, DropBehavior);
	WRITE_BOOL_FIELD(missing_ok);
	WRITE_BOOL_FIELD(concurrent);
	WRITE_BOOL_FIELD(isdynamic);
}

static void
_outDropDirectoryTableStmt(StringInfo str, const DropDirectoryTableStmt *node)
{
	WRITE_NODE_TYPE("DROPDIRECTORYTABLESTMT");

	_outDropStmtInfo(str, (const DropStmt *) node);
	WRITE_BOOL_FIELD(with_content);
}
