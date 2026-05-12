/*-------------------------------------------------------------------------
 *
 * readfuncs_common.c
 *	  Common de-serialization functions for Postgres tree nodes.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 *
 *
 * NOTES
 *    This file contains all common de-serialization functions for both
 *    binary and text representations, and de-serialization functions
 *    that are Cloudberry-specific for text representation.
 *
 *    All text-representation only functions are guarded by the macro
 *    `COMPILING_BINARY_FUNCS`, they appear first in this file.
 *
 *    This C source file SHOULD not be compiled alone, it MUST be
 *    only be included by readfuncs.c, so we intended to not complete
 *    the header files.
 *
 *    When you consider adding new de-serialization functions, you should
 *    folow these rules:
 *    1. Do not add any functions(Cloudberry-specific) to readfuncs.c
 *    2. Add functions to readfuncs_common.c that can be used for both
 *       binary and text representations.
 *    3. Add functions to readfast.c if these functions can only be used
 *       for binary representation.
 *    4. Add functions to readfuncs_common.c if these functions can only
 *       be used for text representation.
 *
 *-------------------------------------------------------------------------
 */

static void unwrapStringList(List *list);

/* functions only used for text representation */
#ifndef COMPILING_BINARY_FUNCS



static A_Const *
_readAConst(void)
{
	READ_LOCALS(A_Const);

	/* We expect either NULL or :val here */
	token = pg_strtok(&length);
	if (length == 4 && strncmp(token, "NULL", 4) == 0)
		local_node->isnull = true;
	else
	{
		union ValUnion *tmp = nodeRead(NULL, 0);

		/* To forestall valgrind complaints, copy only the valid data */
		switch (nodeTag(tmp))
		{
			case T_Integer:
				memcpy(&local_node->val, tmp, sizeof(Integer));
				break;
			case T_Float:
				memcpy(&local_node->val, tmp, sizeof(Float));
				break;
			case T_Boolean:
				memcpy(&local_node->val, tmp, sizeof(Boolean));
				break;
			case T_String:
				memcpy(&local_node->val, tmp, sizeof(String));
				break;
			case T_BitString:
				memcpy(&local_node->val, tmp, sizeof(BitString));
				break;
			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(tmp));
				break;
		}
	}

	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static A_Expr *
_readAExpr(void)
{
	READ_LOCALS(A_Expr);

	token = pg_strtok(&length);

	if (strncmp(token,"OPER",length)==0)
	{
		local_node->kind = AEXPR_OP;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"ANY",length)==0)
	{
		local_node->kind = AEXPR_OP_ANY;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"ALL",length)==0)
	{
		local_node->kind = AEXPR_OP_ALL;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"DISTINCT",length)==0)
	{
		local_node->kind = AEXPR_DISTINCT;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NOT_DISTINCT",length)==0)
	{
		local_node->kind = AEXPR_NOT_DISTINCT;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NULLIF",length)==0)
	{
		local_node->kind = AEXPR_NULLIF;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"IN",length)==0)
	{
		local_node->kind = AEXPR_IN;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"LIKE",length)==0)
	{
		local_node->kind = AEXPR_LIKE;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"ILIKE",length)==0)
	{
		local_node->kind = AEXPR_ILIKE;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"SIMILAR",length)==0)
	{
		local_node->kind = AEXPR_SIMILAR;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"BETWEEN",length)==0)
	{
		local_node->kind = AEXPR_BETWEEN;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NOT_BETWEEN",length)==0)
	{
		local_node->kind = AEXPR_NOT_BETWEEN;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"BETWEEN_SYM",length)==0)
	{
		local_node->kind = AEXPR_BETWEEN_SYM;
		READ_NODE_FIELD(name);
	}
	else if (strncmp(token,"NOT_BETWEEN_SYM",length)==0)
	{
		local_node->kind = AEXPR_NOT_BETWEEN_SYM;
		READ_NODE_FIELD(name);
	}
	else
	{
		elog(ERROR,"Unable to understand A_Expr node %.30s",token);
	}

	READ_NODE_FIELD(lexpr);
	READ_NODE_FIELD(rexpr);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

#endif

/* common functions for both formats */

static GpPartDefElem *
_readGpPartDefElem(void)
{
	READ_LOCALS(GpPartDefElem);

	READ_STRING_FIELD(partName);
	READ_NODE_FIELD(boundSpec);
	READ_NODE_FIELD(subSpec);
	READ_BOOL_FIELD(isDefault);
	READ_NODE_FIELD(options);
	READ_STRING_FIELD(accessMethod);
	READ_STRING_FIELD(tablespacename);
	READ_NODE_FIELD(colencs);

	READ_DONE();
}

static GpPartitionRangeItem *
_readGpPartitionRangeItem(void)
{
	READ_LOCALS(GpPartitionRangeItem);

	READ_NODE_FIELD(val);
	READ_ENUM_FIELD(edge, GpPartitionEdgeBounding);

	READ_DONE();
}

static GpPartitionRangeSpec *
_readGpPartitionRangeSpec(void)
{
	READ_LOCALS(GpPartitionRangeSpec);

	READ_NODE_FIELD(partStart);
	READ_NODE_FIELD(partEnd);
	READ_NODE_FIELD(partEvery);

	READ_DONE();
}

static GpPartitionListSpec *
_readGpPartitionListSpec(void)
{
	READ_LOCALS(GpPartitionListSpec);

	READ_NODE_FIELD(partValues);

	READ_DONE();
}

static GpPartitionDefinition *
_readGpPartitionDefinition(void)
{
	READ_LOCALS(GpPartitionDefinition);

	READ_NODE_FIELD(partDefElems);
	READ_NODE_FIELD(encClauses);
	READ_BOOL_FIELD(isTemplate);

	READ_DONE();
}

/*
 * _readA_ArrayExpr
 */
static A_ArrayExpr *
_readA_ArrayExpr(void)
{
	READ_LOCALS(A_ArrayExpr);

	READ_NODE_FIELD(elements);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static AlterDomainStmt *
_readAlterDomainStmt(void)
{
	READ_LOCALS(AlterDomainStmt);

	READ_CHAR_FIELD(subtype);
	READ_NODE_FIELD(typeName);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(def);
	READ_ENUM_FIELD(behavior, DropBehavior);
    Assert(local_node->behavior <= DROP_CASCADE);

	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static AlterFunctionStmt *
_readAlterFunctionStmt(void)
{
	READ_LOCALS(AlterFunctionStmt);
	READ_ENUM_FIELD(objtype,ObjectType);
	READ_NODE_FIELD(func);
	READ_NODE_FIELD(actions);

	READ_DONE();
}

static AlterObjectSchemaStmt *
_readAlterObjectSchemaStmt(void)
{
	READ_LOCALS(AlterObjectSchemaStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(object);
	READ_STRING_FIELD(newschema);
	READ_BOOL_FIELD(missing_ok);
	READ_ENUM_FIELD(objectType,ObjectType);

	READ_DONE();
}

static AlterOpFamilyStmt *
_readAlterOpFamilyStmt(void)
{
	READ_LOCALS(AlterOpFamilyStmt);
	READ_NODE_FIELD(opfamilyname);
	READ_STRING_FIELD(amname);
	READ_BOOL_FIELD(isDrop);
	READ_NODE_FIELD(items);

	READ_DONE();
}

static AlterOwnerStmt *
_readAlterOwnerStmt(void)
{
	READ_LOCALS(AlterOwnerStmt);

	READ_ENUM_FIELD(objectType,ObjectType);
	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(object);
	READ_NODE_FIELD(newowner);

	READ_DONE();
}

static AlterPolicyStmt *
_readAlterPolicyStmt()
{
	READ_LOCALS(AlterPolicyStmt);

	READ_STRING_FIELD(policy_name);
	READ_NODE_FIELD(table);
	READ_NODE_FIELD(roles);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(with_check);

	READ_DONE();
}

static AlterPublicationStmt *
_readAlterPublicationStmt()
{
	READ_LOCALS(AlterPublicationStmt);

	READ_STRING_FIELD(pubname);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(pubobjects);
	READ_BOOL_FIELD(for_all_tables);
	READ_ENUM_FIELD(action, AlterPublicationAction);

	READ_DONE();
}

static AlterRoleSetStmt *
_readAlterRoleSetStmt(void)
{
	READ_LOCALS(AlterRoleSetStmt);

	READ_NODE_FIELD(role);
	READ_NODE_FIELD(setstmt);

	READ_DONE();
}

static AlterRoleStmt *
_readAlterRoleStmt(void)
{
	READ_LOCALS(AlterRoleStmt);

	READ_NODE_FIELD(role);
	READ_NODE_FIELD(options);
	READ_INT_FIELD(action);
	READ_NODE_FIELD(tags);
	READ_BOOL_FIELD(unsettag);

	READ_DONE();
}

static AlterProfileStmt *
_readAlterProfileStmt(void)
{
	READ_LOCALS(AlterProfileStmt);

	READ_STRING_FIELD(profile_name);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterSeqStmt *
_readAlterSeqStmt(void)
{
	READ_LOCALS(AlterSeqStmt);

	READ_NODE_FIELD(sequence);
	READ_NODE_FIELD(options);
	READ_BOOL_FIELD(for_identity);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static AlterSubscriptionStmt *
_readAlterSubscriptionStmt()
{
	READ_LOCALS(AlterSubscriptionStmt);

	READ_ENUM_FIELD(kind, AlterSubscriptionType);
	READ_STRING_FIELD(subname);
	READ_STRING_FIELD(conninfo);
	READ_NODE_FIELD(publication);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlterSystemStmt *
_readAlterSystemStmt(void)
{
	READ_LOCALS(AlterSystemStmt);

	READ_NODE_FIELD(setstmt);

	READ_DONE();
}

static AlterTableCmd *
_readAlterTableCmd(void)
{
	READ_LOCALS(AlterTableCmd);

	READ_ENUM_FIELD(subtype, AlterTableType);
	READ_STRING_FIELD(name);
	READ_INT_FIELD(num);
	READ_NODE_FIELD(newowner);
	READ_NODE_FIELD(def);
	READ_NODE_FIELD(transform);
	READ_ENUM_FIELD(behavior, DropBehavior);
	READ_BOOL_FIELD(missing_ok);
	READ_BOOL_FIELD(recurse);

	READ_INT_FIELD(backendId);
	READ_NODE_FIELD(policy);
	READ_NODE_FIELD(tags);
	READ_BOOL_FIELD(unsettag);

	READ_DONE();
}

static AlterTableStmt *
_readAlterTableStmt(void)
{
	READ_LOCALS(AlterTableStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(cmds);
	READ_ENUM_FIELD(objtype, ObjectType);
	READ_INT_FIELD(lockmode);
	READ_NODE_FIELD(wqueue);

	READ_DONE();
}

static AlterTypeStmt *
_readAlterTypeStmt(void)
{
	READ_LOCALS(AlterTypeStmt);

	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static AlteredTableInfo *
_readAlteredTableInfo(void)
{
	READ_LOCALS(AlteredTableInfo);

	READ_OID_FIELD(relid);
	READ_CHAR_FIELD(relkind);
	/* oldDesc is omitted */

	for (int i = 0; i < AT_NUM_PASSES; i++)
	{
		READ_NODE_FIELD(subcmds[i]);
	}

	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(newvals);
	READ_NODE_FIELD(afterStmts);
	READ_BOOL_FIELD(verify_new_notnull);
	READ_INT_FIELD(rewrite);
	READ_OID_FIELD(newAccessMethod);
	READ_BOOL_FIELD(dist_opfamily_changed);
	READ_OID_FIELD(new_opclass);
	READ_BOOL_FIELD(chgPersistence);
	READ_CHAR_FIELD(newrelpersistence);
	READ_NODE_FIELD(partition_constraint);
	READ_BOOL_FIELD(validate_default);
	READ_NODE_FIELD(changedConstraintOids);
	READ_NODE_FIELD(changedConstraintDefs);
	/* The QD sends changedConstraintDefs wrapped in Values. Unwrap them. */
	unwrapStringList(local_node->changedConstraintDefs);
	READ_NODE_FIELD(changedIndexOids);
	READ_NODE_FIELD(changedIndexDefs);
	unwrapStringList(local_node->changedIndexDefs);
	READ_NODE_FIELD(beforeStmtLists);
	READ_NODE_FIELD(constraintLists);

	READ_DONE();
}

static CdbProcess *
_readCdbProcess(void)
{
	READ_LOCALS(CdbProcess);

	READ_STRING_FIELD(listenerAddr);
	READ_INT_FIELD(listenerPort);
	READ_INT_FIELD(pid);
	READ_INT_FIELD(contentid);
	READ_INT_FIELD(dbid);

	READ_DONE();
}

static ColumnDef *
_readColumnDef(void)
{
	READ_LOCALS(ColumnDef);

	READ_STRING_FIELD(colname);
	READ_NODE_FIELD(typeName);
	READ_STRING_FIELD(compression);
	READ_INT_FIELD(inhcount);
	READ_BOOL_FIELD(is_local);
	READ_BOOL_FIELD(is_not_null);
	READ_BOOL_FIELD(is_from_type);
	READ_INT_FIELD(attnum);
	READ_INT_FIELD(storage);
	READ_STRING_FIELD(storage_name);
	READ_NODE_FIELD(raw_default);
	READ_NODE_FIELD(cooked_default);

	READ_BOOL_FIELD(hasCookedMissingVal);
	READ_BOOL_FIELD(missingIsNull);
	if (local_node->hasCookedMissingVal && !local_node->missingIsNull)
		local_node->missingVal = readDatum(false);

	READ_CHAR_FIELD(identity);
	READ_NODE_FIELD(identitySequence);
	READ_CHAR_FIELD(generated);
	READ_NODE_FIELD(collClause);
	READ_OID_FIELD(collOid);
	READ_NODE_FIELD(constraints);
	READ_NODE_FIELD(encoding);
	READ_NODE_FIELD(fdwoptions);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static ColumnRef *
_readColumnRef(void)
{
	READ_LOCALS(ColumnRef);

	READ_NODE_FIELD(fields);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static ParamRef *
_readParamRef(void)
{
	READ_LOCALS(ParamRef);

	READ_INT_FIELD(number);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static ClusterStmt *
_readClusterStmt(void)
{
	READ_LOCALS(ClusterStmt);

	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(indexname);

	READ_DONE();
}


static ColumnReferenceStorageDirective *
_readColumnReferenceStorageDirective(void)
{
	READ_LOCALS(ColumnReferenceStorageDirective);

	READ_STRING_FIELD(column);
	READ_BOOL_FIELD(deflt);
	READ_NODE_FIELD(encoding);

	READ_DONE();
}

static CompositeTypeStmt *
_readCompositeTypeStmt(void)
{
	READ_LOCALS(CompositeTypeStmt);

	READ_NODE_FIELD(typevar);
	READ_NODE_FIELD(coldeflist);

	READ_DONE();
}

/*
 * _readConstraint
 */
static Constraint *
_readConstraint(void)
{
	READ_LOCALS(Constraint);

	READ_ENUM_FIELD(contype, ConstrType);
	READ_STRING_FIELD(conname);			/* name, or NULL if unnamed */
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_LOCATION_FIELD(location);

	READ_BOOL_FIELD(is_no_inherit);
	READ_NODE_FIELD(raw_expr);
	READ_STRING_FIELD(cooked_expr);
	READ_CHAR_FIELD(generated_when);
	READ_BOOL_FIELD(nulls_not_distinct);

	READ_NODE_FIELD(keys);
	READ_NODE_FIELD(including);

	READ_NODE_FIELD(exclusions);

	READ_NODE_FIELD(options);
	READ_STRING_FIELD(indexname);
	READ_STRING_FIELD(indexspace);
	READ_BOOL_FIELD(reset_default_tblspc);

	READ_STRING_FIELD(access_method);
	READ_NODE_FIELD(where_clause);

	READ_NODE_FIELD(pktable);
	READ_NODE_FIELD(fk_attrs);
	READ_NODE_FIELD(pk_attrs);
	READ_CHAR_FIELD(fk_matchtype);
	READ_CHAR_FIELD(fk_upd_action);
	READ_CHAR_FIELD(fk_del_action);
	READ_NODE_FIELD(old_conpfeqop);
	READ_OID_FIELD(old_pktable_oid);

	READ_BOOL_FIELD(skip_validation);
	READ_BOOL_FIELD(initially_valid);

	READ_DONE();
}

static ConstraintsSetStmt *
_readConstraintsSetStmt(void)
{
	READ_LOCALS(ConstraintsSetStmt);

	READ_NODE_FIELD(constraints);
	READ_BOOL_FIELD(deferred);

	READ_DONE();
}

static CopyIntoClause *
_readCopyIntoClause(void)
{
	READ_LOCALS(CopyIntoClause);

	READ_NODE_FIELD(attlist);
	READ_BOOL_FIELD(is_program);
	READ_STRING_FIELD(filename);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CreateCastStmt *
_readCreateCastStmt(void)
{
	READ_LOCALS(CreateCastStmt);

	READ_NODE_FIELD(sourcetype);
	READ_NODE_FIELD(targettype);
	READ_NODE_FIELD(func);
	READ_ENUM_FIELD(context, CoercionContext);
	READ_BOOL_FIELD(inout);

	READ_DONE();
}

static CreateConversionStmt *
_readCreateConversionStmt(void)
{
	READ_LOCALS(CreateConversionStmt);

	READ_NODE_FIELD(conversion_name);
	READ_STRING_FIELD(for_encoding_name);
	READ_STRING_FIELD(to_encoding_name);
	READ_NODE_FIELD(func_name);
	READ_BOOL_FIELD(def);

	READ_DONE();
}

static CreateDomainStmt *
_readCreateDomainStmt(void)
{
	READ_LOCALS(CreateDomainStmt);

	READ_NODE_FIELD(domainname);
	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(collClause);
	READ_NODE_FIELD(constraints);

	READ_DONE();
}

static void
_readDropStmt_common(DropStmt *local_node)
{
	READ_TEMP_LOCALS();

	READ_NODE_FIELD(objects);
	READ_ENUM_FIELD(removeType,ObjectType);
	READ_ENUM_FIELD(behavior,DropBehavior);
	READ_BOOL_FIELD(missing_ok);
	READ_BOOL_FIELD(concurrent);
	READ_BOOL_FIELD(isdynamic);

	/* Force 'missing_ok' in QEs */
#ifdef COMPILING_BINARY_FUNCS
	local_node->missing_ok=true;
#endif /* COMPILING_BINARY_FUNCS */
}

static DropDirectoryTableStmt *
_readDropDirectoryTableStmt(void)
{
	READ_LOCALS(DropDirectoryTableStmt);

	_readDropStmt_common(&local_node->base);

	READ_BOOL_FIELD(with_content);

	READ_DONE();
}

static CreateEnumStmt *
_readCreateEnumStmt(void)
{
	READ_LOCALS(CreateEnumStmt);

	READ_NODE_FIELD(typeName);
	READ_NODE_FIELD(vals);

	READ_DONE();
}

static CreateExternalStmt *
_readCreateExternalStmt(void)
{
	READ_LOCALS(CreateExternalStmt);

	READ_NODE_FIELD(relation);
	READ_NODE_FIELD(tableElts);
	READ_NODE_FIELD(exttypedesc);
	READ_STRING_FIELD(format);
	READ_NODE_FIELD(formatOpts);
	READ_BOOL_FIELD(isweb);
	READ_BOOL_FIELD(iswritable);
	READ_NODE_FIELD(sreh);
	READ_NODE_FIELD(extOptions);
	READ_NODE_FIELD(encoding);
	READ_NODE_FIELD(distributedBy);
	READ_NODE_FIELD(tags);

	READ_DONE();
}

static CreateFunctionStmt *
_readCreateFunctionStmt(void)
{
	READ_LOCALS(CreateFunctionStmt);

	READ_BOOL_FIELD(is_procedure);
	READ_BOOL_FIELD(replace);
	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(parameters);
	READ_NODE_FIELD(returnType);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(sql_body);

	READ_DONE();
}

static CreateOpClassItem *
_readCreateOpClassItem(void)
{
	READ_LOCALS(CreateOpClassItem);
	READ_INT_FIELD(itemtype);
	READ_NODE_FIELD(name);
	READ_INT_FIELD(number);
	READ_NODE_FIELD(order_family);
	READ_NODE_FIELD(class_args);
	READ_NODE_FIELD(storedtype);

	READ_DONE();
}


static CreateOpClassStmt *
_readCreateOpClassStmt(void)
{
	READ_LOCALS(CreateOpClassStmt);

	READ_NODE_FIELD(opclassname);
	READ_NODE_FIELD(opfamilyname);
	READ_STRING_FIELD(amname);
	READ_NODE_FIELD(datatype);
	READ_NODE_FIELD(items);
	READ_BOOL_FIELD(isDefault);

	READ_DONE();
}

static CreateOpFamilyStmt *
_readCreateOpFamilyStmt(void)
{
	READ_LOCALS(CreateOpFamilyStmt);
	READ_NODE_FIELD(opfamilyname);
	READ_STRING_FIELD(amname);

	READ_DONE();
}

static CreatePLangStmt *
_readCreatePLangStmt(void)
{
	READ_LOCALS(CreatePLangStmt);

	READ_BOOL_FIELD(replace);
	READ_STRING_FIELD(plname);
	READ_NODE_FIELD(plhandler);
	READ_NODE_FIELD(plinline);
	READ_NODE_FIELD(plvalidator);
	READ_BOOL_FIELD(pltrusted);

	READ_DONE();
}

static CreatePolicyStmt *
_readCreatePolicyStmt()
{
	READ_LOCALS(CreatePolicyStmt);

	READ_STRING_FIELD(policy_name);
	READ_NODE_FIELD(table);
	READ_STRING_FIELD(cmd_name);
	READ_BOOL_FIELD(permissive);
	READ_NODE_FIELD(roles);
	READ_NODE_FIELD(qual);
	READ_NODE_FIELD(with_check);

	READ_DONE();
}

static CreatePublicationStmt *
_readCreatePublicationStmt()
{
	READ_LOCALS(CreatePublicationStmt);

	READ_STRING_FIELD(pubname);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(pubobjects);
	READ_BOOL_FIELD(for_all_tables);

	READ_DONE();
}

static CreateRoleStmt *
_readCreateRoleStmt(void)
{
	READ_LOCALS(CreateRoleStmt);

	READ_ENUM_FIELD(stmt_type, RoleStmtType);
	READ_STRING_FIELD(role);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(tags);

	READ_DONE();
}

static CreateProfileStmt *
_readCreateProfileStmt(void)
{
	READ_LOCALS(CreateProfileStmt);

	READ_STRING_FIELD(profile_name);
	READ_NODE_FIELD(options);

	READ_DONE();
}

static CreateSchemaStmt *
_readCreateSchemaStmt(void)
{
	READ_LOCALS(CreateSchemaStmt);

	READ_STRING_FIELD(schemaname);
	READ_NODE_FIELD(authrole);
	local_node->schemaElts = 0;
	READ_BOOL_FIELD(istemp);
	READ_BOOL_FIELD(pop_search_path);
	READ_NODE_FIELD(tags);

	READ_DONE();
}

static AlterSchemaStmt *
_readAlterSchemaStmt(void)
{
	READ_LOCALS(AlterSchemaStmt);
	
	READ_STRING_FIELD(schemaname);
	READ_NODE_FIELD(tags);
	READ_BOOL_FIELD(unsettag);
	
	READ_DONE();
}

static CreateTagStmt *
_readCreateTagStmt(void)
{
	READ_LOCALS(CreateTagStmt);
	
	READ_STRING_FIELD(tag_name);
	READ_BOOL_FIELD(missing_ok);
	READ_NODE_FIELD(allowed_values);
	
	READ_DONE();
}

static AlterTagStmt *
_readAlterTagStmt(void)
{
	READ_LOCALS(AlterTagStmt);
	
	READ_STRING_FIELD(tag_name);
	READ_INT_FIELD(action);
	READ_NODE_FIELD(tag_values);
	READ_BOOL_FIELD(missing_ok);
	READ_BOOL_FIELD(unset);
	
	READ_DONE();
}

static DropTagStmt *
_readDropTagStmt(void)
{
	READ_LOCALS(DropTagStmt);
	
	READ_NODE_FIELD(tags);
	READ_BOOL_FIELD(missing_ok);
	
	READ_DONE();
}

static CreateSeqStmt *
_readCreateSeqStmt(void)
{
	READ_LOCALS(CreateSeqStmt);
	READ_NODE_FIELD(sequence);
	READ_NODE_FIELD(options);
	READ_OID_FIELD(ownerId);
	READ_BOOL_FIELD(for_identity);
	READ_BOOL_FIELD(if_not_exists);
	READ_NODE_FIELD(tags);

	READ_DONE();
}

static CreateSubscriptionStmt *
_readCreateSubscriptionStmt()
{
	READ_LOCALS(CreateSubscriptionStmt);

	READ_STRING_FIELD(subname);
	READ_STRING_FIELD(conninfo);
	READ_NODE_FIELD(publication);
	READ_NODE_FIELD(options);

	/*
	 * conninfo can be an empty string, but the serialization
	 * doesn't distinguish an empty string from NULL. The
	 * code that executes the command in't prepared for a NULL.
	 */
	if (local_node->conninfo == NULL)
		local_node->conninfo = pstrdup("");

	READ_DONE();
}

static CreateTransformStmt *
_readCreateTransformStmt()
{
	READ_LOCALS(CreateTransformStmt);

	READ_BOOL_FIELD(replace);
	READ_NODE_FIELD(type_name);
	READ_STRING_FIELD(lang);
	READ_NODE_FIELD(fromsql);
	READ_NODE_FIELD(tosql);

	READ_DONE();
}

static CreatedbStmt *
_readCreatedbStmt(void)
{
	READ_LOCALS(CreatedbStmt);

	READ_STRING_FIELD(dbname);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(tags);

	READ_DONE();
}

static CursorPosInfo *
_readCursorPosInfo(void)
{
	READ_LOCALS(CursorPosInfo);

	READ_STRING_FIELD(cursor_name);
	READ_INT_FIELD(gp_segment_id);
	READ_UINT_FIELD(ctid.ip_blkid.bi_hi);
	READ_UINT_FIELD(ctid.ip_blkid.bi_lo);
	READ_UINT_FIELD(ctid.ip_posid);
	READ_OID_FIELD(table_oid);

	READ_DONE();
}

static DQAExpr*
_readDQAExpr(void)
{
    READ_LOCALS(DQAExpr);

    READ_INT_FIELD(agg_expr_id);
    READ_BITMAPSET_FIELD(agg_args_id_bms);
    READ_NODE_FIELD(agg_filter);

    READ_DONE();
}

static DefineStmt *
_readDefineStmt(void)
{
	READ_LOCALS(DefineStmt);
	READ_ENUM_FIELD(kind, ObjectType);
	READ_BOOL_FIELD(oldstyle);
	READ_NODE_FIELD(defnames);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(definition);
	READ_BOOL_FIELD(if_not_exists);
	READ_BOOL_FIELD(replace);
	READ_BOOL_FIELD(trusted);   /* CDB */

	READ_DONE();
}

static DenyLoginInterval *
_readDenyLoginInterval(void)
{
	READ_LOCALS(DenyLoginInterval);

	READ_NODE_FIELD(start);
	READ_NODE_FIELD(end);

	READ_DONE();
}

static DenyLoginPoint *
_readDenyLoginPoint(void)
{
	READ_LOCALS(DenyLoginPoint);

	READ_NODE_FIELD(day);
	READ_NODE_FIELD(time);

	READ_DONE();
}

static DistributionKeyElem *
_readDistributionKeyElem(void)
{
	READ_LOCALS(DistributionKeyElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(opclass);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static DropRoleStmt *
_readDropRoleStmt(void)
{
	READ_LOCALS(DropRoleStmt);

	READ_NODE_FIELD(roles);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static DropProfileStmt *
_readDropProfileStmt(void)
{
	READ_LOCALS(DropProfileStmt);

	READ_NODE_FIELD(profiles);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static DropStmt *
_readDropStmt(void)
{
	READ_LOCALS(DropStmt);

	READ_NODE_FIELD(objects);
	READ_ENUM_FIELD(removeType,ObjectType);
	READ_ENUM_FIELD(behavior,DropBehavior);
	READ_BOOL_FIELD(missing_ok);
	READ_BOOL_FIELD(concurrent);
	READ_BOOL_FIELD(isdynamic);

	/* Force 'missing_ok' in QEs */
#ifdef COMPILING_BINARY_FUNCS
	local_node->missing_ok=true;
#endif /* COMPILING_BINARY_FUNCS */

	READ_DONE();
}

static DropSubscriptionStmt *
_readDropSubscriptionStmt()
{
	READ_LOCALS(DropSubscriptionStmt);

	READ_STRING_FIELD(subname);
	READ_BOOL_FIELD(missing_ok);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

static DropdbStmt *
_readDropdbStmt(void)
{
	READ_LOCALS(DropdbStmt);

	READ_STRING_FIELD(dbname);
	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static ExtTableTypeDesc *
_readExtTableTypeDesc(void)
{
	READ_LOCALS(ExtTableTypeDesc);

	READ_ENUM_FIELD(exttabletype, ExtTableType);
	READ_NODE_FIELD(location_list);
	READ_NODE_FIELD(on_clause);
	READ_STRING_FIELD(command_string);

	READ_DONE();
}

/*
 * _readFuncCall
 *
 * This parsenode is transformed during parse_analyze.
 * It not stored in views = no upgrade implication for changes
 */
static FuncCall *
_readFuncCall(void)
{
	READ_LOCALS(FuncCall);

	READ_NODE_FIELD(funcname);
	READ_NODE_FIELD(args);
	READ_NODE_FIELD(agg_order);
	READ_NODE_FIELD(agg_filter);
	READ_NODE_FIELD(over);
	READ_BOOL_FIELD(agg_within_group);
	READ_BOOL_FIELD(agg_star);
	READ_BOOL_FIELD(agg_distinct);
	READ_BOOL_FIELD(func_variadic);
	READ_ENUM_FIELD(funcformat, CoercionForm);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static FunctionParameter *
_readFunctionParameter(void)
{
	READ_LOCALS(FunctionParameter);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(argType);
	READ_ENUM_FIELD(mode, FunctionParameterMode);
	READ_NODE_FIELD(defexpr);

	READ_DONE();
}

static GrantRoleStmt *
_readGrantRoleStmt(void)
{
	READ_LOCALS(GrantRoleStmt);

	READ_NODE_FIELD(granted_roles);
	READ_NODE_FIELD(grantee_roles);
	READ_BOOL_FIELD(is_grant);
	READ_NODE_FIELD(opt);
	READ_NODE_FIELD(grantor);
	READ_ENUM_FIELD(behavior, DropBehavior);
    Assert(local_node->behavior <= DROP_CASCADE);

	READ_DONE();
}

static GrantStmt *
_readGrantStmt(void)
{
	READ_LOCALS(GrantStmt);

	READ_BOOL_FIELD(is_grant);
	READ_ENUM_FIELD(targtype,GrantTargetType);
	READ_ENUM_FIELD(objtype,ObjectType);
	READ_NODE_FIELD(objects);
	READ_NODE_FIELD(privileges);
	READ_NODE_FIELD(grantees);
	READ_BOOL_FIELD(grant_option);
	READ_ENUM_FIELD(behavior, DropBehavior);

	READ_DONE();
}

/*
 * _readGroupId
 */
static GroupId *
_readGroupId(void)
{
	READ_LOCALS(GroupId);

	READ_INT_FIELD(agglevelsup);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readGroupingSetId
 */
static GroupingSetId *
_readGroupingSetId(void)
{
	READ_LOCALS(GroupingSetId);

	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static IndexElem *
_readIndexElem(void)
{
	READ_LOCALS(IndexElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(expr);
	READ_STRING_FIELD(indexcolname);
	READ_NODE_FIELD(collation);
	READ_NODE_FIELD(opclass);
	READ_NODE_FIELD(opclassopts);
	READ_ENUM_FIELD(ordering, SortByDir);
	READ_ENUM_FIELD(nulls_ordering, SortByNulls);

	READ_DONE();
}

static IndexStmt *
_readIndexStmt(void)
{
	READ_LOCALS(IndexStmt);

	READ_STRING_FIELD(idxname);
	READ_NODE_FIELD(relation);
	READ_OID_FIELD(relationOid);
	READ_STRING_FIELD(accessMethod);
	READ_STRING_FIELD(tableSpace);
	READ_NODE_FIELD(indexParams);
	READ_NODE_FIELD(indexIncludingParams);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(whereClause);
	READ_NODE_FIELD(excludeOpNames);
	READ_STRING_FIELD(idxcomment);
	READ_OID_FIELD(indexOid);
	READ_OID_FIELD(oldNumber);
	READ_UINT_FIELD(oldCreateSubid);
	READ_UINT_FIELD(oldFirstRelfilelocatorSubid);
	READ_BOOL_FIELD(unique);
	READ_BOOL_FIELD(nulls_not_distinct);
	READ_BOOL_FIELD(primary);
	READ_BOOL_FIELD(isconstraint);
	READ_BOOL_FIELD(deferrable);
	READ_BOOL_FIELD(initdeferred);
	READ_BOOL_FIELD(transformed);
	READ_BOOL_FIELD(concurrent);
	READ_BOOL_FIELD(if_not_exists);
	READ_BOOL_FIELD(reset_default_tblspc);
	READ_ENUM_FIELD(concurrentlyPhase,IndexConcurrentlyPhase);
	READ_OID_FIELD(indexRelationOid);
	READ_NODE_FIELD(tags);

	READ_DONE();
}

static LockStmt *
_readLockStmt(void)
{
	READ_LOCALS(LockStmt);

	READ_NODE_FIELD(relations);
	READ_INT_FIELD(mode);
	READ_BOOL_FIELD(nowait);

	READ_DONE();
}

static NewColumnValue *
_readNewColumnValue(void)
{
	READ_LOCALS(NewColumnValue);

	READ_INT_FIELD(attnum);
	READ_NODE_FIELD(expr);
	/* can't serialize exprstate */
	READ_BOOL_FIELD(is_generated);

	READ_DONE();
}

static NewConstraint *
_readNewConstraint(void)
{
	READ_LOCALS(NewConstraint);

	READ_STRING_FIELD(name);
	READ_ENUM_FIELD(contype, ConstrType);
	READ_OID_FIELD(refrelid);
	READ_OID_FIELD(refindid);
	READ_OID_FIELD(conid);
	READ_NODE_FIELD(qual);
	/* can't serialize qualstate */

	READ_DONE();
}

static ObjectWithArgs *
_readObjectWithArgs(void)
{
	READ_LOCALS(ObjectWithArgs);

	READ_NODE_FIELD(objname);
	READ_NODE_FIELD(objargs);
	READ_BOOL_FIELD(args_unspecified);

	READ_DONE();
}

static PartitionCmd *
_readPartitionCmd(void)
{
	READ_LOCALS(PartitionCmd);

	READ_NODE_FIELD(name);
	READ_NODE_FIELD(bound);

	READ_DONE();
}

#ifdef COMPILING_BINARY_FUNCS
static GpAlterPartitionId *
_readGpAlterPartitionId(void)
{
	READ_LOCALS(GpAlterPartitionId);

	READ_ENUM_FIELD(idtype, GpAlterPartitionIdType);
	READ_NODE_FIELD(partiddef);

	READ_DONE();
}

static GpAlterPartitionCmd *
_readGpAlterPartitionCmd(void)
{
	READ_LOCALS(GpAlterPartitionCmd);

	READ_NODE_FIELD(partid);
	READ_NODE_FIELD(arg);

	READ_DONE();
}
#endif

static PartitionElem *
_readPartitionElem(void)
{
	READ_LOCALS(PartitionElem);

	READ_STRING_FIELD(name);
	READ_NODE_FIELD(expr);
	READ_NODE_FIELD(collation);
	READ_NODE_FIELD(opclass);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static PartitionSpec *
_readPartitionSpec(void)
{
	READ_LOCALS(PartitionSpec);

	READ_ENUM_FIELD(strategy, PartitionStrategy);
	READ_NODE_FIELD(partParams);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static RefreshClause *
_readRefreshClause(void)
{
	READ_LOCALS(RefreshClause);

	READ_BOOL_FIELD(concurrent);
	READ_BOOL_FIELD(skipData);
	READ_NODE_FIELD(relation);

	READ_DONE();
}

static ReindexStmt *
_readReindexStmt(void)
{
	READ_LOCALS(ReindexStmt);

	READ_ENUM_FIELD(kind,ReindexObjectType);
	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(name);
	READ_NODE_FIELD(params);
	READ_OID_FIELD(relid);
	READ_ENUM_FIELD(concurrentlyPhase,ReindexConcurrentlyPhase);
	READ_NODE_FIELD(newIndexInfo);
	READ_NODE_FIELD(oldIndexInfo);

	READ_DONE();
}

static ReindexIndexInfo *
_readReindexIndexInfo(void)
{
	READ_LOCALS(ReindexIndexInfo);

	READ_OID_FIELD(indexId);
	READ_OID_FIELD(tableId);
	READ_OID_FIELD(amId);
	READ_BOOL_FIELD(safe);
	READ_STRING_FIELD(ccNewName);
	READ_STRING_FIELD(ccOldName);

	READ_DONE();
}

static RenameStmt *
_readRenameStmt(void)
{
	READ_LOCALS(RenameStmt);

	READ_ENUM_FIELD(renameType, ObjectType);
	READ_ENUM_FIELD(relationType, ObjectType);
	READ_NODE_FIELD(relation);
	READ_OID_FIELD(objid);
	READ_NODE_FIELD(object);
	READ_STRING_FIELD(subname);
	READ_STRING_FIELD(newname);
	READ_ENUM_FIELD(behavior,DropBehavior);

	READ_BOOL_FIELD(missing_ok);

	READ_DONE();
}

static ReplicaIdentityStmt *
_readReplicaIdentityStmt(void)
{
	READ_LOCALS(ReplicaIdentityStmt);

	READ_CHAR_FIELD(identity_type);
	READ_STRING_FIELD(name);

	READ_DONE();
}

static RestrictInfo *
_readRestrictInfo(void)
{
	READ_LOCALS(RestrictInfo);

	/* NB: this isn't a complete set of fields */
	READ_NODE_FIELD(clause);
	READ_BOOL_FIELD(is_pushed_down);
	READ_BOOL_FIELD(can_join);
	READ_BOOL_FIELD(pseudoconstant);
	READ_BOOL_FIELD(has_clone);
	READ_BOOL_FIELD(is_clone);
	READ_BOOL_FIELD(leakproof);
	READ_ENUM_FIELD(has_volatile, VolatileFunctionStatus);
	READ_UINT_FIELD(security_level);
	READ_INT_FIELD(num_base_rels);
	READ_BOOL_FIELD(contain_outer_query_references);
	READ_BITMAPSET_FIELD(clause_relids);
	READ_BITMAPSET_FIELD(required_relids);
	READ_BITMAPSET_FIELD(incompatible_relids);
	READ_BITMAPSET_FIELD(outer_relids);
	READ_BITMAPSET_FIELD(left_relids);
	READ_BITMAPSET_FIELD(right_relids);
	READ_NODE_FIELD(orclause);

	READ_INT_FIELD(rinfo_serial);
	READ_FLOAT_FIELD(eval_cost.startup);
	READ_FLOAT_FIELD(eval_cost.per_tuple);
	READ_FLOAT_FIELD(norm_selec);
	READ_FLOAT_FIELD(outer_selec);
	READ_NODE_FIELD(mergeopfamilies);

	READ_NODE_FIELD(left_em);
	READ_NODE_FIELD(right_em);
	READ_BOOL_FIELD(outer_is_left);
	READ_OID_FIELD(hashjoinoperator);
	READ_FLOAT_FIELD(left_bucketsize);
	READ_FLOAT_FIELD(left_mcvfreq);
	READ_FLOAT_FIELD(right_mcvfreq);
	READ_OID_FIELD(left_hasheqoperator);
	READ_OID_FIELD(right_hasheqoperator);
	READ_OID_FIELD(hasheqoperator);

	READ_DONE();
}

static RuleStmt *
_readRuleStmt(void)
{
	READ_LOCALS(RuleStmt);

	READ_NODE_FIELD(relation);
	READ_STRING_FIELD(rulename);
	READ_NODE_FIELD(whereClause);
	READ_ENUM_FIELD(event,CmdType);
	READ_BOOL_FIELD(instead);
	READ_NODE_FIELD(actions);
	READ_BOOL_FIELD(replace);

	READ_DONE();
}

static SegfileMapNode *
_readSegfileMapNode(void)
{
	READ_LOCALS(SegfileMapNode);

	READ_OID_FIELD(relid);
	READ_INT_FIELD(segno);

	READ_DONE();
}

/*
 * _readSingleRowErrorDesc
 */
static SingleRowErrorDesc *
_readSingleRowErrorDesc(void)
{
	READ_LOCALS(SingleRowErrorDesc);

	READ_INT_FIELD(rejectlimit);
	READ_BOOL_FIELD(is_limit_in_rows);
	READ_CHAR_FIELD(log_error_type);

	READ_DONE();
}

static SliceTable *
_readSliceTable(void)
{
	READ_LOCALS(SliceTable);

	READ_INT_FIELD(localSlice);
	READ_INT_FIELD(numSlices);
	local_node->slices = palloc0(local_node->numSlices * sizeof(ExecSlice));
	for (int i = 0; i < local_node->numSlices; i++)
	{
		READ_INT_FIELD(slices[i].sliceIndex);
		READ_INT_FIELD(slices[i].rootIndex);
		READ_INT_FIELD(slices[i].parentIndex);
		READ_INT_FIELD(slices[i].planNumSegments);
		READ_NODE_FIELD(slices[i].children); /* List of int index */
		READ_ENUM_FIELD(slices[i].gangType, GangType);
		READ_NODE_FIELD(slices[i].segments); /* List of int index */
		READ_BOOL_FIELD(slices[i].useMppParallelMode);
		READ_INT_FIELD(slices[i].parallel_workers);
		local_node->slices[i].primaryGang = NULL;
		READ_NODE_FIELD(slices[i].primaryProcesses); /* List of (CDBProcess *) */
		READ_BITMAPSET_FIELD(slices[i].processesMap);
	}
	READ_BOOL_FIELD(hasMotions);

	READ_INT_FIELD(instrument_options);
	READ_INT_FIELD(ic_instance_id);

	READ_DONE();
}

static SortBy *
_readSortBy(void)
{
	READ_LOCALS(SortBy);

	READ_NODE_FIELD(node);
	READ_ENUM_FIELD(sortby_dir, SortByDir);
	READ_ENUM_FIELD(sortby_nulls, SortByNulls);
	READ_NODE_FIELD(useOp);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readTableFunctionScan
 */
static TableFunctionScan *
_readTableFunctionScan(void)
{
	READ_LOCALS(TableFunctionScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(function);

	READ_DONE();
}

static TableValueExpr *
_readTableValueExpr(void)
{
	READ_LOCALS(TableValueExpr);

	READ_NODE_FIELD(subquery);

	READ_DONE();
}

static TruncateStmt *
_readTruncateStmt(void)
{
	READ_LOCALS(TruncateStmt);

	READ_NODE_FIELD(relations);
	READ_ENUM_FIELD(behavior,DropBehavior);

	READ_DONE();
}

static TupleSplit *
_readTupleSplit(void)
{
	READ_LOCALS(TupleSplit);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numCols);
	READ_ATTRNUMBER_ARRAY(grpColIdx, local_node->numCols);

	READ_NODE_FIELD(dqa_expr_lst);

	READ_DONE();
}

static TypeCast *
_readTypeCast(void)
{
	READ_LOCALS(TypeCast);

	READ_NODE_FIELD(arg);
	READ_NODE_FIELD(typeName);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static TypeName *
_readTypeName(void)
{
	READ_LOCALS(TypeName);

	READ_NODE_FIELD(names);
	READ_OID_FIELD(typeOid);
	READ_BOOL_FIELD(setof);
	READ_BOOL_FIELD(pct_type);
	READ_NODE_FIELD(typmods);
	READ_INT_FIELD(typemod);
	READ_NODE_FIELD(arrayBounds);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

static VacuumRelation *
_readVacuumRelation(void)
{
	READ_LOCALS(VacuumRelation);

	READ_NODE_FIELD(relation);
	READ_OID_FIELD(oid);
	READ_NODE_FIELD(va_cols);

	READ_DONE();
}


/*
 * _readVacuumStmt
 */
static VacuumStmt *
_readVacuumStmt(void)
{
	READ_LOCALS(VacuumStmt);

	READ_NODE_FIELD(options);
	READ_NODE_FIELD(rels);
	READ_BOOL_FIELD(is_vacuumcmd);

	READ_DONE();
}


static VariableSetStmt *
_readVariableSetStmt(void)
{
	READ_LOCALS(VariableSetStmt);

	READ_STRING_FIELD(name);
	READ_ENUM_FIELD(kind, VariableSetKind);
	READ_NODE_FIELD(args);
	READ_BOOL_FIELD(is_local);

	READ_DONE();
}

static ViewStmt *
_readViewStmt(void)
{
	READ_LOCALS(ViewStmt);

	READ_NODE_FIELD(view);
	READ_NODE_FIELD(aliases);
	READ_NODE_FIELD(query);
	READ_BOOL_FIELD(replace);
	READ_NODE_FIELD(options);
	READ_NODE_FIELD(tags);

	READ_DONE();
}

static WithClause *
_readWithClause(void)
{
	READ_LOCALS(WithClause);

	READ_NODE_FIELD(ctes);
	READ_BOOL_FIELD(recursive);
	READ_LOCATION_FIELD(location);

	READ_DONE();
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

/*
 * _readMemoize
 */
static Memoize *
_readMemoize(void)
{
	READ_LOCALS(Memoize);

	ReadCommonPlan(&local_node->plan);

	READ_INT_FIELD(numKeys);
	READ_OID_ARRAY(hashOperators, local_node->numKeys);
	READ_OID_ARRAY(collations, local_node->numKeys);
	READ_NODE_FIELD(param_exprs);
	READ_BOOL_FIELD(singlerow);
	READ_BOOL_FIELD(binary_mode);
	READ_UINT_FIELD(est_entries);
	READ_BITMAPSET_FIELD(keyparamids);

	READ_DONE();
}

/*
 * _readTidRangeScan
 */
static TidRangeScan *
_readTidRangeScan(void)
{
	READ_LOCALS(TidRangeScan);

	ReadCommonScan(&local_node->scan);

	READ_NODE_FIELD(tidrangequals);

	READ_DONE();
}

/*
 * _readCTESearchClause
 */
static CTESearchClause *
_readCTESearchClause(void)
{
	READ_LOCALS(CTESearchClause);

	READ_NODE_FIELD(search_col_list);
	READ_BOOL_FIELD(search_breadth_first);
	READ_STRING_FIELD(search_seq_column);
	READ_LOCATION_FIELD(location);

	READ_DONE();
}

/*
 * _readCTECycleClause
 */
static CTECycleClause *
_readCTECycleClause(void)
{
	READ_LOCALS(CTECycleClause);

	READ_NODE_FIELD(cycle_col_list);
	READ_STRING_FIELD(cycle_mark_column);
	READ_NODE_FIELD(cycle_mark_value);
	READ_NODE_FIELD(cycle_mark_default);
	READ_STRING_FIELD(cycle_path_column);
	READ_LOCATION_FIELD(location);
	READ_OID_FIELD(cycle_mark_type);
	READ_INT_FIELD(cycle_mark_typmod);
	READ_OID_FIELD(cycle_mark_collation);
	READ_OID_FIELD(cycle_mark_neop);

	READ_DONE();
}

static ReturnStmt *
_readReturnStmt(void)
{
	READ_LOCALS(ReturnStmt);

	READ_NODE_FIELD(returnval);

	READ_DONE();
}
