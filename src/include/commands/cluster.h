/*-------------------------------------------------------------------------
 *
 * cluster.h
 *	  header file for postgres cluster command stuff
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/cluster.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_H
#define CLUSTER_H

#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "storage/lock.h"
#include "utils/relcache.h"


/* flag bits for ClusterParams->flags */
#define CLUOPT_RECHECK 0x01		/* recheck relation state */
#define CLUOPT_VERBOSE 0x02		/* print progress info */

/* options for CLUSTER */
typedef struct ClusterParams
{
	bits32		options;		/* bitmask of CLUOPT_* */
} ClusterParams;

extern void cluster(ParseState *pstate, ClusterStmt *stmt, bool isTopLevel);
extern bool cluster_rel(Oid tableOid, Oid indexOid, ClusterParams *params);
extern void check_index_is_clusterable(Relation OldHeap, Oid indexOid,
									   bool recheck, LOCKMODE lockmode);
extern void mark_index_clustered(Relation rel, Oid indexOid, bool is_internal);

extern Oid make_new_heap(Oid OIDOldHeap, Oid NewTableSpace, Oid NewAccessMethod,
						 char relpersistence,
						 LOCKMODE lockmode,
						 bool createAoBlockDirectory,
						 bool makeCdbPolicy);
extern void finish_heap_swap(Oid OIDOldHeap, Oid OIDNewHeap,
							 bool is_system_catalog,
							 bool swap_toast_by_content,
							 bool swap_stats,
							 bool check_constraints,
							 bool is_internal,
							 TransactionId frozenXid,
							 MultiXactId minMulti,
							 char newrelpersistence);

extern void swap_relation_files(Oid r1, Oid r2, bool target_is_pg_class,
					bool swap_toast_by_content,
					bool swap_stats,
					bool is_internal,
					TransactionId frozenXid,
					MultiXactId frozenMulti,
					Oid *mapped_tables);

#endif							/* CLUSTER_H */
