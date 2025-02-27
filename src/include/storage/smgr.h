/*-------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "lib/ilist.h"
#include "storage/block.h"
#include "storage/relfilenode.h"
#include "storage/dbdirnode.h"
#include "storage/fd.h"
#include "utils/relcache.h"

/*
 * Extension can register a new storage manager(smgr) by implementing
 * its own struct f_smgr.
 * Each storage manager has a unique implementation id, which is used
 * to identify the smgr.
 *
 * The smgr_register function is used to register the smgr. it will
 * check if the smgr is already registered, if so, return SMGR_INVALID.
 * otherwise, register the smgr and return the implementation id.
 *
 * The smgr id of each relation is fixed and cannot be changed. because the
 * type of smgr is logged in the commit log and wal log. If the value of
 * smgr is changed, data corruption may occur.
 *
 * How to ensure that the smgr of each extension does not conflict? One way
 * is to predefine the smgr id used by the extension in Cloudberry; the other
 * way is to refer to the practice of custom rmgr and provide a wiki page
 * [https://wiki.postgresql.org/wiki/CustomWALResourceManagers] to record
 * the use of smgr to avoid conflicts.
 *
 * FIXME: For PAX_AM_OID, Cloudberrydb reserves this value for ORCA, a
 * predefined value is used here to reserve the smgr id for PAX_AM_OID.
 */
typedef enum SMgrImplementation
{
	SMGR_INVALID = -1,
	SMGR_MD = 0,
	SMGR_AO = 1,
	SMGR_PAX = 2,
} SMgrImpl;

struct f_smgr;

/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrclose().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.
 * (But smgrclose() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may have an "owner", which is just a pointer to it from
 * somewhere else; smgr.c will clear this pointer if the SMgrRelation is
 * closed.  We use this to avoid dangling pointers from relcache to smgr
 * without having to make the smgr explicitly aware of relcache.  There
 * can't be more than one "owner" pointer per SMgrRelation, but that's
 * all we need.
 *
 * SMgrRelations that do not have an "owner" are considered to be transient,
 * and are deleted at end of transaction.
 */
typedef struct SMgrRelationData
{
	/* rnode is the hashtable lookup key, so it must be first! */
	RelFileNodeBackend smgr_rnode;	/* relation physical identifier */

	/* pointer to owning pointer, or NULL if none */
	struct SMgrRelationData **smgr_owner;

	/*
	 * The following fields are reset to InvalidBlockNumber upon a cache flush
	 * event, and hold the last known size for each fork.  This information is
	 * currently only reliable during recovery, since there is no cache
	 * invalidation for fork extension.
	 */
	BlockNumber smgr_targblock; /* current insertion target block */
	BlockNumber smgr_cached_nblocks[MAX_FORKNUM + 1];	/* last known size */

	/* additional public fields may someday exist here */

	/* copy of pg_class.relpersistence, or 0 if not known */
	char				smgr_relpersistence;
	/* pointer to storage manager */
	const struct f_smgr *smgr;
	/*pointer to AO storage manager */
	const struct f_smgr_ao *smgr_ao; 

	/*
	 * Fields below here are intended to be private to smgr.c and its
	 * submodules.  Do not touch them from elsewhere.
	 */
	SMgrImpl	smgr_which;		/* storage manager selector */

	/*
	 * for md.c; per-fork arrays of the number of open segments
	 * (md_num_open_segs) and the segments themselves (md_seg_fds).
	 */
	int			md_num_open_segs[MAX_FORKNUM + 1];
	struct _MdfdVec *md_seg_fds[MAX_FORKNUM + 1];

	/* if unowned, list link in list of all unowned SMgrRelations */
	dlist_node	node;
} SMgrRelationData;

typedef SMgrRelationData *SMgrRelation;

#define SmgrIsTemp(smgr) \
	RelFileNodeBackendIsTemp((smgr)->smgr_rnode)

/*
 *	Redefinition of storage manager here to make it accessible by other plugins(Union Store),
 * 	and we can introduce more storage managers by smgr_hook.
 */
typedef struct f_smgr
{
	const char 	*smgr_name;
	void		(*smgr_init) (void);	/* may be NULL */
	void		(*smgr_shutdown) (void);	/* may be NULL */
	void		(*smgr_open) (SMgrRelation reln);
	void		(*smgr_close) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_create) (SMgrRelation reln, ForkNumber forknum,
								bool isRedo);
	bool		(*smgr_exists) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_unlink) (RelFileNodeBackend rnode, ForkNumber forknum,
								bool isRedo);
	void		(*smgr_extend) (SMgrRelation reln, ForkNumber forknum,
								BlockNumber blocknum, char *buffer, bool skipFsync);
	bool		(*smgr_prefetch) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber blocknum);
	void		(*smgr_read) (SMgrRelation reln, ForkNumber forknum,
							  BlockNumber blocknum, char *buffer);
	void		(*smgr_write) (SMgrRelation reln, ForkNumber forknum,
							   BlockNumber blocknum, char *buffer, bool skipFsync);
	void		(*smgr_writeback) (SMgrRelation reln, ForkNumber forknum,
								   BlockNumber blocknum, BlockNumber nblocks);
	BlockNumber (*smgr_nblocks) (SMgrRelation reln, ForkNumber forknum);
	void		(*smgr_truncate) (SMgrRelation reln, ForkNumber forknum,
								  BlockNumber nblocks);
	void		(*smgr_immedsync) (SMgrRelation reln, ForkNumber forknum);
} f_smgr;

typedef struct f_smgr_ao {
	off_t			(*smgr_FileDiskSize) (File file);
	void			(*smgr_FileClose) (File file);
	int				(*smgr_FileTruncate) (File file, int64 offset, uint32 wait_event_info);
	File			(*smgr_AORelOpenSegFile) (Oid reloid, const char *filePath, int fileFlags);
	File			(*smgr_AORelOpenSegFileXlog) (RelFileNode node, int32 segmentFileNum, int fileFlags);
	int				(*smgr_FileWrite) (File file, char *buffer, int amount, off_t offset, uint32 wait_event_info);
	int				(*smgr_FileRead) (File file, char *buffer, int amount, off_t offset, uint32 wait_event_info);
	off_t			(*smgr_FileSize) (File file);
	int				(*smgr_FileSync) (File file, uint32 wait_event_info);
} f_smgr_ao;


typedef void (*smgr_init_hook_type) (void);
typedef void (*smgr_hook_type) (SMgrRelation reln, BackendId backend, SMgrImpl which, Relation rel);
typedef void (*smgr_shutdown_hook_type) (void);
extern PGDLLIMPORT smgr_init_hook_type smgr_init_hook;
extern PGDLLIMPORT smgr_hook_type smgr_hook;
extern PGDLLIMPORT smgr_shutdown_hook_type smgr_shutdown_hook;
extern bool smgr_is_heap_relation(SMgrRelation reln);

// must be registered in the shared_preload_libraries phase in extension
// we should check whether smgr and smgr_impl is valid.
extern void smgr_register(const f_smgr *smgr, SMgrImpl smgr_impl);

extern const f_smgr *smgr_get(SMgrImpl smgr_impl);

extern SMgrImpl smgr_get_impl(const Relation rel);

extern void smgrinit(void);
extern SMgrRelation smgropen(RelFileNode rnode, BackendId backend,
                             SMgrImpl smgr_which, Relation rel);
extern bool smgrexists(SMgrRelation reln, ForkNumber forknum);
extern void smgrsetowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclearowner(SMgrRelation *owner, SMgrRelation reln);
extern void smgrclose(SMgrRelation reln);
extern void smgrcloseall(void);
extern void smgrclosenode(RelFileNodeBackend rnode);
extern void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrcreate_ao(RelFileNodeBackend rnode, int32 segmentFileNum, bool isRedo);
extern void smgrdosyncall(SMgrRelation *rels, int nrels);
extern void smgrdounlinkall(SMgrRelation *rels, int nrels, bool isRedo);
extern void smgrextend(SMgrRelation reln, ForkNumber forknum,
					   BlockNumber blocknum, char *buffer, bool skipFsync);
extern bool smgrprefetch(SMgrRelation reln, ForkNumber forknum,
						 BlockNumber blocknum);
extern void smgrread(SMgrRelation reln, ForkNumber forknum,
					 BlockNumber blocknum, char *buffer);
extern void smgrwrite(SMgrRelation reln, ForkNumber forknum,
					  BlockNumber blocknum, char *buffer, bool skipFsync);
extern void smgrwriteback(SMgrRelation reln, ForkNumber forknum,
						  BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber smgrnblocks_cached(SMgrRelation reln, ForkNumber forknum);
extern void smgrtruncate(SMgrRelation reln, ForkNumber *forknum,
						 int nforks, BlockNumber *nblocks);
extern void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void AtEOXact_SMgr(void);

extern const struct f_smgr_ao * smgrAOGetDefault(void);

extern const char* smgr_get_name(SMgrImpl impl);


/*
 * Hook for plugins to collect statistics from storage functions
 * For example, disk quota extension will use these hooks to
 * detect active tables.
 */
typedef void (*file_create_hook_type)(RelFileNodeBackend rnode);
extern PGDLLIMPORT file_create_hook_type file_create_hook;

typedef void (*file_extend_hook_type)(RelFileNodeBackend rnode);
extern PGDLLIMPORT file_extend_hook_type file_extend_hook;

typedef void (*file_truncate_hook_type)(RelFileNodeBackend rnode);
extern PGDLLIMPORT file_truncate_hook_type file_truncate_hook;

typedef void (*file_unlink_hook_type)(RelFileNodeBackend rnode);
extern PGDLLIMPORT file_unlink_hook_type file_unlink_hook;

/*
 * This hook is used to get the smgr implementation id of the relation for extension.
 * If the hook is not set, the default smgr implementation id is SMGR_MD.
 * If the extension register a custom smgr to manage its own relation, it needs
 * to implement this hook and then set the smgr_impl to the correct value for
 * the relation which is managed by the extension, otherwise should ignore it.
 */
typedef void (*smgr_get_impl_hook_type)(const Relation rel, SMgrImpl* smgr_impl);
extern PGDLLIMPORT smgr_get_impl_hook_type smgr_get_impl_hook;

#endif							/* SMGR_H */
