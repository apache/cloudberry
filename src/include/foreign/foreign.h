/*-------------------------------------------------------------------------
 *
 * foreign.h
 *	  support for foreign-data wrappers, servers and user mappings.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 * src/include/foreign/foreign.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef FOREIGN_H
#define FOREIGN_H

#include "nodes/parsenodes.h"


/* Helper for obtaining username for user mapping */
#define MappingUserName(userid) \
	(OidIsValid(userid) ? GetUserNameFromId(userid, false) : "public")


typedef struct ForeignDataWrapper
{
	Oid			fdwid;			/* FDW Oid */
	Oid			owner;			/* FDW owner user Oid */
	char	   *fdwname;		/* Name of the FDW */
	Oid			fdwhandler;		/* Oid of handler function, or 0 */
	Oid			fdwvalidator;	/* Oid of validator function, or 0 */
	List	   *options;		/* fdwoptions as DefElem list */
	char		exec_location;  /* execute on MASTER, ANY or ALL SEGMENTS, Cloudberry MPP specific */
} ForeignDataWrapper;

typedef struct ForeignServer
{
	Oid			serverid;		/* server Oid */
	Oid			fdwid;			/* foreign-data wrapper */
	Oid			owner;			/* server owner user Oid */
	char	   *servername;		/* name of the server */
	char	   *servertype;		/* server type, optional */
	char	   *serverversion;	/* server version, optional */
	List	   *options;		/* srvoptions as DefElem list */
	char		exec_location;  /* execute on MASTER, ANY or ALL SEGMENTS, Cloudberry MPP specific */
	int32		num_segments;	/* the number of segments of the foreign cluster */
} ForeignServer;

typedef struct UserMapping
{
	Oid			umid;			/* Oid of user mapping */
	Oid			userid;			/* local user Oid */
	Oid			serverid;		/* server Oid */
	List	   *options;		/* useoptions as DefElem list */
} UserMapping;

typedef struct ForeignTable
{
	Oid			relid;			/* relation Oid */
	Oid			serverid;		/* server Oid */
	List	   *options;		/* ftoptions as DefElem list */
	char		exec_location;  /* execute on COORDINATOR, ANY or ALL SEGMENTS, Cloudberry MPP specific */
	int32		num_segments;	/* the number of segments of the foreign table */
} ForeignTable;

/* Flags for GetForeignServerExtended */
#define FSV_MISSING_OK	0x01

/* Flags for GetForeignDataWrapperExtended */
#define FDW_MISSING_OK	0x01


extern char SeparateOutMppExecute(List **options);
extern int32 SeparateOutNumSegments(List **options);
extern ForeignServer *GetForeignServer(Oid serverid);
extern ForeignServer *GetForeignServerExtended(Oid serverid,
											   bits16 flags);
extern ForeignServer *GetForeignServerByName(const char *name, bool missing_ok);
extern UserMapping *GetUserMapping(Oid userid, Oid serverid);
extern ForeignDataWrapper *GetForeignDataWrapper(Oid fdwid);
extern ForeignDataWrapper *GetForeignDataWrapperExtended(Oid fdwid,
														 bits16 flags);
extern ForeignDataWrapper *GetForeignDataWrapperByName(const char *name,
													   bool missing_ok);
extern ForeignTable *GetForeignTable(Oid relid);
extern bool rel_is_external_table(Oid relid);

extern List *GetForeignColumnOptions(Oid relid, AttrNumber attnum);

extern Oid	get_foreign_data_wrapper_oid(const char *fdwname, bool missing_ok);
extern Oid	get_foreign_server_oid(const char *servername, bool missing_ok);
extern Oid GetForeignServerSegByRelid(Oid tableOid);
extern List *GetForeignServerSegsByRelId(Oid relid);

/* ----------------
 *		compiler constants for ForeignTable's exec_location
 * ----------------
 */

#define FTEXECLOCATION_ANY 'a'
#define FTEXECLOCATION_COORDINATOR 'c'
#define FTEXECLOCATION_ALL_SEGMENTS 's'
#define FTEXECLOCATION_NOT_DEFINED 'n'

#endif							/* FOREIGN_H */
