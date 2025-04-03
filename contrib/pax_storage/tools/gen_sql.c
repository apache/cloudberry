//
// This program is used to generate sql script file for PAX.
// Build and run of this program is automatically done in the
// cmake script.
//
// The command to build this program alone is:
// gcc -I`pg_config --includedir-server` -I<pax_dir>/src/cpp -o generate_sql gen_sql.c
//
//

#include "postgres.h"  // NOLINT

#include <stdio.h>

/* define these values in pax header file */
#include "comm/cbdb_api.h"

#include "catalog/pg_am.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"

#ifdef printf
#undef printf
#endif

#define PAX_COMMENT "column-optimized PAX table access method handler"
int main() {
  printf("-- insert pax catalog values\n");

#if !defined(USE_MANIFEST_API) || defined(USE_PAX_CATALOG)
  printf("-- create pg_ext_aux.pg_pax_tables\n");
  printf(
      "CREATE TABLE pg_ext_aux.pg_pax_tables(relid oid not null, auxrelid oid "
      "not null);\n");
  printf(
      "DELETE FROM gp_distribution_policy WHERE "
      "localoid='pg_ext_aux.pg_pax_tables'::regclass;\n");

  printf("\n-- update pg_depend\n");
  printf(
      "UPDATE pg_depend SET refobjid = %u WHERE refclassid = %u AND "
      "refobjid='pg_ext_aux.pg_pax_tables'::regclass;\n",
      PAX_TABLES_RELATION_ID, RelationRelationId);
  printf(
      "UPDATE pg_depend SET objid = %u WHERE classid = %u AND "
      "objid='pg_ext_aux.pg_pax_tables'::regclass;\n",
      PAX_TABLES_RELATION_ID, RelationRelationId);

  printf("\n-- update pg_attribute\n");
  printf(
      "UPDATE pg_attribute SET attrelid = %u WHERE attrelid = "
      "'pg_ext_aux.pg_pax_tables'::regclass;\n",
      PAX_TABLES_RELATION_ID);
  printf(
      "UPDATE pg_class SET oid=%u WHERE "
      "oid='pg_ext_aux.pg_pax_tables'::regclass;\n",
      PAX_TABLES_RELATION_ID);

  printf("\n-- add unique index\n");
  printf(
      "CREATE UNIQUE INDEX pg_pax_tables_relid_index on "
      "pg_ext_aux.pg_pax_tables(relid);\n");

  printf("\n-- update pg_attribute\n");
  printf(
      "UPDATE pg_attribute SET attrelid = %u WHERE attrelid = (SELECT "
      "indexrelid FROM pg_index WHERE "
      "indrelid='pg_ext_aux.pg_pax_tables'::regclass);\n",
      PAX_TABLES_RELID_INDEX_ID);

  printf("\n-- update pg_depend\n");
  printf(
      "UPDATE pg_depend SET objid = %u WHERE classid = %u AND refclassid = %u "
      "AND refobjid='pg_ext_aux.pg_pax_tables'::regclass AND objid = (SELECT "
      "indexrelid FROM pg_index WHERE "
      "indrelid='pg_ext_aux.pg_pax_tables'::regclass);",
      PAX_TABLES_RELID_INDEX_ID, RelationRelationId, RelationRelationId);

  printf("\n-- update index oid\n");
  printf(
      "UPDATE pg_class SET oid = %u WHERE oid = (SELECT indexrelid FROM "
      "pg_index WHERE indrelid='pg_ext_aux.pg_pax_tables'::regclass);\n",
      PAX_TABLES_RELID_INDEX_ID);
  printf(
      "UPDATE pg_index SET indexrelid = %u WHERE "
      "indrelid='pg_ext_aux.pg_pax_tables'::regclass;\n",
      PAX_TABLES_RELID_INDEX_ID);
#endif

  printf("\n-- insert proc and am entry\n");
  printf(
      "INSERT INTO pg_proc "
      "VALUES(%u,'%s',%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c','%c','%c','%c','%c',"
      "1,0,%u,'%u',null,null,null,null,null,'%s','%s',null,null,null,'%c','%c')"
      ";\n",
      PAX_AM_HANDLER_OID,    /* oid: pg_proc.oid */
      PAX_AM_HANDLER_NAME,   /* proname */
      PG_CATALOG_NAMESPACE,  /* pronamespace: pg_namespace.oid: pg_catalog */
      BOOTSTRAP_SUPERUSERID, /* proowner: pg_authid.oid */
      ClanguageId,           /* prolang: pg_language.oid */
      1,                     /* procost: 1 */
      0,                     /* prorows: 0 */
      0,                     /* provariadic: pg_type.oid*/
      0,                     /* prosupport: pg_proc.oid */
      'f',                   /* prokind: 'f' normal function */
      'f',                   /* prosecdef */
      'f',                   /* proleakproof */
      't',                   /* proisstrict */
      'f',                   /* proretset */
      's',                   /* provolatile */
      'u',                   /* proparallel */
      /* pronargs: 1 */
      /* pronargdefaults: 0 */
      TABLE_AM_HANDLEROID, /* prorettype: pg_type.oid */
      INTERNALOID,         /* proargtypes: pg_type.oid, internal */
      /* proallargtypes: null */
      /* proargmodes: null */
      /* proargnames: null */
      /* proargdefaults: nulll */
      /* protrftypes: null */
      PAX_AM_HANDLER_NAME, /* prosrc */
      "$libdir/pax",       /* probin */
      /* prosqlbody: null */
      /* proconfig: null */
      /* proacl: null */
      'n', /* prodataaccess */
      'a' /* proexeclocation: all */);

  printf("INSERT INTO pg_am   VALUES(%u,'%s',%u,'%c');\n",
         PAX_TABLE_AM_OID,   /* pg_am.oid */
         PAX_AMNAME,         /* pg_am.amname */
         PAX_AM_HANDLER_OID, /* pg_am.amhandler: pg_proc.oid */
         't' /* pg_am.amtype: TABLE */);

  printf("COMMENT ON FUNCTION %s IS '%s';\n", PAX_AM_HANDLER_NAME, PAX_COMMENT);

  /* create type for micropartition stats */
  printf(
      "INSERT INTO pg_proc "
      "VALUES(%u,'%s',%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c','%c','%c','%c','%c',"
      "1,0,%u,'%u',null,null,null,null,null,'%s','%s',null,null,null,'%c','%c')"
      ";\n",
      PAX_AUX_STATS_IN_OID,  /* oid: pg_proc.oid */
      "paxauxstats_in",      /* proname */
      PG_EXTAUX_NAMESPACE,   /* pronamespace: pg_namespace.oid: pg_catalog */
      BOOTSTRAP_SUPERUSERID, /* proowner: pg_authid.oid */
      ClanguageId,           /* prolang: pg_language.oid */
      1,                     /* procost: 1 */
      0,                     /* prorows: 0 */
      0,                     /* provariadic: pg_type.oid*/
      0,                     /* prosupport: pg_proc.oid */
      'f',                   /* prokind: 'f' normal function */
      'f',                   /* prosecdef */
      'f',                   /* proleakproof */
      't',                   /* proisstrict */
      'f',                   /* proretset */
      'i',                   /* provolatile */
      'u',                   /* proparallel */
      /* pronargs: 1 */
      /* pronargdefaults: 0 */
      PAX_AUX_STATS_TYPE_OID, /* prorettype: pg_type.oid */
      CSTRINGOID,             /* proargtypes: pg_type.oid, internal */
      /* proallargtypes: null */
      /* proargmodes: null */
      /* proargnames: null */
      /* proargdefaults: nulll */
      /* protrftypes: null */
      "MicroPartitionStatsInput", /* prosrc */
      "$libdir/pax",              /* probin */
      /* prosqlbody: null */
      /* proconfig: null */
      /* proacl: null */
      'n', /* prodataaccess */
      'a' /* proexeclocation: all */);

  printf(
      "INSERT INTO pg_proc "
      "VALUES(%u,'%s',%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c','%c','%c','%c','%c',"
      "1,0,%u,'%u',null,null,null,null,null,'%s','%s',null,null,null,'%c','%c')"
      ";\n",
      PAX_AUX_STATS_OUT_OID, /* oid: pg_proc.oid */
      "paxauxstats_out",     /* proname */
      PG_EXTAUX_NAMESPACE,   /* pronamespace: pg_namespace.oid: pg_catalog */
      BOOTSTRAP_SUPERUSERID, /* proowner: pg_authid.oid */
      ClanguageId,           /* prolang: pg_language.oid */
      1,                     /* procost: 1 */
      0,                     /* prorows: 0 */
      0,                     /* provariadic: pg_type.oid*/
      0,                     /* prosupport: pg_proc.oid */
      'f',                   /* prokind: 'f' normal function */
      'f',                   /* prosecdef */
      'f',                   /* proleakproof */
      't',                   /* proisstrict */
      'f',                   /* proretset */
      'i',                   /* provolatile */
      'u',                   /* proparallel */
      /* pronargs: 1 */
      /* pronargdefaults: 0 */
      CSTRINGOID,             /* proargtypes: pg_type.oid, internal */
      PAX_AUX_STATS_TYPE_OID, /* prorettype: pg_type.oid */
      /* proallargtypes: null */
      /* proargmodes: null */
      /* proargnames: null */
      /* proargdefaults: nulll */
      /* protrftypes: null */
      "MicroPartitionStatsOutput", /* prosrc */
      "$libdir/pax",               /* probin */
      /* prosqlbody: null */
      /* proconfig: null */
      /* proacl: null */
      'n', /* prodataaccess */
      'a' /* proexeclocation: all */);

  printf(
      "INSERT INTO pg_type "
      "VALUES(%u,'%s',%u,%u,%d,'%c','%c','%c','%c','%c','%c',"
      "%u,%u,%u,%u,%u,%u,%u,%u,%u,%u,%u,'%c','%c','%c',"
      "%u,%d,%d,%u,null,null,null);\n",
      PAX_AUX_STATS_TYPE_OID,  /* pg_type.oid */
      PAX_AUX_STATS_TYPE_NAME, /* pg_type.typname */
      PG_EXTAUX_NAMESPACE,     /* pg_type.typnamespace: pg_namespace.oid:
                                  pg_catalog */
      BOOTSTRAP_SUPERUSERID,   /* pg_type.typowner: pg_authid.oid */
      -1,                      /* pg_type.typlen: -1 variable length */
      'f',                     /* pg_type.typbyval */
      'b',                     /* pg_type.typtype */
      'U',                     /* pg_type.typcategory */
      'f',                     /* pg_type.typispreferred */
      't',                     /* pg_type.typisdefined */
      ',',                     /* pg_type.typdelim */
      InvalidOid,              /* pg_type.typrelid */
      InvalidOid,              /* pg_type.typsubscript */
      InvalidOid,              /* pg_type.typelem */
      InvalidOid,              /* pg_type.typarray */
      PAX_AUX_STATS_IN_OID,    /* pg_type.typinput */
      PAX_AUX_STATS_OUT_OID,   /* pg_type.typoutput */
      InvalidOid,              /* pg_type.typreceive */
      InvalidOid,              /* pg_type.typsend */
      InvalidOid,              /* pg_type.typmodin */
      InvalidOid,              /* pg_type.typmodout */
      InvalidOid,              /* pg_type.typanalyze */
      'i',                     /* pg_type.typalign */
      'x',                     /* pg_type.typstorage */
      't',                     /* pg_type.typnotnull */
      InvalidOid,              /* pg_type.typbasetype */
      -1,                      /* pg_type.typtypmod */
      0,                       /* pg_type.ndims */
      InvalidOid               /* pg_type.typcollation */
  );

  printf("\n");
  /* create pax auxiliary fast sequence table. */
  printf("CREATE TABLE %s.%s(objid oid NOT NULL, seq int NOT NULL);\n",
         PG_PAX_FASTSEQUENCE_NAMESPACE, PG_PAX_FASTSEQUENCE_TABLE);

  /* create pax auxiliary fast sequence index. */
  printf("CREATE INDEX %s ON %s.%s(objid);\n", PG_PAX_FASTSEQUENCE_INDEX_NAME,
         PG_PAX_FASTSEQUENCE_NAMESPACE, PG_PAX_FASTSEQUENCE_TABLE);

  /* update oid of pg_pax_fastsequence and pg_pax_fastsequence_objid_idx */
  printf("-- update oid of fastsequence: table %u index %u\n",
         PAX_FASTSEQUENCE_OID, PAX_FASTSEQUENCE_INDEX_OID);
  printf("-- pg_type\n");
  printf("UPDATE pg_type SET typrelid = %u WHERE typname='%s';\n",
         PAX_FASTSEQUENCE_OID, PG_PAX_FASTSEQUENCE_TABLE);
  printf("-- pg_depend\n");
  printf(
      "UPDATE pg_depend SET refobjid = %u WHERE refobjid = (SELECT oid FROM "
      "pg_class WHERE relname='%s');\n",
      PAX_FASTSEQUENCE_OID, PG_PAX_FASTSEQUENCE_TABLE);
  printf(
      "UPDATE pg_depend SET objid = %u WHERE objid = (SELECT oid FROM pg_class "
      "WHERE relname='%s');\n",
      PAX_FASTSEQUENCE_OID, PG_PAX_FASTSEQUENCE_TABLE);
  printf(
      "UPDATE pg_depend SET objid = %u WHERE objid = (SELECT oid FROM pg_class "
      "WHERE relname='%s');\n",
      PAX_FASTSEQUENCE_INDEX_OID, PG_PAX_FASTSEQUENCE_INDEX_NAME);
  printf("-- pg_attribute\n");
  printf(
      "UPDATE pg_attribute SET attrelid=%u WHERE attrelid = (SELECT oid FROM "
      "pg_class WHERE relname='%s');\n",
      PAX_FASTSEQUENCE_OID, PG_PAX_FASTSEQUENCE_TABLE);
  printf("\n");

  printf(
      "UPDATE pg_attribute SET attrelid=%u WHERE attrelid = (SELECT oid FROM "
      "pg_class WHERE relname='%s');\n",
      PAX_FASTSEQUENCE_INDEX_OID, PG_PAX_FASTSEQUENCE_INDEX_NAME);
  printf("\n");

  printf("-- pg_index\n");
  printf(
      "UPDATE pg_index SET indexrelid = %u, indrelid = %u WHERE indexrelid = "
      "(SELECT oid FROM pg_class WHERE relname='%s') AND indrelid = (SELECT "
      "oid FROM pg_class WHERE relname='%s');\n",
      PAX_FASTSEQUENCE_INDEX_OID, PAX_FASTSEQUENCE_OID,
      PG_PAX_FASTSEQUENCE_INDEX_NAME, PG_PAX_FASTSEQUENCE_TABLE);

  printf("-- pg_class\n");
  printf("UPDATE pg_class SET oid=%u WHERE relname='%s';\n",
         PAX_FASTSEQUENCE_OID, PG_PAX_FASTSEQUENCE_TABLE);
  printf("UPDATE pg_class SET oid=%u WHERE relname='%s';\n",
         PAX_FASTSEQUENCE_INDEX_OID, PG_PAX_FASTSEQUENCE_INDEX_NAME);

  return 0;
}
