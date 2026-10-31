# Updating orafce

gpcontrib/orafce was added as a squashed git subtree of
https://github.com/orafce/orafce, but later updates (4.9.2, 4.17.1) were
committed as ordinary commits, so `git subtree pull` no longer has a usable
base. Update by replaying the upstream diff between two release tags onto
this directory with a three-way merge, which keeps the Cloudberry changes
listed below.

 1. Fetch the upstream tags.
```
git remote add orafce_remote https://github.com/orafce/orafce.git
git fetch orafce_remote --tags
```

 2. Merge OLD..NEW (e.g. VERSION_4_17_1..VERSION_4_18_0) in a scratch
    worktree, with the upstream tag OLD as the merge base.
```
base=$(git commit-tree -m base OLD^{tree})
ours=$(git commit-tree -p $base -m ours HEAD:gpcontrib/orafce)
theirs=$(git commit-tree -p $base -m theirs NEW^{tree})
git worktree add --detach /tmp/orafce-merge $ours
git -C /tmp/orafce-merge merge --no-commit $theirs
# resolve conflicts, then: git -C /tmp/orafce-merge add -A
```

 3. Replace the directory with the merged tree.
```
tree=$(git -C /tmp/orafce-merge write-tree)
git rm -rq --cached gpcontrib/orafce && rm -rf gpcontrib/orafce
git read-tree --prefix=gpcontrib/orafce/ -u $tree
```

 4. Check the new upgrade scripts (orafce--X--Y.sql) and the new base
    script against the Cloudberry changes below, then run
    `make installcheck` and update the expected files.

# Cloudberry changes to keep

 - decode() is not installed: Cloudberry's parser turns DECODE() into
   CASE WHEN. Its definitions are commented out in the base script and in
   every upgrade script that touches oracle.decode.
 - dbms_alert is not installed: it cannot work across segments as
   implemented. Its definitions are commented out in the base script and in
   every upgrade script that touches dbms_alert, and the dbms_alert tests
   are left out of parallel_schedule.
 - orafce--3.13--3.14.sql uses ALTER FUNCTION ... SUPPORT, as before
   4.16. Upstream now updates pg_proc and pg_depend directly, which a
   regular extension script may not do here, and which would only change
   the coordinator's catalog.
 - orafce--3.23--3.24.sql sets allow_system_table_mods to change
   typstorage.
 - oracle.regexp_not_like() is added in orafce--4.8--4.9.sql and in the
   base script; upstream does not have it.
 - others.c uses SETLOCALE() from common/mdb_locale.h instead of
   setlocale().
 - dbms_assert.c: EMPTY_STR() uses VARSIZE_ANY_EXHDR(), so a 3-byte value
   with a short varlena header is not taken for an empty string.
 - dbms_sql.c: the plan argument types are copied before they are freed.
 - file.c: the utl_file_dir queries qualify every function and operator
   with pg_catalog, so an "=" of the caller's in search_path cannot pass
   the directory check.
 - pipe.c: dbms_pipe.__list_pipes() raises an error instead of crashing
   when it is called without a column definition list.
 - dbms_sql.c: a cursor looks its portal up by name (cursor_portal())
   and does not use one that was dropped with an aborted subtransaction.
 - file.c: the orafce.umask check hook checks every value that comes
   from the client, also one set before the library was loaded, and
   rejects an empty umask.
 - plvstr.c: plvstr.normalize() drops leading whitespace, as before 4.17.
 - datefce.c, plvdate.c: trunc(), round() and the business day functions
   leave infinite dates as they are; the other plvdate functions reject
   them.
 - plvdate.c, plvsubst.c, others.c: the settings made by the plvdate
   set_* functions, plvsubst.setsubst() and oracle.set_nls_sort() are kept
   in the GUCs orafce.plvdate_state, orafce.plvsubst_keyword and
   orafce.nls_sort, set with set_config() and marked GUC_GPDB_NEED_SYNC,
   so the segments see them.
 - regexp_like(text, text, text) and regexp_not_like() test for a match
   with EXISTS, not for the first group; regexp_not_like() is IMMUTABLE
   PARALLEL SAFE. orafce--4.16--4.17.sql redefines them.
 - orafce--4.17.sql uses the upgrade scripts' function bodies, and
   orafce--4.16--4.17.sql also makes orafce_concat2(varchar2, varchar2)
   IMMUTABLE and redefines regexp_substr(), so an updated extension
   matches a fresh install.
 - DATA in the Makefile keeps the 3.x upgrade scripts, which upstream no
   longer installs, so extensions created by older releases can still be
   updated.
 - Tests: gp_distributed_by and gp_partition_by are added, init_file
   masks segment details, nlssort uses a UTF-8 database, orafce2 calls the
   built-in decode(), and dbms_pipe_session_B polls instead of relying on
   a fixed timeout. The other gp_* tests cover the Cloudberry fixes and
   additions.
 - Upstream's GitHub workflows, install_bindist.py, typedefs tooling
   (and the Makefile targets that use it) and SECURITY.md are not
   imported.
