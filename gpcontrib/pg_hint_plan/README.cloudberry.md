# pg_hint_plan in Apache Cloudberry

This directory holds upstream pg_hint_plan
(https://github.com/ossc-db/pg_hint_plan), tag `REL16_1_6_3`, with the
Cloudberry changes listed below on top. Code changes in the C source are marked
with `CBDB:` comments.

## What differs from upstream

- `core.c`, `make_join_rel.c` and `update_copied_funcs.pl` are not carried.
  Upstream keeps private copies of the core join search; Cloudberry instead
  calls `make_join_rel_wrapper()` and `add_paths_to_joinrel_wrapper()` through
  `make_join_rel_hook` and `add_paths_to_joinrel_hook` (see
  `src/backend/optimizer/path/joinrels.c` and `joinpath.c`), and
  `pg_hint_plan_join_search()` calls the core `standard_join_search()`.
  `set_plain_rel_pathlist()` is the one function still copied, from
  `src/backend/optimizer/path/allpaths.c`.
- Rows hints are parsed but not applied by the Postgres planner, since that
  code lives in upstream's `make_join_rel.c`. ORCA does apply them.
- GEQO is not supported, so the hint module never calls `geqo()`.
- `planner_hook` takes an extra `OptimizerOptions *` argument.
- With `USE_ORCA`, `plan_hint_hook` hands the parsed `HintState` to ORCA
  (`src/backend/gpopt/utils/COptTasks.cpp`). ORCA reads it through
  `src/include/optimizer/hints.h`, which mirrors the hint structures in
  `pg_hint_plan.c`; keep the two in sync. `HintState` has an extra `log_level`
  field for ORCA's hint report, and the planner's `debug_print` output is only
  printed for plans the Postgres planner made.
- `pg_hint_plan--1.3.0.sql` adds `id` to the unique index on `hint_plan.hints`,
  because a unique index must contain the distribution key.
- The regression tests that run are listed in `REGRESS` in the Makefile.
  `init`, `base_plan`, `pg_hint_plan`, `hints_anywhere` and `plpgsql` are
  adapted to Cloudberry: they use the Postgres planner, and their expected
  output has MPP plans. `pg_hint_plan` has Cloudberry-specific cases, and
  `oldextversions` also checks that `hint_plan.hints` rows survive the update
  from 1.3.9. The `ut-*` tests other than `ut-fini`, and `ut-fdw`, are kept
  as upstream ships them but not run.
- In PL/pgSQL functions (including dynamic SQL in `EXECUTE`), a statement
  without a hint is planned without hints. Upstream lets it inherit the hint
  last seen in the function, or the hint of the statement calling the
  function, but only when query IDs are not computed (`compute_query_id =
  off`, or `auto` with no module requesting them); its tests run with
  pg_stat_statements loaded and never see this.
- No RPM spec or tarball targets.

## Upgrading

Import the new upstream tag over this directory, drop the files listed above,
re-apply the `CBDB:` changes, update `src/include/optimizer/hints.h`, and
regenerate the expected output of the adapted tests. Also run `planhints`,
`rowhints` and `joinhints` from `src/test/regress` with `optimizer` on and
off; they cover the hints ORCA consumes. Upstream ships an
`ALTER EXTENSION ... UPDATE` script for every release, so existing installations
upgrade through that chain.
