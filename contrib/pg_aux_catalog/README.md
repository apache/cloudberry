# pg_aux_catalog

Auxiliary catalog management for Apache Cloudberry.

This extension provisions the **`mdb_admin`** privilege role, which lets a
non-superuser manage resource groups in managed-service deployments where the
client is never given superuser.

## Background

In Greenplum/Cloudberry only a superuser may `CREATE`/`ALTER`/`DROP` resource
groups or move a running query between groups with `pg_resgroup_move_query()`.
The server gates those four entry points on membership of `mdb_admin`,
identified by a **fixed OID (8067)** rather than by name, so the privilege is
recognised reliably across the coordinator and all segments.

A fixed OID cannot be obtained from a plain `CREATE ROLE` (that assigns an
ordinary OID). This extension provides the one supported way to create the
role at OID 8067.

## Functions

### `pg_create_mdb_admin_role() returns oid`

Creates the `mdb_admin` role with its fixed OID (8067).
Returns the OID of the created role (8067). Errors if a role with that OID or
the name `mdb_admin` already exists. The OID assignment is dispatched to the
segments, so the role has the same OID cluster-wide.

## Usage

```sql
CREATE EXTENSION pg_aux_catalog;

-- Provision the role (the control plane does this once per cluster).
SELECT pg_create_mdb_admin_role();

-- Grant the capability to a tenant admin.
GRANT mdb_admin TO cloud_admin;

-- cloud_admin can now manage resource groups without superuser:
SET ROLE cloud_admin;
CREATE RESOURCE GROUP rg_tenant WITH (concurrency = 4, cpu_max_percent = 20);
ALTER  RESOURCE GROUP rg_tenant SET cpu_max_percent 30;
DROP   RESOURCE GROUP rg_tenant;
```

`admin_group` and `system_group` remain superuser-only for `ALTER`/`DROP`:
they are infrastructure, not user-tunable groups.

## Building and testing

```sh
make -C contrib/pg_aux_catalog install
make -C contrib/pg_aux_catalog installcheck
```

`installcheck` runs a single-session regression test (role creation and the
resource-group permission gate). A multi-session isolation2 test covering the
dispatched / cross-session behaviour lives under `isolation2/` and is run
separately, against a cluster with resource groups enabled
(`gp_resource_manager=group`):

```sh
make -C contrib/pg_aux_catalog installcheck-isolation2
```

## Credits

Based on [pg-sharding/cpg](https://github.com/pg-sharding/cpg) commit
`7b8c912`. Some tests are adapted from open-gpdb/gpdb commit `3ac99962ad2`.
