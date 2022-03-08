# add new version to upgrade or downgrade test

- add a new `schedule` file like `schedule_1.0--2.0`.
- write those new test:

```
test: 1.0_install                         # Install diskquota version 1.0
test: 1.0_set_quota                       # Create some quota configs under "1.0" diskquota schema
test: 1.0_catalog                         # Check if diskquota DDL is expected
test: 2.0_migrate_to_version_2.0          # Migrate 1.0 diskquota DDL to 2.0
test: 2.0_catalog                         # Check if the migration results is expected as a newly created 2.0 diskquota schema
test: 1.0_test_in_2.0_quota_create_in_1.0 # Check if the quota config still works which has been created by 1.0 extension
test: 1.0_cleanup_quota                   # Drop extension
```

the file name means this is a upgrade test from 1.0 to 2.0.

for downgrade test, just reverse the schedule file.

---

`10.1_test_in_10.0_quota_create_in_10.1` means:

- the file is for version 10.1
- this is a test file
- the test occur in 10.0, use 10.0 binary and 10.0 SQL
- the item to test is created in 10.1

----
