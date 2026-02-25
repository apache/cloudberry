-- Verify that gpexpand correctly updates segment-specific flags for wal-g in
-- archive_command and restore_command when initializing new segments.
--
-- Previously, these GUCs were copied verbatim from the template segment
-- (content 0), causing new segments to invoke archiving with an incorrect
-- content-id (possible overwriting archive or restoring other's data).

-- Cleanup any previous state
!\retcode yes | gpexpand -c;
!\retcode gpshrink -c;
!\retcode rm -r /tmp/datadirs/;

-- Set GUCs on all segments, hardcode --content-id to 0
!\retcode gpconfig -c restore_command -v '/bin/true';
!\retcode gpconfig -c archive_command -v 'wal-g seg wal-push %p --content-id=0';
!\retcode gpstop -u;

-- Prepare expansion configuration
!\retcode echo "localhost|localhost|7008|/tmp/datadirs/dbfast4/demoDataDir3|9|3|p
localhost|localhost|7009|/tmp/datadirs/dbfast_mirror4/demoDataDir3|10|3|m" > /tmp/testexpand;

-- Expand
!\retcode gpexpand -i /tmp/testexpand;
!\retcode gpexpand -i /tmp/testexpand;

-- Get the new segment's datadir (content=3)
!\retcode psql -d postgres -Aqt -c "SELECT datadir FROM gp_segment_configuration
WHERE content = 3 AND role = 'p'" > /tmp/new_segment_datadir;

-- Confirm that the --content-id flag within archive_command has been
-- updated to match the new segment's content. The restore_command
-- lacks --content-id flag and should be unchanged.
! grep "^archive_command" $(cat /tmp/new_segment_datadir)/postgresql.conf;
! grep "^restore_command" $(cat /tmp/new_segment_datadir)/postgresql.conf;

-- Cleanup
!\retcode gpconfig -r restore_command;
!\retcode gpconfig -r archive_command;
!\retcode gpstop -u;

!\retcode gpshrink -i /tmp/testexpand;
!\retcode gpshrink -i /tmp/testexpand;

!\retcode yes | gpexpand -c;
!\retcode gpshrink -c;
!\retcode rm -r /tmp/datadirs/;
!\retcode rm /tmp/new_segment_datadir;

