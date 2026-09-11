@gpstop
Feature: gpstop behave tests

    @concourse_cluster
    @demo_cluster
    Scenario: gpstop succeeds
        Given the database is running
          And running postgres processes are saved in context
         When the user runs "gpstop -a"
         Then gpstop should return a return code of 0
         And verify no postgres process is running on all hosts

    @demo_cluster
    Scenario: gpstop runs with given coordinator data directory option
        Given the database is running
          And running postgres processes are saved in context
          And "COORDINATOR_DATA_DIRECTORY" environment variable is not set
         Then the user runs utility "gpstop" with coordinator data directory and "-a"
          And gpstop should return a return code of 0
          And "COORDINATOR_DATA_DIRECTORY" environment variable should be restored
          And verify no postgres process is running on all hosts

    @demo_cluster
    Scenario: gpstop priorities given coordinator data directory over env option
        Given the database is running
          And running postgres processes are saved in context
          And the environment variable "COORDINATOR_DATA_DIRECTORY" is set to "/tmp/"
         Then the user runs utility "gpstop" with coordinator data directory and "-a"
          And gpstop should return a return code of 0
          And "COORDINATOR_DATA_DIRECTORY" environment variable should be restored
          And verify no postgres process is running on all hosts

    @concourse_cluster
    @demo_cluster
    Scenario: when there are user connections gpstop waits to shutdown until user switches to fast mode
        Given the database is running
          And the user asynchronously runs "psql postgres" and the process is saved
          And running postgres processes are saved in context
         When the user runs gpstop -a -t 4 --skipvalidation and selects f
          And gpstop should print "'\(s\)mart_mode', '\(f\)ast_mode', '\(i\)mmediate_mode'" to stdout
         Then gpstop should return a return code of 0
         And verify no postgres process is running on all hosts

    @concourse_cluster
    @demo_cluster
    Scenario: when there are user connections gpstop waits to shutdown until user connections are disconnected
        Given the database is running
          And the user asynchronously runs "psql postgres" and the process is saved
          And the user asynchronously sets up to end that process in 15 seconds
          And running postgres processes are saved in context
         When the user runs gpstop -a -t 2 --skipvalidation and selects s
          And gpstop should print "There were 1 user connections at the start of the shutdown" to stdout
          And gpstop should print "'\(s\)mart_mode', '\(f\)ast_mode', '\(i\)mmediate_mode'" to stdout
         Then gpstop should return a return code of 0
         And verify no postgres process is running on all hosts

    @demo_cluster
    Scenario: gpstop succeeds even if the standby host is unreachable
        Given the database is running
          And the catalog has a standby coordinator entry
         When the standby host is made unreachable
          And the user runs "gpstop -a"
         Then gpstop should print "Standby is unreachable, skipping shutdown on standby" to stdout
          And gpstop should return a return code of 0
          And the standby host is made reachable

    @demo_cluster
    Scenario: gpstop succeeds when pg_ctl command fails
        Given the database is running
          And the user runs psql with "-c "CREATE EXTENSION IF NOT EXISTS gp_inject_fault;"" against database "postgres"
          And the user runs psql with "-c "SELECT gp_inject_fault('checkpoint', 'sleep', '', '', '', 1, -1, 3600, dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p'"" against database "postgres"
          And running postgres processes are saved in context
         When the user runs "gpstop -a -M fast"
          And gpstop should print "Failed to shutdown coordinator with pg_ctl." to stdout
          And gpstop should return a return code of 0
          And verify no postgres process is running on all hosts

    @demo_cluster
    Scenario: gpstop succeeds with immediate option
        Given the database is running
          And the user asynchronously runs "psql postgres" and the process is saved
          And the user asynchronously sets up to end that process in 15 seconds
          And running postgres processes are saved in context
         When the user runs "gpstop -a -M immediate"
          And gpstop should print "Commencing Coordinator instance shutdown with mode='immediate'" to stdout
         Then gpstop should return a return code of 0
          And verify no postgres process is running on all hosts

    Scenario: gpstop fails when the lock file is already held by another gpstop process
        Given the database is running
        And we run a sample background script to generate a pid on "coordinator" segment
        Then a sample gpstop.lock directory is created using the background pid in coordinator_data_directory
        And the user runs "gpstop -a"
        And gpstop should print "gpstop.lock indicates that an instance of gpstop is already running" to stdout
        # proceeding graceful shutdown of the database.
        And the background pid is killed on "coordinator" segment
        And the user runs gpstop -a and selects f
        And gpstop should return a return code of 0

    Scenario: gpstop removes the lock directory when it is empty
        Given the database is running
        Then a sample gpstop.lock directory is created using the background pid in coordinator_data_directory
        And the user runs "gpstop -a"
        And gpstop should return a return code of 0
