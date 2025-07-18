from behave import fixture


@fixture
def init_cluster(context):
    context.execute_steps(u"""
    Given the database is not running
        And a working directory of the test as '/tmp/concourse_cluster'
        And the user runs command "rm -rf ~/gpAdminLogs/gpinitsystem*"
        And a cluster is created with mirrors on "cdw" and "sdw1, sdw2, sdw3"
    """)
