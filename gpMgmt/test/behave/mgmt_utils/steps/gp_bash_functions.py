import os
from pathlib import Path
import subprocess
import tempfile

from behave import then


@then('SED_PG_CONF preserves unrelated settings on "{host}" with KEEP_PREV "{keep_prev}"')
def check_sed_pg_conf_parameter_boundary(context, host, keep_prev):
    # Use an active work_mem setting so the replacement branch is exercised;
    # a file containing only the default #work_mem would take the append path.
    unrelated_lines = [
        "maintenance_work_mem = 65536",
        "# work_mem should not change inside a comment",
    ]
    original = "  work_mem = 4096\n" + "\n".join(unrelated_lines) + "\n"
    functions = Path(os.environ["GPHOME"]) / "bin/lib/gp_bash_functions.sh"

    # localhost uses the real SSH branch against the same temporary file.
    # No database is required; SSH must be configured as for the other tests
    # in gp_bash_functions.feature.
    with tempfile.TemporaryDirectory(prefix="sed_pg_conf_") as temp_dir:
        config = Path(temp_dir) / "postgresql.conf"
        config.write_text(original)
        script = '''
source "$1" || exit $?
LOG_FILE="$2"
SED_PG_CONF "$3" work_mem work_mem=999 "$4" "$5"
'''
        result = subprocess.run(
            ["bash", "-c", script, "sed_pg_conf_test", str(functions),
             str(Path(temp_dir) / "test.log"), str(config), keep_prev,
             "" if host == "local" else host],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, timeout=60,
        )
        assert result.returncode == 0, (
            "SED_PG_CONF failed: stdout={!r}, stderr={!r}".format(
                result.stdout, result.stderr)
        )
        lines = config.read_text().splitlines()
        assert lines[1:] == unrelated_lines, (
            "SED_PG_CONF modified unrelated settings or comments: {!r}".format(lines)
        )
        assert lines[0].split("#", 1)[0].strip() == "work_mem=999", (
            "SED_PG_CONF did not update work_mem: {!r}".format(lines)
        )
