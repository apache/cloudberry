#!/usr/bin/env python3
import os
import re
import sys
import tempfile

from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.commands.base import Command, LOCAL

description = ("""
This utility is NOT SUPPORTED and is for internal-use only.

updates the pg_hba.conf file for segments.
""")

def parseargs():
    parser = OptParser(option_class=OptChecker, description=' '.join(description.split()))

    parser.setHelp([])

    parser.add_option('-d', '--data-dir', dest='datadir', metavar='<data dir of the segment>',
                      help='Data dir of the segment to update pg_hba.conf')
    parser.add_option("-e", "--entries", dest="entries", metavar="<entries to be added>",
                      help="entries to be added to pg_hba.conf")
    options, args = parser.parse_args()
    return validate_args(options)

def validate_args(options):
    if not options.datadir:
        sys.stderr.write("--file required")
        sys.exit(1)

    if not options.entries:
        sys.stderr.write("--entries required")
        sys.exit(1)

    return options

def lineToCanonical(s):
    s = s.strip()
    s = re.sub(r"\s+", " ", s) # reduce whitespace runs to single space
    return s

def read_from_hba_file_and_get_empty_tempfile(hba_filename):
    with open(hba_filename, 'r') as infile:
        lines = infile.readlines()

    (tempFD, temp_hba_filename) = tempfile.mkstemp(prefix="pg_hba", suffix=".tmp", dir=os.path.abspath(os.path.dirname(hba_filename)))

    os.chmod(os.path.abspath(temp_hba_filename), os.stat(os.path.abspath(hba_filename)).st_mode)
    return lines, temp_hba_filename

def write_entries(out_entries, temp_hba_filename, hba_filename):
    with open(temp_hba_filename, 'w') as outfile:
        outfile.write('\n'.join(out_entries))
        outfile.write('\n')
    os.rename(os.path.abspath(temp_hba_filename), hba_filename)

def remove_dup_add_entries(lines, entries):
    existingLineMap = {}
    out_entries = []
    for line in lines:
        canonical = lineToCanonical(line)
        if canonical not in existingLineMap:
            existingLineMap[canonical] = True
            out_entries.append(line.strip())

    for entry in entries.split('\n'):
        canonical = lineToCanonical(entry)
        if canonical not in existingLineMap:
            existingLineMap[canonical] = True
            out_entries.append(entry)

    return out_entries

# remove_stale_replication_entries() and is_stale_replication_entry() come
# from Greenplum Database (greenplum-db/gpdb-archive @ 482967c1b4),
# gpMgmt/sbin/seg_update_pg_hba.py. Greenplum compares lines as written and
# searches the whole line for "replication" and the local addresses; here
# lines are compared in canonical form and the fields are read instead.
HBA_CONNECTION_TYPES = ('local', 'host', 'hostssl', 'hostnossl', 'hostgssenc', 'hostnogssenc')
LOCAL_ADDRESSES = ('127.0.0.1/32', '::1/128', 'samehost', 'localhost')

def hba_field_spans(line):
    # Like hba.c's next_field_expand(), a comma continues the current field,
    # including when whitespace follows it. Keep spans for editing the field
    # without changing the surrounding authentication options or comment.
    pos = 0
    while pos < len(line):
        while pos < len(line) and (line[pos].isspace() or line[pos] == ','):
            pos += 1
        if pos == len(line) or line[pos] == '#':
            return
        start = pos
        quoted = False
        while pos < len(line):
            char = line[pos]
            if char == '"':
                quoted = not quoted
            elif not quoted:
                if char == '#' or char.isspace():
                    break
                if char == ',':
                    pos += 1
                    while pos < len(line) and (line[pos].isspace() or line[pos] == ','):
                        pos += 1
                    continue
            pos += 1
        yield start, pos
        if pos < len(line) and line[pos] == '#':
            return


def hba_fields(line):
    # The fields of a pg_hba.conf line, without its comment. A field may be
    # double-quoted, and a quoted field keeps its quotes, because "replication"
    # names a database while replication is the keyword.
    return [line[start:end] for start, end in hba_field_spans(line)]

def hba_database_names(field):
    # Split only on unquoted commas; quoted "replication" is a database name.
    return [name.strip() for name in re.findall(r'(?:"[^"]*"|[^,"])+', field)]


def is_replication_entry(fields):
    return (len(fields) >= 2 and fields[0] in HBA_CONNECTION_TYPES
            and 'replication' in hba_database_names(fields[1]))

def remove_stale_replication_entries(existing_hba, entries):
    final_entries = []
    new_entries = entries.split('\n')
    wanted = set(lineToCanonical(entry) for entry in new_entries)
    replication_entry = any(is_replication_entry(hba_fields(entry)) for entry in new_entries)

    for line in existing_hba:
        if lineToCanonical(line) not in wanted:
            # check for stale replication entries on existing pg_hba.conf
            if replication_entry and is_stale_replication_entry(line):
                # A mixed rule also grants ordinary database access. Remove
                # only the replication keyword, retaining that access and the
                # original authentication settings, whitespace and comment.
                start, end = list(hba_field_spans(line))[1]
                databases = [name for name in hba_database_names(line[start:end])
                             if name != 'replication']
                if not databases:
                    continue
                line = line[:start] + ','.join(databases) + line[end:]
        final_entries.append(line.strip())

    return final_entries

def is_stale_replication_entry(line):
    # Here considering all non-local replication entries as stale.
    # This assumes 1:1 replication and will not work with 1:N replication.
    fields = hba_fields(line)
    # return False if line is a comment or not a replication entry
    if not is_replication_entry(fields):
        return False
    # keep replication entries present for localhost
    if fields[0] == 'local':
        return False
    if len(fields) > 3:
        address = re.sub(r'"((?:[^"]|"")*)"',
                         lambda match: match.group(1).replace('""', '"'), fields[3])
        # Unlike numeric addresses and hostnames, samehost is a keyword only
        # when it is unquoted.
        if fields[3] == 'samehost' or (address != 'samehost' and address in LOCAL_ADDRESSES):
            return False

    return True

def run_pg_ctl_reload(datadir):
    name = "pg_ctl reload"
    cmd_str = "$GPHOME/bin/pg_ctl reload -D %s" % datadir
    cmd = Command(name, cmd_str, ctxt=LOCAL)
    cmd.run(validateAfter=True)

def main():
    options = parseargs()
    hba_filename = options.datadir +'/pg_hba.conf'
    lines, temp_hba_filename = read_from_hba_file_and_get_empty_tempfile(hba_filename)
    out_entries = remove_dup_add_entries(lines, options.entries)
    final_entries = remove_stale_replication_entries(out_entries, options.entries)
    write_entries(final_entries, temp_hba_filename, hba_filename)
    run_pg_ctl_reload(options.datadir)

if __name__ == "__main__":
    main()
