#!/usr/bin/env python3

from gppylib import gplog
from gppylib.db.catalog import RemoteQueryCommand

logger = gplog.get_default_logger()


def is_seg_in_backup_mode(hostname, port):
    """
    To check if segment is already in backup mode. If yes, then differential recovery might be
    running already to recover its mirror. And in that case the mirror should be skipped from
    being recovered again.

    In PG16, pg_is_in_backup() was removed along with exclusive backup mode.
    Use pg_backup_start_time() which returns non-null if a non-exclusive backup is in progress.

    Parameters:
        hostname: host name of source server
        port: port of source server

    Returns:
         boolean: true if backup is in progress for the segment
    """
    logger.debug(
        "Checking if backup is already in progress for the source server with host {} and port {}".format(
            hostname, port))

    sql = "SELECT pg_backup_start_time() IS NOT NULL"
    try:
        query_cmd = RemoteQueryCommand("pg_backup_start_time", sql, hostname, port)
        query_cmd.run()
        res = query_cmd.get_results()

    except Exception as e:
        raise Exception("Failed to query pg_backup_start_time() for segment with hostname {}, port {}, error: {}".format(
            hostname, str(port), str(e)))

    return res[0][0]
