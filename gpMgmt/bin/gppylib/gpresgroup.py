#!/usr/bin/env python3
#
# Copyright (c) 2017, VMware, Inc. or its affiliates.
#

from gppylib.commands import base
from gppylib.commands.unix import *
from gppylib.commands.gp import *
from gppylib.gparray import GpArray
from gppylib.gplog import get_default_logger
from gppylib.db import dbconn


class GpResGroup(object):

    def __init__(self):
        self.logger = get_default_logger()

    @staticmethod
    def validate():
        pool = base.WorkerPool()
        gp_array = GpArray.initFromCatalog(dbconn.DbURL(), utility=True)
        host_list = list(set(gp_array.get_hostlist(True)))
        msg = None

        for h in host_list:
            cmd = Command(h, "gpcheckresgroupimpl", REMOTE, h)
            pool.addCommand(cmd)
        pool.join()

        items = pool.getCompletedItems()
        failed = []
        for i in items:
            if not i.was_successful():
                failed.append("[{}:{}]".format(i.remoteHost, i.get_stderr().rstrip()))
        pool.haltWork()
        pool.joinWorkers()
        if failed:
            msg = ",".join(failed)
        return msg

    @staticmethod
    def validate_v2():
        """
        Validate cgroup v2 configuration on all hosts.

        This method:
        1. Connects to the master database to retrieve gp_resource_group_cgroup_parent
        2. Passes this value to gpcheckresgroupv2impl on each host via command line
        3. Each host validates its local cgroup filesystem permissions
        """
        pool = base.WorkerPool()
        gp_array = GpArray.initFromCatalog(dbconn.DbURL(), utility=True)
        host_list = list(set(gp_array.get_hostlist(True)))
        msg = None

        # Get cgroup_parent value from master database
        cgroup_parent = None
        try:
            # Connect to master database to get the GUC parameter
            master_dburl = dbconn.DbURL()
            with dbconn.connect(master_dburl, utility=True) as conn:
                sql = "SHOW gp_resource_group_cgroup_parent"
                cursor = dbconn.query(conn, sql)
                result = cursor.fetchone()
                if result and result[0]:
                    cgroup_parent = result[0]
                else:
                    return "failed to retrieve gp_resource_group_cgroup_parent parameter from master database"
        except Exception as e:
            return "failed to retrieve gp_resource_group_cgroup_parent parameter: {}".format(str(e))

        # Build command with cgroup_parent parameter
        cmd_str = "gpcheckresgroupv2impl --cgroup-parent '{}'".format(cgroup_parent)

        for h in host_list:
            cmd = Command(h, cmd_str, REMOTE, h)
            pool.addCommand(cmd)
        pool.join()

        items = pool.getCompletedItems()
        failed = []
        for i in items:
            if not i.was_successful():
                failed.append("[{}:{}]".format(i.remoteHost, i.get_stderr().rstrip()))
        pool.haltWork()
        pool.joinWorkers()
        if failed:
            msg = ",".join(failed)
        return msg
