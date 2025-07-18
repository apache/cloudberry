#!/usr/bin/env python
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved.
#
# Greengage DB related utility functions

import os

def get_gp_prefix(masterDatadir):
    base = os.path.basename(masterDatadir)
    idx = base.rfind('-1')
    if idx == -1:
        return None
    return base[0:idx]

