#!/usr/bin/env python3
#
# Copyright (c) Greenplum Inc 2010. All Rights Reserved. 
#
""" 
Unit testing for gpversion module
"""
import unittest

from gpversion import *

# Cloudberry: GpVersion keeps at most three version numbers, compares
# releases by the major number only, and has no "<<" operator.  Build the
# test versions from MAIN_VERSION, so that the tests do not break when the
# major version changes.
CURRENT = MAIN_VERSION[0]
OLDER = CURRENT - 1


class GpVersionTestCase(unittest.TestCase):

    def _assert_older_release(self, v, build):
        self.assertFalse(v.isVersionCurrentRelease())
        self.assertEqual(v.getVersionBuild(), build)
        self.assertEqual(v.getVersionRelease(), str(OLDER))
        self.assertTrue(v.isVersionRelease("%d.1" % OLDER))
        self.assertTrue(v.isVersionRelease("%d.9" % OLDER))
        self.assertFalse(v.isVersionRelease(str(CURRENT)))
        self.assertTrue(v > "%d.0.5" % OLDER)
        self.assertTrue(v < "%d.0" % CURRENT)

    def test_main(self):
        vMain = GpVersion("main")

        self.assertTrue(vMain.isVersionCurrentRelease())
        self.assertEqual(vMain.getVersionBuild(), 'dev')
        self.assertEqual(str(vMain), 'main build dev')

    def test_list(self):
        self._assert_older_release(GpVersion([OLDER, 1, 0]), 'dev')

    def test_string_with_build(self):
        self._assert_older_release(GpVersion("%d.1 build dev" % OLDER), 'dev')

    def test_special_build(self):
        self._assert_older_release(GpVersion("%d.1.filerep" % OLDER), 'filerep')

    def test_gp_version_output(self):
        # Format of "postgres --gp-version"
        v = GpVersion("postgres (Apache Cloudberry) %d.1.0 build 3" % OLDER)
        self._assert_older_release(v, '3')

    def test_select_version_output(self):
        # Format of "SELECT version()"
        v = GpVersion("PostgreSQL 16.9 (Apache Cloudberry %d.0.0-devel build dev) "
                      "on x86_64-pc-linux-gnu, compiled by gcc (GCC) 11.5.0, "
                      "64-bit compiled on Nov 16 2026 19:03:57" % CURRENT)
        self.assertTrue(v.isVersionCurrentRelease())
        self.assertEqual(v.getVersionRelease(), str(CURRENT))
        self.assertTrue(v.isVersionRelease("%d.0" % CURRENT))
        self.assertFalse(v.isVersionRelease(str(OLDER)))
        self.assertTrue(v > "%d.9.9" % OLDER)

    def test_version_too_long(self):
        with self.assertRaisesRegex(Exception, 'Version too long'):
            GpVersion([OLDER, 1, 0, 0])

    def test_unrecognised_version(self):
        with self.assertRaisesRegex(Exception, 'unable to coerce to version'):
            GpVersion("nodigits")


#------------------------------- Mainline --------------------------------
if __name__ == '__main__':
    unittest.main()
