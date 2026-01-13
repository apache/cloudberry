#!/usr/bin/env python3
#
# Copyright (c) Greenplum Inc 2008. All Rights Reserved.
#
"""
  gpversion.py:

  Contains GpVersion class for handling version comparisons.
"""

# ===========================================================
import sys, os, re

# Python version 3.5 or newer expected
if sys.version_info < (3, 5, 0):
    sys.stderr.write("Error: %s is supported on Python versions 3.5 or greater\n"
                     "Please upgrade python installed on this machine."
                     % os.path.split(__file__)[-1])
    sys.exit(1)

MAIN_VERSION = [3,99]    # version number for main


#============================================================
class GpVersion:
    '''
    The gpversion class is an abstraction of a given Cloudberry release
    version.  It exists in order to facilitate version comparisons,
    formatting, printing, etc.

      x = GpVersion([3,2,0,4]) => Cloudberry 3.2.0.4
      x = GpVersion('3.2')     => Cloudberry 3.2 dev
      x = GpVersion('3.2 build dev') => Cloudberry 3.2 dev
      x = GpVersion('main')    => Cloudberry main
      x.major()                => Major release, eg "3.2"
      x.isrelease('3.2')       => Major version comparison
    '''

    """
    Past major versions of GPDB. Used for shift arithmetic
    and reasoning about separation between versions.
    """
    history = [
                '3.2',
                '3.3',
                '4.0',
                '4.1',
                '4.2',
                '4.3',
                '5',
                '6',
                '7',
                '8'
    ]

    #------------------------------------------------------------
    def __init__(self, version):
        '''
        The initializer for GpVersion is complicated so that it can handle
        creation via several methods:
           x = GpVersion('3.2 dev')  => via string
           x = GpVersion([3,2])  => via tuple
           y = GpVersion(x)      => copy constructor

        If the input datatype is not recognised then it is first cast to
        a string and then converted.
        '''
        try:
            self.version = None
            self.build   = None

            # Local copy that we can safely manipulate
            v = version

            # Copy constructor
            if isinstance(v, GpVersion):
                self.version = v.version
                self.build   = v.build
                return

            # if version isn't a type we recognise then convert to a string
            # first
            if not (isinstance(v, str) or
                    isinstance(v, list) or
                    isinstance(v, tuple)):
                v = str(v)

            # Convert a string into the version components.
            #
            # There are several version formats that we anticipate receiving:
            #
            # Versions from "postgres --gp-version":
            #    ".* (Apache Cloudberry) <VERSION> build <BUILD>"
            #
            # Version from sql "select version()"
            #    ".* (Apache Cloudberry <VERSION> build <BUILD>) .*"
            #
            # Versions from python code:
            #    "<VERSION>"
            #    "<VERSION> <BUILD>"
            #
            if isinstance(v, str):
                # See if it matches one of the two the long formats
                regex = r"\(Apache Cloudberry\)? ([^ ]+) build ([^ )]+)"
                m = re.search(regex, v)
                if m:
                    (v, self.build) = m.groups()   # (version, build)

                # Remove any surplus whitespace, if present
                v = v.strip()

                # We should have either "<VERSION> <BUILD>" or "VERSION" so
                # split on whitespace.
                vlist = v.split(' ')
                if len(vlist) == 2:
                    (v, self.build) = vlist
                elif len(vlist) == 3 and vlist[1] == 'build':
                    (v, _, self.build) = vlist
                elif len(vlist) > 2:
                    raise Exception("too many tokens in version")

                # We should now just have "<VERSION>"
                if v == 'main' or v.endswith('_MAIN'):
                    self.version = MAIN_VERSION
                    if not self.build:
                        self.build = 'dev'
                    return

                # Check if version contains any "special build" tokens
                # e.g. "3.4.0.0_EAP1" or "3.4.filerep".
                #
                # For special builds we use the special value for <BUILD>
                # rather than any value calculated above.

                # <VERSION> consists of:
                #    2 digits for major version
                #    optionally another 2 digits for minor version
                #    optionally a string specifying a "special build", eg:
                #
                #        we ignore the usual build version and use the special
                #        vilue for "<BUILD>" instead.
                regex = r"[0123456789.]*\d"
                m = re.search(regex, v)
                if not m:
                    raise Exception("unable to coerce to version")
                if m.end() < len(v):
                    self.build = v[m.end()+1:]
                v = v[m.start():m.end()]

                # version is now just the digits, split on '.' and fall
                # into the default handling of a list argument.
                v = v.split('.')

            # Convert a tuple to a list so that extend and slicing will work
            # nicely
            if isinstance(v, tuple):
                v = list(v)

            # Any input we received should have been
            if not isinstance(v, list):
                raise Exception("Internal coding error")

            minlen = 1
            maxlen = 3
            if len(v) < minlen:
                raise Exception("Version too short")
            elif len(v) > maxlen:
                raise Exception("Version too long")
            v = list(map(int, v))  # Convert to integers
            self.version = v

            if not self.build:
                self.build = 'dev'


        # If part of the conversion process above failed, throw an error,
        except Exception as e:
            raise Exception("Unrecognised Cloudberry Version '%s' due to %s" %
                                (str(version), str(e)))

    #------------------------------------------------------------
    # One of the main reasons for this class is so that we can safely compare
    # versions with each other.  This needs to be pairwise integer comparison
    # of the tuples, not a string comparison, which is why we maintain the
    # internal version as a list.
    #
    # The following functions overload all comparison operators for GpVersion,
    # as per PEP 207 -- Rich Comparisons.

    def __get_version(other):
        if isinstance(other, GpVersion):
            return other.version
        return GpVersion(other).version

    def __lt__(self, other):
        return self.version < GpVersion.__get_version(other)

    def __le__(self, other):
        return self.version <= GpVersion.__get_version(other)

    def __gt__(self, other):
        return self.version > GpVersion.__get_version(other)

    def __ge__(self, other):
        return self.version >= GpVersion.__get_version(other)

    def __eq__(self, other):
        return self.version == GpVersion.__get_version(other)

    def __ne__(self, other):
        return self.version != GpVersion.__get_version(other)

    #------------------------------------------------------------
    def __str__(self):
        '''
        The other main reason for this class is that the display version is
        not the same as the internal version for main and development releases.
        '''
        if self.version == MAIN_VERSION:
            v = 'main'
        else:
            v = '.'.join(map(str,self.version))

        if self.build:
            return "%s build %s" % (v, self.build)
        else:
            return v

#    def __lshift__(self, num):
#        i = self.history.index(self.getVersionRelease())
#        i -= num
#        if i < 0 or num < 0:
#            raise Exception('invalid version shift')
#        return GpVersion(self.history[i] + ".0.0")

    #------------------------------------------------------------
    def getVersionBuild(self):
        '''
        Returns the build number portion of the version.
        '''
        return self.build

    #------------------------------------------------------------
    def getVersionRelease(self):
        '''
        Returns the major (first value) portion of the version.
        '''
        return '%d' % self.version[0]

    #------------------------------------------------------------
    def isVersionRelease(self, version):
        '''
        Returns true if the version matches a particular major release.
        '''
        other = GpVersion(version)
        return self.getVersionRelease() == other.getVersionRelease()

    #------------------------------------------------------------
    def isVersionCurrentRelease(self):
        '''
        Returns true if the version matches the current MAIN_VERSION
        '''
        return self.isVersionRelease(MAIN_VERSION)


