#!/usr/bin/env perl
# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership.  The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
#
# --------------------------------------------------------------------
#
# Script: parse_results.pl
# Description: Processes Cloudberry test output to extract statistics
# and results.
#             Analyzes test log files to determine:
#             1. Overall test status (pass/fail)
#             2. Total number of tests run
#             3. Number of passed, failed, and ignored tests
#             4. Names of failed and ignored tests
#             5. Validates test counts for consistency
#             Results are written to a file for shell script processing.
#
# Arguments:
#   log-file    Path to test log file (required)
#
# Input File Format:
#   Expects test log files containing one of the following summary formats:
#   - "All X tests passed."
#   - "Y of X tests failed."
#   - "X of Y tests passed, Z failed test(s) ignored."
#   - "X of Y tests failed, Z of these failures ignored."
#
#   And failed or ignored test entries in format:
#   - "test_name ... FAILED"
#   - "test_name ... failed (ignored)"
#
# Output File (test_results.txt):
#   Environment variable format:
#   STATUS=passed|failed
#   TOTAL_TESTS=<number>
#   FAILED_TESTS=<number>
#   PASSED_TESTS=<number>
#   IGNORED_TESTS=<number>
#   FAILED_TEST_NAMES=<comma-separated-list>
#   IGNORED_TEST_NAMES=<comma-separated-list>
#
# Prerequisites:
#   - Read access to input log file
#   - Write access to current directory
#   - Perl 5.x or higher
#
# Exit Codes:
#   0 - All tests passed, or only ignored failures occurred
#   1 - Some non-ignored tests failed
#   2 - Parse error or cannot access files
#
# Example Usage:
#   ./parse_results.pl test_output.log
#
# Error Handling:
#   - Validates input file existence and readability
#   - Verifies failed and ignored test counts match found entries
#   - Reports parsing errors with detailed messages
#
# --------------------------------------------------------------------

use strict;
use warnings;

# Exit codes
use constant {
    SUCCESS => 0,
    TEST_FAILURE => 1,
    PARSE_ERROR => 2
};

# Get log file path from command line argument
my $file = $ARGV[0] or die "Usage: $0 LOG_FILE\n";
print "Parsing test results from: $file\n";

# Check if file exists and is readable
unless (-e $file) {
    print "Error: File does not exist: $file\n";
    exit PARSE_ERROR;
}
unless (-r $file) {
    print "Error: File is not readable: $file\n";
    exit PARSE_ERROR;
}

# Open and parse the log file
open(my $fh, '<', $file) or do {
    print "Cannot open log file: $! (looking in $file)\n";
    exit PARSE_ERROR;
};

# Initialize variables
my ($status, $total_tests, $failed_tests, $ignored_tests, $passed_tests) = ('', 0, 0, 0, 0);
my @failed_test_list = ();
my @ignored_test_list = ();

while (<$fh>) {
    # Match the summary lines
    if (/All (\d+) tests passed\./) {
        $status = 'passed';
        $total_tests = $1;
        $passed_tests = $1;
    }
    elsif (/(\d+) of (\d+) tests passed, (\d+) failed test\(s\) ignored\./) {
        $status = 'passed';
        $passed_tests = $1;
        $total_tests = $2;
        $ignored_tests = $3;
    }
    elsif (/(\d+) of (\d+) tests failed\./) {
        $status = 'failed';
        $failed_tests = $1;
        $total_tests = $2;
        $passed_tests = $2 - $1;
    }
    elsif (/(\d+) of (\d+) tests failed, (\d+) of these failures ignored\./) {
        $status = 'failed';
        $failed_tests = $1 - $3;
        $ignored_tests = $3;
        $total_tests = $2;
        $passed_tests = $2 - $1;
    }

    # Capture failed tests
    if (/^(?:\s+|test\s+)(\S+)\s+\.\.\.\s+FAILED\s+/) {
        push @failed_test_list, $1;
    }

    # Capture ignored tests
    if (/^(?:\s+|test\s+)(\S+)\s+\.\.\.\s+failed \(ignored\)/) {
        push @ignored_test_list, $1;
    }
}

# Close the log file
close $fh;

# Validate failed test count matches found test names
if ($status eq 'failed' && scalar(@failed_test_list) != $failed_tests) {
    print "Error: Found $failed_tests failed tests in summary but found " . scalar(@failed_test_list) . " failed test names\n";
    print "Failed test names found:\n";
    foreach my $test (@failed_test_list) {
        print "  - $test\n";
    }
    exit PARSE_ERROR;
}

# Validate ignored test count matches found test names
if ($ignored_tests != scalar(@ignored_test_list)) {
    print "Error: Found $ignored_tests ignored tests in summary but found " . scalar(@ignored_test_list) . " ignored test names\n";
    print "Ignored test names found:\n";
    foreach my $test (@ignored_test_list) {
        print "  - $test\n";
    }
    exit PARSE_ERROR;
}

# Write results to the results file
open my $result_fh, '>', 'test_results.txt' or die "Cannot write to results file: $!\n";
print $result_fh "STATUS=$status\n";
print $result_fh "TOTAL_TESTS=$total_tests\n";
print $result_fh "PASSED_TESTS=$passed_tests\n";
print $result_fh "FAILED_TESTS=$failed_tests\n";
print $result_fh "IGNORED_TESTS=$ignored_tests\n";
if (@failed_test_list) {
    print $result_fh "FAILED_TEST_NAMES=" . join(',', @failed_test_list) . "\n";
}
if (@ignored_test_list) {
    print $result_fh "IGNORED_TEST_NAMES=" . join(',', @ignored_test_list) . "\n";
}
close $result_fh;

# Print to stdout for logging
print "Test Results:\n";
print "Status: $status\n";
print "Total Tests: $total_tests\n";
print "Failed Tests: $failed_tests\n";
print "Ignored Tests: $ignored_tests\n";
print "Passed Tests: $passed_tests\n";
if (@failed_test_list) {
    print "Failed Test Names:\n";
    foreach my $test (@failed_test_list) {
        print "  - $test\n";
    }
}
if (@ignored_test_list) {
    print "Ignored Test Names:\n";
    foreach my $test (@ignored_test_list) {
        print "  - $test\n";
    }
}

# Exit with appropriate code
if ($status eq 'passed') {
    exit SUCCESS;
} elsif ($status eq 'failed') {
    exit TEST_FAILURE;
} else {
    exit PARSE_ERROR;
}
