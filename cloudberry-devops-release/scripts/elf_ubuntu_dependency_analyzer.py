#!/usr/bin/env python3

"""
ELF Dependency Analyzer for Ubuntu

This script analyzes ELF (Executable and Linkable Format) binaries to determine their runtime
package dependencies on Ubuntu systems. It can process individual files or recursively analyze directories.

The script provides information about:
- Required packages and their versions
- Custom or non-APT libraries
- Core system libraries
- Missing libraries
- Other special cases

Usage:
    python3 elf_dependency_analyzer.py [file_or_directory] [file_or_directory] ...

Requirements:
- Python 3.6+
- prettytable (pip install prettytable)
- ldd (usually pre-installed on Linux systems)
- file (usually pre-installed on Linux systems)
- dpkg (pre-installed on Ubuntu)
"""

import os
import subprocess
import re
import sys
import argparse
from collections import defaultdict
from prettytable import PrettyTable

def run_command(command):
    """
    Execute a shell command and return its output.

    Args:
    command (list): The command to execute as a list of strings.

    Returns:
    str: The output of the command, or None if an error occurred.
    """
    try:
        return subprocess.check_output(command, stderr=subprocess.STDOUT).decode('utf-8')
    except subprocess.CalledProcessError as e:
        print(f"Error running command {' '.join(command)}: {e.output.decode('utf-8').strip()}")
        return None

def get_package_info(lib_path):
    """
    Get package information for a given library path.

    Args:
    lib_path (str): The path to the library.

    Returns:
    tuple: A tuple containing the package name and full package information.
    """
    if lib_path.startswith('/usr/local/cloudberry-db'):
        return "cloudberry-custom", f"Cloudberry custom library: {lib_path}"

    dpkg_output = run_command(['dpkg', '-S', lib_path])
    if dpkg_output:
        package_name = dpkg_output.split(':')[0]
        return package_name, dpkg_output.strip()

    # List of core system libraries that might not be individually tracked by dpkg
    core_libs = {
        'libc.so': 'libc6',
        'libm.so': 'libc6',
        'libdl.so': 'libc6',
        'libpthread.so': 'libc6',
        'libresolv.so': 'libc6',
        'librt.so': 'libc6',
        'libgcc_s.so': 'libgcc-s1',
        'libstdc++.so': 'libstdc++6',
        'libz.so': 'zlib1g',
        'libbz2.so': 'libbz2-1.0',
        'libpam.so': 'libpam0g',
        'libaudit.so': 'libaudit1',
        'libcap-ng.so': 'libcap-ng0',
        'libkeyutils.so': 'libkeyutils1',
        'liblzma.so': 'liblzma5',
        'libcom_err.so': 'libcomerr2'
    }

    lib_name = os.path.basename(lib_path)
    for core_lib, package in core_libs.items():
        if lib_name.startswith(core_lib):
            return package, f"Core system library: {lib_path}"

    # If not a recognized core library, return as system library
    file_output = run_command(['file', lib_path])
    if file_output:
        return "system-library", f"System library: {lib_path} - {file_output.strip()}"

    return None

def print_summary(packages, special_cases, missing_libraries, binary_path):
    """
    Print a summary of the dependencies for a binary.

    Args:
    packages (list): List of package tuples (package_name, full_package_name).
    special_cases (list): List of special case strings.
    missing_libraries (list): List of missing library names.
    binary_path (str): Path to the binary being analyzed.
    """
    print("\nSummary of runtime dependencies:")
    table = PrettyTable(['Category', 'Package/Library', 'Details'])
    table.align['Category'] = 'l'
    table.align['Package/Library'] = 'l'
    table.align['Details'] = 'l'

    categories = {
        'cloudberry-custom': 'Cloudberry Custom',
        'system-library': 'System Library',
    }

    for package_name, full_package_name in sorted(set(packages)):
        category = categories.get(package_name, 'System Package')
        table.add_row([category, package_name, full_package_name])

    print(table)

    if missing_libraries:
        print("\nMISSING LIBRARIES:")
        for lib in missing_libraries:
            print(f"  - {lib}")

    if special_cases:
        print("\nSPECIAL CASES:")
        for case in special_cases:
            print(f"  - {case}")

def process_binary(binary_path):
    """
    Process a single binary file to determine its dependencies.

    Args:
    binary_path (str): Path to the binary file.

    Returns:
    tuple: A tuple containing lists of packages, special cases, and missing libraries.
    """
    print(f"Binary: {binary_path}\n")
    print("Libraries and their corresponding packages:")
    packages, special_cases, missing_libraries = [], [], []

    ldd_output = run_command(['ldd', binary_path])
    if ldd_output is None:
        return packages, special_cases, missing_libraries

    for line in ldd_output.splitlines():
        if "=>" not in line:
            continue

        parts = line.split('=>')
        lib_name = parts[0].strip()
        lib_path = parts[1].split()[0].strip()
        lib_path = os.path.realpath(lib_path)

        if lib_path == "not":
            missing_libraries.append(lib_name)
            print(f"MISSING: {line.strip()}")
        else:
            package_info = get_package_info(lib_path)
            if package_info:
                print(f"{lib_path} => {package_info[1]}")
                packages.append(package_info)
            else:
                special_case = f"{lib_path} is not found and might be a special case"
                special_cases.append(special_case)
                print(f"{lib_path} => Not found, might be a special case")

    print_summary(packages, special_cases, missing_libraries, binary_path)
    print("-------------------------------------------")
    return packages, special_cases, missing_libraries

def is_elf_binary(file_path):
    """
    Check if a file is an ELF binary.

    Args:
    file_path (str): Path to the file.

    Returns:
    bool: True if the file is an ELF binary, False otherwise.
    """
    file_output = run_command(['file', file_path])
    return 'ELF' in file_output and ('executable' in file_output or 'shared object' in file_output)

def print_grand_summary(grand_summary, grand_special_cases, grand_missing_libraries):
    """
    Print a grand summary of all analyzed binaries.

    Args:
    grand_summary (dict): Dictionary of all packages and their details.
    grand_special_cases (list): List of all special cases.
    grand_missing_libraries (dict): Dictionary of all missing libraries.
    """
    if grand_summary or grand_special_cases or grand_missing_libraries:
        print("\nGrand Summary of runtime packages required across all binaries:")
        table = PrettyTable(['Package', 'Included Packages'])
        table.align['Package'] = 'l'
        table.align['Included Packages'] = 'l'
        for package_name, full_package_names in sorted(grand_summary.items()):
            included_packages = '\n'.join(sorted(full_package_names))
            table.add_row([package_name, included_packages])
        print(table)

        if grand_missing_libraries:
            print("\nGrand Summary of MISSING LIBRARIES across all binaries:")
            missing_table = PrettyTable(['Missing Library', 'Referenced By'])
            missing_table.align['Missing Library'] = 'l'
            missing_table.align['Referenced By'] = 'l'
            for lib, binaries in sorted(grand_missing_libraries.items()):
                missing_table.add_row([lib, '\n'.join(sorted(binaries))])
            print(missing_table)

        print("\nGrand Summary of special cases across all binaries:")
        if grand_special_cases:
            special_table = PrettyTable(['Library/Case', 'Referenced By', 'Category'])
            special_table.align['Library/Case'] = 'l'
            special_table.align['Referenced By'] = 'l'
            special_table.align['Category'] = 'l'
            for case, binary in sorted(set(grand_special_cases)):
                category = "System Library" if "system library" in case else "Other"
                library = case.split(" is ")[0] if " is " in case else case
                special_table.add_row([library, binary, category])
            print(special_table)
        else:
            print("No special cases found.")

def analyze_path(path, grand_summary, grand_special_cases, grand_missing_libraries):
    """
    Analyze a file or directory for ELF binaries and their dependencies.

    Args:
    path (str): Path to the file or directory to analyze.
    grand_summary (dict): Dictionary to store all package information.
    grand_special_cases (list): List to store all special cases.
    grand_missing_libraries (dict): Dictionary to store all missing libraries.
    """
    if os.path.isfile(path):
        if is_elf_binary(path):
            packages, special_cases, missing_libraries = process_binary(path)
            for package_name, full_package_name in packages:
                grand_summary[package_name].add(full_package_name)
            grand_special_cases.extend((case, path) for case in special_cases)
            for lib in missing_libraries:
                grand_missing_libraries[lib].add(path)
    elif os.path.isdir(path):
        for root, dirs, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                if is_elf_binary(file_path):
                    packages, special_cases, missing_libraries = process_binary(file_path)
                    for package_name, full_package_name in packages:
                        grand_summary[package_name].add(full_package_name)
                    grand_special_cases.extend((case, file_path) for case in special_cases)
                    for lib in missing_libraries:
                        grand_missing_libraries[lib].add(file_path)
    else:
        print(f"Error: {path} is neither a valid file nor a directory.")

def main():
    """
    Main function to handle command-line arguments and initiate the analysis.
    """
    parser = argparse.ArgumentParser(description="ELF Dependency Analyzer for Ubuntu")
    parser.add_argument('paths', nargs='+', help="Paths to files or directories to analyze")
    args = parser.parse_args()

    grand_summary = defaultdict(set)
    grand_special_cases = []
    grand_missing_libraries = defaultdict(set)

    for path in args.paths:
        analyze_path(path, grand_summary, grand_special_cases, grand_missing_libraries)

    print_grand_summary(grand_summary, grand_special_cases, grand_missing_libraries)

if __name__ == '__main__':
    main()
