#!/usr/bin/env python3

"""
ELF Dependency Analyzer

This script analyzes ELF (Executable and Linkable Format) binaries to determine their runtime
package dependencies. It can process individual files or recursively analyze directories.

The script provides information about:
- Required packages and their versions
- Missing libraries
- Custom or non-RPM libraries
- Other special cases

It also groups packages by their high-level dependencies, which can be cached for performance.

Usage:
    python3 elf_dependency_analyzer.py [--rebuild-cache] <file_or_directory> [<file_or_directory> ...]

The script will automatically determine if each argument is a file or directory and process accordingly.
Use --rebuild-cache to force rebuilding of the high-level packages cache.

Requirements:
- Python 3.6+
- prettytable (pip install prettytable)
- python-dateutil (pip install python-dateutil)
- ldd (usually pre-installed on Linux systems)
- file (usually pre-installed on Linux systems)
- rpm (usually pre-installed on RPM-based Linux distributions)
- repoquery (part of yum-utils package)

Functions:
- check_requirements(): Checks if all required commands are available.
- run_command(command): Executes a shell command and returns its output.
- parse_ldd_line(line): Parses a line of ldd output to extract the library name.
- find_library_in_ld_library_path(lib_name): Searches for a library in LD_LIBRARY_PATH.
- get_package_info(lib_path): Gets package information for a given library.
- get_package_dependencies(package): Gets dependencies of a package using repoquery.
- build_high_level_packages(grand_summary): Builds a mapping of high-level packages to their dependencies.
- load_or_build_high_level_packages(grand_summary, force_rebuild): Loads or builds the high-level packages cache.
- print_summary(packages, special_cases, missing_libraries, binary_path): Prints a summary for a single binary.
- process_binary(binary_path): Processes a single binary file.
- is_elf_binary(file_path): Checks if a file is an ELF binary.
- print_grand_summary(...): Prints a grand summary of all processed binaries.
- analyze_path(path, grand_summary, grand_special_cases, grand_missing_libraries): Analyzes a file or directory.
- main(): Main function to handle command-line arguments and initiate the analysis.

This script is designed to help system administrators and developers understand the dependencies
of ELF binaries in their systems, which can be useful for troubleshooting, optimizing, or
preparing deployment packages.
"""

import os, subprocess, re, sys, json, shutil
from collections import defaultdict
from datetime import datetime, timedelta
import argparse
from prettytable import PrettyTable
from dateutil import parser

CACHE_FILE = 'high_level_packages_cache.json'
CACHE_EXPIRY_DAYS = 7

def check_requirements():
    required_commands = ['ldd', 'file', 'rpm', 'repoquery']
    missing_commands = [cmd for cmd in required_commands if shutil.which(cmd) is None]
    if missing_commands:
        print("Error: The following required commands are missing:")
        for cmd in missing_commands:
            print(f"  - {cmd}")
        print("\nPlease install these commands and try again.")
        if 'repoquery' in missing_commands:
            print("Note: 'repoquery' is typically part of the 'yum-utils' package.")
        sys.exit(1)

def run_command(command):
    try:
        return subprocess.check_output(command, stderr=subprocess.STDOUT).decode('utf-8')
    except subprocess.CalledProcessError as e:
        print(f"Error running command {' '.join(command)}: {e.output.decode('utf-8').strip()}")
        return None

def parse_ldd_line(line):
    match = re.search(r'\s*(\S+) => (\S+) \((0x[0-9a-f]+)\)', line)
    return match.group(1) if match else None

def find_library_in_ld_library_path(lib_name):
    ld_library_path = os.environ.get('LD_LIBRARY_PATH', '')
    for directory in ld_library_path.split(':'):
        potential_path = os.path.join(directory, lib_name)
        if os.path.isfile(potential_path):
            return potential_path
    return None

def get_package_info(lib_path):
    if not os.path.isfile(lib_path):
        lib_name = os.path.basename(lib_path)
        lib_path = find_library_in_ld_library_path(lib_name)
        if not lib_path:
            return None
    try:
        full_package_name = run_command(['rpm', '-qf', lib_path])
        if full_package_name:
            package_name = full_package_name.split('-')[0]
            return package_name, full_package_name.strip()
    except subprocess.CalledProcessError:
        pass
    return None

def get_package_dependencies(package):
    try:
        output = subprocess.check_output(['repoquery', '--requires', '--resolve', package],
                                         universal_newlines=True, stderr=subprocess.DEVNULL)
        return set(output.strip().split('\n'))
    except subprocess.CalledProcessError:
        return set()

def build_high_level_packages(grand_summary):
    all_packages = set()
    for packages in grand_summary.values():
        all_packages.update(package.split('-')[0] for package in packages)
    high_level_packages = {}
    for package in all_packages:
        deps = get_package_dependencies(package)
        if deps:
            high_level_packages[package] = [dep.split('-')[0] for dep in deps]
    return high_level_packages

def load_or_build_high_level_packages(grand_summary, force_rebuild=False):
    if not force_rebuild and os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, 'r') as f:
            cache_data = json.load(f)
        if datetime.now() - parser.parse(cache_data['timestamp']) < timedelta(days=CACHE_EXPIRY_DAYS):
            return cache_data['packages']
    packages = build_high_level_packages(grand_summary)
    with open(CACHE_FILE, 'w') as f:
        json.dump({'timestamp': datetime.now().isoformat(), 'packages': packages}, f)
    return packages

def print_summary(packages, special_cases, missing_libraries, binary_path):
    print("\nSummary of unique runtime packages required:")
    table = PrettyTable(['Package Name', 'Full Package Name'])
    table.align['Package Name'] = 'l'
    table.align['Full Package Name'] = 'l'
    unique_packages = sorted(set(packages))
    for package_name, full_package_name in unique_packages:
        table.add_row([package_name, full_package_name])
    print(table)
    if missing_libraries:
        print("\nMISSING LIBRARIES:")
        missing_table = PrettyTable(['Missing Library', 'Referenced By'])
        missing_table.align['Missing Library'] = 'l'
        missing_table.align['Referenced By'] = 'l'
        for lib in missing_libraries:
            missing_table.add_row([lib, binary_path])
        print(missing_table)
    if special_cases:
        print("\nSPECIAL CASES:")
        special_table = PrettyTable(['Library/Case', 'Referenced By', 'Category'])
        special_table.align['Library/Case'] = 'l'
        special_table.align['Referenced By'] = 'l'
        special_table.align['Category'] = 'l'
        for case in special_cases:
            category = "Custom/Non-RPM" if "custom or non-RPM library" in case else "Other"
            library = case.split(" is ")[0] if " is " in case else case
            special_table.add_row([library, binary_path, category])
        print(special_table)
    else:
        print("\nSPECIAL CASES: None found")

def process_binary(binary_path):
    print(f"Binary: {binary_path}\n")
    print("Libraries and their corresponding packages:")
    packages, special_cases, missing_libraries = [], [], []
    known_special_cases = ['linux-vdso.so.1', 'ld-linux-x86-64.so.2']
    ldd_output = run_command(['ldd', binary_path])
    if ldd_output is None:
        return packages, special_cases, missing_libraries
    for line in ldd_output.splitlines():
        if any(special in line for special in known_special_cases):
            continue
        parts = line.split('=>')
        lib_name = parts[0].strip()
        if "not found" in line:
            missing_libraries.append(lib_name)
            print(f"MISSING: {line.strip()}")
        else:
            if len(parts) > 1:
                lib_path = parts[1].split()[0]
                if lib_path != "not":
                    package_info = get_package_info(lib_path)
                    if package_info:
                        print(f"{lib_path} => {package_info[1]}")
                        packages.append(package_info)
                    else:
                        if os.path.exists(lib_path):
                            special_case = f"{lib_path} is a custom or non-RPM library"
                            special_cases.append(special_case)
                            print(f"{lib_path} => Custom or non-RPM library")
                        else:
                            special_case = f"{lib_path} is not found and might be a special case"
                            special_cases.append(special_case)
                            print(f"{lib_path} => Not found, might be a special case")
                else:
                    special_case = f"{line.strip()} is a special case or built-in library"
                    special_cases.append(special_case)
                    print(f"{line.strip()} => Special case or built-in library")
            else:
                special_case = f"{line.strip()} is a special case or built-in library"
                special_cases.append(special_case)
                print(f"{line.strip()} => Special case or built-in library")
    if special_cases:
        print(f"Special cases found for {binary_path}:")
        for case in special_cases:
            print(f"  - {case}")
    else:
        print(f"No special cases found for {binary_path}")
    print_summary(packages, special_cases, missing_libraries, binary_path)
    print("-------------------------------------------")
    return packages, special_cases, missing_libraries

def is_elf_binary(file_path):
    file_output = run_command(['file', file_path])
    return 'ELF' in file_output and ('executable' in file_output or 'shared object' in file_output)

def print_grand_summary(grand_summary, grand_special_cases, grand_missing_libraries, HIGH_LEVEL_PACKAGES, PACKAGE_TO_HIGH_LEVEL):
    if grand_summary or grand_special_cases or grand_missing_libraries:
        print("\nGrand Summary of high-level runtime packages required across all binaries:")
        high_level_summary = defaultdict(set)
        for package_name, full_package_names in grand_summary.items():
            high_level_package = PACKAGE_TO_HIGH_LEVEL.get(package_name.split('-')[0], package_name.split('-')[0])
            high_level_summary[high_level_package].update(full_package_names)
        table = PrettyTable(['High-Level Package', 'Included Packages'])
        table.align['High-Level Package'] = 'l'
        table.align['Included Packages'] = 'l'
        for high_level_package, full_package_names in sorted(high_level_summary.items()):
            included_packages = '\n'.join(sorted(full_package_names))
            table.add_row([high_level_package, included_packages])
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
                category = "Custom/Non-RPM" if "custom or non-RPM library" in case else "Other"
                library = case.split(" is ")[0] if " is " in case else case
                special_table.add_row([library, binary, category])
            print(special_table)
        else:
            print("No special cases found.")

def analyze_path(path, grand_summary, grand_special_cases, grand_missing_libraries):
    if os.path.isfile(path):
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
    if grand_special_cases:
        print(f"Accumulated special cases after processing {path}:")
        for case, binary in grand_special_cases:
            print(f"  - {case} (in {binary})")
    else:
        print(f"No special cases accumulated after processing {path}")

def main():
    check_requirements()
    parser = argparse.ArgumentParser(description="ELF Dependency Analyzer")
    parser.add_argument('paths', nargs='+', help="Paths to files or directories to analyze")
    parser.add_argument('--rebuild-cache', action='store_true', help="Force rebuild of the high-level packages cache")
    args = parser.parse_args()
    grand_summary = defaultdict(set)
    grand_special_cases = []
    grand_missing_libraries = defaultdict(set)
    for path in args.paths:
        analyze_path(path, grand_summary, grand_special_cases, grand_missing_libraries)
    HIGH_LEVEL_PACKAGES = load_or_build_high_level_packages(grand_summary, args.rebuild_cache)
    PACKAGE_TO_HIGH_LEVEL = {low: high for high, lows in HIGH_LEVEL_PACKAGES.items() for low in lows}
    print_grand_summary(grand_summary, grand_special_cases, grand_missing_libraries, HIGH_LEVEL_PACKAGES, PACKAGE_TO_HIGH_LEVEL)

if __name__ == '__main__':
    main()
