#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build standalone Anser tests with strict compilers and ASan/UBSan.

Uses a configured source tree and its built src/common and src/port libraries.
All binaries and diagnostic logs go to a temporary directory; existing build
flags, configuration headers and test binaries are left untouched.
"""

from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[3]
TEST = ROOT / "gpcontrib/anser/test"
TESTS = (
    "anserfold_test", "anserdetach_test", "ansernormalize_test",
    "anserplanwalk_test", "ansercancel_test", "anserapi_test",
)


def find_compiler(candidates, want_gcc):
    for name in candidates:
        path = shutil.which(name)
        if path is None:
            continue
        version = subprocess.check_output([path, "--version"], text=True)
        if ("clang" not in version.lower()) == want_gcc:
            return path
    return None


def main():
    output = Path(tempfile.mkdtemp(prefix="anser-localcheck-"))
    print("Builds and logs:", output, flush=True)
    gcc = find_compiler(["gcc-15", "gcc-14", "gcc-13", "gcc"], True)
    clang = find_compiler(["clang"], False)
    profiles = []
    if gcc:
        profiles.append(("gcc-strict", gcc, [
            "-O3", "-Warray-bounds=2", "-Wstringop-overflow=2",
            "-Wimplicit-fallthrough=3", "-Wmaybe-uninitialized",
        ]))
    else:
        print("GNU GCC unavailable: GCC-specific diagnostics skipped.")
    if clang:
        profiles.append(("clang-strict", clang, [
            "-O3", "-Warray-bounds", "-Wconditional-uninitialized",
        ]))
        profiles.append(("clang-sanitizers", clang, [
            "-O1", "-g", "-fsanitize=address,undefined",
            "-fno-sanitize-recover=all", "-fno-omit-frame-pointer",
        ]))
    else:
        print("Clang unavailable: Clang and sanitizer checks skipped.")
    if not profiles:
        raise SystemExit("No supported compiler found.")
    common = [
        "-Wall", "-Werror", "-Wmissing-prototypes", "-Wpointer-arith",
        "-Wvla", "-Wcast-function-type", "-Wformat=2", "-Wnull-dereference",
        "-fno-strict-aliasing", "-fwrapv", "-ffunction-sections", "-fdata-sections",
    ]
    includes = ["-I" + str(p) for p in (
        ROOT / "src/include", ROOT / "src/backend",
        ROOT / "src/interfaces/libpq", ROOT / "gpcontrib/anser/include",
    )]
    dead_strip = "-Wl,-dead_strip" if platform.system() == "Darwin" else "-Wl,--gc-sections"
    failures = 0
    for profile, compiler, flags in profiles:
        extra = []
        if profile.startswith("clang"):
            # Match the configured PostgreSQL build: executor callbacks use
            # first-member casts between ScanState and CustomScanState.
            extra.append("-Wno-cast-function-type-strict")
        # Clang-configured Darwin headers use its syslog format attribute.
        # GCC understands gnu_printf instead. This changes only attribute
        # spelling for this invocation, preserving format checking itself.
        if profile == "gcc-strict" and platform.system() == "Darwin":
            config = (ROOT / "src/include/pg_config.h").read_text()
            if "#define PG_PRINTF_ATTRIBUTE __syslog__" in config:
                extra.append("-D__syslog__=__gnu_printf__")
        for name in TESTS:
            binary = output / (profile + "-" + name)
            sources = [str(TEST / (name + ".c"))]
            libraries = []
            if name == "anserfold_test":
                sources.append(str(ROOT / "gpcontrib/anser/src/anserfilter.c"))
                libraries = ["-L" + str(ROOT / "src/common"),
                             "-L" + str(ROOT / "src/port"),
                             "-lpgcommon", "-lpgport", "-lbz2", "-lm"]
            elif name == "anserdetach_test":
                sources.append(str(ROOT / "src/interfaces/libpq/libpq-events.c"))
            # This regression deliberately exercises unoptimized recursion.
            optimization = ["-O0"] if name == "anserplanwalk_test" else []
            command = [compiler] + common + flags + extra + includes + optimization
            command += sources + [dead_strip] + libraries + ["-o", str(binary)]
            log_path = binary.with_suffix(".log")
            with log_path.open("w") as log:
                log.write(shlex.join(command) + "\n")
                log.flush()
                result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=log)
                if result.returncode == 0:
                    # Fixture allocators intentionally live until process exit;
                    # test address/UB errors, not those fixture-only leaks.
                    run = ["env", "ASAN_OPTIONS=detect_leaks=0:halt_on_error=1",
                           "UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1", str(binary)]
                    result = subprocess.run(run, cwd=ROOT, stdout=log, stderr=log)
            if result.returncode:
                failures += 1
                print("FAIL", profile, name, "--", log_path, flush=True)
            else:
                print("PASS", profile, name, flush=True)
        if profile.endswith("strict"):
            sources = sorted((ROOT / "gpcontrib/anser/src").glob("*.c"))
            source_failures = 0
            for source in sources:
                command = [compiler] + common + flags + extra + includes
                command += ["-fsyntax-only", str(source)]
                log_path = output / (profile + "-source-" + source.stem + ".log")
                with log_path.open("w") as log:
                    log.write(shlex.join(command) + "\n")
                    log.flush()
                    result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=log)
                if result.returncode:
                    failures += 1
                    source_failures += 1
                    print("FAIL", profile, source.name, "--", log_path, flush=True)
            if not source_failures:
                print("PASS", profile, "production syntax (" + str(len(sources)) + " files)", flush=True)
    if failures:
        raise SystemExit(str(failures) + " check(s) failed; inspect the logs above.")


if __name__ == "__main__":
    main()
