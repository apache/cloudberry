# vendored nanoarrow

- Upstream: https://github.com/apache/arrow-nanoarrow, release `apache-arrow-nanoarrow-0.7.0`
- Generated with: `python3 ci/scripts/bundle.py --output-dir <out> --symbol-namespace LanceFdw`
  (single-file amalgamation; all symbols prefixed `LanceFdw` so another extension that also
  vendors nanoarrow cannot clash inside one backend)
- License: Apache-2.0 (see LICENSE.txt / NOTICE.txt here)
- Do not edit these files by hand; regenerate from the upstream release and bump this note.
