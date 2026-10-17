#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""Sync the generated Lance fixtures into the S3 bucket used by the s3:// cases.

Connection details and credentials come from the environment, or from a file
of ``export NAME=value`` lines given with ``--env-file``:

    LANCE_S3_ENDPOINT_HOST   endpoint reachable from this host, e.g. http://localhost:9000
    LANCE_S3_BUCKET          bucket to upload into
    LANCE_S3_REGION          region to sign with (MinIO needs one)
    LANCE_S3_KEY             access key id
    LANCE_S3_SECRET          secret access key

Each dataset lands at ``s3://<bucket>/<prefix><name>.lance/...``, whose prefix
the regress suites hand to the foreign table as its ``uri``.  Nothing outside
that prefix is touched, and no credential is ever printed.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import urlparse

DEFAULT_PREFIX = "fixtures/"
REQUIRED = ("LANCE_S3_ENDPOINT_HOST", "LANCE_S3_BUCKET", "LANCE_S3_REGION",
            "LANCE_S3_KEY", "LANCE_S3_SECRET")


@dataclass
class Config:
    endpoint: str
    bucket: str
    region: str
    key: str
    secret: str

    @property
    def netloc_and_tls(self) -> tuple[str, bool]:
        return split_endpoint(self.endpoint)

    def uri(self, prefix: str, name: str) -> str:
        return "s3://%s/%s%s.lance" % (self.bucket, prefix, name)


def parse_env_file(text: str) -> dict:
    """Read ``export NAME=value`` lines the way a shell would, roughly.

    Only what such a file needs is supported: one assignment
    per line, optional ``export``, optional single or double quotes, ``#``
    comments on their own line.
    """
    out = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        name, _, value = line.partition("=")
        name, value = name.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if name:
            out[name] = value
    return out


def load_config(env_file: Optional[str], environ=None) -> Config:
    """Environment first, then the env file on top of it (the file wins)."""
    values = dict(environ if environ is not None else os.environ)
    if env_file:
        if not os.path.exists(env_file):
            raise SystemExit("no such env file: %s" % env_file)
        with open(env_file) as fh:
            values.update(parse_env_file(fh.read()))
    missing = [k for k in REQUIRED if not values.get(k)]
    if missing:
        raise SystemExit("missing setting(s): %s" % ", ".join(missing))
    return Config(endpoint=values["LANCE_S3_ENDPOINT_HOST"],
                  bucket=values["LANCE_S3_BUCKET"],
                  region=values["LANCE_S3_REGION"],
                  key=values["LANCE_S3_KEY"],
                  secret=values["LANCE_S3_SECRET"])


def split_endpoint(url: str) -> tuple[str, bool]:
    """``http://localhost:9000`` -> ``("localhost:9000", False)``."""
    if "//" not in url:
        url = "//" + url
    parsed = urlparse(url, scheme="http")
    if not parsed.netloc:
        raise SystemExit("cannot parse endpoint %r" % url)
    return parsed.netloc, parsed.scheme == "https"


def dataset_names(data_dir: str, wanted: Optional[Iterable[str]] = None) -> list[str]:
    present = sorted(d[:-len(".lance")] for d in os.listdir(data_dir)
                     if d.endswith(".lance") and os.path.isdir(os.path.join(data_dir, d)))
    if wanted is None:
        return present
    wanted = list(wanted)
    missing = [n for n in wanted if n not in present]
    if missing:
        raise SystemExit("not generated yet: %s (run 'make gen')" % ", ".join(missing))
    return wanted


def plan_uploads(data_dir: str, name: str, prefix: str) -> list[tuple[str, str]]:
    """Return the ``(object key, local path)`` pairs for one dataset, sorted."""
    root = os.path.join(data_dir, "%s.lance" % name)
    pairs = []
    for dirpath, _, filenames in os.walk(root):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            rel = os.path.relpath(path, root).replace(os.sep, "/")
            pairs.append(("%s%s.lance/%s" % (prefix, name, rel), path))
    return sorted(pairs)


def main(argv=None) -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=here, help="fixture root holding data/")
    ap.add_argument("--env-file", default=None,
                    help="read the settings from this file, over the environment")
    ap.add_argument("--no-env-file", action="store_true",
                    help="take every setting from the environment (the default)")
    ap.add_argument("--prefix", default=DEFAULT_PREFIX,
                    help="key prefix inside the bucket (default: %s)" % DEFAULT_PREFIX)
    ap.add_argument("--datasets", default=None, help="comma-separated subset to upload")
    ap.add_argument("--keep-existing", action="store_true",
                    help="do not delete the dataset's existing objects first")
    ap.add_argument("--dry-run", action="store_true",
                    help="list what would be uploaded; no connection is made")
    args = ap.parse_args(argv)

    prefix = args.prefix
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    data_dir = os.path.join(args.root, "data")
    if not os.path.isdir(data_dir):
        raise SystemExit("no fixtures in %s (run 'make gen')" % data_dir)
    names = dataset_names(data_dir, args.datasets.split(",") if args.datasets else None)
    cfg = load_config(None if args.no_env_file else args.env_file)

    client = None
    if not args.dry_run:
        from minio import Minio
        from minio.deleteobjects import DeleteObject

        netloc, secure = cfg.netloc_and_tls
        client = Minio(netloc, access_key=cfg.key, secret_key=cfg.secret,
                       secure=secure, region=cfg.region)
        if not client.bucket_exists(cfg.bucket):
            raise SystemExit("bucket %r does not exist; create it first, e.g. "
                             "'mc mb <alias>/%s'" % (cfg.bucket, cfg.bucket))

    for name in names:
        uploads = plan_uploads(data_dir, name, prefix)
        total = sum(os.path.getsize(path) for _, path in uploads)
        if args.dry_run:
            for key, path in uploads:
                print("would upload %s <- %s" % (key, os.path.relpath(path, args.root)))
        else:
            if not args.keep_existing:
                stale = [DeleteObject(o.object_name) for o in client.list_objects(
                    cfg.bucket, prefix="%s%s.lance/" % (prefix, name), recursive=True)]
                for error in client.remove_objects(cfg.bucket, stale):
                    raise SystemExit("could not remove %s: %s"
                                     % (error.name, error.message))
                if stale:
                    print("removed %d stale object(s) under %s%s.lance/"
                          % (len(stale), prefix, name))
            for key, path in uploads:
                client.fput_object(cfg.bucket, key, path)
        print("%-12s %3d file(s) %8.1f MiB -> %s"
              % (name, len(uploads), total / (1 << 20), cfg.uri(prefix, name)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
