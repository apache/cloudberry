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
"""Shared state for the fixture tests.

The tests run against the datasets in ``data/``, which is gitignored: run.sh
regenerates it.  So the session fixtures below generate anything that is
missing before the first test looks at it.
"""

import json
import os

import lance
import pytest

import gen_fixtures

ROOT = os.path.dirname(os.path.abspath(__file__))


def load_manifest(root=ROOT):
    with open(os.path.join(root, "manifest.json")) as fh:
        return json.load(fh)


def dataset_path(name, root=ROOT):
    return os.path.join(root, "data", "%s.lance" % name)


@pytest.fixture(scope="session")
def fixtures_root():
    return ROOT


@pytest.fixture(scope="session")
def manifest():
    """The committed manifest, with data/ generated if it is not there yet."""
    missing = [n for n in gen_fixtures.BUILDERS if not os.path.isdir(dataset_path(n))]
    if missing or os.environ.get("LANCE_FIXTURES_REGEN"):
        print("regenerating fixtures (missing: %s)" % (", ".join(missing) or "none"))
        gen_fixtures.generate(ROOT)
    return load_manifest()


@pytest.fixture(scope="session")
def regenerated(tmp_path_factory):
    """A second, independent generation used to prove the generator idempotent."""
    root = str(tmp_path_factory.mktemp("regen"))
    return root, gen_fixtures.generate(root)


@pytest.fixture(scope="session")
def opened(manifest):
    cache = {}

    def open_dataset(name):
        if name not in cache:
            cache[name] = lance.dataset(dataset_path(name))
        return cache[name]

    return open_dataset
