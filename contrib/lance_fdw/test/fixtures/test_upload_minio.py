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
"""Tests for the MinIO uploader that do not need a MinIO.

Everything that decides *what* goes *where* is a pure function; only the
transfer itself needs the server, and that is exercised by running
``make upload`` (or ``upload_minio.py --dry-run``) by hand.
"""

import os

import pytest

import upload_minio as up

ENV_SH = """\
# an env file
export LANCE_S3_ENDPOINT_HOST=http://localhost:9000
export LANCE_S3_ENDPOINT_CONTAINER=http://172.17.0.1:9000
export LANCE_S3_BUCKET=lance-test
export LANCE_S3_REGION=us-east-1
export LANCE_S3_KEY="minio-key"
export LANCE_S3_SECRET='minio-secret'
"""


def test_parse_env_file_handles_export_comments_and_quotes():
    values = up.parse_env_file(ENV_SH)
    assert values == {
        "LANCE_S3_ENDPOINT_HOST": "http://localhost:9000",
        "LANCE_S3_ENDPOINT_CONTAINER": "http://172.17.0.1:9000",
        "LANCE_S3_BUCKET": "lance-test",
        "LANCE_S3_REGION": "us-east-1",
        "LANCE_S3_KEY": "minio-key",
        "LANCE_S3_SECRET": "minio-secret",
    }


def test_load_config_prefers_the_env_file_over_the_environment(tmp_path):
    env_file = tmp_path / "env.sh"
    env_file.write_text(ENV_SH)
    cfg = up.load_config(str(env_file), environ={"LANCE_S3_BUCKET": "stale"})
    assert cfg.bucket == "lance-test"
    assert cfg.netloc_and_tls == ("localhost:9000", False)
    assert cfg.uri("fixtures/", "types_all") == "s3://lance-test/fixtures/types_all.lance"


def test_load_config_reports_what_is_missing_without_leaking_secrets(tmp_path):
    env_file = tmp_path / "env.sh"
    env_file.write_text("export LANCE_S3_BUCKET=lance-test\n"
                        "export LANCE_S3_SECRET=hunter2\n")
    with pytest.raises(SystemExit) as excinfo:
        up.load_config(str(env_file), environ={})
    message = str(excinfo.value)
    assert "LANCE_S3_KEY" in message and "LANCE_S3_ENDPOINT_HOST" in message
    assert "hunter2" not in message


def test_load_config_rejects_a_missing_env_file(tmp_path):
    with pytest.raises(SystemExit):
        up.load_config(str(tmp_path / "nope.sh"), environ={})


@pytest.mark.parametrize("url,expected", [
    ("http://localhost:9000", ("localhost:9000", False)),
    ("https://minio.example:9000", ("minio.example:9000", True)),
    ("localhost:9000", ("localhost:9000", False)),
])
def test_split_endpoint(url, expected):
    assert up.split_endpoint(url) == expected


def test_plan_uploads_mirrors_the_dataset_tree(tmp_path):
    root = tmp_path / "data" / "demo.lance"
    (root / "_versions").mkdir(parents=True)
    (root / "data").mkdir()
    (root / "data" / "a.lance").write_bytes(b"x")
    (root / "_versions" / "1.manifest").write_bytes(b"y")
    pairs = up.plan_uploads(str(tmp_path / "data"), "demo", "fixtures/")
    assert [key for key, _ in pairs] == [
        "fixtures/demo.lance/_versions/1.manifest",
        "fixtures/demo.lance/data/a.lance",
    ]
    assert all(os.path.exists(path) for _, path in pairs)


def test_dataset_names_lists_and_validates(tmp_path):
    data = tmp_path / "data"
    (data / "a.lance").mkdir(parents=True)
    (data / "b.lance").mkdir()
    (data / "notes.txt").write_text("ignored")
    assert up.dataset_names(str(data)) == ["a", "b"]
    assert up.dataset_names(str(data), ["b"]) == ["b"]
    with pytest.raises(SystemExit):
        up.dataset_names(str(data), ["c"])


def test_dry_run_plans_the_real_fixtures_without_connecting(manifest, fixtures_root,
                                                            tmp_path, capsys):
    env_file = tmp_path / "env.sh"
    env_file.write_text(ENV_SH)
    rc = up.main(["--root", fixtures_root, "--env-file", str(env_file),
                  "--datasets", "frag_2", "--dry-run"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "s3://lance-test/fixtures/frag_2.lance" in out
    assert "would upload fixtures/frag_2.lance/" in out
    assert "minio-secret" not in out
