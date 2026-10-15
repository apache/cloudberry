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
"""Assert that the generated fixtures are what manifest.json claims they are.

Every assertion here is about the fixtures themselves: shape (rows, fragments,
deletion files, data files), schema (Arrow type, C format string, tier), and
the expected/*.jsonl references.  The SQL suites build on top of these
numbers, so a fixture that drifts from its manifest entry has to fail here
rather than show up as a mysterious regress diff later.
"""

import json
import os
from decimal import Decimal

import lance
import pyarrow as pa
import pytest
from lance.blob import BlobType

import gen_fixtures as gen
from conftest import dataset_path

#: These names are fixed; 'big' is opt-in and not listed here.
FROZEN_NAMES = [
    "types_all", "types_b", "deleted", "empty", "empty_b",
    "frag_1", "frag_2", "frag_3", "frag_7", "frag_100", "frag_gap",
    "evolved", "blob", "large_text", "versions", "strict",
    # A1: struct / map / blob v2, for A2 to read against.
    "nested", "nested_btier", "maps", "blobv2",
    # Vector Top-K.
    "vectors", "vectors_idx", "vectors_nulls", "vectors_seg",
]

#: Every A-tier Arrow type has to appear in types_all.  Losing a
#: dimension here silently would make the type suites vacuous, so spell them
#: all out.
A_TIER_TYPES = [
    pa.bool_(), pa.int8(), pa.int16(), pa.int32(), pa.int64(),
    pa.uint8(), pa.uint16(), pa.uint32(),
    pa.float16(), pa.float32(), pa.float64(),
    pa.string(), pa.large_string(), pa.binary(), pa.large_binary(),
    pa.date32(),
    pa.timestamp("s"), pa.timestamp("ms"), pa.timestamp("us"), pa.timestamp("ns"),
    pa.timestamp("s", tz="UTC"), pa.timestamp("ms", tz="UTC"),
    pa.timestamp("us", tz="Asia/Shanghai"), pa.timestamp("ns", tz="UTC"),
    pa.decimal128(38, 10), pa.decimal128(10, 2),
    pa.list_(pa.float32(), 4), pa.list_(pa.float64(), 4),
    pa.list_(pa.int64()), pa.list_(pa.string()),
]

DELETED_IDS = [0, 3, 11, 19]


def entry(manifest, name):
    assert name in manifest["datasets"], "manifest has no entry for %r" % name
    return manifest["datasets"][name]


#: Datasets that hold another dataset's rows and so share its reference.
SHARED_EXPECTED = {"vectors_idx": "vectors", "vectors_seg": "vectors"}


def expected_lines(fixtures_root, name):
    name = SHARED_EXPECTED.get(name, name)
    with open(os.path.join(fixtures_root, "expected", "%s.jsonl" % name)) as fh:
        return fh.readlines()


# --------------------------------------------------------------------------
# manifest <-> data
# --------------------------------------------------------------------------

def test_manifest_covers_the_frozen_dataset_names(manifest):
    assert list(manifest["datasets"]) == FROZEN_NAMES
    assert list(gen.BUILDERS) == FROZEN_NAMES
    assert manifest["manifest_format"] == gen.MANIFEST_FORMAT
    assert manifest["generated_by"]["pylance"] == lance.__version__


@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_shape_matches_manifest(manifest, opened, name):
    e = entry(manifest, name)
    ds = opened(name)
    frags = ds.get_fragments()

    assert ds.version == e["version"]
    assert ds.count_rows() == e["rows"]
    assert len(frags) == e["fragments"]
    assert [f.fragment_id for f in frags] == e["fragment_ids"]
    assert [f.count_rows() for f in frags] == e["fragment_rows"]
    assert [f.metadata.physical_rows for f in frags] == e["fragment_physical_rows"]
    assert [f.fragment_id for f in frags
            if f.metadata.deletion_file is not None] == e["fragments_with_deletion_file"]
    assert [len(f.metadata.files) for f in frags] == e["data_files_per_fragment"]
    assert sum(e["fragment_rows"]) == e["rows"]


@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_lance_format_version_matches_manifest(manifest, opened, name):
    e = entry(manifest, name)
    ds = opened(name)
    assert ds.data_storage_version == e["data_storage_version"]
    seen = sorted({"%d.%d" % (f.file_major_version, f.file_minor_version)
                   for frag in ds.get_fragments() for f in frag.metadata.files})
    assert seen == e["file_format_versions"]


@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_schema_matches_manifest(manifest, opened, name):
    e = entry(manifest, name)
    ds = opened(name)
    assert [f.name for f in ds.schema] == [c["name"] for c in e["schema"]]

    for field, col in zip(ds.schema, e["schema"]):
        assert str(field.type) == col["arrow_type"], field.name
        assert gen.arrow_c_format(field) == col["arrow_format"], field.name
        assert field.nullable == col["nullable"], field.name
        assert (gen.decode_metadata(field.metadata) or None) == col["metadata"], field.name
        tier, pg_type = gen.classify(field.type, field.metadata)
        assert (tier, pg_type) == (col["tier"], col["pg_type"]), field.name
        fields = gen.composite_fields(field.type) if tier == "A" else None
        assert fields == col.get("composite_fields"), field.name


@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_history_matches_manifest(manifest, name):
    e = entry(manifest, name)
    assert [v["version"] for v in e["history"]] == list(range(1, e["version"] + 1))
    for v in e["history"]:
        ds = lance.dataset(dataset_path(name), version=v["version"])
        assert ds.count_rows() == v["rows"], (name, v["version"])
    assert e["history"][-1]["rows"] == e["rows"]


# --------------------------------------------------------------------------
# expected/*.jsonl
# --------------------------------------------------------------------------

@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_expected_has_one_line_per_row(manifest, opened, fixtures_root, name):
    e = entry(manifest, name)
    lines = expected_lines(fixtures_root, name)
    assert len(lines) == opened(name).count_rows()
    if name in SHARED_EXPECTED:
        assert e["expected"] is None and e["expected_rows"] is None
    else:
        assert len(lines) == e["expected_rows"]


@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_expected_is_what_pylance_reads_back(manifest, opened, fixtures_root, name):
    """The reference is generated, never hand-written: re-derive and compare."""
    rows, encodings = gen.encode_table(opened(name).to_table())
    on_disk = "".join(expected_lines(fixtures_root, name))
    assert gen.dump_expected(rows) == on_disk
    for col in entry(manifest, SHARED_EXPECTED.get(name, name))["schema"]:
        assert encodings[col["name"]] == col["expected_encoding"], col["name"]


@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_expected_rows_are_objects_in_schema_order(manifest, fixtures_root, name):
    order = [c["name"] for c in entry(manifest, name)["schema"]]
    for line in expected_lines(fixtures_root, name):
        row = json.loads(line)
        assert list(row) == order


@pytest.mark.parametrize("name", FROZEN_NAMES)
def test_expected_ids_ascend(manifest, fixtures_root, name):
    """SQL cases order by id; the reference must already be in that order."""
    if entry(manifest, name)["schema"][0]["name"] != "id":
        pytest.skip("%s has no id column" % name)
    ids = [json.loads(line)["id"] for line in expected_lines(fixtures_root, name)]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)


# --------------------------------------------------------------------------
# per-dataset properties the later packages rely on
# --------------------------------------------------------------------------

def test_types_all_covers_every_a_tier_arrow_type(manifest, opened):
    present = {str(f.type) for f in opened("types_all").schema}
    missing = [str(t) for t in A_TIER_TYPES if str(t) not in present]
    assert not missing, "types_all lost A-tier dimensions: %s" % missing
    assert all(c["tier"] == "A" for c in entry(manifest, "types_all")["schema"])


def test_types_all_has_nulls_and_out_of_range_values(fixtures_root, manifest):
    rows = [json.loads(line) for line in expected_lines(fixtures_root, "types_all")]
    for col in entry(manifest, "types_all")["schema"]:
        name = col["name"]
        values = [r[name] for r in rows]
        if name == "id":
            assert None not in values
            continue
        assert None in values, "%s has no NULL" % name
        assert any(v is not None for v in values), "%s is all NULL" % name
    # The type suites need values that do not fit the narrower PG types.
    assert 9223372036854775807 in [r["c_int64"] for r in rows]
    assert -9223372036854775808 in [r["c_int64"] for r in rows]
    assert any(v is not None and len(v.split(".")[0].lstrip("-")) > 5
               for v in [r["c_dec128_38_10"] for r in rows])
    assert "NaN" in [r["c_float32"] for r in rows]
    assert "Infinity" in [r["c_float32"] for r in rows]


def test_types_b_mixes_b_tier_columns_with_importable_ones(manifest):
    schema = entry(manifest, "types_b")["schema"]
    by_name = {c["name"]: c for c in schema}
    b_tier = [c["name"] for c in schema if c["tier"] == "B"]
    a_tier = [c["name"] for c in schema if c["tier"] == "A"]
    assert a_tier, "IMPORT would fail outright with no A-tier column left"
    for expect in ("c_dict", "c_uint64", "c_list_list", "c_duration"):
        assert expect in b_tier, expect
    assert all(by_name[n]["pg_type"] is None for n in b_tier)


def test_types_b_struct_columns_moved_to_a_tier_with_the_struct_rule(manifest):
    """A1 turned these two readable; they are the regression guard for it.

    ``c_struct`` is ``struct<a int32, b utf8>`` and ``c_list_struct`` is a list
    of one -- every subfield A tier, so the struct rule makes both A tier.
    They were B tier only because ``classify()`` did not know structs yet.
    """
    by_name = {c["name"]: c for c in entry(manifest, "types_b")["schema"]}
    assert (by_name["c_struct"]["tier"], by_name["c_struct"]["pg_type"]) == \
        ("A", "composite")
    assert by_name["c_struct"]["composite_fields"] == [["a", "integer"], ["b", "text"]]
    assert (by_name["c_list_struct"]["tier"], by_name["c_list_struct"]["pg_type"]) == \
        ("A", "composite[]")
    assert by_name["c_list_struct"]["composite_fields"] == [["a", "integer"]]
    # The values themselves did not move: expected/types_b.jsonl is frozen, and
    # only the label the manifest puts on the list column changed.
    assert by_name["c_struct"]["expected_encoding"] == "json"
    assert by_name["c_list_struct"]["expected_encoding"] == "array"


def test_deleted_hides_exactly_the_deleted_rows(manifest, fixtures_root):
    ids = [json.loads(line)["id"] for line in expected_lines(fixtures_root, "deleted")]
    assert ids == [i for i in range(20) if i not in DELETED_IDS]
    e = entry(manifest, "deleted")
    assert e["fragments_with_deletion_file"] == [0, 2, 3]
    assert e["fragment_rows"] != e["fragment_physical_rows"]


def test_frag_gap_has_non_contiguous_fragment_ids(manifest):
    e = entry(manifest, "frag_gap")
    ids = e["fragment_ids"]
    assert ids == sorted(ids)
    assert ids[-1] - ids[0] + 1 > len(ids), "no gap in %r" % ids


def test_frag_datasets_have_the_advertised_fragment_counts(manifest):
    for name in ("frag_1", "frag_2", "frag_3", "frag_7", "frag_100"):
        e = entry(manifest, name)
        assert e["fragments"] == int(name.split("_")[1])
        assert len(set(e["fragment_rows"])) == 1, "fragments must be even-sized"


def test_empty_dataset_has_a_schema_but_no_rows(manifest, opened, fixtures_root):
    e = entry(manifest, "empty")
    assert (e["rows"], e["fragments"]) == (0, 0)
    assert e["schema"]
    assert expected_lines(fixtures_root, "empty") == []
    assert opened("empty").to_table().num_rows == 0


def test_evolved_spreads_columns_over_several_data_files(manifest):
    e = entry(manifest, "evolved")
    assert min(e["data_files_per_fragment"]) >= 2
    assert [c["name"] for c in e["schema"]] == ["id", "v", "n", "v10", "vup"]


def test_versions_are_append_only_prefixes(manifest, fixtures_root):
    e = entry(manifest, "versions")
    assert [v["rows"] for v in e["history"]] == [10, 20, 35]
    lines = expected_lines(fixtures_root, "versions")
    for v in e["history"]:
        ds = lance.dataset(dataset_path("versions"), version=v["version"])
        rows, _ = gen.encode_table(ds.to_table())
        assert gen.dump_expected(rows) == "".join(lines[:v["rows"]]), v["version"]


def test_blob_column_keeps_the_lance_blob_encoding(manifest, opened):
    field = opened("blob").schema.field("blob")
    assert field.metadata == {b"lance-encoding": b"blob"}
    col = {c["name"]: c for c in entry(manifest, "blob")["schema"]}["blob"]
    assert col["arrow_type"] == "large_binary"
    assert col["expected_encoding"] == "bytes-digest"


def test_strict_holds_values_postgresql_cannot_represent(opened):
    """The point of this fixture is the values themselves, so check them.

    A regress suite asserting "reading this column errors" proves nothing if
    the fixture stopped containing anything unrepresentable.  The nanosecond
    columns are read as int64: pyarrow itself refuses to turn 1234567 ns into a
    Python datetime, which is the same limit PostgreSQL has.
    """
    t = opened("strict").to_table()

    def ns(name):
        return [v for v in t.column(name).cast(pa.int64()).to_pylist()
                if v is not None]

    odd, aligned = ns("ts_ns_odd"), ns("ts_ns_aligned")
    assert odd, "ts_ns_odd lost its values"
    assert all(v % 1000 != 0 for v in odd), odd
    assert aligned, "ts_ns_aligned lost its values"
    assert all(v % 1000 == 0 for v in aligned), aligned

    assert [v for v in t.column("s_nul").to_pylist() if v and "\x00" in v], \
        "s_nul lost its zero byte"
    assert all("\x00" not in (v or "") for v in t.column("s_plain").to_pylist())


def test_blob_and_large_text_reach_the_advertised_sizes(fixtures_root, manifest):
    blobs = [json.loads(line)["blob"] for line in expected_lines(fixtures_root, "blob")]
    assert max(b["bytes"] for b in blobs if b) >= 1 << 20
    texts = [json.loads(line)["txt"] for line in expected_lines(fixtures_root, "large_text")]
    assert max(t["bytes"] for t in texts if t) >= 1 << 20
    assert any(t and t["bytes"] > t["chars"] for t in texts), "no multi-byte value"
    col = {c["name"]: c for c in entry(manifest, "large_text")["schema"]}
    assert col["txt"]["arrow_type"] == "large_string"
    assert col["small"]["expected_encoding"] == "text", "short column must stay literal"


def test_nested_holds_every_a_tier_struct_shape(manifest):
    """A2 reads these four shapes; the manifest is where it learns their PG side."""
    by_name = {c["name"]: c for c in entry(manifest, "nested")["schema"]}
    assert all(c["tier"] == "A" for c in by_name.values())
    assert by_name["c_struct"]["pg_type"] == "composite"
    assert by_name["c_struct"]["composite_fields"] == [["a", "integer"], ["b", "text"]]
    # Nested two deep: the subfield keeps its own pg_type and its own pairs.
    assert by_name["c_nested"]["composite_fields"] == \
        [["inner", "composite", [["x", "integer"], ["y", "text"]]], ["z", "integer"]]
    # A list of structs is composite[], described by its element.
    assert by_name["c_list_struct"]["pg_type"] == "composite[]"
    assert by_name["c_list_struct"]["composite_fields"] == \
        [["a", "integer"], ["b", "text"]]
    # A list inside a struct is just another subfield type.
    assert by_name["c_struct_list"]["composite_fields"] == \
        [["arr", "integer[]"], ["name", "text"]]


def test_nested_values_survive_the_round_trip(fixtures_root, opened):
    """Read the values, not just the types: a NULL struct is not a struct of NULLs."""
    rows = [json.loads(line) for line in expected_lines(fixtures_root, "nested")]
    assert len(rows) == 6
    assert rows[0]["c_struct"] == {"a": 1, "b": "x"}
    assert rows[0]["c_nested"]["inner"] == {"x": 1, "y": "y"}
    assert rows[0]["c_nested"]["z"] == 2
    assert rows[0]["c_struct_list"] == {"arr": [1, 2, 3], "name": "n0"}
    assert rows[0]["c_list_struct"] == [{"a": 1, "b": "x"}]

    # The three ways of being empty, which a converter must not collapse.
    assert rows[1]["c_struct"] is None, "whole value NULL"
    assert rows[2]["c_struct"] == {"a": None, "b": None}, "present, subfields NULL"
    assert rows[2]["c_nested"] == {"inner": None, "z": None}, "NULL nested subfield"
    assert rows[2]["c_list_struct"] == [], "empty list is not a NULL list"
    assert rows[1]["c_list_struct"] is None
    assert rows[5]["c_list_struct"][0] is None, "NULL element in a non-NULL list"
    assert rows[5]["c_struct_list"]["arr"] is None, "NULL list in a non-NULL struct"

    # And what pylance itself hands back agrees, since that is the whole claim.
    assert opened("nested").to_table().to_pylist()[0]["c_nested"]["inner"]["y"] == "y"


def test_nested_btier_sinks_the_whole_struct_for_one_subfield(manifest, opened):
    """No partial projection: 'ok' is A tier and still unreachable."""
    e = entry(manifest, "nested_btier")
    col = {c["name"]: c for c in e["schema"]}["c_struct_b"]
    assert col["arrow_type"] == "struct<ok: int32, bad: uint64>"
    assert (col["tier"], col["pg_type"]) == ("B", None)
    assert "composite_fields" not in col, "a B-tier struct has no PG composite"
    # The A-tier subfield really is A tier on its own -- that is what makes the
    # refusal a decision rather than an accident.
    struct_type = opened("nested_btier").schema.field("c_struct_b").type
    assert gen.classify(struct_type.field("ok").type) == ("A", "integer")
    assert gen.classify(struct_type.field("bad").type) == ("B", None)


def test_maps_classify_in_opposite_directions(manifest, fixtures_root):
    """map is readable only with a string key; the two columns hold the same rows."""
    schema = {c["name"]: c for c in entry(manifest, "maps")["schema"]}
    assert (schema["c_map_utf8_i32"]["tier"], schema["c_map_utf8_i32"]["pg_type"]) == \
        ("A", "jsonb")
    assert (schema["c_map_i32_utf8"]["tier"], schema["c_map_i32_utf8"]["pg_type"]) == \
        ("B", None)
    assert schema["c_map_utf8_i32"]["arrow_format"] == "+m"
    assert schema["c_map_i32_utf8"]["arrow_format"] == "+m", "same Arrow shape"

    rows = [json.loads(line) for line in expected_lines(fixtures_root, "maps")]
    # Pairs, not an object: a non-string key has nowhere else to go.
    assert rows[0]["c_map_utf8_i32"] == [["a", 1], ["b", 2]]
    assert rows[0]["c_map_i32_utf8"] == [[1, "x"], [2, "yy"]]
    assert rows[1]["c_map_utf8_i32"] == [], "empty map"
    assert rows[2]["c_map_utf8_i32"] is None, "NULL map is not an empty one"
    assert rows[3]["c_map_utf8_i32"] == [["k", None]], "present key, NULL value"


def test_blobv2_column_is_refused_whatever_it_holds(manifest, opened):
    """The explicit rule: a v2 descriptor is all A-tier scalars and still B."""
    col = {c["name"]: c for c in entry(manifest, "blobv2")["schema"]}["c_blob_v2"]
    assert (col["tier"], col["pg_type"]) == ("B", None)
    assert col["arrow_format"] == "+s", "a struct, as far as the C interface knows"
    assert entry(manifest, "blobv2")["data_storage_version"] == "2.2"

    field = opened("blobv2").schema.field("c_blob_v2")
    assert gen.is_blob_v2(field.type, field.metadata)
    # The thresholds only pick the storage strategy; they are not what makes it v2.
    assert field.metadata == {b"lance-encoding:blob-inline-size-threshold": b"65536"}

    # What a scan returns is a descriptor of five A-tier scalars, so an FDW that
    # went by the returned type alone would hand out file offsets as data.
    returned = opened("blobv2").to_table().schema.field("c_blob_v2").type
    assert [returned.field(i).name for i in range(returned.num_fields)] == \
        ["kind", "position", "size", "blob_id", "blob_uri"]
    assert not gen.is_blob_v2(returned), "the tag does not survive into the scan"

    # An A-tier neighbour is still readable: the refusal is per column.
    assert {c["name"]: c["tier"] for c in entry(manifest, "blobv2")["schema"]}["note"] \
        == "A"


# --------------------------------------------------------------------------
# the generator itself
# --------------------------------------------------------------------------

def test_vectors_idx_is_vectors_with_an_l2_ivf_pq_index(opened):
    """Same rows, plus the index the vector suite's approximate cases need."""
    plain, indexed = opened("vectors"), opened("vectors_idx")
    assert indexed.to_table() == plain.to_table()
    assert plain.list_indices() == []
    [index] = indexed.list_indices()
    assert index["fields"] == ["emb"]
    assert index["type"] == "IVF_PQ"


def test_vectors_seg_is_vectors_with_one_segment_per_indexed_fragment(opened):
    """Three segments of one logical index, and a fragment none of them covers."""
    plain, segmented = opened("vectors"), opened("vectors_seg")
    assert segmented.to_table() == plain.to_table()
    assert [f.fragment_id for f in segmented.get_fragments()] == [0, 1, 2, 3]
    segments = segmented.list_indices()
    assert {s["name"] for s in segments} == {"emb_idx"}
    assert all(s["fields"] == ["emb"] and s["type"] == "IVF_PQ" for s in segments)
    assert sorted(sorted(s["fragment_ids"]) for s in segments) == [[0], [1], [2]]


def test_vectors_nulls_has_null_and_zero_vectors(opened):
    emb = opened("vectors_nulls").to_table().column("emb").to_pylist()
    assert [i for i, v in enumerate(emb) if v is None] == [2, 4, 8]
    assert [i for i, v in enumerate(emb) if v == [0.0] * 4] == [5]


def test_regeneration_is_byte_identical(manifest, fixtures_root, regenerated):
    other_root, other = regenerated
    for name in other["datasets"]:
        mine = json.dumps(entry(manifest, name), sort_keys=True)
        theirs = json.dumps(json.loads(gen.dumps_manifest(other["datasets"][name])),
                            sort_keys=True)
        assert mine == theirs, "manifest entry for %s is not reproducible" % name
        expected = entry(manifest, name)["expected"]
        if expected:
            with open(os.path.join(fixtures_root, expected), "rb") as fh:
                mine_bytes = fh.read()
            with open(os.path.join(other_root, expected), "rb") as fh:
                theirs_bytes = fh.read()
            assert mine_bytes == theirs_bytes, "%s is not reproducible" % expected


def test_generator_can_build_the_big_dataset(tmp_path):
    """'big' is opt-in and huge; exercise its code path at 1/100000 the size."""
    m = gen.generate(str(tmp_path), names=["big"], with_big=True,
                     big_shape=(3, 4, 1024))
    e = m["datasets"]["big"]
    assert (e["rows"], e["fragments"]) == (12, 3)
    assert e["expected"] is None and e["expected_rows"] is None
    assert not os.path.exists(os.path.join(str(tmp_path), "expected", "big.jsonl"))
    ds = lance.dataset(os.path.join(str(tmp_path), "data", "big.lance"))
    assert ds.count_rows() == 12
    assert len(set(ds.to_table().column("payload").to_pylist())) == 12


def test_generator_can_build_the_vectors_big_dataset(tmp_path):
    """'vectors_big' rides on --with-big; exercise it at a few rows."""
    m = gen.generate(str(tmp_path), names=["vectors_big"], with_big=True,
                     vectors_big_rows=12)
    e = m["datasets"]["vectors_big"]
    assert (e["rows"], e["fragments"]) == (12, 4)
    assert e["expected"] is None and e["expected_rows"] is None
    ds = lance.dataset(os.path.join(str(tmp_path), "data", "vectors_big.lance"))
    assert ds.schema.field("emb").type == pa.list_(pa.float32(), 64)
    assert ds.list_indices() == []


def test_generator_regenerates_a_subset_without_disturbing_the_others(tmp_path):
    root = str(tmp_path)
    path = os.path.join(root, "manifest.json")
    gen.generate(root, names=["frag_1", "frag_2"])
    assert list(json.load(open(path))["datasets"]) == ["frag_1", "frag_2"]
    with open(path, "rb") as fh:
        before = fh.read()

    # Rewriting frag_1 must leave the carried-over frag_2 entry byte for byte
    # alone, formatting included, or subset regenerations produce noisy diffs.
    gen.generate(root, names=["frag_1"])
    with open(path, "rb") as fh:
        assert fh.read() == before

    gen.generate(root, names=["frag_3"])
    assert list(json.load(open(path))["datasets"]) == ["frag_1", "frag_2", "frag_3"]


def test_command_line_writes_where_it_is_told(tmp_path, capsys):
    root = str(tmp_path)
    assert gen.main(["--root", root, "--datasets", "frag_2,empty"]) == 0
    assert "frag_2" in capsys.readouterr().out
    # written in the frozen order, whatever order they were asked for in
    assert list(json.load(open(os.path.join(root, "manifest.json")))["datasets"]) == \
        ["empty", "frag_2"]
    assert os.path.isdir(os.path.join(root, "data", "frag_2.lance"))
    with pytest.raises(SystemExit):
        gen.main(["--root", root, "--datasets", "no_such_dataset"])


def test_command_line_can_pin_the_lance_file_format(tmp_path):
    """Fallback: regenerate as 2.0 if lance-c cannot read 2.1."""
    root = str(tmp_path)
    gen.main(["--root", root, "--datasets", "frag_1",
              "--data-storage-version", "2.0", "--quiet"])
    m = json.load(open(os.path.join(root, "manifest.json")))
    assert m["generated_by"]["data_storage_version_requested"] == "2.0"
    assert m["datasets"]["frag_1"]["data_storage_version"] == "2.0"
    assert m["datasets"]["frag_1"]["file_format_versions"] == ["2.0"]


def test_a_dataset_that_names_its_format_overrides_the_command_line(tmp_path,
                                                                    monkeypatch):
    """Some Arrow shapes exist in exactly one Lance format (maps, blob v2).

    Pinning 2.0 for the fallback must not silently produce a dataset
    pylance cannot write; the builder's own version wins, and the manifest says
    which version each dataset actually got.
    """
    def build_pinned():
        return gen.Built(purpose="storage-version override probe",
                         table=gen._simple_table(2), storage_version="2.2")

    monkeypatch.setitem(gen.BUILDERS, "frag_1", build_pinned)
    root = str(tmp_path)
    gen.main(["--root", root, "--datasets", "frag_1",
              "--data-storage-version", "2.0", "--quiet"])
    m = json.load(open(os.path.join(root, "manifest.json")))
    # The request is still recorded globally; the dataset reports what it is.
    assert m["generated_by"]["data_storage_version_requested"] == "2.0"
    assert m["datasets"]["frag_1"]["data_storage_version"] == "2.2"
    assert m["datasets"]["frag_1"]["file_format_versions"] == ["2.2"]


def test_datasets_without_an_override_still_follow_the_command_line(manifest):
    """Only the datasets that have to pin a format may pin one."""
    pinned = {n: gen.BUILDERS[n]().storage_version for n in gen.BUILDERS}
    for name, version in pinned.items():
        if version is None:
            continue
        assert entry(manifest, name)["data_storage_version"] == version, name


def test_classify_follows_the_design_type_table():
    assert gen.classify(pa.int32()) == ("A", "integer")
    assert gen.classify(pa.uint32()) == ("A", "bigint")
    assert gen.classify(pa.timestamp("ns", tz="Asia/Shanghai")) == ("A", "timestamptz(6)")
    assert gen.classify(pa.timestamp("ns")) == ("A", "timestamp(6)")
    assert gen.classify(pa.decimal128(10, 2)) == ("A", "numeric(10,2)")
    assert gen.classify(pa.list_(pa.float32(), 4)) == ("A", "real[]")
    assert gen.classify(pa.list_(pa.string())) == ("A", "text[]")
    # B tier: kept out of the first block.
    for t in (pa.uint64(), pa.dictionary(pa.int32(), pa.string()),
              pa.list_(pa.list_(pa.int64())), pa.duration("us"),
              pa.month_day_nano_interval(), pa.list_(pa.int32(), 2)):
        assert gen.classify(t) == ("B", None), t


def test_classify_takes_a_struct_only_when_every_subfield_is_a_tier():
    """The struct rule: all of it or none of it, never a partial projection."""
    ok = pa.struct([("a", pa.int32()), ("b", pa.string())])
    assert gen.classify(ok) == ("A", "composite")
    assert gen.classify(pa.struct([("ok", pa.int32()), ("bad", pa.uint64())])) == \
        ("B", None)
    # One B-tier subfield anywhere below sinks the whole column.
    assert gen.classify(pa.struct([("inner", pa.struct([("bad", pa.uint64())]))])) == \
        ("B", None)
    assert gen.classify(pa.struct([("arr", pa.list_(pa.int32())),
                                   ("name", pa.string())])) == ("A", "composite")
    assert gen.classify(pa.struct([("arr", pa.list_(pa.list_(pa.int32())))])) == \
        ("B", None)
    # A list of A-tier structs rides along; a list of B-tier structs does not.
    assert gen.classify(pa.list_(ok)) == ("A", "composite[]")
    assert gen.classify(pa.list_(pa.struct([("bad", pa.uint64())]))) == ("B", None)


def test_classify_takes_a_map_only_with_string_keys_and_a_scalar_value():
    """The map rule: anything else would need stringifying to reach jsonb."""
    assert gen.classify(pa.map_(pa.string(), pa.int32())) == ("A", "jsonb")
    assert gen.classify(pa.map_(pa.large_string(), pa.float64())) == ("A", "jsonb")
    for t in (pa.map_(pa.int32(), pa.string()),          # key is not a string
              pa.map_(pa.binary(), pa.string()),         # nor is this one
              pa.map_(pa.string(), pa.uint64()),         # value is B tier
              pa.map_(pa.string(), pa.list_(pa.int32())),  # value is not a scalar
              pa.map_(pa.string(), pa.struct([("a", pa.int32())]))):
        assert gen.classify(t) == ("B", None), t
    # A map is only ever a leaf: a struct containing a bad one goes down with it.
    assert gen.classify(pa.struct([("m", pa.map_(pa.string(), pa.int32()))])) == \
        ("A", "composite")
    assert gen.classify(pa.struct([("m", pa.map_(pa.int32(), pa.string()))])) == \
        ("B", None)


def test_classify_refuses_blob_v2_by_an_explicit_rule():
    """The v2 descriptor is a struct of A-tier scalars, so the rule must be explicit."""
    storage = pa.struct([pa.field("data", pa.large_binary()),
                         pa.field("uri", pa.string())])
    # Without the tag it is an ordinary A-tier struct -- that is the whole point.
    assert gen.classify(storage) == ("A", "composite")
    tag = {b"ARROW:extension:name": gen.BLOB_V2_EXTENSION_NAME.encode()}
    assert gen.classify(storage, tag) == ("B", None)
    assert gen.is_blob_v2(storage, tag)
    assert not gen.is_blob_v2(storage, {b"lance-encoding": b"blob"})
    # ... and once pyarrow has resolved the registered extension, off the type.
    ext = BlobType()
    assert gen.is_blob_v2(ext)
    assert gen.classify(ext) == ("B", None)
    # A tagged field nested in a container drags the container down with it.
    assert gen.classify(pa.struct([pa.field("b", storage, metadata=tag)])) == ("B", None)
    assert gen.classify(pa.list_(pa.field("item", storage, metadata=tag))) == ("B", None)


def test_composite_fields_describe_the_struct_recursively():
    inner = pa.struct([("x", pa.int32()), ("y", pa.string())])
    assert gen.composite_fields(pa.struct([("a", pa.int32()), ("b", pa.string())])) == \
        [["a", "integer"], ["b", "text"]]
    # A struct subfield keeps its pg_type and carries its own pairs in slot 3.
    assert gen.composite_fields(pa.struct([("inner", inner), ("z", pa.int32())])) == \
        [["inner", "composite", [["x", "integer"], ["y", "text"]]], ["z", "integer"]]
    # A list reports its element's pairs; the "[]" lives in the column pg_type.
    assert gen.composite_fields(pa.list_(inner)) == [["x", "integer"], ["y", "text"]]
    # Nothing to describe: not a struct, or a struct with no PG type at all.
    assert gen.composite_fields(pa.int32()) is None
    assert gen.composite_fields(pa.list_(pa.string())) is None
    assert gen.composite_fields(pa.struct([("bad", pa.uint64())])) is None
    assert gen.composite_fields(pa.map_(pa.string(), pa.int32())) is None


def test_timestamp_encoding_is_utc_iso8601():
    assert gen._iso_timestamp(0, "s", None) == "1970-01-01T00:00:00"
    assert gen._iso_timestamp(0, "us", "UTC") == "1970-01-01T00:00:00.000000Z"
    assert gen._iso_timestamp(-1, "ns", None) == "1969-12-31T23:59:59.999999999"
    assert gen._iso_timestamp(1, "ms", "Asia/Shanghai") == "1970-01-01T00:00:00.001Z"
    assert gen._decimal_string(Decimal("0E-10")) == "0.0000000000"
