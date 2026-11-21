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
"""Generate the Lance fixture datasets used by the lance_fdw regression suites.

One command produces three things under the fixture root (default: this
directory):

  data/<name>.lance/      the datasets themselves (gitignored, regenerated)
  expected/<name>.jsonl   one JSON object per row, read back with pylance
  manifest.json           shape + schema + Lance format version of every dataset

The dataset names and the expected-output format are fixed;
see README.md for what each dataset is for and how a value of each Arrow type
is spelled in the expected files.

Everything is deterministic: no clock, no RNG (except the opt-in ``big``
dataset, which is excluded from the byte-identical requirement).  Re-running
the generator rewrites manifest.json and expected/*.jsonl byte for byte; only
the Lance data files differ, because Lance names them with random UUIDs.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import math
import os
import shutil
import sys
from dataclasses import dataclass, field as dc_field
from decimal import Decimal
from typing import Callable, Iterable, Iterator, Optional

import lance
import numpy as np
import pyarrow as pa

MANIFEST_FORMAT = 1

#: A string / binary column whose longest value exceeds this many bytes is
#: written to the expected file as a digest instead of the literal value, so
#: that the committed reference stays reviewable.  SQL compares such columns
#: with octet_length()/length()/md5().
DIGEST_THRESHOLD_BYTES = 4096

EPOCH_DATE = datetime.date(1970, 1, 1)
TS_SCALE = {"s": 1, "ms": 10 ** 3, "us": 10 ** 6, "ns": 10 ** 9}
TS_DIGITS = {"s": 0, "ms": 3, "us": 6, "ns": 9}


# --------------------------------------------------------------------------
# type classification
# --------------------------------------------------------------------------

#: Arrow extension name Lance puts on a Blob v2 column.  The field's declared
#: type is ``struct<data: large_binary, uri: utf8>``; what a scan hands back is
#: a five-child descriptor.  Either way the column is B tier.
BLOB_V2_EXTENSION_NAME = "lance.blob.v2"


def decode_metadata(metadata) -> dict:
    """A pyarrow field's metadata as str -> str; pyarrow spells it in bytes."""
    if not metadata:
        return {}
    return {k.decode() if isinstance(k, bytes) else k:
            v.decode() if isinstance(v, bytes) else v
            for k, v in metadata.items()}


def is_blob_v2(t: pa.DataType, metadata=None) -> bool:
    """Is this field a Lance Blob v2 column?

    The tag reaches us one of two ways.  ``import lance`` registers the
    extension, so pyarrow resolves ``ARROW:extension:name`` into an
    ExtensionType and the key disappears from the field metadata; a schema read
    without that registration keeps the raw key instead.  Accept both.
    """
    if isinstance(t, pa.ExtensionType) and t.extension_name == BLOB_V2_EXTENSION_NAME:
        return True
    return decode_metadata(metadata).get("ARROW:extension:name") == BLOB_V2_EXTENSION_NAME


def classify(t: pa.DataType, metadata=None) -> tuple[str, Optional[str]]:
    """Return ``(tier, pg_type)`` for an Arrow type, per the type table in README.md.

    Tier "A" types are the ones the first block must read; tier "B" types must
    be skipped on IMPORT and rejected on scan.  ``metadata`` is the owning
    field's metadata where there is a field: the Blob v2 rule keys off a field
    tag, not off the type.

    There is deliberately no "unrecognised means A tier" and no "unrecognised
    means jsonb" fallback: anything this function does not name is B tier.
    """
    if is_blob_v2(t, metadata):
        # Explicit, and ahead of the struct rule on purpose.  A v2 descriptor is
        # a struct of A-tier scalars, so without this rule it would classify A
        # and the FDW would hand out positions and sizes as if they were data.
        return "B", None
    if pa.types.is_boolean(t):
        return "A", "boolean"
    if pa.types.is_int8(t) or pa.types.is_int16(t) or pa.types.is_uint8(t):
        return "A", "smallint"
    if pa.types.is_int32(t) or pa.types.is_uint16(t):
        return "A", "integer"
    if pa.types.is_int64(t) or pa.types.is_uint32(t):
        return "A", "bigint"
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "A", "real"
    if pa.types.is_float64(t):
        return "A", "double precision"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "A", "text"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        return "A", "bytea"
    if pa.types.is_date32(t):
        return "A", "date"
    if pa.types.is_timestamp(t):
        return "A", "timestamptz(6)" if t.tz else "timestamp(6)"
    if pa.types.is_decimal128(t):
        return "A", "numeric(%d,%d)" % (t.precision, t.scale)
    if pa.types.is_fixed_size_list(t):
        # A-tier: fixed_size_list<float32|float64, N> only.
        if pa.types.is_float32(t.value_type) or pa.types.is_float64(t.value_type):
            return "A", classify(t.value_type)[1] + "[]"
        return "B", None
    if pa.types.is_map(t):
        # A-tier: map<utf8|large_utf8, A-tier scalar> -> jsonb.  A
        # non-string key would have to be stringified on the way into a jsonb
        # object, and a container value would have to be flattened; both are
        # silent distortion, so every other map is B tier.
        if not (pa.types.is_string(t.key_type) or pa.types.is_large_string(t.key_type)):
            return "B", None
        item = t.item_field
        if pa.types.is_nested(item.type) or classify(item.type, item.metadata)[0] != "A":
            return "B", None
        return "A", "jsonb"
    if pa.types.is_struct(t):
        # A-tier: a struct is readable only as a whole.  One B-tier
        # subfield sinks the column -- there is no partial projection.
        for i in range(t.num_fields):
            sub = t.field(i)
            if classify(sub.type, sub.metadata)[0] == "B":
                return "B", None
        return "A", "composite"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        # A-tier: list<A-tier element>; nested lists are B tier.
        vf = t.value_field
        if pa.types.is_list(vf.type) or pa.types.is_large_list(vf.type) or \
                pa.types.is_fixed_size_list(vf.type):
            return "B", None
        etier, epg = classify(vf.type, vf.metadata)
        return ("A", epg + "[]") if etier == "A" else ("B", None)
    return "B", None


def composite_fields(t: pa.DataType) -> Optional[list]:
    """Ordered ``[subfield name, pg_type]`` pairs for an A-tier struct.

    Returns None for anything that is not one (including a list of them, which
    reports its element's pairs, and any B-tier struct, which has no PG type to
    describe).  A subfield that is itself a struct carries its own pairs in a
    third slot, so the shape is recoverable without parsing ``arrow_type``.

    The manifest cannot name the composite type: PostgreSQL derives that name
    from the foreign table IMPORT creates, which does not exist at generation
    time.  A2 matches on this structure instead of on a type name.
    """
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return composite_fields(t.value_type)
    if not pa.types.is_struct(t) or classify(t)[0] == "B":
        return None
    out = []
    for i in range(t.num_fields):
        sub = t.field(i)
        pair = [sub.name, classify(sub.type, sub.metadata)[1]]
        nested = composite_fields(sub.type)
        if nested is not None:
            pair.append(nested)
        out.append(pair)
    return out


def arrow_c_format(field: pa.Field) -> str:
    """The Arrow C data interface format string, as lance-c / nanoarrow see it."""
    from pyarrow.cffi import ffi

    c_schema = ffi.new("struct ArrowSchema*")
    ptr = int(ffi.cast("uintptr_t", c_schema))
    field._export_to_c(ptr)
    try:
        return ffi.string(c_schema.format).decode()
    finally:
        if c_schema.release != ffi.NULL:
            c_schema.release(c_schema)


# --------------------------------------------------------------------------
# expected-value encoding
# --------------------------------------------------------------------------

def _encode_float(v) -> object:
    f = float(v)
    if math.isnan(f):
        return "NaN"
    if math.isinf(f):
        return "Infinity" if f > 0 else "-Infinity"
    return f


def _decimal_string(v: Decimal) -> str:
    """Plain (never scientific) decimal string, keeping the column's scale."""
    return format(v, "f")


def _digest_bytes(b: bytes) -> dict:
    return {"bytes": len(b), "md5": hashlib.md5(b).hexdigest()}


def _digest_text(s: str) -> dict:
    raw = s.encode("utf-8")
    return {"chars": len(s), "bytes": len(raw), "md5": hashlib.md5(raw).hexdigest()}


def _encode_scalar(v, digest: bool) -> object:
    """Encode one Python value produced by ``Array.to_pylist()``."""
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return _encode_float(v)
    if isinstance(v, Decimal):
        return _decimal_string(v)
    if isinstance(v, bytes):
        return _digest_bytes(v) if digest else v.hex()
    if isinstance(v, str):
        return _digest_text(v) if digest else v
    if isinstance(v, (list, tuple)):
        # A tuple is how to_pylist() spells one map entry, so a map arrives as a
        # list of [key, value] pairs.  Keeping the pair form rather than folding
        # it into a JSON object is what makes a non-string key and a repeated
        # key representable at all.
        return [_encode_scalar(x, False) for x in v]
    if isinstance(v, dict):
        return {k: _encode_scalar(x, False) for k, x in v.items()}
    if isinstance(v, datetime.timedelta):  # duration, B tier
        return int(v / datetime.timedelta(microseconds=1))
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    return str(v)


def _iso_timestamp(raw: int, unit: str, tz: Optional[str]) -> str:
    """Epoch count in ``unit`` -> ISO-8601.  tz-aware values are UTC ("Z")."""
    scale, digits = TS_SCALE[unit], TS_DIGITS[unit]
    secs, frac = divmod(raw, scale)  # floor division: correct before 1970 too
    dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=secs)
    out = "%04d-%02d-%02dT%02d:%02d:%02d" % (dt.year, dt.month, dt.day,
                                             dt.hour, dt.minute, dt.second)
    if digits:
        out += "." + str(frac).rjust(digits, "0")
    return out + ("Z" if tz else "")


def _has_temporal_child(t: pa.DataType) -> bool:
    if pa.types.is_temporal(t):
        return True
    return any(_has_temporal_child(t.field(i).type) for i in range(t.num_fields))


def encode_column(field: pa.Field, column: pa.ChunkedArray) -> tuple[list, str]:
    """Return ``(values, encoding_name)`` for one column of a fixture table."""
    arr = column.combine_chunks()
    t = field.type

    if pa.types.is_timestamp(t):
        raws = arr.cast(pa.int64()).to_pylist()
        return ([None if r is None else _iso_timestamp(r, t.unit, t.tz) for r in raws],
                "timestamp")
    if pa.types.is_date32(t):
        raws = arr.cast(pa.int32()).to_pylist()
        return ([None if r is None else (EPOCH_DATE + datetime.timedelta(days=r)).isoformat()
                 for r in raws], "date")

    # Everything else goes through to_pylist(); a temporal value nested inside a
    # container would lose sub-microsecond precision there, so refuse A-tier
    # columns that hide one.  B-tier columns are not read by the FDW at all.
    tier, _ = classify(t, field.metadata)
    if tier == "A" and _has_temporal_child(t):
        raise NotImplementedError(
            "column %r: nested temporal types have no lossless expected encoding" % field.name)

    values = arr.to_pylist()
    if tier == "B":
        return [_encode_scalar(v, False) for v in values], "json"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        longest = max((len(v.encode("utf-8")) for v in values if v is not None), default=0)
        digest = longest > DIGEST_THRESHOLD_BYTES
        return ([_encode_scalar(v, digest) for v in values],
                "text-digest" if digest else "text")
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        longest = max((len(v) for v in values if v is not None), default=0)
        digest = longest > DIGEST_THRESHOLD_BYTES
        return ([_encode_scalar(v, digest) for v in values],
                "bytes-digest" if digest else "hex")
    if pa.types.is_boolean(t):
        return values, "bool"
    if pa.types.is_floating(t):
        return [None if v is None else _encode_float(v) for v in values], "float"
    if pa.types.is_decimal(t):
        return [None if v is None else _decimal_string(v) for v in values], "decimal-string"
    if pa.types.is_integer(t):
        return values, "int"
    if pa.types.is_fixed_size_list(t) or pa.types.is_list(t):
        return [_encode_scalar(v, False) for v in values], "array"
    return [_encode_scalar(v, False) for v in values], "json"


def encode_table(table: pa.Table) -> tuple[list[dict], dict[str, str]]:
    """Encode a whole read-back table into expected-file rows."""
    columns, encodings = {}, {}
    for i, f in enumerate(table.schema):
        columns[f.name], encodings[f.name] = encode_column(f, table.column(i))
    names = list(columns)
    rows = [{n: columns[n][r] for n in names} for r in range(table.num_rows)]
    return rows, encodings


def dump_expected(rows: Iterable[dict]) -> str:
    return "".join(
        json.dumps(r, ensure_ascii=True, allow_nan=False, separators=(",", ":")) + "\n"
        for r in rows)


# --------------------------------------------------------------------------
# deterministic payloads
# --------------------------------------------------------------------------

def det_bytes(seed: str, n: int) -> bytes:
    """A deterministic, poorly compressible byte string of length ``n``."""
    out = bytearray()
    h = hashlib.sha256(seed.encode("utf-8")).digest()
    while len(out) < n:
        h = hashlib.sha256(h).digest()
        out += h
    return bytes(out[:n])


def det_text(seed: str, n_chars: int) -> str:
    """A deterministic ASCII string of exactly ``n_chars`` characters."""
    return det_bytes(seed, n_chars // 2 + 1).hex()[:n_chars]


def punch_nulls(values: list, k: int) -> list:
    """Replace every 7th value (offset by column index ``k``) with NULL.

    Spreading the NULLs by column keeps validity bitmaps from lining up, and
    the period is coprime with every fixture's rows-per-fragment, so NULLs land
    at different in-batch offsets in every fragment.
    """
    return [None if (i + 3 * k) % 7 == 0 else v for i, v in enumerate(values)]


def cycled(base: list, n: int) -> list:
    return [base[i % len(base)] for i in range(n)]


# --------------------------------------------------------------------------
# dataset builders
# --------------------------------------------------------------------------

@dataclass
class Built:
    purpose: str
    table: Optional[pa.Table] = None
    reader: Optional[pa.RecordBatchReader] = None
    max_rows_per_file: Optional[int] = None
    post: Optional[Callable[[lance.LanceDataset, str], None]] = None
    notes: list = dc_field(default_factory=list)
    has_expected: bool = True
    #: Lance file format this dataset *must* be written in, overriding both
    #: pylance's default and --data-storage-version.  Some Arrow shapes exist in
    #: exactly one format: pylance 11 writes map only at 2.2, and refuses a v1
    #: blob column from 2.2 on, so one global version cannot cover the set.
    storage_version: Optional[str] = None


def build_types_all() -> Built:
    """24 rows x 3 fragments, every A-tier Arrow type, NULLs everywhere."""
    n = 24
    cols: dict[str, pa.Array] = {}
    fields = [pa.field("id", pa.int32(), nullable=False)]
    cols["id"] = pa.array(list(range(n)), pa.int32())

    def add(name, typ, base):
        k = len(fields)
        fields.append(pa.field(name, typ))
        values = punch_nulls(cycled(base, n), k)
        present = [v for v in values if v is not None]
        assert present, name
        for b in base:
            assert b in present or (isinstance(b, float) and math.isnan(b)), \
                "%s: edge value %r fell entirely into NULLs" % (name, b)
        cols[name] = pa.array(values, typ)

    d = datetime.date
    dt = datetime.datetime
    utc = datetime.timezone.utc

    add("c_bool", pa.bool_(), [True, False, True, False, False])
    add("c_int8", pa.int8(), [0, -1, 127, -128, 42])
    add("c_int16", pa.int16(), [0, -1, 32767, -32768, 4242])
    add("c_int32", pa.int32(), [0, -1, 2147483647, -2147483648, 424242])
    add("c_int64", pa.int64(), [0, -1, 9223372036854775807, -9223372036854775808,
                                2147483648, -2147483649])
    add("c_uint8", pa.uint8(), [0, 255, 1, 128, 7])
    add("c_uint16", pa.uint16(), [0, 65535, 1, 32768, 7])
    add("c_uint32", pa.uint32(), [0, 4294967295, 1, 2147483648, 7])
    add("c_float16", pa.float16(),
        np.array([0.0, 1.5, -2.25, 65504.0, 0.00006103515625], dtype=np.float16))
    add("c_float32", pa.float32(),
        [0.0, -0.0, 3.4028234663852886e+38, float("nan"), float("inf"), -1.5])
    add("c_float64", pa.float64(),
        [0.0, 1.7976931348623157e+308, 2.2250738585072014e-308, float("-inf"), 0.1])
    add("c_utf8", pa.string(), ["", "ascii", "quote\"back\\slash", "tab\tnl\n", "ünicøde"])
    add("c_large_utf8", pa.large_string(), ["", "LARGE", "日本語", "x" * 100, "\U0001f600"])
    add("c_binary", pa.binary(), [b"", b"\x00\x01\x02", b"\xff\xfe", b"ascii", bytes(range(16))])
    add("c_large_binary", pa.large_binary(),
        [b"", b"\x00", b"\xde\xad\xbe\xef", b"L" * 40, bytes(range(8))])
    add("c_date32", pa.date32(),
        [EPOCH_DATE, d(2026, 9, 3), d(1, 1, 1), d(9999, 12, 31), d(2000, 2, 29)])
    for unit in ("s", "ms", "us", "ns"):
        # microsecond-aligned on purpose: PG timestamps are microsecond precise,
        # so an A-tier read must be exact.  See README.
        add("c_ts_%s" % unit, pa.timestamp(unit),
            [dt(1970, 1, 1), dt(2026, 9, 3, 12, 34, 56, 123456),
             dt(1969, 7, 20, 20, 17, 40), dt(2262, 4, 11, 23, 47, 16), dt(2000, 1, 1)])
        add("c_ts_%s_tz" % unit,
            pa.timestamp(unit, tz="UTC" if unit != "us" else "Asia/Shanghai"),
            [dt(1970, 1, 1, tzinfo=utc), dt(2026, 9, 3, 12, 34, 56, 123456, tzinfo=utc),
             dt(1969, 7, 20, 20, 17, 40, tzinfo=utc),
             dt(2262, 4, 11, 23, 47, 16, tzinfo=utc), dt(2000, 1, 1, tzinfo=utc)])
    add("c_dec128_38_10", pa.decimal128(38, 10),
        [Decimal("0"), Decimal("1.0000000001"), Decimal("-1.0000000001"),
         Decimal("9999999999999999999999999999.9999999999"), Decimal("3.1415926536")])
    add("c_dec128_10_2", pa.decimal128(10, 2),
        [Decimal("0.00"), Decimal("1.05"), Decimal("-99999999.99"),
         Decimal("99999999.99"), Decimal("12.30")])
    add("c_fsl_f32_4", pa.list_(pa.float32(), 4),
        [[0.0, 1.5, -2.5, 3.25], [1, 2, 3, 4], [-1, -2, -3, -4],
         [0.1, 0.2, 0.3, 0.4], [1e38, -1e38, 0, 1]])
    add("c_fsl_f64_4", pa.list_(pa.float64(), 4),
        [[0.0, 1.5, -2.5, 3.25], [1, 2, 3, 4], [-1, -2, -3, -4],
         [0.1, 0.2, 0.3, 0.4], [1e308, -1e308, 0, 1]])
    add("c_list_i64", pa.list_(pa.int64()),
        [[], [1], [1, 2, 3], [None, 5], [9223372036854775807, -9223372036854775808]])
    add("c_list_utf8", pa.list_(pa.string()),
        [[], ["a"], ["a", "bb", "ccc"], [None, "d"], ["ü", ""]])

    table = pa.table(cols, schema=pa.schema(fields))
    return Built(
        purpose="Every A-tier Arrow type with NULLs in every column; 3 fragments "
                "of 8 rows so batch_size=3 straddles batch and fragment edges.",
        table=table, max_rows_per_file=8,
        notes=["timestamp values are microsecond-aligned: PG has microsecond "
               "precision, so an A-tier read of any unit must be exact",
               "c_ts_us/c_ts_us_tz carry a non-UTC zone name (Asia/Shanghai) to "
               "cover the rule that the zone name is ignored and the epoch is UTC"])


def build_types_b() -> Built:
    """B-tier columns that IMPORT must skip and a scan must reject."""
    n = 12
    fields = [
        pa.field("id", pa.int32(), nullable=False),
        pa.field("keep_utf8", pa.string()),
        pa.field("c_uint64", pa.uint64()),
        pa.field("c_struct", pa.struct([("a", pa.int32()), ("b", pa.string())])),
        pa.field("c_dict", pa.dictionary(pa.int32(), pa.string())),
        pa.field("c_list_list", pa.list_(pa.list_(pa.int64()))),
        pa.field("c_list_struct", pa.list_(pa.struct([("a", pa.int32())]))),
        pa.field("c_duration", pa.duration("us")),
    ]
    cols = {
        "id": pa.array(list(range(n)), pa.int32()),
        "keep_utf8": pa.array(punch_nulls(cycled(["a", "bb", "ccc"], n), 1), pa.string()),
        "c_uint64": pa.array(punch_nulls(
            cycled([0, 18446744073709551615, 1, 9223372036854775808], n), 2), pa.uint64()),
        "c_struct": pa.array(punch_nulls(
            cycled([{"a": 1, "b": "x"}, {"a": None, "b": None}, {"a": -1, "b": ""}], n), 3),
            pa.struct([("a", pa.int32()), ("b", pa.string())])),
        "c_dict": pa.array(punch_nulls(cycled(["red", "green", "blue"], n), 4),
                           pa.dictionary(pa.int32(), pa.string())),
        "c_list_list": pa.array(punch_nulls(cycled([[[1, 2], [3]], [], [[None]]], n), 5),
                                pa.list_(pa.list_(pa.int64()))),
        "c_list_struct": pa.array(punch_nulls(cycled([[{"a": 1}], [], [{"a": None}]], n), 6),
                                  pa.list_(pa.struct([("a", pa.int32())]))),
        "c_duration": pa.array(punch_nulls(cycled([0, 1, -1, 86400000000], n), 7),
                               pa.duration("us")),
    }
    return Built(
        purpose="B-tier Arrow types (struct, dictionary, uint64, nested list, "
                "list<struct>, duration) next to two A-tier columns, so IMPORT "
                "has something left to import after skipping them.",
        table=pa.table(cols, schema=pa.schema(fields)), max_rows_per_file=6,
        notes=["no map column: pylance 11 refuses to write Arrow map into Lance "
               "file format 2.0 and 2.1 alike ('Map data type is not enabled by "
               "the selected file format'), so a map column is not "
               "representable here"])


def _simple_table(n: int, first_id: int = 0) -> pa.Table:
    schema = pa.schema([pa.field("id", pa.int32(), nullable=False),
                        pa.field("v", pa.string()),
                        pa.field("n", pa.int64())])
    ids = list(range(first_id, first_id + n))
    return pa.table({"id": pa.array(ids, pa.int32()),
                     "v": pa.array(["r%d" % i for i in ids], pa.string()),
                     "n": pa.array([i * i for i in ids], pa.int64())}, schema=schema)


def build_empty() -> Built:
    return Built(purpose="Zero rows and zero fragments: every QE gets an empty share.",
                 table=_simple_table(0),
                 notes=["pylance writes no fragment at all for an empty table"])


def build_empty_b() -> Built:
    """Zero rows and a schema the wrapper cannot read.

    The empty-dataset projection check has to work off the schema, so proving
    it covers the tier decision needs a dataset with no rows at all *and* a
    B-tier column: with rows, the refusal could come from the stream converter
    instead and the test would prove nothing about the new path.
    """
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("c_uint64", pa.uint64()),
    ])
    return Built(
        purpose="Zero rows with a B-tier column: an empty dataset whose "
                "projection still has to be refused.",
        table=pa.table({"id": pa.array([], pa.int32()),
                        "c_uint64": pa.array([], pa.uint64())}, schema=schema),
        notes=["pylance writes no fragment at all for an empty table, so this "
               "dataset has a schema and nothing else"])


def build_frag(n_fragments: int, rows_per_fragment: int) -> Callable[[], Built]:
    def build() -> Built:
        return Built(
            purpose="%d fragment(s) of %d rows: fragment-count coverage for the "
                    "3-segment modulo split." % (n_fragments, rows_per_fragment),
            table=_simple_table(n_fragments * rows_per_fragment),
            max_rows_per_file=rows_per_fragment)
    return build


def build_deleted() -> Built:
    def post(ds, uri):
        ds.delete("id in (0, 3, 11, 19)")
    return Built(
        purpose="4 fragments of 5 rows with rows deleted from three of them: "
                "deletion files, and scan batches that start at a non-zero offset.",
        table=_simple_table(20), max_rows_per_file=5, post=post,
        notes=["ids 0, 3, 11, 19 are deleted; fragment 1 keeps all its rows"])


def build_frag_gap() -> Built:
    def post(ds, uri):
        ds.delete("id >= 5 and id < 10")
    return Built(
        purpose="Fragment ids with a hole in them: fragment 1 is deleted whole, "
                "so the surviving ids are 0, 2, 3 and nothing may assume 0..N-1.",
        table=_simple_table(20), max_rows_per_file=5, post=post,
        notes=["pylance 11 drops a fragment once all of its rows are deleted, so "
               "the observable artifact is a gap in the id sequence rather than a "
               "retained zero-row fragment; LanceFragment.create() refuses empty "
               "input, so a truly empty fragment cannot be built from pylance"])


def build_evolved() -> Built:
    def post(ds, uri):
        ds.add_columns({"v10": "id * 10"})
        ds.add_columns({"vup": "upper(v)"})
    return Built(
        purpose="Schema evolution: two add_columns leave each fragment's columns "
                "spread over three data files.",
        table=_simple_table(20), max_rows_per_file=5, post=post)


def build_versions() -> Built:
    def post(ds, uri):
        lance.write_dataset(_simple_table(10, first_id=10), uri, mode="append")
        lance.write_dataset(_simple_table(15, first_id=20), uri, mode="append")
    return Built(
        purpose="Three versions with 10, 20 and 35 rows: version pinning and "
                "snapshot consistency.",
        table=_simple_table(10), post=post,
        notes=["append-only: version N's rows are the first N-th prefix of "
               "expected/versions.jsonl, which reflects the latest version"])


def build_blob() -> Built:
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("note", pa.string()),
        pa.field("plain_lb", pa.large_binary()),
        pa.field("blob", pa.large_binary(), metadata={"lance-encoding": "blob"}),
    ])
    mib = 1 << 20
    table = pa.table({
        "id": pa.array([0, 1, 2, 3], pa.int32()),
        "note": pa.array(["1MiB", "empty", "null", "2MiB"], pa.string()),
        "plain_lb": pa.array([det_bytes("plain0", mib // 2), b"", None,
                              det_bytes("plain3", mib)], pa.large_binary()),
        "blob": pa.array([det_bytes("blob0", mib), b"", None,
                          det_bytes("blob3", 2 * mib)], pa.large_binary()),
    }, schema=schema)
    return Built(
        purpose="MiB-sized binary payloads, once as a plain large_binary column "
                "and once with Lance's lance-encoding:blob field metadata.",
        table=table, max_rows_per_file=2,
        notes=["pylance 11 returns the blob-encoded column inlined from "
               "to_table(), not as a position/size descriptor (as "
               "seen from the Python side)",
               "both binary columns exceed the digest threshold, so the expected "
               "file carries {bytes, md5} instead of the literal bytes"])


def build_large_text() -> Built:
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("small", pa.string()),
        pa.field("txt", pa.large_string()),
    ])
    values = ["", "a", det_text("lt2", 4096), det_text("lt3", 65536),
              det_text("lt4", 1 << 20), None, "漢字" * 2000, det_text("lt7", 10)]
    return Built(
        purpose="large_utf8 with values from empty to 1 MiB, including a "
                "multi-byte one so byte length and character length differ.",
        table=pa.table({
            "id": pa.array(list(range(len(values))), pa.int32()),
            "small": pa.array(["s%d" % i for i in range(len(values))], pa.string()),
            "txt": pa.array(values, pa.large_string()),
        }, schema=schema),
        max_rows_per_file=4,
        notes=["'small' stays under the digest threshold and is written "
               "literally; 'txt' is written as {chars, bytes, md5}"])


def build_strict() -> Built:
    """Values the wrapper must refuse per value rather than distort.

    Two pairs of columns, each an ordinary value next to one PostgreSQL cannot
    hold: a nanosecond timestamp that is not a whole microsecond, and a UTF-8
    string with a zero byte in it.  Reading the ordinary column of a pair has
    to keep working, which is what tells a refusal from a broken converter.
    """
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("ts_ns_aligned", pa.timestamp("ns")),
        pa.field("ts_ns_odd", pa.timestamp("ns")),
        pa.field("s_plain", pa.string()),
        pa.field("s_nul", pa.string()),
    ])
    aligned = [1_000_000_000, 1_700_000_000_123_456_000, None]
    odd = [1_234_567, 1_700_000_000_123_456_789, None]
    return Built(
        purpose="Per-value strictness: a nanosecond timestamp that is not a "
                "whole microsecond, and a string carrying a zero byte.",
        table=pa.table({
            "id": pa.array([0, 1, 2], pa.int32()),
            "ts_ns_aligned": pa.array(aligned, pa.timestamp("ns")),
            "ts_ns_odd": pa.array(odd, pa.timestamp("ns")),
            "s_plain": pa.array(["plain", "\u00fcnicode", None], pa.string()),
            "s_nul": pa.array(["a\x00b", "ok", None], pa.string()),
        }, schema=schema),
        notes=["ts_ns_odd row 0 is 1234567 ns and row 1 ends in 789 ns: "
               "neither is a whole microsecond",
               "s_nul row 0 holds a zero byte, which a PostgreSQL text datum "
               "cannot contain at all"])


def build_nested() -> Built:
    """A-tier struct in the four shapes A2 has to read.

    Values are spelled out rather than generated: there are six rows, and a
    reviewer comparing them against expected/nested.jsonl should be able to see
    what a NULL struct, a struct of NULLs and a NULL list element each look like
    without running anything.  That keeps the no-clock/no-RNG rule too.
    """
    s = pa.struct([("a", pa.int32()), ("b", pa.string())])
    inner = pa.struct([("x", pa.int32()), ("y", pa.string())])
    nested = pa.struct([("inner", inner), ("z", pa.int32())])
    ls = pa.list_(s)
    sl = pa.struct([("arr", pa.list_(pa.int32())), ("name", pa.string())])
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("c_struct", s),
        pa.field("c_nested", nested),
        pa.field("c_list_struct", ls),
        pa.field("c_struct_list", sl),
    ])
    i32max, i32min = 2147483647, -2147483648
    return Built(
        purpose="A-tier struct: flat, nested two deep, inside a list and holding "
                "a list, with NULLs at every level (whole value, subfield and "
                "list element).",
        table=pa.table({
            "id": pa.array([0, 1, 2, 3, 4, 5], pa.int32()),
            "c_struct": pa.array([
                {"a": 1, "b": "x"},
                None,
                {"a": None, "b": None},
                {"a": -1, "b": ""},
                {"a": i32max, "b": "ünicøde"},
                {"a": 0, "b": "quote\"back\\slash"},
            ], s),
            "c_nested": pa.array([
                {"inner": {"x": 1, "y": "y"}, "z": 2},
                None,
                {"inner": None, "z": None},
                {"inner": {"x": None, "y": None}, "z": -1},
                {"inner": {"x": i32min, "y": "tab\tnl\n"}, "z": 0},
                {"inner": {"x": 0, "y": ""}, "z": i32max},
            ], nested),
            "c_list_struct": pa.array([
                [{"a": 1, "b": "x"}],
                None,
                [],
                [{"a": None, "b": None}, {"a": 2, "b": "yy"}],
                [{"a": 0, "b": ""}],
                [None, {"a": 5, "b": "e"}],
            ], ls),
            "c_struct_list": pa.array([
                {"arr": [1, 2, 3], "name": "n0"},
                None,
                {"arr": [], "name": ""},
                {"arr": [None, 7], "name": None},
                {"arr": [i32min, i32max], "name": "edge"},
                {"arr": None, "name": "n5"},
            ], sl),
        }, schema=schema),
        max_rows_per_file=3,
        notes=["every column is A tier: each carries pg_type 'composite' (or "
               "'composite[]') and an ordered composite_fields, which is what A2 "
               "builds the PostgreSQL composite from",
               "row 1 is NULL in every column, row 2 is a struct whose subfields "
               "are all NULL: a reader must tell those two apart",
               "c_list_struct row 5 holds a NULL element inside a non-NULL list, "
               "and c_struct_list row 5 a NULL list inside a non-NULL struct"])


def build_nested_btier() -> Built:
    """One B-tier subfield, and the whole struct goes with it."""
    s = pa.struct([("ok", pa.int32()), ("bad", pa.uint64())])
    schema = pa.schema([pa.field("id", pa.int32(), nullable=False),
                        pa.field("c_struct_b", s)])
    return Built(
        purpose="A struct whose subfields are one A-tier int32 and one B-tier "
                "uint64: the column must be B tier as a whole, because there is "
                "no partial projection of a struct.",
        table=pa.table({
            "id": pa.array([0, 1, 2, 3], pa.int32()),
            "c_struct_b": pa.array([
                {"ok": 1, "bad": 0},
                None,
                {"ok": None, "bad": 18446744073709551615},
                {"ok": -1, "bad": None},
            ], s),
        }, schema=schema),
        notes=["'ok' on its own would be A tier; the point of the fixture is "
               "that reading it anyway is not on offer",
               "'bad' reaches uint64's maximum, which is exactly what no PG "
               "integer type can hold"])


def build_maps() -> Built:
    """Two maps that must classify in opposite directions."""
    ok = pa.map_(pa.string(), pa.int32())
    bad = pa.map_(pa.int32(), pa.string())
    schema = pa.schema([pa.field("id", pa.int32(), nullable=False),
                        pa.field("c_map_utf8_i32", ok),
                        pa.field("c_map_i32_utf8", bad)])
    return Built(
        purpose="map<utf8,int32> (A tier, jsonb) beside map<int32,utf8> (B tier: "
                "a non-string key would have to be stringified to become a jsonb "
                "object, which is silent distortion).",
        table=pa.table({
            "id": pa.array([0, 1, 2, 3, 4], pa.int32()),
            "c_map_utf8_i32": pa.array([
                [("a", 1), ("b", 2)],
                [],
                None,
                [("k", None)],
                [("", 0), ("ünicøde", -1)],
            ], ok),
            "c_map_i32_utf8": pa.array([
                [(1, "x"), (2, "yy")],
                None,
                [],
                [(7, None)],
                [(-1, ""), (0, "z")],
            ], bad),
        }, schema=schema),
        max_rows_per_file=3, storage_version="2.2",
        notes=["written at 2.2 because pylance 11 writes Arrow map in no other "
               "Lance file format: 2.0 and 2.1 both refuse it",
               "an empty map (row 1/2) and a NULL map are different values, as "
               "are a present key with a NULL value (row 3) and an absent key",
               "expected/maps.jsonl spells each map as a list of [key, value] "
               "pairs, which is the only form that survives a non-string key"])


def build_blobv2() -> Built:
    """A Lance Blob v2 column, which must be refused whatever it contains.

    The field is declared as ``struct<data: large_binary, uri: utf8>`` carrying
    ``ARROW:extension:name = lance.blob.v2``; the size thresholds beside it only
    pick the storage strategy.  A scan does not return that struct -- it returns
    a five-child descriptor of position, size and strategy -- so an FDW that let
    the column through would hand out file offsets as if they were the payload.
    """
    storage = pa.struct([pa.field("data", pa.large_binary()),
                         pa.field("uri", pa.string())])
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("note", pa.string()),
        pa.field("c_blob_v2", storage, nullable=True, metadata={
            "ARROW:extension:name": BLOB_V2_EXTENSION_NAME,
            # Pinned, not defaulted: every value stays inline (kind=0), so the
            # descriptors in the expected file do not move when pylance changes
            # its own default thresholds.
            "lance-encoding:blob-inline-size-threshold": "65536",
        }),
    ])
    return Built(
        purpose="A Blob v2 column beside two A-tier ones: the v2 descriptor must "
                "be refused even though all five of its children are A-tier "
                "scalars.",
        table=pa.table({
            "id": pa.array([0, 1, 2, 3], pa.int32()),
            "note": pa.array(["three bytes", "empty", None, "sixty-four bytes"],
                             pa.string()),
            "c_blob_v2": pa.array([
                {"data": b"abc", "uri": None},
                {"data": b"", "uri": None},
                None,
                {"data": det_bytes("blobv2", 64), "uri": None},
            ], storage),
        }, schema=schema),
        max_rows_per_file=2, storage_version="2.2",
        notes=["written at 2.2 because Blob v2 needs it: pylance 11 refuses the "
               "extension type below 2.2, and refuses the v1 lance-encoding:blob "
               "column from 2.2 on, so blob.lance and this one cannot share a "
               "storage version",
               "reading lance registers the extension, so pyarrow resolves the "
               "field's arrow_type to extension<lance.blob.v2<BlobType>> and the "
               "ARROW:extension:name key is no longer in the field metadata; "
               "classify() accepts the tag in either place",
               "to_table() returns {kind, position, size, blob_id, blob_uri}, "
               "not the bytes: kind 0 is inline, and size is the real payload "
               "size, so the expected file records offsets rather than data"])


def build_big(fragments: int, rows_per_fragment: int, blob_bytes: int) -> Callable[[], Built]:
    """Timing fixture: not deterministic, not byte-compared, opt-in."""
    def build() -> Built:
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False),
                            pa.field("payload", pa.large_binary())])
        total = fragments * rows_per_fragment
        rng = np.random.default_rng(20260903)
        rows_per_batch = max(1, min(rows_per_fragment, (32 << 20) // max(blob_bytes, 1)))

        def batches() -> Iterator[pa.RecordBatch]:
            for start in range(0, total, rows_per_batch):
                count = min(rows_per_batch, total - start)
                yield pa.record_batch(
                    {"id": pa.array(range(start, start + count), pa.int64()),
                     "payload": pa.array([rng.bytes(blob_bytes) for _ in range(count)],
                                         pa.large_binary())}, schema=schema)

        reader = pa.RecordBatchReader.from_batches(schema, batches())
        return Built(
            purpose="Wall-time comparison only: %d fragments of %d rows x %d "
                    "bytes. Random payloads, no expected output."
                    % (fragments, rows_per_fragment, blob_bytes),
            reader=reader, max_rows_per_file=rows_per_fragment, has_expected=False,
            notes=["random payload: this dataset is exempt from the "
                   "byte-identical regeneration requirement"])
    return build


#: The dataset names, in a fixed order.
BUILDERS: dict[str, Callable[[], Built]] = {
    "types_all": build_types_all,
    "types_b": build_types_b,
    "deleted": build_deleted,
    "empty": build_empty,
    "empty_b": build_empty_b,
    "frag_1": build_frag(1, 5),
    "frag_2": build_frag(2, 5),
    "frag_3": build_frag(3, 5),
    "frag_7": build_frag(7, 5),
    "frag_100": build_frag(100, 1),
    "frag_gap": build_frag_gap,
    "evolved": build_evolved,
    "blob": build_blob,
    "large_text": build_large_text,
    "versions": build_versions,
    "strict": build_strict,
    # A1 appends: inserting them anywhere else would reorder the
    # dataset keys of a manifest whose existing entries are frozen.
    "nested": build_nested,
    "nested_btier": build_nested_btier,
    "maps": build_maps,
    "blobv2": build_blobv2,
}

BIG = "big"


# --------------------------------------------------------------------------
# manifest
# --------------------------------------------------------------------------

class Compact:
    """Marker: serialize this value on a single line inside the manifest."""

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value


#: Dataset-entry fields that describe returns wrapped in Compact.  Entries read
#: back from an existing manifest have to be re-wrapped, or regenerating one
#: dataset would reformat all the others.
COMPACT_FIELDS = ("fragment_ids", "fragment_rows", "fragment_physical_rows",
                  "fragments_with_deletion_file", "data_files_per_fragment",
                  "file_format_versions")
COMPACT_LIST_FIELDS = ("history", "schema")


def recompact(entry: dict) -> dict:
    """Restore the one-line formatting of an entry loaded from manifest.json."""
    out = dict(entry)
    for key in COMPACT_FIELDS:
        if key in out and not isinstance(out[key], Compact):
            out[key] = Compact(out[key])
    for key in COMPACT_LIST_FIELDS:
        out[key] = [v if isinstance(v, Compact) else Compact(v) for v in out.get(key, [])]
    return out


def dumps_manifest(obj) -> str:
    marks: dict[str, str] = {}

    def prep(o):
        if isinstance(o, Compact):
            key = "@@compact:%d@@" % len(marks)
            marks[key] = json.dumps(o.value, ensure_ascii=True, separators=(", ", ": "))
            return key
        if isinstance(o, dict):
            return {k: prep(v) for k, v in o.items()}
        if isinstance(o, list):
            return [prep(v) for v in o]
        return o

    text = json.dumps(prep(obj), indent=2, ensure_ascii=True, allow_nan=False)
    for key, rep in marks.items():
        text = text.replace('"%s"' % key, rep)
    return text + "\n"


def describe(name: str, built: Built, ds: lance.LanceDataset,
             encodings: dict, expected_rows: Optional[int]) -> dict:
    frags = ds.get_fragments()
    file_versions = sorted({"%d.%d" % (f.file_major_version, f.file_minor_version)
                            for frag in frags for f in frag.metadata.files})
    schema = []
    for f in ds.schema:
        tier, pg_type = classify(f.type, f.metadata)
        md = decode_metadata(f.metadata) or None
        col = {
            "name": f.name,
            "arrow_type": str(f.type),
            "arrow_format": arrow_c_format(f),
            "nullable": f.nullable,
            "tier": tier,
            "pg_type": pg_type,
        }
        # Only A-tier structs get this key, so every column that had no
        # composite to describe keeps the entry it already had.
        fields = composite_fields(f.type) if tier == "A" else None
        if fields is not None:
            col["composite_fields"] = fields
        col["metadata"] = md
        col["expected_encoding"] = encodings.get(f.name)
        schema.append(Compact(col))

    history = []
    for v in ds.versions():
        n = v["version"]
        history.append(Compact({"version": n,
                                "rows": lance.dataset(ds.uri, version=n).count_rows()}))

    return {
        "name": name,
        "purpose": built.purpose,
        "path": "data/%s.lance" % name,
        "version": ds.version,
        "rows": ds.count_rows(),
        "fragments": len(frags),
        "fragment_ids": Compact([f.fragment_id for f in frags]),
        "fragment_rows": Compact([f.count_rows() for f in frags]),
        "fragment_physical_rows": Compact([f.metadata.physical_rows for f in frags]),
        "fragments_with_deletion_file": Compact(
            [f.fragment_id for f in frags if f.metadata.deletion_file is not None]),
        "data_files_per_fragment": Compact([len(f.metadata.files) for f in frags]),
        "data_storage_version": ds.data_storage_version,
        "file_format_versions": Compact(file_versions),
        "history": history,
        "expected": ("expected/%s.jsonl" % name) if built.has_expected else None,
        "expected_rows": expected_rows,
        "notes": built.notes,
        "schema": schema,
    }


# --------------------------------------------------------------------------
# generation
# --------------------------------------------------------------------------

def _write_dataset(path: str, built: Built, storage_version: Optional[str]):
    if os.path.isdir(path):
        assert path.endswith(".lance"), path
        shutil.rmtree(path)
    kwargs = {}
    if built.max_rows_per_file is not None:
        kwargs["max_rows_per_file"] = built.max_rows_per_file
    # A dataset that names its own format wins over --data-storage-version:
    # asking for 2.0 across the board must not silently produce a maps.lance
    # that pylance cannot write.
    version = built.storage_version or storage_version
    if version:
        kwargs["data_storage_version"] = version
    source = built.table if built.reader is None else built.reader
    lance.write_dataset(source, path, **kwargs)
    if built.post is not None:
        built.post(lance.dataset(path), path)
    return lance.dataset(path)


def generate(root: str, names: Optional[Iterable[str]] = None, with_big: bool = False,
             storage_version: Optional[str] = None, big_shape: tuple = (3, 512, 1 << 20),
             log: Callable[[str], None] = lambda msg: None) -> dict:
    """Generate datasets under ``root`` and return the manifest dict it wrote."""
    builders = dict(BUILDERS)
    if with_big:
        builders[BIG] = build_big(*big_shape)
    if names is not None:
        unknown = [n for n in names if n not in builders]
        if unknown:
            raise SystemExit("unknown dataset(s): %s" % ", ".join(unknown))
        builders = {n: builders[n] for n in builders if n in names}

    data_dir = os.path.join(root, "data")
    expected_dir = os.path.join(root, "expected")
    manifest_path = os.path.join(root, "manifest.json")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(expected_dir, exist_ok=True)

    manifest = {
        "manifest_format": MANIFEST_FORMAT,
        "generated_by": {
            "script": "test/fixtures/gen_fixtures.py",
            "pylance": lance.__version__,
            "pyarrow": pa.__version__,
            "data_storage_version_requested": storage_version,
        },
        "expected_format": {
            "layout": "one JSON object per row, keys in schema order",
            "null": "null",
            "int": "JSON number",
            "float": "JSON number with Python repr; NaN/Infinity/-Infinity as strings",
            "bool": "JSON true/false",
            "text": "JSON string",
            "hex": "lowercase hex string",
            "date": "YYYY-MM-DD",
            "timestamp": "ISO-8601; tz-aware values converted to UTC and suffixed Z",
            "decimal-string": "decimal string, scale preserved",
            "array": "JSON array of encoded elements",
            "text-digest": "{chars, bytes, md5}",
            "bytes-digest": "{bytes, md5}",
            "json": "B-tier column, best-effort JSON; not read by the FDW",
        },
        "digest_threshold_bytes": DIGEST_THRESHOLD_BYTES,
        "datasets": {},
    }
    if os.path.exists(manifest_path) and names is not None:
        with open(manifest_path) as fh:
            kept = json.load(fh)["datasets"]
        manifest["datasets"] = {n: recompact(e) for n, e in kept.items()}

    for name, builder in builders.items():
        built = builder()
        path = os.path.join(data_dir, "%s.lance" % name)
        ds = _write_dataset(path, built, storage_version)

        expected_rows = None
        if built.has_expected:
            rows, encodings = encode_table(ds.to_table())
            with open(os.path.join(expected_dir, "%s.jsonl" % name), "w") as fh:
                fh.write(dump_expected(rows))
            expected_rows = len(rows)
        else:
            encodings = {}

        manifest["datasets"][name] = describe(name, built, ds, encodings, expected_rows)
        entry = manifest["datasets"][name]
        log("%-12s %6d rows  %4d fragment(s)  format %s"
            % (name, entry["rows"], entry["fragments"],
               ",".join(entry["file_format_versions"].value)))

    ordered = [n for n in list(BUILDERS) + [BIG] if n in manifest["datasets"]]
    manifest["datasets"] = {n: manifest["datasets"][n] for n in ordered}
    with open(manifest_path, "w") as fh:
        fh.write(dumps_manifest(manifest))
    return manifest


def main(argv=None) -> int:
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=here,
                    help="fixture root holding data/, expected/ and manifest.json")
    ap.add_argument("--datasets", default=None,
                    help="comma-separated subset to regenerate (manifest entries "
                         "for the other datasets are kept)")
    ap.add_argument("--with-big", action="store_true",
                    help="also generate the multi-hundred-MB 'big' dataset (timing runs)")
    ap.add_argument("--big-fragments", type=int, default=3)
    ap.add_argument("--big-rows-per-fragment", type=int, default=512)
    ap.add_argument("--big-blob-bytes", type=int, default=1 << 20)
    ap.add_argument("--data-storage-version", default=None,
                    help="Lance file format to write, e.g. 2.0 (default: pylance's)")
    ap.add_argument("-q", "--quiet", action="store_true")
    args = ap.parse_args(argv)

    names = args.datasets.split(",") if args.datasets else None
    generate(root=args.root, names=names, with_big=args.with_big,
             storage_version=args.data_storage_version,
             big_shape=(args.big_fragments, args.big_rows_per_fragment,
                        args.big_blob_bytes),
             log=(lambda msg: None) if args.quiet else (lambda msg: print(msg)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
