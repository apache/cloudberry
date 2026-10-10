#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
Turn database values into what an MCP tool result can carry.

A tool result is JSON.  asyncpg hands back Python objects, and two of them have
no JSON form that the serializer can find on its own: ``bytes`` from a bytea
column, which it tries to read as UTF-8 and fails on the first byte that is
not, and ``asyncpg.Record`` from a composite column, which it does not know at
all.  Either one fails the whole call, not just the value.  Lance struct
columns arrive as composites, so every nested Lance table hits the second.

The whole result lands in the agent's context, so it is bounded here too: a
string longer than the value limit is cut, and rows stop being added once the
result reaches its byte budget.  The budget is what bounds an array, which has
no length to cut - one 1536-dimension embedding per row is enough to make a
thousand-row result tens of megabytes.
"""

import datetime
import decimal
import json
import math
import uuid
from typing import Any, Dict, List, Sequence

import asyncpg
from asyncpg import types as pgtypes

# Values the serializer already writes correctly.
_PASSTHROUGH = (
    type(None), bool, int, decimal.Decimal,
    datetime.date, datetime.time, datetime.timedelta, uuid.UUID,
)


class RowShaper:
    """Convert rows to JSON-ready values within a value limit and a byte budget.

    One shaper serves one result, so that it can report what it cut: how many
    values and in which columns, and whether it stopped adding rows.
    """

    def __init__(self, max_value_length: int, max_result_bytes: int) -> None:
        self.max_value_length = max_value_length
        self.max_result_bytes = max_result_bytes
        self.truncated_values = 0
        self.truncated_columns: List[str] = []
        self.result_bytes = 0
        self.out_of_budget = False

    def shape(self, records: Sequence[Any]) -> List[List[Any]]:
        rows: List[List[Any]] = []
        for record in records:
            # A row that does not fit is dropped whole, so what it would have
            # cut must not be reported.
            values_before = self.truncated_values
            columns_before = len(self.truncated_columns)
            row = []
            for column, value in record.items():
                before = self.truncated_values
                row.append(self._convert(value))
                if self.truncated_values != before and column not in self.truncated_columns:
                    self.truncated_columns.append(column)
            size = len(json.dumps(row, default=str, ensure_ascii=False).encode()) + 1
            if self.result_bytes + size > self.max_result_bytes:
                self.truncated_values = values_before
                del self.truncated_columns[columns_before:]
                self.out_of_budget = True
                break
            self.result_bytes += size
            rows.append(row)
        return rows

    def report(self) -> Dict[str, Any]:
        """The fields a result carries when anything was cut."""
        fields: Dict[str, Any] = {}
        if self.out_of_budget:
            fields.update(
                truncated=True,
                truncated_by="max_result_bytes",
                max_result_bytes=self.max_result_bytes,
            )
        if self.truncated_values:
            fields.update(
                truncated_values=self.truncated_values,
                truncated_columns=self.truncated_columns,
                max_value_length=self.max_value_length,
            )
        return fields

    def _convert(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._cut(value)
        if isinstance(value, float):
            # JSON has no NaN or infinity, and the serializer writes them as
            # null, which reads as SQL NULL.  These are PostgreSQL's spellings,
            # the same ones a numeric NaN already comes back as.
            if math.isnan(value):
                return "NaN"
            if math.isinf(value):
                return "Infinity" if value > 0 else "-Infinity"
            return value
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self._hex(bytes(value))
        if isinstance(value, asyncpg.Record):
            return {key: self._convert(item) for key, item in value.items()}
        if isinstance(value, asyncpg.Range):
            return {
                "lower": self._convert(value.lower),
                "upper": self._convert(value.upper),
                "lower_inc": value.lower_inc,
                "upper_inc": value.upper_inc,
                "isempty": value.isempty,
            }
        if isinstance(value, pgtypes.BitString):
            return self._cut(value.as_string())
        if isinstance(value, pgtypes.Polygon):
            return [self._convert(point) for point in value.points]
        if isinstance(value, pgtypes.Path):
            return {
                "points": [self._convert(point) for point in value.points],
                "is_closed": value.is_closed,
            }
        if isinstance(value, dict):
            return {key: self._convert(item) for key, item in value.items()}
        # Point, Box, Line, LineSegment and Circle are tuples.
        if isinstance(value, (list, tuple)):
            return [self._convert(item) for item in value]
        if isinstance(value, _PASSTHROUGH):
            return value
        # A type with a text form of its own, such as an IP address.  One
        # without would print as a Python object and its memory address.
        if type(value).__str__ is not object.__str__:
            return self._cut(str(value))
        return f"<unsupported value of type {type(value).__name__}>"

    def _hex(self, data: bytes) -> str:
        """The bytea hex form, which psql prints and '...'::bytea reads back.

        Cut on a byte boundary, so that a cut value still reads back.
        """
        room = max(0, (self.max_value_length - 2) // 2)
        if len(data) > room:
            self.truncated_values += 1
            data = data[:room]
        return "\\x" + data.hex()

    def _cut(self, text: str) -> str:
        if len(text) <= self.max_value_length:
            return text
        self.truncated_values += 1
        return text[: self.max_value_length]
