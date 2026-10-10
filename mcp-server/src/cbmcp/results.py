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

Long values are cut here too.  A single text cell can be megabytes, and the
whole result lands in the agent's context.
"""

import datetime
import decimal
import uuid
from typing import Any, Dict, List, Sequence

import asyncpg

# Values the serializer already writes correctly.  Anything else that reaches
# the end of the conversion is written as its text form rather than allowed to
# fail the call.
_PASSTHROUGH = (
    type(None), bool, int, float, decimal.Decimal,
    datetime.date, datetime.time, datetime.timedelta, uuid.UUID,
)


class RowShaper:
    """Convert rows to JSON-ready values, cutting strings over a length limit.

    One shaper serves one result, so that it can report which columns had a
    value cut and how many.
    """

    def __init__(self, max_value_length: int) -> None:
        self.max_value_length = max_value_length
        self.truncated_values = 0
        self.truncated_columns: List[str] = []

    def shape(self, records: Sequence[Any]) -> List[List[Any]]:
        rows: List[List[Any]] = []
        for record in records:
            row = []
            for column, value in record.items():
                before = self.truncated_values
                row.append(self._convert(value))
                if self.truncated_values != before and column not in self.truncated_columns:
                    self.truncated_columns.append(column)
            rows.append(row)
        return rows

    def report(self) -> Dict[str, Any]:
        """The fields a result carries when any value was cut."""
        if not self.truncated_values:
            return {}
        return {
            "truncated_values": self.truncated_values,
            "truncated_columns": self.truncated_columns,
            "max_value_length": self.max_value_length,
        }

    def _convert(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._cut(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            # The bytea hex form: what psql prints, and what '...'::bytea reads back.
            return self._cut("\\x" + bytes(value).hex())
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
        if isinstance(value, dict):
            return {key: self._convert(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._convert(item) for item in value]
        if isinstance(value, _PASSTHROUGH):
            return value
        return self._cut(str(value))

    def _cut(self, text: str) -> str:
        if len(text) <= self.max_value_length:
            return text
        self.truncated_values += 1
        return text[: self.max_value_length]
