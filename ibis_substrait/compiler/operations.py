from __future__ import annotations

import pathlib
from typing import Any

import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import ibis.expr.schema as sch
import pyarrow.parquet as pq
from cached_property import cached_property
from ibis.expr.signature import Argument as Arg


class LocalFilesTable(ops.PhysicalTable):
    files = Arg(rlz.list_of(rlz.instance_of((str, pathlib.Path))))
    name = Arg(rlz.instance_of(str), default=None)

    @cached_property  # type: ignore[misc]
    def schema(self) -> sch.Schema:
        return sch.infer(pq.ParquetDataset(self.files).schema)


@sch.infer.register(pq.ParquetSchema)  # type: ignore[misc]
def _infer_parquet_schema(schema: pq.ParquetSchema, **kwargs: Any) -> sch.Schema:
    return sch.infer(schema.to_arrow_schema())
