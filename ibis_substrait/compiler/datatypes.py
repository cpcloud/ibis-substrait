from __future__ import annotations

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import pyarrow as pa

_to_ibis_dtypes = {
    pa.int8(): dt.Int8,
    pa.int16(): dt.Int16,
    pa.int32(): dt.Int32,
    pa.int64(): dt.Int64,
    pa.uint8(): dt.UInt8,
    pa.uint16(): dt.UInt16,
    pa.uint32(): dt.UInt32,
    pa.uint64(): dt.UInt64,
    pa.float16(): dt.Float16,
    pa.float32(): dt.Float32,
    pa.float64(): dt.Float64,
    pa.string(): dt.String,
    pa.binary(): dt.Binary,
    pa.bool_(): dt.Boolean,
    pa.date32(): dt.Date,
    pa.date64(): dt.Date,
}


@dt.dtype.register(pa.DataType)  # type: ignore[misc]
def from_pyarrow_primitive(
    arrow_type: pa.DataType,
    nullable: bool = True,
) -> dt.DataType:
    return _to_ibis_dtypes[arrow_type](nullable=nullable)


@dt.dtype.register(pa.Time32Type)  # type: ignore[misc]
@dt.dtype.register(pa.Time64Type)  # type: ignore[misc]
def from_pyarrow_time(
    arrow_type: pa.TimestampType,
    nullable: bool = True,
) -> dt.DataType:
    return dt.Time(nullable=nullable)


@dt.dtype.register(pa.ListType)  # type: ignore[misc]
def from_pyarrow_list(arrow_type: pa.ListType, nullable: bool = True) -> dt.DataType:
    return dt.Array(dt.dtype(arrow_type.value_type), nullable=nullable)


@dt.dtype.register(pa.MapType)  # type: ignore[misc]
def from_pyarrow_map(arrow_type: pa.MapType, nullable: bool = True) -> dt.DataType:
    return dt.Map(
        dt.dtype(arrow_type.key_type),
        dt.dtype(arrow_type.item_type),
        nullable=nullable,
    )


@dt.dtype.register(pa.StructType)  # type: ignore[misc]
def from_pyarrow_struct(
    arrow_type: pa.StructType,
    nullable: bool = True,
) -> dt.DataType:
    return dt.Struct.from_tuples(
        ((field.name, dt.dtype(field.type)) for field in arrow_type),
        nullable=nullable,
    )


@dt.dtype.register(pa.TimestampType)  # type: ignore[misc]
def from_pyarrow_timestamp(
    arrow_type: pa.TimestampType,
    nullable: bool = True,
) -> dt.DataType:
    return dt.Timestamp(timezone=arrow_type.tz)


@sch.infer.register(pa.Schema)  # type: ignore[misc]
def infer_pyarrow_schema(schema: pa.Schema) -> sch.Schema:
    return sch.schema([(f.name, dt.dtype(f.type, nullable=f.nullable)) for f in schema])
