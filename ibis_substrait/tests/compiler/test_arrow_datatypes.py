import ibis.expr.datatypes as dt
import pyarrow as pa
import pytest


@pytest.mark.parametrize(  # type: ignore[misc]
    ("arrow_type", "ibis_type"),
    [
        (pa.int8(), dt.int8),
        (pa.int16(), dt.int16),
        (pa.int32(), dt.int32),
        (pa.int64(), dt.int64),
        (pa.uint8(), dt.uint8),
        (pa.uint16(), dt.uint16),
        (pa.uint32(), dt.uint32),
        (pa.uint64(), dt.uint64),
        (pa.float16(), dt.float16),
        (pa.float32(), dt.float32),
        (pa.float64(), dt.float64),
        (pa.string(), dt.string),
        (pa.binary(), dt.binary),
        (pa.bool_(), dt.boolean),
        (pa.date32(), dt.date),
        (pa.date64(), dt.date),
        (pa.time32("s"), dt.time),
        (pa.time32("ms"), dt.time),
        (pa.time64("us"), dt.time),
        (pa.time64("ns"), dt.time),
        (pa.timestamp("s"), dt.timestamp),
        (pa.timestamp("ms"), dt.timestamp),
        (pa.timestamp("us"), dt.timestamp),
        (pa.timestamp("ns"), dt.timestamp),
        (pa.timestamp("s", tz="UTC"), dt.Timestamp(timezone="UTC")),
        (pa.timestamp("ms", tz="UTC"), dt.Timestamp(timezone="UTC")),
        (pa.timestamp("us", tz="UTC"), dt.Timestamp(timezone="UTC")),
        (pa.timestamp("ns", tz="UTC"), dt.Timestamp(timezone="UTC")),
        (
            pa.struct([("a", pa.struct([("b", pa.int64())]))]),
            dt.Struct.from_tuples([("a", dt.Struct.from_tuples([("b", dt.int64)]))]),
        ),
        (pa.list_(pa.list_(pa.float64())), dt.Array(dt.Array(dt.float64))),
        (
            pa.map_(pa.string(), pa.list_(pa.struct([("z", pa.list_(pa.string()))]))),
            dt.Map(
                dt.string,
                dt.Array(dt.Struct.from_tuples([("z", dt.Array(dt.String()))])),
            ),
        ),
    ],
)
def test_from_pyarrow_struct(arrow_type: pa.DataType, ibis_type: dt.DataType) -> None:
    result = dt.dtype(arrow_type)
    assert result == ibis_type
