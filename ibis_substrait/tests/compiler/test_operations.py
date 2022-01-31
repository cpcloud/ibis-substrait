import ibis.expr.datatypes as dt
import ibis.expr.schema as sch

from ibis_substrait.compiler.decompile import decompile
from ibis_substrait.compiler.operations import LocalFilesTable


def test_local_files_table() -> None:
    files = [
        "/home/cloud/downloads/lineitem.1.parquet",
        "/home/cloud/downloads/lineitem.parquet",
    ]
    table = LocalFilesTable(files).to_expr()
    schema = table.schema()
    expected = sch.Schema.from_tuples(
        [
            ("l_orderkey", dt.int64),
            ("l_partkey", dt.int64),
            ("l_suppkey", dt.int64),
            ("l_linenumber", dt.int64),
            ("l_quantity", dt.float32),
            ("l_extendedprice", dt.float32),
            ("l_discount", dt.float32),
            ("l_tax", dt.float32),
            ("l_returnflag", dt.string),
            ("l_linestatus", dt.string),
            ("l_shipdate", dt.date),
            ("l_commitdate", dt.date),
            ("l_receiptdate", dt.date),
            ("l_shipinstruct", dt.string),
            ("l_shipmode", dt.string),
            ("l_comment", dt.string),
        ]
    )
    assert schema == expected


def test_translate_local_files(compiler):
    files = ["/home/cloud/downloads/lineitem.1.parquet"]
    table = LocalFilesTable(files).to_expr()
    result = compiler.compile(table)
    (expr,) = decompile(result)
    assert expr.equals(table)
