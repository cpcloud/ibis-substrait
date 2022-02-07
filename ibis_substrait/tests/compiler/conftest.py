import datetime

import ibis
import ibis.expr.datatypes as dt
import pytest


def pytest_runtest_setup(item):
    if item.function.__name__ == "test_decompile" and any(
        item.iter_markers(name="no_decompile")
    ):
        item.add_marker(
            pytest.mark.xfail(
                raises=(AssertionError, NotImplementedError),
                reason=f"`{item.callspec.id}` cannot yet be reified",
            )
        )


@pytest.fixture
def lineitem():
    return ibis.table(
        [
            ("l_orderkey", dt.int64),
            ("l_partkey", dt.int64),
            ("l_suppkey", dt.int64),
            ("l_linenumber", dt.int64),
            ("l_quantity", dt.Decimal(15, 2)),
            ("l_extendedprice", dt.Decimal(15, 2)),
            ("l_discount", dt.Decimal(15, 2)),
            ("l_tax", dt.Decimal(15, 2)),
            ("l_returnflag", dt.string),
            ("l_linestatus", dt.string),
            ("l_shipdate", dt.date),
            ("l_commitdate", dt.date),
            ("l_receiptdate", dt.date),
            ("l_shipinstruct", dt.string),
            ("l_shipmode", dt.string),
            ("l_comment", dt.string),
        ],
        name="lineitem",
    )


@pytest.fixture
def tpch1(lineitem):
    return (
        lineitem.filter(lambda t: t.l_shipdate <= datetime.date(1998, 9, 2))
        .group_by(["l_returnflag", "l_linestatus"])
        .aggregate(
            sum_qty=lambda t: t.l_quantity.sum(),
            sum_base_price=lambda t: t.l_extendedprice.sum(),
            sum_disc_price=lambda t: (t.l_extendedprice * (1 - t.l_discount)).sum(),
            sum_charge=lambda t: (
                t.l_extendedprice * (1 - t.l_discount) * (1 + t.l_tax)
            ).sum(),
            avg_qty=lambda t: t.l_quantity.mean(),
            avg_price=lambda t: t.l_extendedprice.mean(),
            avg_disc=lambda t: t.l_discount.mean(),
            count_order=lambda t: t.count(),
        )
        .sort_by(["l_returnflag", "l_linestatus"])
    )


@pytest.fixture
def part():
    return ibis.table(
        [
            ("p_partkey", "int64"),
            ("p_name", "string"),
            ("p_mfgr", "string"),
            ("p_brand", "string"),
            ("p_type", "string"),
            ("p_size", "int32"),
            ("p_container", "string"),
            ("p_retailprice", "float64"),
            ("p_comment", "string"),
        ],
        name="part",
    )


@pytest.fixture
def supplier():
    return ibis.table(
        [
            ("s_suppkey", "int64"),
            ("s_name", "string"),
            ("s_address", "string"),
            ("s_nationkey", "int32"),
            ("s_phone", "string"),
            ("s_acctbal", "float64"),
            ("s_comment", "string"),
        ],
        name="supplier",
    )


@pytest.fixture
def partsupp():
    return ibis.table(
        [
            ("ps_partkey", "int64"),
            ("ps_suppkey", "int64"),
            ("ps_availqty", "int64"),
            ("ps_supplycost", "float64"),
            ("ps_comment", "string"),
        ],
        name="partsupp",
    )


@pytest.fixture
def nation():
    return ibis.table(
        [
            ("n_nationkey", "int32"),
            ("n_name", "string"),
            ("n_regionkey", "int32"),
            ("n_comment", "string"),
        ],
        name="nation",
    )


@pytest.fixture
def region():
    return ibis.table(
        [
            ("r_regionkey", "int32"),
            ("r_name", "string"),
            ("r_comment", "string"),
        ],
        name="region",
    )


@pytest.fixture
def tpch2(part, supplier, partsupp, nation, region):
    REGION = "EUROPE"
    SIZE = 25
    TYPE = "BRASS"

    expr = (
        part.join(partsupp, part.p_partkey == partsupp.ps_partkey)
        .join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    ).materialize()

    subexpr = (
        partsupp.join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    ).materialize()

    subexpr = subexpr[
        (subexpr.r_name == REGION) & (expr.p_partkey == subexpr.ps_partkey)
    ]

    return (
        expr.filter(
            [
                lambda t: t.p_size == SIZE,
                lambda t: t.p_type.like(f"%{TYPE}"),
                lambda t: t.r_name == REGION,
                lambda t: t.ps_supplycost == subexpr.ps_supplycost.min(),
            ]
        )
        .select(
            [
                "s_acctbal",
                "s_name",
                "n_name",
                "p_partkey",
                "p_mfgr",
                "s_address",
                "s_phone",
                "s_comment",
            ]
        )
        .sort_by(
            [
                lambda t: ibis.desc(t.s_acctbal),
                "n_name",
                "s_name",
                "p_partkey",
            ]
        )
        .limit(100)
    )
