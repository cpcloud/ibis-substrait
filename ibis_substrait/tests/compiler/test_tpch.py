from datetime import date

import ibis

from ibis_substrait.compiler.decompile import decompile


def test_tpch1(tpch1, lineitem, compiler):
    plan = compiler.compile(tpch1)
    assert plan.SerializeToString()

    (result,) = decompile(plan)
    expected = (
        lineitem.filter(lambda t: t.l_shipdate <= date(year=1998, month=9, day=2))
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
    assert result.equals(expected)


def test_correlated_subquery(compiler):
    part = ibis.table([("p_partkey", "int64")], name="part")
    partsupp = ibis.table(
        [
            ("ps_partkey", "int64"),
            ("ps_supplycost", "float64"),
            ("ps_suppkey", "int64"),
        ],
        name="partsupp",
    )
    supplier = ibis.table([("s_suppkey", "int64")], name="supplier")

    q = part.join(partsupp, part.p_partkey == partsupp.ps_partkey).select(
        [
            part.p_partkey,
            partsupp.ps_supplycost,
        ]
    )
    subq = (
        partsupp.join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .projection([partsupp.ps_partkey, partsupp.ps_supplycost])
        .filter(lambda t: t.ps_partkey == q.p_partkey)
    )

    expr = subq
    #  expr = q[q.ps_supplycost == subq.ps_supplycost.min()]
    plan = compiler.compile(expr)

    (result,) = decompile(plan)
    breakpoint()
    assert result.equals(expr)


def test_tpch2(tpch2, compiler):
    plan = compiler.compile(tpch2)
    assert plan.SerializeToString()

    (result,) = decompile(plan)
