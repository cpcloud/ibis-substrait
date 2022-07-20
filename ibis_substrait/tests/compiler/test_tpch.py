from datetime import date

import ibis
import pytest
from packaging import version

from ibis_substrait.compiler.decompile import decompile


@pytest.fixture
def tpch1(lineitem):
    return (
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


@pytest.mark.xfail(
    version.parse("2.1.1") < version.parse(ibis.__version__) <= version.parse("3.0.2"),
    reason="issue with unbounded decimal precision fixed on ibis-master",
)
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
    """
    -- t1, t2
    SELECT *
    FROM t1
    WHERE t1.y > (
        SELECT avg(t2.y)
        FROM t2
        WHERE t1.dept_id = t2.department_id
    )
    """
    t1 = ibis.table(dict(dept_id="string", y="double"), name="t1")
    t2 = ibis.table(dict(department_id="string", y="double"), name="t2")
    expr = t2[t1.dept_id == t2.department_id].y.mean()
    # Subquery { Expression { AggregateFunction {} }}
    # ^ this is a "column" expression == scalar subquery from SQLAlchemy
    expr = t1[t1.y > expr]
    plan = compiler.compile(expr)

    (result,) = decompile(plan)
    assert result.equals(expr)


@pytest.fixture
def con():
    return ibis.duckdb.connect("/data/tpch.ddb")


@pytest.fixture
def tpc_h02(con):
    "Minimum Cost Supplier Query (Q2)"

    REGION = "EUROPE"
    SIZE = 25
    TYPE = "BRASS"

    part, supplier, partsupp, nation, region = (
        ibis.table(con.table(name).schema(), name=name)
        for name in "part supplier partsupp nation region".split()
    )

    expr = (
        part.join(partsupp, part.p_partkey == partsupp.ps_partkey)
        .join(supplier, partsupp.ps_suppkey == supplier.s_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )
    #  return expr

    subexpr = (
        partsupp.join(supplier, supplier.s_suppkey == partsupp.ps_suppkey)
        .join(nation, supplier.s_nationkey == nation.n_nationkey)
        .join(region, nation.n_regionkey == region.r_regionkey)
    )

    subexpr = subexpr[
        (subexpr.r_name == REGION)# & (expr.p_partkey == subexpr.ps_partkey)
    ]
    #  return subexpr
    #
    filters = [
        #  expr.p_size == SIZE,
        #  expr.p_type.like("%" + TYPE),
        #  expr.r_name == REGION,
        expr.ps_supplycost == subexpr.ps_supplycost.min(),
    ]
    q = expr.filter(filters)
    return q
    #
    #  q = q.select(
    #      [
    #          q.s_acctbal,
    #          q.s_name,
    #          q.n_name,
    #          q.p_partkey,
    #          q.p_mfgr,
    #          q.s_address,
    #          q.s_phone,
    #          q.s_comment,
    #      ]
    #  )
    #
    #  return q.sort_by(
    #      [
    #          ibis.desc(q.s_acctbal),
    #          q.n_name,
    #          q.s_name,
    #          q.p_partkey,
    #      ]
    #  ).limit(100)


def test_tpc_h02(tpc_h02, compiler):
    expr = tpc_h02
    plan = compiler.compile(expr)

    breakpoint()
    (result,) = decompile(plan)
    breakpoint()
    assert result.equals(expr)
