from django.db import models


class LineItem(models.Model):
    """Read-only mapping of tpch_sf1.lineitem. The real key is composite
    (l_orderkey, l_linenumber); orderkey is declared primary_key only to
    satisfy Django — this model is used for aggregate reads exclusively."""

    orderkey = models.BigIntegerField(db_column="l_orderkey", primary_key=True)
    partkey = models.BigIntegerField(db_column="l_partkey")
    suppkey = models.BigIntegerField(db_column="l_suppkey")
    linenumber = models.IntegerField(db_column="l_linenumber")
    quantity = models.DecimalField(db_column="l_quantity", max_digits=15, decimal_places=2)
    extendedprice = models.DecimalField(
        db_column="l_extendedprice", max_digits=15, decimal_places=2
    )
    discount = models.DecimalField(db_column="l_discount", max_digits=15, decimal_places=2)
    tax = models.DecimalField(db_column="l_tax", max_digits=15, decimal_places=2)
    returnflag = models.CharField(db_column="l_returnflag", max_length=1)
    linestatus = models.CharField(db_column="l_linestatus", max_length=1)
    shipdate = models.DateField(db_column="l_shipdate")
    shipmode = models.CharField(db_column="l_shipmode", max_length=10)

    class Meta:
        managed = False
        db_table = "lineitem"
        app_label = "tpch"
