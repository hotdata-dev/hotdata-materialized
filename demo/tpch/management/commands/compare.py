"""Compare running a heavy TPC-H aggregation directly on Neon vs through
hotdata-materialized: one miss (runs on Neon, captured into Hotdata), then
hits that never touch Neon, plus a server-side transform on the cached entry.

Usage:
    python manage.py compare                 # Q1 pricing summary (tiny result)
    python manage.py compare --scenario parts  # revenue by part (~200k rows)
"""

import datetime
import statistics
import time
from decimal import Decimal

import pyarrow as pa
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, DecimalField, ExpressionWrapper, F, Sum, Value
from hotdata.models.query_request import QueryRequest

from hotdata_materialized import Config, EntryStore, Registry, get_clients
from hotdata_materialized.fingerprint import fingerprint_queryset
from tpch.models import LineItem

DEC = DecimalField(max_digits=25, decimal_places=4)
DISC_PRICE = ExpressionWrapper(
    F("extendedprice") * (Value(1) - F("discount")), output_field=DEC
)
CHARGE = ExpressionWrapper(
    F("extendedprice") * (Value(1) - F("discount")) * (Value(1) + F("tax")),
    output_field=DEC,
)


def q1_queryset():
    """TPC-H Q1: pricing summary report. Scans every lineitem row."""
    return (
        LineItem.objects.filter(shipdate__lte=datetime.date(1998, 9, 2))
        .values("returnflag", "linestatus")
        .annotate(
            sum_qty=Sum("quantity"),
            sum_base_price=Sum("extendedprice"),
            sum_disc_price=Sum(DISC_PRICE),
            sum_charge=Sum(CHARGE),
            avg_qty=ExpressionWrapper(
                Sum("quantity") * Value(1.0) / Count("orderkey"), output_field=DEC
            ),
            count_order=Count("orderkey"),
        )
        .order_by("returnflag", "linestatus")
    )


def parts_queryset():
    """Revenue by part: a wide group-by producing a large (~200k row) result."""
    return (
        LineItem.objects.values("partkey")
        .annotate(revenue=Sum(DISC_PRICE), orders=Count("orderkey"))
        .order_by("-revenue")
    )


SCENARIOS = {
    "q1": (
        q1_queryset,
        "SELECT returnflag, linestatus, sum_disc_price FROM data "
        "ORDER BY sum_disc_price DESC LIMIT 3",
    ),
    "parts": (parts_queryset, "SELECT partkey, revenue FROM data ORDER BY revenue DESC LIMIT 10"),
}


def rows_to_arrow(rows):
    def clean(value):
        return float(value) if isinstance(value, Decimal) else value

    return pa.Table.from_pylist(
        [{k: clean(v) for k, v in row.items()} for row in rows]
    )


def ms(seconds):
    return f"{seconds * 1000:,.0f} ms"


class Command(BaseCommand):
    help = "Compare a heavy TPC-H query on Neon vs the hotdata-materialized path"

    def add_arguments(self, parser):
        parser.add_argument("--scenario", choices=SCENARIOS, default="q1")
        parser.add_argument("--runs", type=int, default=3)
        parser.add_argument(
            "--keep", action="store_true",
            help="reuse an existing entry instead of starting from a miss",
        )

    def handle(self, *args, scenario, runs, keep, **options):
        queryset_factory, transform_sql = SCENARIOS[scenario]
        config = Config.from_django()
        clients = get_clients(config)
        registry = Registry(clients, config)
        store = EntryStore(clients, config, registry)

        fingerprint = fingerprint_queryset(queryset_factory())
        self.stdout.write(f"scenario={scenario}  fingerprint={fingerprint[:16]}…\n")

        # -- direct: Neon does the work every time --------------------------
        direct_times = []
        for run in range(runs):
            start = time.perf_counter()
            rows = list(queryset_factory())
            direct_times.append(time.perf_counter() - start)
            self.stdout.write(
                f"  neon direct run {run + 1}: {ms(direct_times[-1])} ({len(rows):,} rows)"
            )

        # -- miss: caller gets rows at Neon speed; persist runs write-behind --
        if not keep:
            store.evict(fingerprint)
        entry = registry.lookup(fingerprint)
        miss_time = None
        background_time = None
        if entry is None or entry.status != "ready":
            start = time.perf_counter()
            rows = list(queryset_factory())
            entry = store.materialize(
                fingerprint, rows_to_arrow(rows), key=scenario, ttl=3600
            )
            miss_time = time.perf_counter() - start
            self.stdout.write(
                f"  materialized miss, perceived: {ms(miss_time)} "
                f"({len(rows):,} rows returned to caller)"
            )
            start = time.perf_counter()
            errors = store.flush()
            background_time = time.perf_counter() - start
            if errors:
                raise CommandError(f"background persist FAILED: {errors[0]}")
            self.stdout.write(
                f"  background persist landed {ms(background_time)} after "
                f"return (off the request path)"
            )

        # -- hits: Neon is never touched, data comes back as Arrow ----------
        hit_times = []
        for _ in range(5):
            start = time.perf_counter()
            entry = registry.lookup(fingerprint)
            result = store.read_table(entry)
            hit_times.append(time.perf_counter() - start)
        backing = (
            "inline payload" if entry.inline_payload else "entry database (arrow)"
        )
        self.stdout.write(
            f"  materialized hit ×5 via {backing}: median "
            f"{ms(statistics.median(hit_times))} ({result.num_rows:,} rows)"
        )

        # -- transform: SQL over the cached entry, server-side ---------------
        start = time.perf_counter()
        response = clients.query.query(
            QueryRequest(sql=transform_sql), x_database_id=entry.database_id
        )
        transform_time = time.perf_counter() - start
        self.stdout.write(f"  server-side SQL on cached entry: {ms(transform_time)}")
        for row in response.rows:
            self.stdout.write(f"    {row}")

        # -- summary ----------------------------------------------------------
        median_direct = statistics.median(direct_times)
        median_hit = statistics.median(hit_times)
        self.stdout.write("")
        self.stdout.write(f"  {'neon direct (median)':34s} {ms(median_direct)}")
        if miss_time is not None:
            self.stdout.write(
                f"  {'materialized miss, perceived':34s} {ms(miss_time)}"
                f"  (direct + {ms(miss_time - median_direct)} on the request path)"
            )
            self.stdout.write(
                f"  {'  background persist (hidden)':34s} {ms(background_time)}"
            )
        self.stdout.write(
            f"  {'materialized hit (median)':34s} {ms(median_hit)}"
            f"  ({median_direct / median_hit:,.1f}× faster, zero load on Neon)"
        )
