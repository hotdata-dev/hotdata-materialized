"""Compare running a heavy TPC-H aggregation directly on Postgres vs through
the @materialize decorator: one miss (runs on Postgres, persisted
write-behind), then hits that never touch Postgres, plus server-side SQL on
the cached entry.

Usage:
    python manage.py compare                 # Q1 pricing summary (tiny result)
    python manage.py compare --scenario parts  # revenue by part (large result)
"""

import datetime
import statistics
import time

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, DecimalField, ExpressionWrapper, F, Sum, Value

from hotdata_materialized import fingerprint_call, materialize
from hotdata_materialized.decorator import get_runtime
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
    """Revenue by part: a wide group-by producing a large result."""
    return (
        LineItem.objects.values("partkey")
        .annotate(revenue=Sum(DISC_PRICE), orders=Count("orderkey"))
        .order_by("-revenue")
    )


@materialize(ttl=3600, key="tpch-q1")
def q1_report():
    return q1_queryset()


@materialize(ttl=3600, key="tpch-parts")
def parts_report():
    return parts_queryset()


SCENARIOS = {
    "q1": (
        q1_queryset,
        q1_report,
        "SELECT returnflag, linestatus, sum_disc_price FROM this "
        "ORDER BY sum_disc_price DESC LIMIT 3",
    ),
    "parts": (
        parts_queryset,
        parts_report,
        "SELECT partkey, revenue FROM this ORDER BY revenue DESC LIMIT 10",
    ),
}


def ms(seconds):
    return f"{seconds * 1000:,.0f} ms"


class Command(BaseCommand):
    help = "Compare a heavy TPC-H query on Postgres vs the @materialize path"

    def add_arguments(self, parser):
        parser.add_argument("--scenario", choices=SCENARIOS, default="q1")
        parser.add_argument("--runs", type=int, default=3)
        parser.add_argument(
            "--keep", action="store_true",
            help="reuse an existing entry instead of starting from a miss",
        )

    def handle(self, *args, scenario, runs, keep, **options):
        queryset_factory, report, transform_sql = SCENARIOS[scenario]
        _, store = get_runtime()
        # the decorator fingerprints the wrapped function + its (empty) args
        fingerprint = fingerprint_call(report.__wrapped__)
        self.stdout.write(f"scenario={scenario}  fingerprint={fingerprint[:16]}…\n")

        # -- direct: Postgres does the work every time -----------------------
        direct_times = []
        for run in range(runs):
            start = time.perf_counter()
            rows = list(queryset_factory())
            direct_times.append(time.perf_counter() - start)
            self.stdout.write(
                f"  postgres direct run {run + 1}: {ms(direct_times[-1])} "
                f"({len(rows):,} rows)"
            )

        # -- miss: caller gets the frame at Postgres speed; persist is hidden --
        if not keep:
            store.evict(fingerprint)
        miss_time = None
        background_time = None
        start = time.perf_counter()
        frame = report()
        elapsed = time.perf_counter() - start
        if not frame.cached:
            miss_time = elapsed
            self.stdout.write(
                f"  materialized miss, perceived: {ms(miss_time)} "
                f"({len(frame):,} rows returned to caller)"
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

        # -- hits: Postgres is never touched, data comes back as Arrow -------
        hit_times = []
        for _ in range(5):
            start = time.perf_counter()
            frame = report()
            table = frame.arrow()
            hit_times.append(time.perf_counter() - start)
        if not frame.cached:
            raise CommandError("expected a hit; entry never became ready")
        backing = (
            "inline payload" if frame.entry.inline_payload
            else "entry database (arrow)"
        )
        self.stdout.write(
            f"  materialized hit ×5 via {backing}: median "
            f"{ms(statistics.median(hit_times))} ({table.num_rows:,} rows)"
        )

        # -- transform: SQL over the cached entry, server-side ---------------
        start = time.perf_counter()
        top = frame.sql(transform_sql)
        transform_time = time.perf_counter() - start
        self.stdout.write(f"  server-side SQL on cached entry: {ms(transform_time)}")
        for row in top.to_pylist():
            self.stdout.write(f"    {row}")

        # -- summary ----------------------------------------------------------
        median_direct = statistics.median(direct_times)
        median_hit = statistics.median(hit_times)
        self.stdout.write("")
        self.stdout.write(f"  {'postgres direct (median)':34s} {ms(median_direct)}")
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
            f"  ({median_direct / median_hit:,.1f}× faster, zero load on Postgres)"
        )
