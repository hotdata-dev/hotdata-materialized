import datetime
import decimal
import uuid

import pytest

from hotdata_materialized import FingerprintError
from hotdata_materialized.fingerprint import (
    SHORT_LENGTH,
    fingerprint_call,
    fingerprint_queryset,
    short,
)


def sample(a, b=None):
    return a, b


def test_same_call_same_fingerprint():
    assert fingerprint_call(sample, (1,), {"b": 2}) == fingerprint_call(
        sample, (1,), {"b": 2}
    )


def test_different_args_different_fingerprint():
    assert fingerprint_call(sample, (1,)) != fingerprint_call(sample, (2,))


def test_kwarg_order_is_irrelevant():
    one = fingerprint_call(sample, (), {"a": 1, "b": 2})
    two = fingerprint_call(sample, (), {"b": 2, "a": 1})
    assert one == two


def test_version_busts_fingerprint():
    assert fingerprint_call(sample, (1,), version=1) != fingerprint_call(
        sample, (1,), version=2
    )


def test_typed_values_do_not_collide_with_their_string_forms():
    # a false hit serves someone else's data — types must be part of identity
    assert fingerprint_call(sample, (datetime.date(2026, 7, 22),)) != fingerprint_call(
        sample, ("2026-07-22",)
    )
    assert fingerprint_call(sample, (decimal.Decimal("1.50"),)) != fingerprint_call(
        sample, ("1.50",)
    )
    assert fingerprint_call(
        sample, (uuid.UUID("12345678-1234-5678-1234-567812345678"),)
    ) != fingerprint_call(sample, ("12345678-1234-5678-1234-567812345678",))


def test_common_scalar_types_are_stable():
    args = (
        datetime.datetime(2026, 7, 22, 12, 0, tzinfo=datetime.timezone.utc),
        datetime.date(2026, 7, 22),
        decimal.Decimal("1.50"),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        frozenset({"b", "a"}),
        b"\x01\x02",
    )
    assert fingerprint_call(sample, args) == fingerprint_call(sample, args)


def test_unserializable_argument_is_rejected():
    class Opaque:
        pass

    with pytest.raises(FingerprintError, match="key_fn"):
        fingerprint_call(sample, (Opaque(),))


def test_key_fn_overrides_argument_hashing():
    class Opaque:
        token = "x"

    fp = fingerprint_call(
        sample, (Opaque(),), key_fn=lambda obj: {"token": obj.token}
    )
    assert fp == fingerprint_call(
        sample, (Opaque(),), key_fn=lambda obj: {"token": obj.token}
    )


def test_short_length():
    fp = fingerprint_call(sample, (1,))
    assert len(short(fp)) == SHORT_LENGTH
    assert fp.startswith(short(fp))


def test_queryset_fingerprint_stable_and_param_sensitive():
    from tests.models import Event

    base = Event.objects.filter(event_type="signup").values("user_id")
    assert fingerprint_queryset(base) == fingerprint_queryset(
        Event.objects.filter(event_type="signup").values("user_id")
    )
    assert fingerprint_queryset(base) != fingerprint_queryset(
        Event.objects.filter(event_type="churn").values("user_id")
    )
    assert fingerprint_queryset(base) != fingerprint_queryset(base, version=1)
