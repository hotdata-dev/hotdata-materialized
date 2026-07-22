import datetime

import pytest

from hotdata_materialized.exceptions import RegistryError
from hotdata_materialized._sql import quote_literal


def test_none_is_null():
    assert quote_literal(None) == "NULL"


def test_bool_before_int():
    assert quote_literal(True) == "TRUE"
    assert quote_literal(False) == "FALSE"


def test_numbers():
    assert quote_literal(42) == "42"
    assert quote_literal(1.5) == "1.5"


def test_non_finite_floats_rejected():
    with pytest.raises(RegistryError):
        quote_literal(float("nan"))
    with pytest.raises(RegistryError):
        quote_literal(float("inf"))


def test_string_quote_escaping():
    assert quote_literal("it's") == "'it''s'"
    assert quote_literal("plain") == "'plain'"


def test_nul_byte_rejected():
    with pytest.raises(RegistryError):
        quote_literal("bad\x00value")


def test_datetime_encodes_as_iso_string():
    moment = datetime.datetime(2026, 7, 22, 12, 0, tzinfo=datetime.timezone.utc)
    assert quote_literal(moment) == "'2026-07-22T12:00:00+00:00'"


def test_unknown_type_rejected():
    with pytest.raises(RegistryError):
        quote_literal(object())
