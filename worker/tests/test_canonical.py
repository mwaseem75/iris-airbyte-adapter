import json
from decimal import Decimal

from app import canonical, runner

SCHEMA = {"properties": {
    "id": {"type": "number", "airbyte_type": "integer"},
    "balance": {"type": ["null", "number"], "airbyte_type": "numeric"},
    "updated_at": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"},
    "local_ts": {"type": "string", "format": "date-time", "airbyte_type": "timestamp_without_timezone"},
    "name": {"type": "string"},
    "email": {"type": ["null", "string"]},
}}


def load(text):  # same parsing as the runner
    return json.loads(text, parse_float=Decimal)


def test_decimal_keeps_scale_as_string():
    out = canonical.convert_record(load('{"balance": 120.50}'), SCHEMA)
    assert out["balance"] == "120.50"
    assert canonical.convert_record(load('{"balance": 10000000.00}'), SCHEMA)["balance"] == "10000000.00"
    assert canonical.convert_record(load('{"balance": -45.10}'), SCHEMA)["balance"] == "-45.10"


def test_whole_number_in_number_field_is_string():
    assert canonical.convert_record({"balance": 7}, SCHEMA)["balance"] == "7"


def test_integer_stays_integer():
    assert canonical.convert_record(load('{"id": 42}'), SCHEMA)["id"] == 42


def test_timestamp_gets_offset():
    out = canonical.convert_record({"updated_at": "2026-11-06T04:59:59.000000Z"}, SCHEMA)
    assert out["updated_at"] == "2026-11-06T04:59:59+00:00"
    out = canonical.convert_record({"updated_at": "2026-11-01T10:00:00.123456+03:00"}, SCHEMA)
    assert out["updated_at"] == "2026-11-01T10:00:00.123456+03:00"


def test_timestamp_without_zone_is_not_given_an_invented_offset():
    assert canonical.convert_record({"local_ts": "2026-11-01T10:00:00"}, SCHEMA)["local_ts"] == "2026-11-01T10:00:00"


def test_unicode_intact_and_null_kept_absent_not_added():
    out = canonical.convert_record({"name": "Zoë Müller", "email": None}, SCHEMA)
    assert out == {"name": "Zoë Müller", "email": None}
    assert "balance" not in out
    assert canonical.convert_record({"name": "محمد"}, SCHEMA)["name"] == "محمد"


def test_airbyte_metadata_removed():
    out = canonical.convert_record({"id": 1, "_airbyte_raw_id": "x", "_airbyte_extracted_at": "y"}, SCHEMA)
    assert out == {"id": 1}


def test_single_key():
    assert canonical.extract_key({"id": 7}, ["id"]) == (["7"], None)


def test_composite_key_order_follows_key_fields():
    assert canonical.extract_key({"order_id": 1, "line_no": 2}, ["order_id", "line_no"]) == (["1", "2"], None)


def test_missing_key_field_is_flagged_not_invented():
    assert canonical.extract_key({"name": "x"}, ["id"]) == (None, "missing_key_field:id")


def test_null_or_empty_key_is_flagged():
    assert canonical.extract_key({"id": None}, ["id"]) == (None, "null_key_field:id")
    assert canonical.extract_key({"id": "  "}, ["id"]) == (None, "invalid_key_field:id")


def test_composite_key_with_one_null_part_is_flagged():
    assert canonical.extract_key({"a": 1, "b": None}, ["a", "b"]) == (None, "null_key_field:b")


def test_no_key_defined_is_flagged():
    assert canonical.extract_key({"id": 1}, []) == (None, "no_key_defined")
    assert canonical.extract_key({"id": 1}, None) == (None, "no_key_defined")


def test_output_shape():
    out = canonical.to_output("customers", {"id": 1, "_airbyte_x": 1}, SCHEMA, ["id"])
    assert out == {"stream": "customers", "key": ["1"], "data": {"id": 1}, "hash": canonical.data_hash({"id": 1})}
    bad = canonical.to_output("customers", {"name": "x"}, SCHEMA, ["id"])
    assert bad["key"] is None and bad["key_error"] == "missing_key_field:id"


def test_plan_incremental_with_and_without_cursor():
    discovered = {"customers": {"default_cursor_field": []}, "order_lines": {"default_cursor_field": []}}
    plan = runner.plan_streams(
        [{"name": "customers", "cursor_field": "updated_at"}, {"name": "order_lines"}], discovered, "incremental")
    assert plan["customers"] == {"mode": "incremental", "cursor_field": "updated_at", "fallback": False}
    assert plan["order_lines"] == {"mode": "full_refresh", "cursor_field": None, "fallback": True}


def test_state_only_passed_for_incremental_streams_and_nulls_stripped():
    plan = {"customers": {"mode": "incremental"}, "order_lines": {"mode": "full_refresh"}}
    state = [
        {"type": "STREAM", "data": None, "stream": {"stream_descriptor": {"name": "customers"},
                                                      "stream_state": {"cursors": {"updated_at": "x"}}}},
        {"type": "STREAM", "stream": {"stream_descriptor": {"name": "order_lines"}, "stream_state": {}}},
    ]
    kept = runner.filter_state(state, plan)
    assert len(kept) == 1 and "data" not in kept[0]


def test_integer_declared_as_number_with_airbyte_type_stays_integer():
    assert canonical.convert_record(load('{"id": 42}'), SCHEMA)["id"] == 42
    assert canonical.convert_record({"id": Decimal("42")}, SCHEMA)["id"] == 42


def test_data_hash_does_not_depend_on_field_order_but_on_values():
    a = canonical.data_hash({"b": 1, "a": "x"})
    assert a == canonical.data_hash({"a": "x", "b": 1})
    assert a != canonical.data_hash({"a": "x", "b": 2})
    assert a != canonical.data_hash({"a": "x", "b": "1"})  # 1 and "1" are different values
    assert len(a) == 64


def test_data_hash_keeps_unicode_and_null_and_nested_order():
    assert canonical.data_hash({"n": None, "t": "café €"}) != canonical.data_hash({"n": "", "t": "café €"})
    # only the top-level field names are sorted; nested content keeps its order
    assert canonical.data_hash({"o": {"b": 1, "a": 2}}) != canonical.data_hash({"o": {"a": 2, "b": 1}})
