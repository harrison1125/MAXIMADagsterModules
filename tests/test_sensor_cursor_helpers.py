from __future__ import annotations

from MaximaDagster import sensors


def test_parse_girder_cursor_handles_invalid_json() -> None:
    since, checksums = sensors._parse_girder_cursor("not-json")
    assert since == ""
    assert checksums == {}


def test_parse_girder_cursor_handles_non_dict_json() -> None:
    since, checksums = sensors._parse_girder_cursor('["not", "dict"]')
    assert since == ""
    assert checksums == {}


def test_parse_girder_cursor_filters_blank_checksum_entries() -> None:
    cursor = sensors._serialize_girder_cursor(
        "2026-01-01T00:00:00+00:00",
        {
            "exp_1": "chk_1",
            "": "skip",
            "exp_2": "",
        },
    )

    since, checksums = sensors._parse_girder_cursor(cursor)

    assert since == "2026-01-01T00:00:00+00:00"
    assert checksums == {"exp_1": "chk_1"}