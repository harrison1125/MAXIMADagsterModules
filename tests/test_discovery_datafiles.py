from __future__ import annotations

import pytest

from MaximaDagster.utils.discovery import (
    DatafilesDiscoveryError,
    call_with_retries,
    fetch_partition_details,
    fetch_partitions,
    get_datafiles_retry_count,
    get_datafiles_retry_delay_seconds,
)


class _FakePartitionClient:
    def __init__(self, partition_response=None, details_response=None):
        self.partition_response = partition_response
        self.details_response = details_response

    def get(self, route, parameters=None):
        _ = parameters
        if route == "aimdl/partition":
            return self.partition_response
        if route == "aimdl/partition/details":
            return self.details_response
        raise AssertionError(route)


def test_fetch_partitions_returns_empty_dict_on_none_response() -> None:
    gc = _FakePartitionClient(partition_response=None)

    result = fetch_partitions(gc, data_type="xrd_raw", since="1970-01-01T00:00:00+00:00")

    assert result == {}


def test_fetch_partitions_raises_for_non_dict_response() -> None:
    gc = _FakePartitionClient(partition_response=["not", "a", "dict"])

    with pytest.raises(DatafilesDiscoveryError, match="Expected dict"):
        fetch_partitions(gc, data_type="xrd_raw", since="1970-01-01T00:00:00+00:00")


def test_fetch_partitions_filters_blank_keys_and_checksums() -> None:
    gc = _FakePartitionClient(
        partition_response={
            "exp_1": "abc",
            "": "skip",
            "exp_2": "",
            "exp_3": " def ",
            None: "ghi",
        }
    )

    result = fetch_partitions(gc, data_type="xrd_raw", since="1970-01-01T00:00:00+00:00")

    assert result == {"exp_1": "abc", "exp_3": "def"}


def test_fetch_partition_details_returns_empty_list_on_none_response() -> None:
    gc = _FakePartitionClient(details_response=None)

    result = fetch_partition_details(gc, key="exp_1", data_type="xrd_raw")

    assert result == []


def test_fetch_partition_details_raises_for_non_list_response() -> None:
    gc = _FakePartitionClient(details_response={"bad": "shape"})

    with pytest.raises(DatafilesDiscoveryError, match="Expected list"):
        fetch_partition_details(gc, key="exp_1", data_type="xrd_raw")


def test_fetch_partition_details_filters_non_dict_rows() -> None:
    gc = _FakePartitionClient(details_response=[{"_id": "a"}, "bad", 1, {"_id": "b"}])

    result = fetch_partition_details(gc, key="exp_1", data_type="xrd_raw")

    assert result == [{"_id": "a"}, {"_id": "b"}]


def test_retry_env_parsing_clamps_values(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "999")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "999")
    assert get_datafiles_retry_count() == 5
    assert get_datafiles_retry_delay_seconds() == 10.0

    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "-5")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "-1")
    assert get_datafiles_retry_count() == 0
    assert get_datafiles_retry_delay_seconds() == 0.0


def test_retry_env_parsing_invalid_values_fall_back_to_defaults(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "abc")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "abc")

    assert get_datafiles_retry_count() == 2
    assert get_datafiles_retry_delay_seconds() == 0.5


def test_call_with_retries_retries_then_succeeds(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "2")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")

    state = {"attempts": 0}

    def flaky() -> str:
        state["attempts"] += 1
        if state["attempts"] < 3:
            raise RuntimeError("transient")
        return "ok"

    assert call_with_retries(flaky) == "ok"
    assert state["attempts"] == 3


def test_call_with_retries_raises_last_error_after_exhaustion(monkeypatch) -> None:
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_COUNT", "1")
    monkeypatch.setenv("DISCOVERY_DATAFILES_RETRY_DELAY_SECONDS", "0")

    state = {"attempts": 0}

    def always_fail() -> None:
        state["attempts"] += 1
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        call_with_retries(always_fail)

    assert state["attempts"] == 2
