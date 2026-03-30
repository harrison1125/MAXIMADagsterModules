from types import SimpleNamespace

from MaximaDagster import assets


def _run_record(run_id: str):
    return SimpleNamespace(dagster_run=SimpleNamespace(run_id=run_id))


class _FakeInstance:
    def __init__(self, records):
        self._records = records

    def get_run_records(self, filters=None, limit=None):
        _ = filters
        _ = limit
        return self._records


class _FakeContext:
    def __init__(self, run_id: str, records):
        self.run_id = run_id
        self.instance = _FakeInstance(records)


def test_has_inflight_calibration_precompute_true_for_other_run():
    context = _FakeContext(
        run_id="current_run",
        records=[_run_record("current_run"), _run_record("other_run")],
    )

    assert assets._has_inflight_calibration_precompute(context, "cal_file_1") is True


def test_has_inflight_calibration_precompute_false_for_self_only():
    context = _FakeContext(
        run_id="current_run",
        records=[_run_record("current_run")],
    )

    assert assets._has_inflight_calibration_precompute(context, "cal_file_1") is False
