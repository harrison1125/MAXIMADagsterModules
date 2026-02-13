from pathlib import Path

from MaximaDagster.modules import MCAtoFit


def test_run_pymca_batch(tmp_path: Path, monkeypatch):
    root_dir = tmp_path / "root"
    root_dir.mkdir()
    (root_dir / "a.mca").write_text("data")

    calls = []

    def fake_run(command, capture_output, text):
        calls.append(command)
        class Result:
            returncode = 0
            stdout = "ok"
            stderr = ""
        return Result()

    monkeypatch.setattr(MCAtoFit.subprocess, "run", fake_run)

    processed = MCAtoFit.run_pymca_batch(
        root_dir=str(root_dir),
        config_path=str(tmp_path / "cfg.cfg"),
    )

    assert processed
    assert calls
    assert "pymcabatch" in calls[0][0]
