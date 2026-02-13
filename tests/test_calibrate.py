import pytest

pytest.importorskip("torch")
pytest.importorskip("torchvision")
pytest.importorskip("transformers")
pytest.importorskip("fabio")
pytest.importorskip("pyFAI")
pytest.importorskip("skimage")
pytest.importorskip("scipy")

from MaximaDagster.modules import calibrate


def test_calibrate_image_uses_wrapper(monkeypatch):
    class StubCalibrator:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def calibrate(self, image_path, output_path=None):
            return {"image": image_path, "output": output_path}

    monkeypatch.setattr(calibrate, "MaximaCalibrator", StubCalibrator)

    result = calibrate.calibrate_image(
        image_path="image.tif",
        model_path="model.pth",
        output_path="out.poni",
    )

    assert result["image"] == "image.tif"
    assert result["output"] == "out.poni"
