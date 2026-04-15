import pytest
import numpy as np

pytest.importorskip("torch")
pytest.importorskip("torchvision")
pytest.importorskip("transformers")
pytest.importorskip("fabio")
pytest.importorskip("pyFAI")
pytest.importorskip("skimage")
pytest.importorskip("scipy")

from MaximaDagster.modules import calibrate


def test_calibrate_image_builds_calibrator_with_expected_arguments(monkeypatch) -> None:
    class StubCalibrator:
        init_kwargs = None

        def __init__(self, **kwargs):
            StubCalibrator.init_kwargs = kwargs

        def calibrate(self, **kwargs):
            return kwargs

    monkeypatch.setattr(calibrate, "MaximaCalibrator", StubCalibrator)

    result = calibrate.calibrate_image(
        image_path="image.tif",
        model_path="model.pth",
        output_path="out.poni",
        calibrant="alpha_Al2O3",
        detector="Eiger2Cdte_1M",
        energy=12.0,
        image_size=512,
        backbone="my-backbone",
        hidden_dim=128,
        device="cpu",
    )

    assert result["output_path"] == "out.poni"
    assert StubCalibrator.init_kwargs is not None
    assert StubCalibrator.init_kwargs["model_path"] == "model.pth"
    assert StubCalibrator.init_kwargs["calibrant"] == "alpha_Al2O3"
    assert StubCalibrator.init_kwargs["detector"] == "Eiger2Cdte_1M"
    assert StubCalibrator.init_kwargs["image_size"] == 512
    assert StubCalibrator.init_kwargs["backbone"] == "my-backbone"
    assert StubCalibrator.init_kwargs["hidden_dim"] == 128
    assert StubCalibrator.init_kwargs["device"] == "cpu"


@pytest.mark.xfail(
    strict=True,
    raises=TypeError,
    reason="BUG: calibrate_image forwards image_path keyword but MaximaCalibrator.calibrate expects image",
)
def test_calibrate_image_passes_image_argument_to_calibrate(monkeypatch) -> None:
    class StubCalibrator:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def calibrate(self, image, output_path=None):
            return {"image": image, "output": output_path}

    monkeypatch.setattr(calibrate, "MaximaCalibrator", StubCalibrator)

    result = calibrate.calibrate_image(
        image_path="image.tif",
        model_path="model.pth",
        output_path="out.poni",
    )

    assert result["image"] == "image.tif"
    assert result["output"] == "out.poni"


@pytest.mark.parametrize(
    ("energy", "wavelength", "expected"),
    [
        (12.0, None, calibrate.HC_KEV_M / 12.0),
        (None, 1.23e-10, 1.23e-10),
        (None, None, calibrate.DEFAULT_WAVELENGTH_M),
    ],
)
def test_resolve_wavelength_valid_inputs(energy: float | None, wavelength: float | None, expected: float) -> None:
    assert calibrate.resolve_wavelength(energy=energy, wavelength=wavelength) == pytest.approx(expected)


def test_resolve_wavelength_rejects_conflicting_inputs() -> None:
    with pytest.raises(ValueError, match="either `energy` or `wavelength`"):
        calibrate.resolve_wavelength(energy=12.0, wavelength=1.0)


def test_resolve_wavelength_rejects_nonpositive_energy() -> None:
    with pytest.raises(ValueError, match="must be > 0"):
        calibrate.resolve_wavelength(energy=0.0, wavelength=None)


class _FakeDetector:
    pixel1 = 0.1
    pixel2 = 0.1


class _FakeGeometry:
    dist = 1.0
    poni1 = 2.0
    poni2 = 3.0
    rot1 = 4.0
    rot2 = 5.0
    rot3 = 6.0
    detector = _FakeDetector()

    def tth(self, y, x):
        _ = x
        return np.array(y, dtype=float) * 0.001


class _FakeCalibrant:
    wavelength = 1.0

    def get_2th(self):
        return [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007]


def test_peak_optimizer_objective_handles_peak_extraction_exception(monkeypatch) -> None:
    optimizer = calibrate.PeakOptimizer(np.ones((10, 10), dtype=float), _FakeGeometry(), _FakeCalibrant())
    monkeypatch.setattr(calibrate, "peak_local_max", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad")))

    assert optimizer._objective([5, 0.1, 1.0]) == 1e6


def test_peak_optimizer_objective_requires_minimum_peak_count(monkeypatch) -> None:
    optimizer = calibrate.PeakOptimizer(np.ones((10, 10), dtype=float), _FakeGeometry(), _FakeCalibrant())
    monkeypatch.setattr(calibrate, "peak_local_max", lambda *args, **kwargs: np.array([[1, 1], [2, 2], [3, 3], [4, 4]]))

    assert optimizer._objective([5, 0.1, 1.0]) == 1e5


def test_peak_optimizer_objective_requires_minimum_masked_points(monkeypatch) -> None:
    optimizer = calibrate.PeakOptimizer(np.ones((20, 20), dtype=float), _FakeGeometry(), _FakeCalibrant())

    peaks = np.array([[i, i] for i in range(5, 15)])
    monkeypatch.setattr(calibrate, "peak_local_max", lambda *args, **kwargs: peaks)
    monkeypatch.setattr(_FakeGeometry, "tth", lambda self, y, x: np.full(shape=(len(y),), fill_value=np.deg2rad(90)))

    assert optimizer._objective([5, 0.1, 0.1]) == 1e4


def test_peak_optimizer_objective_updates_best_geometry_on_success(monkeypatch) -> None:
    optimizer = calibrate.PeakOptimizer(np.ones((20, 20), dtype=float), _FakeGeometry(), _FakeCalibrant())
    peaks = np.array([[i, i] for i in range(10)])
    monkeypatch.setattr(calibrate, "peak_local_max", lambda *args, **kwargs: peaks)
    monkeypatch.setattr(_FakeGeometry, "tth", lambda self, y, x: np.linspace(0.001, 0.007, len(y)))

    class _Refiner:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def refine2(self):
            return 0.123

        def get_geometry(self):
            return {"refined": True}

    monkeypatch.setattr(calibrate, "GeometryRefinement", _Refiner)

    result = optimizer._objective([5, 0.1, 1.0])

    assert result == pytest.approx(0.123)
    assert optimizer.best_error == pytest.approx(0.123)
    assert optimizer.best_geometry == {"refined": True}


def test_peak_optimizer_optimize_returns_best_geometry(monkeypatch) -> None:
    optimizer = calibrate.PeakOptimizer(np.ones((10, 10), dtype=float), _FakeGeometry(), _FakeCalibrant())

    def fake_minimize(func, x0, method, tol, options):
        _ = (x0, method, tol, options)
        func([5, 0.1, 1.0])
        optimizer.best_geometry = {"optimized": True}
        return None

    monkeypatch.setattr(calibrate, "minimize", fake_minimize)
    monkeypatch.setattr(calibrate, "peak_local_max", lambda *args, **kwargs: np.array([[i, i] for i in range(10)]))
    monkeypatch.setattr(_FakeGeometry, "tth", lambda self, y, x: np.linspace(0.001, 0.007, len(y)))

    class _Refiner:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def refine2(self):
            return 0.5

        def get_geometry(self):
            return {"refined": True}

    monkeypatch.setattr(calibrate, "GeometryRefinement", _Refiner)

    assert optimizer.optimize() == {"optimized": True}
