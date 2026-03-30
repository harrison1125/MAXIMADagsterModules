from __future__ import annotations

import importlib
from typing import List


__all__: List[str] = [
	"AbsorptionCorrection",
	"AzimuthalIntegrator",
	"calibrate",
	"concentrations",
	"ConverterXRFtoMCA",
	"LatticeParameters",
	"Inputs",
	"MCAtoFit",
]


def __getattr__(name: str):
	if name in __all__:
		return importlib.import_module(f"{__name__}.{name}")
	raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
