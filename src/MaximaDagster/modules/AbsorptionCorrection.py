"""
Placeholder for absorption correction based on sample thickness.
"""

from __future__ import annotations

from typing import Any, Dict


def absorption_value(alloy: Any, thickness_mm: float, wavelength_A: float) -> Dict[str, Any]:
   """
   Return a structured placeholder until the physical model is implemented.
   """
   return {
      "alloy": alloy,
      "thickness_mm": thickness_mm,
      "wavelength_A": wavelength_A,
   }


__all__ = [
   "absorption_value",
]
