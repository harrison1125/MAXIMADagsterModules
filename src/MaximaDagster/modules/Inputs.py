"""
Simple configuration helpers for XRD/XRF workflows.
"""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class InputsConfig:
    root_dir: str
    poni_file: str
    config_path: str


def load_inputs_from_env(
    root_env: str = "MAXIMA_ROOT_DIR",
    poni_env: str = "MAXIMA_PONI_FILE",
    config_env: str = "MAXIMA_PYMCA_CONFIG",
) -> InputsConfig:
    """
    Load InputsConfig from environment variables.
    """
    root_dir = os.environ.get(root_env, "")
    poni_file = os.environ.get(poni_env, "")
    config_path = os.environ.get(config_env, "")

    if not root_dir or not poni_file or not config_path:
        raise ValueError("Missing required environment variables for InputsConfig")

    return InputsConfig(root_dir=root_dir, poni_file=poni_file, config_path=config_path)


__all__ = [
    "InputsConfig",
    "load_inputs_from_env",
]
