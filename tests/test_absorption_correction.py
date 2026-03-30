from MaximaDagster.modules import AbsorptionCorrection as ac


def test_absorption_value_structured():
    result = ac.absorption_value("Cu", 1.5, 1.2)
    assert result["alloy"] == "Cu"
    assert result["thickness_mm"] == 1.5
    assert result["wavelength_A"] == 1.2
