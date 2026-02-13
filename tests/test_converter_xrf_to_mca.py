from pathlib import Path

from MaximaDagster.modules import ConverterXRFtoMCA


def test_convert_xrf_to_mca(tmp_path: Path):
    xrf_content = "\n".join(
        [
            "xrf_data header",
            "0 10",
            "1 20",
            "2 30",
        ]
    )
    input_path = tmp_path / "sample.xrf"
    output_path = tmp_path / "sample.mca"
    input_path.write_text(xrf_content)

    out = ConverterXRFtoMCA.convert_xrf_to_mca(input_path=str(input_path), output_path=str(output_path))

    assert out == str(output_path)
    assert output_path.exists()
    text = output_path.read_text().splitlines()
    assert text[0] == "<<PMCA SPECTRUM>>"
    assert "<<DATA>>" in text
    assert text[-1] == "<<END>>"
    assert "10" in text
    assert "20" in text
    assert "30" in text


def test_convert_xrf_folder(tmp_path: Path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "a.xrf").write_text("0 1\n1 2\n")
    (input_dir / "b.xrf").write_text("0 3\n1 4\n")

    outputs = ConverterXRFtoMCA.convert_xrf_folder(str(input_dir))

    assert len(outputs) == 2
    for output in outputs:
        assert Path(output).exists()
