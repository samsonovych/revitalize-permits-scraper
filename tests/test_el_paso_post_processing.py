"""Test the El Paso post processing."""

import pytest
from permits_post_processing.processors.tx.el_paso.post_processor import ElPasoDefaultPostProcessor
import pandas as pd


@pytest.mark.parametrize(
    "df,output_path,expected_output",
    [
        (
            pd.read_csv("tests/src/test_el_paso_post_processing/case_1/inputs.csv"),
            None,
            pd.read_csv("tests/src/test_el_paso_post_processing/case_1/expected.csv", dtype=str)
        ),
    ]
)
def test_el_paso_post_processing(df: pd.DataFrame, output_path: str, expected_output: pd.DataFrame):
    """Test the El Paso post processing."""
    processor = ElPasoDefaultPostProcessor()
    output = processor.process(df, output_path)
    assert expected_output.equals(output.df)
