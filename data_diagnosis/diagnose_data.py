import argparse
import importlib
import os
import sys
from typing import Dict, Iterable, Tuple

import pandas as pd
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from datetime import datetime


EXPECTED_VALUES: Dict[str, Iterable[str]] = {
    "trim_level": {"Base", "Sport", "Premium", "Limited"},
    "transmission": {"automatic", "manual", "CVT", "dual clutch"},
    "fuel_type": {"gasoline", "diesel", "hybrid", "electric"},
}


def _module_name(module_path: str) -> str:
    """Convert a filesystem path like 'data_generation.py' to an importable module string."""
    module_name = module_path
    if module_path.endswith(".py"):
        module_name = module_path[:-3]
    return module_name.replace(os.sep, ".")


def _resolve_candidate_path(source: str) -> Path | None:
    """Return the first existing path matching source in likely locations."""
    script_dir = Path(__file__).resolve().parent
    candidates = []
    input_path = Path(source)
    if input_path.is_absolute():
        candidates.append(input_path)
    else:
        candidates.extend(
            [
                input_path,
                script_dir / input_path,
                script_dir.parent / input_path,
            ]
        )

    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def _load_dataframe(source: str) -> pd.DataFrame:
    """Load a DataFrame either from a generator module or a CSV file."""
    resolved_path = _resolve_candidate_path(source)

    if resolved_path is not None:
        if resolved_path.suffix.lower() == ".csv":
            return pd.read_csv(resolved_path)

        if resolved_path.suffix.lower() == ".py":
            spec = spec_from_file_location("generator_module", resolved_path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Unable to load module from {resolved_path}")
            module = module_from_spec(spec)
            sys.modules["generator_module"] = module
            spec.loader.exec_module(module)  # type: ignore[arg-type]
        else:
            raise ValueError(
                f"Unsupported file type '{resolved_path.suffix}' for data source"
            )
    else:
        module_name = _module_name(source)
        module = importlib.import_module(module_name)

    if not hasattr(module, "generate_raw_car_equipment_data"):
        raise AttributeError(
            "Provided module does not expose generate_raw_car_equipment_data()"
        )

    generator_fn = module.generate_raw_car_equipment_data
    return generator_fn()


def diagnose_generated_data(source: str = "data_generation.py", report_path: str | None = None) -> None:
    """Generate a diagnosis report for the raw car equipment data."""
    df = _load_dataframe(source)
    report_lines = [
        "Data diagnosis report",
        f"- Rows: {len(df)}",
        f"- Columns: {len(df.columns)}",
    ]

    duplicate_count = int(df.duplicated().sum())
    report_lines.append(f"- Duplicate rows detected: {duplicate_count}")

    null_counts = df.isna().sum()
    null_columns = [
        f"{col} ({count})" for col, count in null_counts.items() if int(count) > 0
    ]
    if null_columns:
        report_lines.append("- Columns containing null values: " + ", ".join(null_columns))
    else:
        report_lines.append("- Columns containing null values: none")

    for column in df.columns:
        distinct_types = {type(val).__name__ for val in df[column].dropna()}
        if len(distinct_types) > 1:
            report_lines.append(
                f"- Mixed data types in '{column}': {', '.join(sorted(distinct_types))}"
            )

    string_issues: Dict[str, Tuple[int, str]] = {}
    for column in df.select_dtypes(include="object").columns:
        series = df[column].dropna()
        string_series = series[series.apply(lambda value: isinstance(value, str))]
        trimmed_mismatch = string_series[string_series != string_series.str.strip()]
        if not trimmed_mismatch.empty:
            string_issues[column] = (
                len(trimmed_mismatch),
                "leading/trailing spaces",
            )

    for column, (count, description) in string_issues.items():
        report_lines.append(f"- {count} values in '{column}' have {description}")

    for column, expected in EXPECTED_VALUES.items():
        if column not in df.columns:
            continue
        unexpected_values = set()
        for value in df[column].dropna():
            if not isinstance(value, str):
                unexpected_values.add(type(value).__name__)
                continue
            if value not in expected:
                unexpected_values.add(value)
        if unexpected_values:
            report_lines.append(
                f"- Unexpected values in '{column}': {', '.join(sorted(unexpected_values))}"
            )

    if "msrp" in df.columns:
        numeric_series = pd.to_numeric(df["msrp"], errors="coerce")
        non_numeric_count = int(numeric_series.isna().sum())
        if non_numeric_count:
            report_lines.append(
                f"- 'msrp' contains {non_numeric_count} non-numeric values"
            )

    if "model_year" in df.columns:
        model_year_numeric = pd.to_numeric(df["model_year"], errors="coerce")
        if int(model_year_numeric.isna().sum()):
            report_lines.append("- 'model_year' includes non-numeric entries")

    report_text = "\n".join(report_lines)
    print(report_text)

    if report_path is None:
        default_dir = Path("generated_data")
        default_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_file = default_dir / f"diagnosis_report_{timestamp}.txt"
    else:
        report_file = Path(report_path)
        report_file.parent.mkdir(parents=True, exist_ok=True)

    report_file.write_text(report_text + "\n", encoding="utf-8")
    print(f"Report saved to {report_file.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Diagnose quality issues in the generated car equipment data. "
            "Accepts either a generator module (.py / import path) or a CSV file."
        )
    )
    parser.add_argument(
        "source",
        nargs="?",
        default="data_generation.py",
        help="Path to generator module (default: data_generation.py) or CSV file",
    )
    parser.add_argument(
        "--report",
        dest="report_path",
        help="Optional path for the diagnosis report file (defaults to generated_data/diagnosis_report_<timestamp>.txt)",
    )
    args = parser.parse_args()
    diagnose_generated_data(args.source, args.report_path)
