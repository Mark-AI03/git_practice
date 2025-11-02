# Car Equipment ETL Playground

This project provides a small synthetic data pipeline to practice ETL skills. It generates intentionally messy car equipment data, inspects quality issues, and exports a diagnosis report you can use to guide cleaning steps.

## Prerequisites
- Python 3.12 (the repo uses `uv` for dependency management)
- `uv` installed (<https://docs.astral.sh/uv/>)

Create the virtual environment configured in `pyproject.toml`:

```bash
uv venv
uv pip install -r requirements.txt  # if you add dependencies later
uv sync                               # installs the locked dependencies
```

Activate the environment (example for Linux/macOS shells):

```bash
source .venv/bin/activate
```

## Generate Synthetic Data

Run the generator script to produce a messy dataset with duplicates, nulls, inconsistent types, and typos:

```bash
uv run python data_generation.py
```

This prints a sample of the DataFrame and writes the full dataset to `generated_data/raw_car_equipment.csv`. Each run overwrites the CSV so you always have a fresh version to work with.

## Diagnose Data Quality Issues

Use the diagnosis script to analyze either the generator module or a saved CSV.

Diagnose the CSV created above:

```bash
uv run python data_diagnosis/diagnose_data.py generated_data/raw_car_equipment.csv
```

Diagnose by calling the generator in-memory (no CSV required):

```bash
uv run python data_diagnosis/diagnose_data.py
```

The script prints a summary of detected problems (duplicate counts, nulls, mixed types, unexpected categorical values, etc.) and writes a timestamped report to `generated_data/diagnosis_report_<timestamp>.txt`.

## Custom Report Location

Pass `--report` to control where the diagnosis report is saved:

```bash
uv run python data_diagnosis/diagnose_data.py generated_data/raw_car_equipment.csv --report reports/latest_diagnosis.txt
```

The script creates any missing directories in the given path.

## Next Steps

With the diagnosis report in hand, you can design transformations to clean the data and load it into your target system. Add your cleaning scripts, tests, and pipeline orchestration here as you build out the rest of the ETL workflow.
