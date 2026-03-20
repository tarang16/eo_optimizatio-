# Energy Optimization Pipeline (EO)

A generic, configuration-driven energy optimization pipeline for industrial plants.  
Any plant can integrate by providing its own **feature file** (Excel workbook) — zero code changes needed.

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run with the sample feature file (simulation mode)
python run_pipeline.py --feature-file sample_data/EO_UN_FF_Rev_14.xlsx --simulate

# 3. Run with a custom plant's feature file
python run_pipeline.py --feature-file /path/to/YOUR_PLANT_feature_file.xlsx --simulate
```

## Pipeline Stages

| Stage | Name | Purpose |
|-------|------|---------|
| 1 | **Data Ingestion** | Fetch PI historian data (or simulate) |
| 2 | **CCP Quality Check** | NaN / stuck / out-of-bound imputation |
| 3 | **Inferred Tag Engine** | DAG-sorted formula evaluation |
| 4 | **Sub-Model Execution** | Equation / ML sub-models |
| 5 | **Optimizer Prep** | Variable bounds, constraints, objective |
| 6 | **Optimizer** | Differential Evolution + SLSQP fallback |
| 7 | **Post-Optimizer** | Actual vs optimum delta (opportunity) |
| 8 | **ODS & SEU Reporting** | Operator alerts + energy metrics |

## Onboarding a New Plant

1. **Create a feature file** — An Excel workbook with sheets: `tag`, `inferred_details`, `variables`, `constraints`, `objective`, `seu_detail`, `cause`, `effect`, `ods`, etc.  
   Use `sample_data/EO_UN_FF_Rev_14.xlsx` as a reference template.

2. **Run the pipeline** — Point the `--feature-file` argument at your new file.

3. **Outputs** — The pipeline produces:
   - `model_output` — Wide DataFrame with actual/optimum/opportunity values
   - `model_alert_output` — Operator-facing ODS alert messages
   - `seu_metrics` — Per-equipment energy performance (gain, EnPI)

## Project Structure

```
optimizer_eo/
├── eo_pipeline/               ← Core Python package
│   ├── runner.py              ← 8-stage pipeline orchestrator
│   ├── config/loader.py       ← Feature file → EOConfig loader
│   ├── utils/formula_engine.py ← DAG, topological sort, safe eval
│   └── pipelines/             ← One module per stage
├── sample_data/               ← Reference feature file
├── archive/                   ← Legacy docs, notebooks, data
├── run_pipeline.py            ← CLI entry point
├── requirements.txt           ← Python dependencies
└── pyproject.toml             ← Package metadata
```

## CLI Options

```
python run_pipeline.py [OPTIONS]

--feature-file PATH   Path to the plant's feature file (Excel)
--model-id INT        Model ID to filter config tables (default: 1)
--simulate            Use simulated PI data (no historian needed)
--optimizer-iter INT  Max optimizer iterations (default: 50)
--output-dir PATH     Directory for output CSVs (default: ./output)
--log-level LEVEL     Logging level: DEBUG|INFO|WARNING (default: INFO)
```

## Dependencies

- Python ≥ 3.9
- pandas, numpy, scipy, networkx, openpyxl

## Configuration-First Principle

> **Zero hardcoded values.** All tags, formulas, bounds, constraints, objectives, quality rules, and reporting logic live in the feature file. The Python code is a generic execution engine.
