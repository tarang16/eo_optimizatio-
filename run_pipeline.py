#!/usr/bin/env python
"""
run_pipeline.py
-----------------
Top-level entry point for the Energy Optimization pipeline.

Usage:
    python run_pipeline.py --feature-file sample_data/EO_UN_FF_Rev_14.xlsx --simulate
    python run_pipeline.py --feature-file /path/to/plant_feature_file.xlsx --model-id 1
"""

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Ensure eo_pipeline is importable from project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

from eo_pipeline.runner import run_eo_pipeline, setup_logging


def main():
    parser = argparse.ArgumentParser(
        description="Run the Energy Optimization pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_pipeline.py --feature-file sample_data/EO_UN_FF_Rev_14.xlsx --simulate
  python run_pipeline.py --feature-file /path/to/plant.xlsx --model-id 2 --optimizer-iter 100
        """,
    )
    parser.add_argument(
        "--feature-file",
        default="sample_data/EO_UN_FF_Rev_14.xlsx",
        help="Path to the plant's EO feature file (Excel workbook)",
    )
    parser.add_argument("--model-id", type=int, default=1, help="Model ID to filter config tables (default: 1)")
    parser.add_argument("--simulate", action="store_true", default=True,
                        help="Use simulated PI data (no PI historian required)")
    parser.add_argument("--optimizer-iter", type=int, default=50, help="Max optimizer iterations (default: 50)")
    parser.add_argument("--output-dir", default="output", help="Directory for output CSV files (default: ./output)")
    parser.add_argument("--log-level", default="INFO", help="Logging level: DEBUG|INFO|WARNING (default: INFO)")

    args = parser.parse_args()

    setup_logging(args.log_level)

    # Resolve feature file path
    feature_path = Path(args.feature_file)
    if not feature_path.is_absolute():
        feature_path = Path(__file__).resolve().parent / feature_path
    if not feature_path.exists():
        print(f"ERROR: Feature file not found: {feature_path}", file=sys.stderr)
        sys.exit(1)

    # Run the pipeline
    results = run_eo_pipeline(
        feature_file_path=str(feature_path),
        model_id=args.model_id,
        simulation_mode=args.simulate,
        max_optimizer_iter=args.optimizer_iter,
    )

    # Write outputs to CSV
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not results["seu_metrics"].empty:
        results["seu_metrics"].to_csv(output_dir / "seu_metrics.csv", index=False)

    if not results["model_alert_output"].empty:
        results["model_alert_output"].to_csv(output_dir / "ods_alerts.csv", index=False)

    import json
    with open(output_dir / "run_metadata.json", "w") as f:
        json.dump(results["run_metadata"], f, indent=2, default=str)

    # Print summary
    print("\n" + "=" * 70)
    print("  PIPELINE COMPLETE")
    print("=" * 70)
    for k, v in results["run_metadata"].items():
        print(f"  {k:<30} : {v}")

    print(f"\n  Outputs written to: {output_dir.resolve()}")

    if not results["seu_metrics"].empty:
        print(f"\n  SEU Metrics: {len(results['seu_metrics'])} equipment units")
        cols = ["seu_name", "seu_display_name", "energy_source", "actual_duty", "target_duty", "gain", "enpi"]
        available = [c for c in cols if c in results["seu_metrics"].columns]
        print(results["seu_metrics"][available].head(10).to_string(index=False))

    if not results["model_alert_output"].empty:
        print(f"\n  ODS Alerts: {len(results['model_alert_output'])} alerts")
    else:
        print("\n  ODS Alerts: No alerts triggered.")

    return results


if __name__ == "__main__":
    main()
