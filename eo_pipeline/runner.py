"""
runner.py
----------
Main pipeline runner for the Energy Optimization (EO) system.
Generic and plant-agnostic — all process logic lives in the feature file.

Orchestrates all 8 pipeline stages in sequence:
  Stage 1 → Ingestion
  Stage 2 → CCP Quality Check
  Stage 3 → Inferred Tag Engine
  Stage 4 → Sub-Model Execution
  Stage 5 → Optimizer Prep
  Stage 6 → Optimizer (Differential Evolution / SLSQP)
  Stage 7 → Post-Optimizer Calculations
  Stage 8 → ODS & SEU Reporting

Usage:
  python -m eo_pipeline.runner --feature-file sample_data/EO_UN_FF_Rev_14.xlsx
  python -m eo_pipeline.runner --feature-file ... --model-id 1 --simulate
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

# Config
from eo_pipeline.config.loader import EOConfig, load_config_from_excel

# Stage 1 — Ingestion
from eo_pipeline.pipelines.ingestion.nodes import (
    create_pi_connection,
    fetch_pi_data_simulated,
    standardize_columns,
    validate_data_coverage,
)

# Stage 2 — CCP Quality
from eo_pipeline.pipelines.ccp_quality.nodes import (
    apply_defaults,
    build_quality_report,
    check_tag_nan,
    check_tag_out_of_bound,
    check_tag_stuck,
)

# Stage 3 — Inferred Engine
from eo_pipeline.pipelines.inferred_engine.nodes import (
    build_inferred_formula_map,
    build_tag_dependency_dag,
    compute_inferred_tags,
    validate_inferred_outputs,
)

# Stage 4 — Sub-Model
from eo_pipeline.pipelines.sub_model.nodes import run_sub_model_pipeline

# Stage 5 — Optimizer Prep
from eo_pipeline.pipelines.optimizer_prep.nodes import (
    assemble_opt_problem,
    build_constraints,
    build_objective,
    build_variables,
)

# Stage 6 — Optimizer
from eo_pipeline.pipelines.optimizer.nodes import (
    build_optimum_df,
    run_bayesian_optimizer,
    run_scipy_minlp,
    validate_solution,
)

# Stage 7 — Post-Optimizer
from eo_pipeline.pipelines.post_optimizer.nodes import (
    compute_derived_pre_opt,
    compute_opportunity_tags,
    compute_optimum_inferred,
    merge_actual_optimum,
)

# Stage 8 — ODS Reporting
from eo_pipeline.pipelines.ods_reporting.nodes import (
    compute_seu_metrics,
    evaluate_cause_expressions,
    generate_ods_messages,
    write_output_tables,
)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def setup_logging(log_level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)-40s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


logger = logging.getLogger("eo_pipeline.runner")


# ---------------------------------------------------------------------------
# Pipeline stages — each wrapped for timing and error isolation
# ---------------------------------------------------------------------------

def run_stage(stage_name: str, func, *args, **kwargs):
    """Execute a pipeline stage with timing and error capture."""
    logger.info("")
    logger.info("=" * 70)
    logger.info("  STAGE: %s", stage_name)
    logger.info("=" * 70)
    t0 = time.time()
    try:
        result = func(*args, **kwargs)
        elapsed = time.time() - t0
        logger.info("  ✓ %s completed in %.2fs", stage_name, elapsed)
        return result, None
    except Exception as exc:
        elapsed = time.time() - t0
        logger.error("  ✗ %s FAILED after %.2fs: %s", stage_name, elapsed, exc)
        return None, exc


# ---------------------------------------------------------------------------
# Main pipeline runner
# ---------------------------------------------------------------------------

def run_eo_pipeline(
    feature_file_path: str,
    model_id: int = 1,
    simulation_mode: bool = True,
    max_optimizer_iter: int = 50,
    reference_timestamp: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Execute the full 8-stage EO optimization pipeline.

    Args:
        feature_file_path: Path to EO_UN_FF_Rev_XX.xlsx
        model_id: Model identifier (default 1)
        simulation_mode: If True, generate synthetic PI data
        max_optimizer_iter: Max iterations for optimizer
        reference_timestamp: Override run timestamp

    Returns:
        Dict with keys: model_output, model_alert_output, seu_metrics,
                        quality_report, validation_report, run_metadata
    """
    pipeline_start = time.time()
    run_ts = reference_timestamp or datetime.utcnow()

    logger.info("")
    logger.info("█" * 70)
    logger.info("  ENERGY OPTIMIZATION PIPELINE")
    logger.info("  Feature file : %s", Path(feature_file_path).name)
    logger.info("  Model ID     : %d", model_id)
    logger.info("  Run timestamp: %s", run_ts.isoformat())
    logger.info("  Simulation   : %s", simulation_mode)
    logger.info("█" * 70)

    # -------------------------------------------------------------------------
    # PRE-STAGE: Load configuration from feature file
    # -------------------------------------------------------------------------
    logger.info("")
    logger.info("Loading EO configuration from feature file...")
    cfg: EOConfig = load_config_from_excel(feature_file_path, model_id)

    pi_config = {
        "pi_server": cfg.pipeline_macros.get("pi_rootpath", "localhost"),
        "simulation_mode": str(simulation_mode),
    }

    # -------------------------------------------------------------------------
    # STAGE 1: Data Ingestion
    # -------------------------------------------------------------------------
    (pi_conn, err) = run_stage("Stage 1: PI Connection", create_pi_connection, pi_config)
    if err:
        raise RuntimeError(f"Cannot proceed without PI connection: {err}")

    (raw_df, err) = run_stage(
        "Stage 1: Fetch PI Data (Simulated)",
        fetch_pi_data_simulated,
        pi_conn, cfg.tag, cfg.pipeline_macros, run_ts,
    )
    if err or raw_df is None:
        raise RuntimeError("Data fetch failed")

    (df, err) = run_stage("Stage 1: Validate Coverage", validate_data_coverage,
                           raw_df, cfg.tag, 30.0)  # 30% threshold for sim
    df = df if df is not None else raw_df

    # -------------------------------------------------------------------------
    # STAGE 2: CCP Quality Check
    # -------------------------------------------------------------------------
    all_events = []
    if not cfg.case_configuration_portal.empty:
        (df_nan, events_nan), _ = run_stage(
            "Stage 2: NaN Check", check_tag_nan, df, cfg.case_configuration_portal
        ), None
        if df_nan is not None:
            df, all_events = df_nan[0], df_nan[1]
            events_nan = df_nan[1]

            (df_stuck, events_stuck), _ = run_stage(
                "Stage 2: Stuck Check", check_tag_stuck, df, cfg.case_configuration_portal
            ), None
            if df_stuck is not None:
                df, all_events = df_stuck[0], all_events + df_stuck[1]

            (df_oob, events_oob), _ = run_stage(
                "Stage 2: Out-of-Bound Check", check_tag_out_of_bound,
                df, cfg.case_configuration_portal
            ), None
            if df_oob is not None:
                df, all_events = df_oob[0], all_events + df_oob[1]

            df, _ = run_stage("Stage 2: Apply Defaults", apply_defaults,
                               df, cfg.case_configuration_portal, all_events)
            df = df if df is not None else df_nan[0]

    quality_report = build_quality_report(all_events)
    logger.info("Quality report: %d QC events", len(quality_report))

    # -------------------------------------------------------------------------
    # STAGE 3: Inferred Tag Engine
    # -------------------------------------------------------------------------
    known_pi_tags = set(cfg.tag[cfg.tag["tag_type"] == "pi"]["tag_name"])

    (formula_map, _) = run_stage(
        "Stage 3: Build Formula Map", build_inferred_formula_map,
        cfg.inferred_details, cfg.inferred_tag_rm_block_mapping,
        "data_enrichment_inferred_calculation",
    )
    formula_map = formula_map or {}

    (dag, _) = run_stage("Stage 3: Build DAG", build_tag_dependency_dag,
                          formula_map, known_pi_tags)

    if dag is not None:
        (df, _) = run_stage("Stage 3: Compute Inferred Tags",
                              compute_inferred_tags, df, formula_map, dag)

        run_stage("Stage 3: Validate Inferred", validate_inferred_outputs, df, formula_map)

    # -------------------------------------------------------------------------
    # STAGE 4: Sub-Model Execution
    # -------------------------------------------------------------------------
    if not cfg.sub_model.empty:
        (df, _) = run_stage("Stage 4: Sub-Model Pipeline",
                              run_sub_model_pipeline, df, cfg.sub_model)

    # -------------------------------------------------------------------------
    # STAGE 5: Optimizer Prep
    # -------------------------------------------------------------------------
    (variables, _) = run_stage("Stage 5: Build Variables",
                                 build_variables, cfg.variables, df)
    variables = variables or {}

    (constraints, _) = run_stage("Stage 5: Build Constraints",
                                   build_constraints, cfg.constraints, df)
    constraints = constraints or []

    (objective, _) = run_stage("Stage 5: Build Objective",
                                 build_objective, cfg.objective)
    objective = objective or {"tag_name": "Objective_2", "direction": "minimize", "minimize": True}

    (opt_problem, _) = run_stage("Stage 5: Assemble Problem",
                                   assemble_opt_problem, variables, constraints, objective)

    # -------------------------------------------------------------------------
    # STAGE 6: Optimizer
    # -------------------------------------------------------------------------
    converged = False
    optimal_x = None

    if opt_problem and opt_problem["n_vars"] > 0:
        (de_result, err_de) = run_stage(
            "Stage 6: Differential Evolution",
            run_bayesian_optimizer, opt_problem, df, max_optimizer_iter,
        )

        if de_result is not None:
            optimal_x, obj_val, converged = de_result

            # Fallback to SLSQP if DE didn't converge
            if not converged:
                logger.info("DE not converged — falling back to SLSQP")
                (slsqp_result, _) = run_stage(
                    "Stage 6: SLSQP Fallback",
                    run_scipy_minlp, opt_problem, df, optimal_x,
                )
                if slsqp_result is not None:
                    optimal_x, obj_val, converged = slsqp_result

            (validation_report, _) = run_stage(
                "Stage 6: Validate Solution",
                validate_solution, optimal_x, opt_problem, df, obj_val, converged,
            )
            validation_report = validation_report or {}

            (df_with_optimum, _) = run_stage(
                "Stage 6: Build Optimum DataFrame",
                build_optimum_df, optimal_x, opt_problem, df,
            )
            df_with_optimum = df_with_optimum if df_with_optimum is not None else df
        else:
            df_with_optimum = df
            validation_report = {"converged": False, "error": str(err_de)}
    else:
        logger.warning("No decision variables found — skipping optimizer")
        df_with_optimum = df
        validation_report = {"converged": False, "reason": "no_variables"}

    # -------------------------------------------------------------------------
    # STAGE 7: Post-Optimizer Calculations
    # -------------------------------------------------------------------------
    (df_post, _) = run_stage(
        "Stage 7: Derived Pre-Opt Equations",
        compute_derived_pre_opt, df_with_optimum, cfg.derived_equations,
    )
    df_post = df_post if df_post is not None else df_with_optimum

    (df_merged, _) = run_stage(
        "Stage 7: Merge Actual+Optimum",
        merge_actual_optimum, df, df_post, variables,
    )
    df_merged = df_merged if df_merged is not None else df_post

    (df_final, _) = run_stage(
        "Stage 7: Compute Opportunity Tags",
        compute_opportunity_tags, df_merged,
    )
    df_final = df_final if df_final is not None else df_merged

    # -------------------------------------------------------------------------
    # STAGE 8: ODS & SEU Reporting
    # -------------------------------------------------------------------------
    (seu_metrics, _) = run_stage(
        "Stage 8: SEU Metrics", compute_seu_metrics, df_final, cfg.seu_detail,
    )
    seu_metrics = seu_metrics if seu_metrics is not None else pd.DataFrame()

    (cause_evaluated, _) = run_stage(
        "Stage 8: Evaluate Causes", evaluate_cause_expressions,
        df_final, cfg.cause,
    )
    cause_evaluated = cause_evaluated if cause_evaluated is not None else cfg.cause

    (ods_alerts, _) = run_stage(
        "Stage 8: Generate ODS Messages",
        generate_ods_messages, df_final, cfg.ods, cause_evaluated, cfg.effect,
    )
    ods_alerts = ods_alerts if ods_alerts is not None else pd.DataFrame()

    (output_tables, _) = run_stage(
        "Stage 8: Write Output Tables",
        write_output_tables, df_final, seu_metrics, ods_alerts, run_ts,
    )
    output_tables = output_tables or {}

    # -------------------------------------------------------------------------
    # Pipeline Summary
    # -------------------------------------------------------------------------
    total_time = time.time() - pipeline_start

    logger.info("")
    logger.info("█" * 70)
    logger.info("  PIPELINE COMPLETE in %.2fs", total_time)
    logger.info("  Tags processed    : %d", len(df.columns) - 1)
    logger.info("  Inferred computed : %d", len(formula_map))
    logger.info("  Variables         : %d", len(variables))
    logger.info("  Constraints       : %d", len(constraints))
    logger.info("  Optimizer converged: %s", converged)
    logger.info("  SEU units         : %d", len(seu_metrics))
    logger.info("  ODS alerts        : %d", len(ods_alerts))
    logger.info("█" * 70)

    return {
        "model_output": output_tables.get("model_output", df_final),
        "model_alert_output": output_tables.get("model_alert_output", ods_alerts),
        "seu_metrics": output_tables.get("seu_metrics", seu_metrics),
        "quality_report": quality_report,
        "validation_report": validation_report,
        "run_metadata": {
            "feature_file": str(feature_file_path),
            "model_id": model_id,
            "run_timestamp": run_ts.isoformat(),
            "pipeline_duration_s": round(total_time, 2),
            "n_tags": len(df.columns) - 1,
            "n_inferred": len(formula_map),
            "n_variables": len(variables),
            "n_constraints": len(constraints),
            "optimizer_converged": converged,
        },
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Run EO optimization pipeline")
    parser.add_argument(
        "--feature-file",
        default="sample_data/EO_UN_FF_Rev_14.xlsx",
        help="Path to EO feature file (Excel workbook)",
    )
    parser.add_argument("--model-id", type=int, default=1)
    parser.add_argument("--simulate", action="store_true", default=True,
                        help="Use simulated PI data (no PI historian required)")
    parser.add_argument("--optimizer-iter", type=int, default=50,
                        help="Max optimizer iterations")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    setup_logging(args.log_level)

    results = run_eo_pipeline(
        feature_file_path=args.feature_file,
        model_id=args.model_id,
        simulation_mode=args.simulate,
        max_optimizer_iter=args.optimizer_iter,
    )

    # Print summary tables
    print("\n" + "=" * 70)
    print("SEU METRICS (Top 10):")
    print("=" * 70)
    if not results["seu_metrics"].empty:
        cols = ["seu_name", "seu_display_name", "energy_source",
                "actual_duty", "target_duty", "gain", "enpi"]
        available = [c for c in cols if c in results["seu_metrics"].columns]
        print(results["seu_metrics"][available].head(10).to_string(index=False))

    print("\n" + "=" * 70)
    print("ODS ALERTS:")
    print("=" * 70)
    if not results["model_alert_output"].empty:
        alert_cols = ["effect_name", "cause_name", "message", "monitoring_tag",
                      "actual_value", "optimum_value"]
        available = [c for c in alert_cols if c in results["model_alert_output"].columns]
        print(results["model_alert_output"][available].to_string(index=False))
    else:
        print("No ODS alerts triggered.")

    print("\n" + "=" * 70)
    print("RUN METADATA:")
    print("=" * 70)
    for k, v in results["run_metadata"].items():
        print(f"  {k:<30} : {v}")

    return results


if __name__ == "__main__":
    main()
