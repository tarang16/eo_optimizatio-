"""
pipelines/ods_reporting/nodes.py
----------------------------------
Stage 8: ODS & SEU Reporting  (rm_block: optimizer_model_inferred_calculation)

Generates:
  1. SEU metrics  — per-equipment actual/target/baseline duty, gain, EnPI
  2. ODS messages — operator alerts via cause-effect mapping
  3. Output tables — model_output and model_alert_output DataFrames

Nodes:
  1. compute_seu_metrics          — evaluate gain/EnPI per equipment unit
  2. evaluate_cause_expressions   — assess which causes are triggered
  3. generate_ods_messages        — produce operator message DataFrame
  4. write_output_tables          — collate final model_output / model_alert_output
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from eo_pipeline.utils.formula_engine import safe_eval_scalar

logger = logging.getLogger(__name__)


def compute_seu_metrics(
    df_merged: pd.DataFrame,
    seu_detail: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute SEU (Specific Energy Unit) metrics for each equipment unit.

    For each SEU row, evaluates:
      - baseline_duty  : baseline_duty_expression
      - actual_duty    : actual_duty_expression
      - target_duty    : target_duty_expression
      - gain           : gain_expression (actual - target savings)
      - enpi           : enpi_expression (energy performance indicator)
      - benefit        : gain * benefit_factor

    Args:
        df_merged: Merged actual+optimum DataFrame
        seu_detail: seu_detail config table

    Returns:
        DataFrame with one row per SEU and computed metrics
    """
    context = df_merged.iloc[0].to_dict() if not df_merged.empty else {}
    results: List[Dict[str, Any]] = []

    for _, row in seu_detail.iterrows():
        seu_name = str(row.get("seu_name", ""))
        seu_id = row.get("seu_id", "")
        display_name = str(row.get("seu_display_name", seu_name))
        energy_source = str(row.get("energy_source", ""))
        seu_category = str(row.get("seu_category", ""))
        benefit_factor = row.get("benefit_factor", 1.0)

        def eval_expr(col_name):
            expr = row.get(col_name, "")
            if not isinstance(expr, str) or not expr.strip():
                return np.nan
            return safe_eval_scalar(expr, context)

        baseline_duty = eval_expr("baseline_duty_expression")
        actual_duty   = eval_expr("actual_duty_expression")
        target_duty   = eval_expr("target_duty_expression")
        gain          = eval_expr("gain_expression")
        enpi          = eval_expr("enpi_expression")

        # Benefit = gain * benefit_factor (benefit_factor may itself be an expression)
        if isinstance(benefit_factor, str):
            benefit_factor_val = safe_eval_scalar(benefit_factor, context)
        else:
            benefit_factor_val = float(benefit_factor) if not pd.isna(benefit_factor) else 1.0

        benefit = (gain * benefit_factor_val) if not (np.isnan(gain) or np.isnan(benefit_factor_val)) else np.nan

        results.append({
            "seu_name": seu_name,
            "seu_id": seu_id,
            "seu_display_name": display_name,
            "seu_category": seu_category,
            "energy_source": energy_source,
            "baseline_duty": round(baseline_duty, 4) if not np.isnan(baseline_duty) else None,
            "actual_duty": round(actual_duty, 4) if not np.isnan(actual_duty) else None,
            "target_duty": round(target_duty, 4) if not np.isnan(target_duty) else None,
            "gain": round(gain, 4) if not np.isnan(gain) else None,
            "enpi": round(enpi, 4) if not np.isnan(enpi) else None,
            "benefit": round(benefit, 4) if not np.isnan(benefit) else None,
        })

    seu_df = pd.DataFrame(results)
    valid_gains = seu_df["gain"].dropna()
    total_gain = valid_gains.sum()
    logger.info(
        "SEU metrics computed: %d equipment units | total gain=%.4f",
        len(seu_df), total_gain,
    )
    return seu_df


def evaluate_cause_expressions(
    df_merged: pd.DataFrame,
    cause_config: pd.DataFrame,
) -> pd.DataFrame:
    """
    Evaluate each cause expression to determine if the cause is triggered.

    A cause is triggered when its expression evaluates to a truthy value (1).

    Args:
        df_merged: Merged actual+optimum DataFrame
        cause_config: cause table with cause_name, cause_expression, casue_message

    Returns:
        cause_config DataFrame with 'triggered' and 'value' columns added
    """
    context = df_merged.iloc[0].to_dict() if not df_merged.empty else {}
    result = cause_config.copy()
    triggered_vals = []
    raw_vals = []

    for _, row in cause_config.iterrows():
        expr = str(row.get("cause_expression", ""))
        if not expr.strip():
            triggered_vals.append(False)
            raw_vals.append(np.nan)
            continue

        val = safe_eval_scalar(expr, context)
        triggered_vals.append(bool(val) if not np.isnan(val) else False)
        raw_vals.append(val)

    result["triggered"] = triggered_vals
    result["value"] = raw_vals

    n_triggered = sum(triggered_vals)
    logger.info("Cause evaluation: %d/%d causes triggered", n_triggered, len(cause_config))
    return result


def generate_ods_messages(
    df_merged: pd.DataFrame,
    ods_config: pd.DataFrame,
    cause_evaluated: pd.DataFrame,
    effect_config: pd.DataFrame,
) -> pd.DataFrame:
    """
    Generate ODS operator alert messages by joining:
      effect -> ods -> cause (where triggered=True)

    Each message includes:
      - effect_name, effect_description
      - cause_name, cause_description, cause_message
      - monitoring_tag_name and its actual/optimum values

    Returns:
        DataFrame of alert messages (model_alert_output schema)
    """
    triggered_causes = set(
        cause_evaluated[cause_evaluated["triggered"] == True]["cause_name"].tolist()
    )

    if not triggered_causes:
        logger.info("No ODS causes triggered — no alerts generated")
        return pd.DataFrame(columns=[
            "effect_name", "cause_name", "cause_description",
            "message", "monitoring_tag", "actual_value", "optimum_value",
        ])

    # Join ods -> effect for descriptions
    effect_map = {row["effect_name"]: row for _, row in effect_config.iterrows()}

    # Join ods -> cause for messages
    cause_map = {row["cause_name"]: row for _, row in cause_evaluated.iterrows()}

    messages: List[Dict] = []
    context = df_merged.iloc[0].to_dict() if not df_merged.empty else {}

    for _, ods_row in ods_config.iterrows():
        effect_name = str(ods_row.get("effect_name", ""))
        cause_name  = str(ods_row.get("cause_name", ""))

        if cause_name not in triggered_causes:
            continue

        cause_row  = cause_map.get(cause_name, {})
        effect_row = effect_map.get(effect_name, {})

        monitoring_tag = str(cause_row.get("monitoring_tag_name", ""))
        actual_val  = context.get(f"{monitoring_tag}_actual", context.get(monitoring_tag, np.nan))
        optimum_val = context.get(f"{monitoring_tag}_optimum", np.nan)

        messages.append({
            "effect_name": effect_name,
            "effect_description": str(effect_row.get("effect_description", "")),
            "cause_name": cause_name,
            "cause_description": str(cause_row.get("cause_description", "")),
            "message": str(cause_row.get("cause_message", cause_row.get("casue_message", ""))),
            "monitoring_tag": monitoring_tag,
            "actual_value": round(float(actual_val), 4) if not pd.isna(actual_val) else None,
            "optimum_value": round(float(optimum_val), 4) if not pd.isna(optimum_val) else None,
        })

    alerts_df = pd.DataFrame(messages)
    logger.info("ODS alerts generated: %d messages", len(alerts_df))
    return alerts_df


def write_output_tables(
    df_merged: pd.DataFrame,
    seu_metrics: pd.DataFrame,
    ods_alerts: pd.DataFrame,
    run_timestamp: Optional[datetime] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Collate all pipeline outputs into final output table DataFrames.
    In production, these would be written to PostgreSQL.

    Returns:
        {
          "model_output":       wide DataFrame with actual/optimum/opportunity values,
          "model_alert_output": ODS alert messages,
          "seu_metrics":        SEU gain/EnPI per equipment
        }
    """
    if run_timestamp is None:
        run_timestamp = datetime.utcnow()

    model_output = df_merged.copy()
    model_output["run_timestamp"] = run_timestamp

    seu_out = seu_metrics.copy()
    seu_out["run_timestamp"] = run_timestamp

    ods_out = ods_alerts.copy()
    ods_out["run_timestamp"] = run_timestamp

    logger.info(
        "Output tables ready — model_output: %d cols | seu_metrics: %d rows | alerts: %d rows",
        len(model_output.columns), len(seu_out), len(ods_out),
    )

    return {
        "model_output": model_output,
        "model_alert_output": ods_out,
        "seu_metrics": seu_out,
    }
