"""
pipelines/sub_model/nodes.py
------------------------------
Stage 4: Sub-Model Execution  (rm_block: pre_optimizer_iterative_inferred)

Executes sub_model table in 'order' sequence.
Supports three sub_model_type values:
  - 'equation'   : df.eval() expression
  - 'regression' : placeholder for ML model (scikit-learn / LightGBM)
  - 'iterative'  : fixed-point iteration for circular refs

Nodes:
  1. sort_sub_models          — order sub_model table by 'order' column
  2. execute_equation_model   — run equation-type sub-models
  3. execute_ml_model         — run regression sub-models (stubbed)
  4. run_sub_model_pipeline   — orchestrates all sub-model types in order
"""

import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from eo_pipeline.utils.formula_engine import evaluate_formula_on_row, safe_eval_scalar

logger = logging.getLogger(__name__)


def sort_sub_models(sub_model: pd.DataFrame) -> pd.DataFrame:
    """Sort sub_model table by execution order column."""
    order_col = "order " if "order " in sub_model.columns else "order"
    sub_model_sorted = sub_model.dropna(subset=["sub_model_name"]).copy()
    if order_col in sub_model_sorted.columns:
        sub_model_sorted = sub_model_sorted.sort_values(order_col).reset_index(drop=True)
    logger.info("Sub-models sorted: %d models in execution order", len(sub_model_sorted))
    return sub_model_sorted


def execute_equation_model(
    df: pd.DataFrame,
    sub_model_row: pd.Series,
) -> pd.DataFrame:
    """
    Execute a single equation-type sub-model.
    Sets df[tag_name] = eval(expression) for each row.
    """
    result = df.copy()
    tag_name = str(sub_model_row.get("sub_model_name", ""))
    expression = str(sub_model_row.get("sub_model_expression", ""))

    if not tag_name or not expression:
        return result

    computed = []
    for _, row in result.iterrows():
        val = evaluate_formula_on_row(expression, row)
        computed.append(val)

    result[tag_name] = computed
    logger.debug("  Sub-model equation: %s = %s", tag_name, expression[:60])
    return result


def execute_ml_model(
    df: pd.DataFrame,
    sub_model_row: pd.Series,
    model_registry: Dict[str, Any] = None,
) -> pd.DataFrame:
    """
    Execute an ML regression sub-model (scikit-learn / LightGBM).
    In simulation mode, falls back to equation evaluation if no model loaded.

    Args:
        df: Current pipeline DataFrame
        sub_model_row: Row from sub_model table
        model_registry: {sub_model_name: trained_model} dict (optional)

    Returns:
        DataFrame with sub-model output tag appended
    """
    result = df.copy()
    tag_name = str(sub_model_row.get("sub_model_name", ""))
    algorithm = str(sub_model_row.get("algorithm", "")).lower()
    expression = str(sub_model_row.get("sub_model_expression", ""))

    if model_registry and tag_name in model_registry:
        model = model_registry[tag_name]
        # Feature extraction would be sub_model_child-driven in production
        logger.info("ML sub-model '%s' (%s): running inference", tag_name, algorithm)
        # result[tag_name] = model.predict(feature_df)  # production path
        raise NotImplementedError(f"ML model '{tag_name}' loaded but inference not wired")
    else:
        # Fallback: evaluate expression if available
        logger.warning(
            "ML sub-model '%s' (%s): no model in registry — falling back to expression",
            tag_name, algorithm,
        )
        return execute_equation_model(result, sub_model_row)


def run_sub_model_pipeline(
    df: pd.DataFrame,
    sub_model: pd.DataFrame,
    model_registry: Dict[str, Any] = None,
) -> pd.DataFrame:
    """
    Execute all sub-models in 'order' sequence.
    Routes each sub-model to the correct execution function by sub_model_type.

    Args:
        df: Input DataFrame
        sub_model: Sub-model config table (sorted by order)
        model_registry: Optional pre-loaded ML models dict

    Returns:
        DataFrame with all sub-model outputs computed
    """
    result = df.copy()
    model_registry = model_registry or {}

    sorted_models = sort_sub_models(sub_model)
    logger.info("Running %d sub-models...", len(sorted_models))

    for _, row in sorted_models.iterrows():
        model_type = str(row.get("sub_model_type", "equation")).lower()
        model_name = str(row.get("sub_model_name", ""))

        try:
            if model_type == "equation":
                result = execute_equation_model(result, row)
            elif model_type in ("regression", "ml", "xgb", "lgbm"):
                result = execute_ml_model(result, row, model_registry)
            elif model_type == "iterative":
                # Fixed-point handled by inferred engine — sub_model just records linkage
                logger.debug("Iterative sub-model '%s' handled by inferred engine", model_name)
            else:
                logger.warning("Unknown sub_model_type '%s' for model '%s'", model_type, model_name)
        except Exception as exc:
            logger.error("Sub-model '%s' failed: %s — skipping", model_name, exc)

    logger.info("Sub-model pipeline complete")
    return result
