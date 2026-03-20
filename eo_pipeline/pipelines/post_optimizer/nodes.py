"""
pipelines/post_optimizer/nodes.py
------------------------------------
Stage 7: Post-Optimizer Calculations  (rm_block: post_optimizer_inferred_calculation)

After the optimizer returns setpoints, this stage:
  1. Computes derived_equations (pre-opt tags that depend on optimizer output)
  2. Re-runs the inferred pipeline on the OPTIMUM data snapshot
  3. Merges actual vs optimum columns into a unified DataFrame
  4. Computes opportunity tags (actual - optimum delta = savings potential)

Nodes:
  1. compute_derived_pre_opt   — evaluate derived_equations table formulas
  2. compute_optimum_inferred  — run inferred pipeline on optimum data
  3. merge_actual_optimum      — join actual and optimum into one wide DataFrame
  4. compute_opportunity_tags  — calculate delta (savings opportunity) columns
"""

import logging
from typing import Dict, List, Set

import numpy as np
import pandas as pd

from eo_pipeline.utils.formula_engine import (
    build_dependency_dag,
    evaluate_all_formulas,
    extract_tag_refs,
    topological_sort_tags,
)

logger = logging.getLogger(__name__)


def compute_derived_pre_opt(
    df: pd.DataFrame,
    derived_equations: pd.DataFrame,
) -> pd.DataFrame:
    """
    Evaluate derived_equations (pre-optimizer derived tags).
    These are computed AFTER optimizer setpoints are applied to context.

    Args:
        df: DataFrame with actual + optimum tag values
        derived_equations: derived_equations table from EOConfig

    Returns:
        DataFrame with derived tag columns appended
    """
    result = df.copy()

    formula_map: Dict[str, str] = {}
    for _, row in derived_equations.dropna(subset=["tag_name", "formula_expression"]).iterrows():
        tag = str(row["tag_name"])
        formula = str(row["formula_expression"])
        if tag and formula:
            formula_map[tag] = formula

    if not formula_map:
        return result

    dag = build_dependency_dag(formula_map, known_inputs=set(result.columns))
    sorted_tags, circular = topological_sort_tags(dag)

    result = evaluate_all_formulas(result, formula_map, sorted_tags)
    logger.info("Derived pre-opt equations computed: %d tags", len(formula_map))
    return result


def compute_optimum_inferred(
    df_actual: pd.DataFrame,
    df_optimum_vars: pd.DataFrame,
    inferred_details: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build an optimum snapshot by replacing actual variable values with
    optimizer setpoints, then re-running the inferred tag pipeline.

    Args:
        df_actual: Actual data snapshot
        df_optimum_vars: Columns from build_optimum_df (includes *_optimum columns)
        inferred_details: Inferred tag formulas

    Returns:
        DataFrame with optimum inferred tags (suffix _optimum)
    """
    result = df_actual.copy()

    # Extract optimum setpoints and build optimum context
    optimum_context = {}
    for col in df_optimum_vars.columns:
        if col.endswith("_optimum"):
            base_tag = col[:-len("_optimum")]
            optimum_context[base_tag] = df_optimum_vars[col].iloc[0]

    # Create optimum data snapshot
    optimum_snapshot = df_actual.copy()
    for tag, val in optimum_context.items():
        optimum_snapshot[tag] = val

    # Build formula map for inferred tags
    formula_map: Dict[str, str] = {}
    for _, row in inferred_details.dropna(subset=["tag_name", "formula_expression"]).iterrows():
        tag = str(row["tag_name"])
        formula = str(row["formula_expression"])
        if tag and formula:
            formula_map[tag] = formula

    dag = build_dependency_dag(formula_map, known_inputs=set(optimum_snapshot.columns))
    sorted_tags, _ = topological_sort_tags(dag)
    optimum_computed = evaluate_all_formulas(optimum_snapshot, formula_map, sorted_tags)

    # Add _optimum suffix to all inferred tags in result
    for tag in formula_map:
        if tag in optimum_computed.columns:
            result[f"{tag}_optimum"] = optimum_computed[tag]

    logger.info("Optimum inferred tags computed: %d tags", len(formula_map))
    return result


def merge_actual_optimum(
    df_actual: pd.DataFrame,
    df_with_optimum: pd.DataFrame,
    variables: Dict,
) -> pd.DataFrame:
    """
    Merge actual and optimum values into a unified wide DataFrame.
    All actual columns get _actual suffix; optimum columns retain _optimum suffix.

    Args:
        df_actual: Actual data snapshot
        df_with_optimum: DataFrame containing *_optimum columns
        variables: Decision variable dict (keys = variable tag names)

    Returns:
        Wide DataFrame with both actual and optimum for each key tag
    """
    result = df_actual.copy()

    # Rename non-timestamp actual columns to *_actual
    actual_cols = [c for c in result.columns if c != "timestamp"]
    rename_actual = {c: f"{c}_actual" for c in actual_cols}
    result = result.rename(columns=rename_actual)

    # Merge in optimum columns from df_with_optimum
    optimum_cols = [c for c in df_with_optimum.columns if c.endswith("_optimum")]
    for col in optimum_cols:
        result[col] = df_with_optimum[col].values

    logger.info("Merged actual+optimum: %d _actual | %d _optimum columns",
                len(rename_actual), len(optimum_cols))
    return result


def compute_opportunity_tags(
    df_merged: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute opportunity (savings) tags as delta between actual and optimum.
    Opportunity = actual - optimum (positive = savings available).

    Operates on all tags that have BOTH a _actual and _optimum column.

    Returns:
        DataFrame with _opportunity columns appended for each paired tag
    """
    result = df_merged.copy()

    actual_tags = {c[:-7] for c in result.columns if c.endswith("_actual")}
    optimum_tags = {c[:-8] for c in result.columns if c.endswith("_optimum")}
    paired_tags = actual_tags & optimum_tags

    for tag in paired_tags:
        actual_col = f"{tag}_actual"
        optimum_col = f"{tag}_optimum"
        try:
            result[f"{tag}_opportunity"] = result[actual_col] - result[optimum_col]
        except Exception:
            pass

    logger.info("Opportunity tags computed: %d delta columns", len(paired_tags))
    return result
