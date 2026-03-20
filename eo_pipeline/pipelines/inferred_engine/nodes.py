"""
pipelines/inferred_engine/nodes.py
------------------------------------
Stage 3: Inferred Tag Engine  (rm_block: data_enrichment_inferred_calculation)

Reads inferred_details and inferred_tag_rm_block_mapping to compute all
inferred tags in correct dependency order via DAG topological sort.

Nodes:
  1. build_inferred_formula_map   — extract tag→formula from inferred_details
  2. build_tag_dependency_dag     — NetworkX DAG
  3. compute_inferred_tags        — evaluate in topo order + solve circular refs
  4. validate_inferred_outputs    — assert no unexpected NaN in inferred tags
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple

import networkx as nx
import numpy as np
import pandas as pd

from eo_pipeline.utils.formula_engine import (
    build_dependency_dag,
    evaluate_all_formulas,
    extract_tag_refs,
    safe_eval_scalar,
    solve_circular_block,
    topological_sort_tags,
    validate_dag,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Node 1 — Build formula map from inferred_details
# ---------------------------------------------------------------------------

def build_inferred_formula_map(
    inferred_details: pd.DataFrame,
    rm_block_mapping: pd.DataFrame,
    block_column: str = "data_enrichment_inferred_calculation",
) -> Dict[str, str]:
    """
    Extract {tag_name: formula_expression} for the specified RM block.

    Args:
        inferred_details: Full inferred_details table
        rm_block_mapping: inferred_tag_rm_block_mapping table
        block_column: Which pipeline block column to filter on

    Returns:
        Dict mapping tag_name -> formula_expression
    """
    # Get tags assigned to this RM block
    if block_column in rm_block_mapping.columns:
        block_tags = set(
            rm_block_mapping[
                rm_block_mapping[block_column].notna() &
                (rm_block_mapping[block_column] != 0)
            ]["tag_name"].dropna()
        )
    else:
        # If no block mapping, include all inferred tags
        block_tags = set(inferred_details["tag_name"].dropna())

    formula_map: Dict[str, str] = {}
    for _, row in inferred_details.iterrows():
        tag = str(row.get("tag_name", ""))
        formula = row.get("formula_expression", "")
        if tag and isinstance(formula, str) and tag in block_tags:
            formula_map[tag] = formula

    logger.info("Inferred formula map: %d tags for block '%s'", len(formula_map), block_column)
    return formula_map


# ---------------------------------------------------------------------------
# Node 2 — Build DAG
# ---------------------------------------------------------------------------

def build_tag_dependency_dag(
    formula_map: Dict[str, str],
    known_pi_tags: Set[str],
) -> nx.DiGraph:
    """
    Build and validate dependency DAG for inferred tag computation.

    Args:
        formula_map: {tag_name: formula_expression}
        known_pi_tags: Set of PI tags (inputs — no formula needed)

    Returns:
        Validated NetworkX DiGraph
    """
    dag = build_dependency_dag(formula_map, known_inputs=known_pi_tags)
    is_dag = validate_dag(dag)
    logger.info(
        "DAG built: %d nodes, %d edges, is_DAG=%s",
        dag.number_of_nodes(), dag.number_of_edges(), is_dag,
    )
    if not is_dag:
        logger.warning("Cycles detected — will use topological_sort_tags to isolate them")
    return dag


# ---------------------------------------------------------------------------
# Node 3 — Compute inferred tags
# ---------------------------------------------------------------------------

def compute_inferred_tags(
    df: pd.DataFrame,
    formula_map: Dict[str, str],
    dag: nx.DiGraph,
) -> pd.DataFrame:
    """
    Evaluate all inferred tag formulas in topological order.
    Circular reference groups are resolved via fsolve fixed-point iteration.

    Args:
        df: Input DataFrame with PI tag values
        formula_map: {tag_name: formula_expression}
        dag: Dependency DAG

    Returns:
        DataFrame with inferred tag columns computed/appended
    """
    sorted_tags, circular_groups = topological_sort_tags(dag)

    result = df.copy()

    # Evaluate linear tags in topo order
    linear_formula_map = {t: f for t, f in formula_map.items()
                          if t in sorted_tags}
    result = evaluate_all_formulas(result, linear_formula_map, sorted_tags)

    # Solve circular blocks
    for group in circular_groups:
        circular_formulas = {t: formula_map[t] for t in group if t in formula_map}
        if circular_formulas:
            logger.info("Solving circular block: %s", group)
            result = solve_circular_block(result, group, formula_map)

    inferred_cols = [t for t in formula_map if t in result.columns]
    logger.info("Inferred tags computed: %d tags", len(inferred_cols))
    return result


# ---------------------------------------------------------------------------
# Node 4 — Validate inferred outputs
# ---------------------------------------------------------------------------

def validate_inferred_outputs(
    df: pd.DataFrame,
    formula_map: Dict[str, str],
    max_nan_pct: float = 20.0,
) -> pd.DataFrame:
    """
    Warn if too many inferred tag values are NaN after computation.
    Does NOT raise — some formulas may legitimately produce NaN for
    inactive equipment. Just logs and returns df unchanged.
    """
    nan_tags = []
    for tag in formula_map:
        if tag in df.columns and df[tag].isna().any():
            nan_tags.append(tag)

    nan_pct = len(nan_tags) / len(formula_map) * 100 if formula_map else 0.0
    logger.info("Inferred output NaN check: %d/%d tags have NaN (%.1f%%)",
                len(nan_tags), len(formula_map), nan_pct)

    if nan_pct > max_nan_pct:
        logger.warning(
            "High NaN rate in inferred tags (%.1f%% > %.1f%%). "
            "Check formulas for: %s",
            nan_pct, max_nan_pct, nan_tags[:10],
        )

    return df
