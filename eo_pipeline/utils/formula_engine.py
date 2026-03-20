"""
utils/formula_engine.py
------------------------
Shared utilities for formula parsing, DAG construction, and safe expression
evaluation.  All nodes import from here — no duplication.

Key responsibilities:
- Extract [tag] references from formula strings
- Build NetworkX DAG of tag dependencies
- Topological sort for safe evaluation order
- Detect and return circular reference groups
- Safe df.eval() wrapper with bracket-to-plain-name translation
- Fixed-point solver for circular dependency blocks
"""

import logging
import re
from typing import Dict, List, Optional, Set, Tuple

import networkx as nx
import numpy as np
import pandas as pd
from scipy.optimize import fsolve

logger = logging.getLogger(__name__)

# Regex to extract [tag_name] references from formula strings
TAG_REF_PATTERN = re.compile(r"\[([^\[\]]+)\]")


# ---------------------------------------------------------------------------
# Expression parsing helpers
# ---------------------------------------------------------------------------

def extract_tag_refs(formula: str) -> List[str]:
    """Return all [tag] names referenced in a formula expression."""
    if not isinstance(formula, str):
        return []
    return TAG_REF_PATTERN.findall(formula)


def formula_to_eval_expr(formula: str, available_tags: Set[str]) -> str:
    """
    Convert feature-file formula syntax to pandas-eval compatible syntax.

    Transformations applied:
      [tag_name]  →  tag_name          (bracket removal)
      if(cond, a, b)  →  where(cond, a, b)   (ternary)
      sum([x])    →  x.sum()
      ceil(x)     →  np.ceil(x)
      round(x,n)  →  round(x, n)
    """
    if not isinstance(formula, str):
        return str(formula)

    expr = formula

    # Replace [tag] refs with plain tag names
    expr = re.sub(r"\[([^\[\]]+)\]", r"\1", expr)

    # Replace if(cond, a, b) with Python-friendly where — approximated
    # pandas eval doesn't support ternary natively so we handle via Python eval
    return expr


def safe_eval_scalar(expr: str, context: dict) -> float:
    """
    Evaluate a scalar expression using a context dict.
    Used for bound expressions and constraint evaluations.
    Falls back to NaN on error.
    """
    clean = formula_to_eval_expr(expr, set(context.keys()))
    try:
        # Build a local namespace with common math helpers
        ns = {
            **context,
            "np": np,
            "ceil": np.ceil,
            "floor": np.floor,
            "round": round,
            "abs": abs,
            "max": max,
            "min": min,
            "sum": sum,
            "sqrt": np.sqrt,
            "log": np.log,
            "exp": np.exp,
            "if_": lambda c, a, b: a if c else b,  # ternary helper
        }
        # Replace if( with if_( for eval
        clean = re.sub(r"\bif\(", "if_(", clean)
        result = eval(clean, {"__builtins__": {}}, ns)  # noqa: S307
        return float(result)
    except Exception as exc:
        logger.debug("scalar_eval failed for '%s': %s", expr, exc)
        return np.nan


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------

def build_dependency_dag(
    formulas: Dict[str, str],
    known_inputs: Optional[Set[str]] = None,
) -> nx.DiGraph:
    """
    Build a directed acyclic graph of tag dependencies from a formula dict.

    Args:
        formulas: {tag_name: formula_expression}
        known_inputs: Set of tags that are data inputs (no formula needed).
                      Edges to unknown tags outside formulas are added as
                      virtual source nodes.

    Returns:
        nx.DiGraph where edge A→B means "A must be computed before B"
    """
    dag = nx.DiGraph()
    known_inputs = known_inputs or set()

    # Add all formula tags as nodes first
    for tag in formulas:
        dag.add_node(tag)

    for tag, formula in formulas.items():
        refs = extract_tag_refs(formula)
        for ref in refs:
            dag.add_node(ref)
            dag.add_edge(ref, tag)  # ref must be ready before tag

    return dag


def topological_sort_tags(dag: nx.DiGraph) -> Tuple[List[str], List[Set[str]]]:
    """
    Compute evaluation order using topological sort.

    Returns:
        (sorted_tags, circular_groups)
        sorted_tags     — tags in safe evaluation order (circular groups excluded)
        circular_groups — list of strongly-connected component sets with >1 member
    """
    # Find strongly connected components (cycles)
    sccs = list(nx.strongly_connected_components(dag))
    circular_groups = [s for s in sccs if len(s) > 1]
    circular_tags = {t for group in circular_groups for t in group}

    if circular_groups:
        logger.warning(
            "Circular dependencies detected in %d group(s): %s",
            len(circular_groups),
            circular_groups,
        )

    # Build condensed DAG excluding circular nodes for clean topo sort
    clean_dag = dag.copy()
    clean_dag.remove_nodes_from(circular_tags)

    try:
        sorted_tags = list(nx.topological_sort(clean_dag))
    except nx.NetworkXUnfeasible:
        logger.error("DAG has cycles after removing known circular groups — check formulas")
        sorted_tags = list(clean_dag.nodes())

    return sorted_tags, circular_groups


def validate_dag(dag: nx.DiGraph) -> bool:
    """Return True if the graph is a valid DAG with no cycles."""
    return nx.is_directed_acyclic_graph(dag)


# ---------------------------------------------------------------------------
# Formula evaluation on a DataFrame row (scalar context)
# ---------------------------------------------------------------------------

def evaluate_formula_on_row(
    formula: str,
    row: pd.Series,
    extra_context: Optional[dict] = None,
) -> float:
    """
    Evaluate a formula expression against a single DataFrame row.
    [tag] refs are resolved from the row's index.
    """
    context = row.to_dict()
    if extra_context:
        context.update(extra_context)
    return safe_eval_scalar(formula, context)


def evaluate_all_formulas(
    df: pd.DataFrame,
    formulas: Dict[str, str],
    sorted_tags: List[str],
) -> pd.DataFrame:
    """
    Evaluate all inferred tag formulas row-by-row in topological order.

    Args:
        df: Input DataFrame (one row = one timestamp / case snapshot)
        formulas: {tag_name: formula_expression}
        sorted_tags: Tags in evaluation order (from topological_sort_tags)

    Returns:
        DataFrame with computed inferred tag columns appended / updated
    """
    result = df.copy()

    for tag in sorted_tags:
        formula = formulas.get(tag)
        if not formula:
            continue  # input tag, no formula needed

        computed = []
        for _, row in result.iterrows():
            val = evaluate_formula_on_row(formula, row)
            computed.append(val)

        result[tag] = computed
        logger.debug("  Evaluated inferred tag: %s", tag)

    return result


# ---------------------------------------------------------------------------
# Circular reference solver
# ---------------------------------------------------------------------------

def solve_circular_block(
    df: pd.DataFrame,
    circular_tags: Set[str],
    formulas: Dict[str, str],
    max_iter: int = 200,
    tol: float = 1e-6,
) -> pd.DataFrame:
    """
    Solve a block of mutually-dependent inferred tags using fixed-point iteration
    (scipy.optimize.fsolve).

    For each row, treats circular tag values as unknowns x and finds x such that
    f(x) = evaluate_formula(x) - x ≈ 0.

    Args:
        df: DataFrame with current tag values (all non-circular tags already computed)
        circular_tags: Set of tag names forming a cycle
        formulas: Full formula dict
        max_iter: Max solver iterations

    Returns:
        DataFrame with circular tag columns updated to solved values
    """
    result = df.copy()
    tag_list = sorted(circular_tags)

    for idx, row in result.iterrows():
        context = row.to_dict()
        initial_guess = [float(context.get(t, 0.0)) for t in tag_list]

        def residuals(x):
            local_ctx = {**context, **dict(zip(tag_list, x))}
            res = []
            for t in tag_list:
                formula = formulas.get(t, str(x[tag_list.index(t)]))
                computed = safe_eval_scalar(formula, local_ctx)
                res.append(computed - x[tag_list.index(t)])
            return res

        try:
            solution, _, ier, msg = fsolve(
                residuals, initial_guess, full_output=True, maxfev=max_iter
            )
            if ier == 1:
                for t, val in zip(tag_list, solution):
                    result.at[idx, t] = val
            else:
                logger.warning("fsolve did not converge for row %s: %s", idx, msg)
        except Exception as exc:
            logger.warning("fsolve failed for row %s: %s", idx, exc)

    return result
