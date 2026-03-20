"""
pipelines/optimizer_prep/nodes.py
------------------------------------
Stage 5: Constraint & Variable Prep  (rm_block: optimizer_model_preprocessing)

Assembles the optimization problem structure from config tables:
  - variables  -> decision variables with bounds
  - constraints -> equality/inequality constraint expressions
  - objective  -> minimize/maximize target

Nodes:
  1. build_variables     — evaluate lower/upper bounds per switch logic
  2. build_constraints   — parse and assemble constraint definitions
  3. build_objective     — extract objective tag and direction
  4. assemble_opt_problem — combine into a single problem dict
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from eo_pipeline.utils.formula_engine import safe_eval_scalar

logger = logging.getLogger(__name__)

# Bound/initial value switch codes from variables table
SWITCH_USE_VALUE      = 3   # use explicit lower_bound_value / upper_bound_value
SWITCH_USE_EXPRESSION = 5   # evaluate lower_bound_expression / upper_bound_expression
SWITCH_USE_CURRENT    = 6   # use current tag value from df as initial


def evaluate_bound(switch: int, value: float, expression: str, context: dict) -> Optional[float]:
    """Evaluate one bound (lower or upper) per switch code."""
    try:
        switch = int(switch) if not pd.isna(switch) else SWITCH_USE_VALUE
    except (ValueError, TypeError):
        switch = SWITCH_USE_VALUE

    if switch == SWITCH_USE_VALUE:
        return float(value) if not pd.isna(value) else None
    elif switch == SWITCH_USE_EXPRESSION and isinstance(expression, str) and expression.strip():
        result = safe_eval_scalar(expression, context)
        return result if not np.isnan(result) else None
    elif switch == SWITCH_USE_CURRENT:
        tag_val = context.get(expression.strip() if isinstance(expression, str) else "", np.nan)
        return float(tag_val) if not pd.isna(tag_val) else None
    return None


def build_variables(
    variables_config: pd.DataFrame,
    current_data: pd.DataFrame,
) -> Dict[str, Dict[str, Any]]:
    """
    Evaluate decision variable definitions from variables table.

    For each variable, resolve:
      - lower_bound   (via lower_bound_switch + lower_bound_value/expression)
      - upper_bound   (via upper_bound_switch + upper_bound_value/expression)
      - initial_value (via initial_value_switch: use current tag value)
      - flag_integer  (binary/integer variable flag)

    Args:
        variables_config: variables table from EOConfig
        current_data: Latest data snapshot (one row DataFrame)

    Returns:
        Dict[tag_name -> {lower_bound, upper_bound, initial_value, is_integer}]
    """
    context = current_data.iloc[0].to_dict() if not current_data.empty else {}
    variables: Dict[str, Dict[str, Any]] = {}

    clean_vars = variables_config.dropna(subset=["tag_name"]).copy()
    clean_vars = clean_vars[~clean_vars["tag_name"].astype(str).str.contains("lower_bound", na=False)]

    for _, row in clean_vars.iterrows():
        tag = str(row.get("tag_name", ""))
        if not tag:
            continue

        lb = evaluate_bound(
            row.get("lower_bound_switch"),
            row.get("lower_bound_value"),
            row.get("lower_bound_expression", ""),
            context,
        )
        ub = evaluate_bound(
            row.get("upper_bound_switch"),
            row.get("upper_bound_value"),
            row.get("upper_bound_expression", ""),
            context,
        )

        init_switch = row.get("initial_value_switch")
        try:
            init_switch = int(init_switch) if not pd.isna(init_switch) else SWITCH_USE_CURRENT
        except (ValueError, TypeError):
            init_switch = SWITCH_USE_CURRENT

        if init_switch == SWITCH_USE_CURRENT:
            init_val = context.get(tag, (lb or 0.0))
        else:
            init_val = lb or 0.0

        is_integer = bool(row.get("flag_integer", 0)) if not pd.isna(row.get("flag_integer", 0)) else False

        variables[tag] = {
            "lower_bound": lb,
            "upper_bound": ub,
            "initial_value": float(init_val) if init_val is not None else 0.0,
            "is_integer": is_integer,
        }

    logger.info("Variables assembled: %d decision variables", len(variables))
    return variables


def build_constraints(
    constraints_config: pd.DataFrame,
    current_data: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Parse constraint table into a list of constraint definition dicts.

    Each constraint dict:
      {type: 'eq'|'ineq', expression: str, system: str, raw: str}

    Constraint type inferred from expression:
      - Contains '==' -> equality ('eq')
      - Contains '<=' or '>=' -> inequality ('ineq')

    Args:
        constraints_config: constraints table from EOConfig
        current_data: Current snapshot for context evaluation

    Returns:
        List of constraint dicts
    """
    constraints: List[Dict[str, Any]] = []

    for _, row in constraints_config.dropna(subset=["expression"]).iterrows():
        expr = str(row.get("expression", "")).strip()
        system = str(row.get("system", ""))

        if "==" in expr:
            ctype = "eq"
        elif "<=" in expr or ">=" in expr:
            ctype = "ineq"
        else:
            ctype = "ineq"

        # Strip outer parentheses
        clean_expr = re.sub(r"^\(|\)$", "", expr.strip())
        # Replace [tag] with tag name
        clean_expr = re.sub(r"\[([^\[\]]+)\]", r"\1", clean_expr)

        constraints.append({
            "type": ctype,
            "expression": clean_expr,
            "system": system,
            "raw": expr,
        })

    logger.info("Constraints assembled: %d constraints", len(constraints))
    return constraints


def build_objective(objective_config: pd.DataFrame) -> Dict[str, Any]:
    """
    Extract objective tag and optimization direction from objective table.

    direction: -1 = minimize (default), +1 = maximize

    Returns:
        {tag_name: str, direction: str, minimize: bool}
    """
    if objective_config.empty:
        return {"tag_name": "Objective_2", "direction": "minimize", "minimize": True}

    row = objective_config.iloc[0]
    tag_name = str(row.get("tag_name", "Objective_2"))
    direction_code = int(row.get("direction", -1))
    minimize = (direction_code == -1)

    obj = {
        "tag_name": tag_name,
        "direction": "minimize" if minimize else "maximize",
        "minimize": minimize,
    }
    logger.info("Objective: %s (%s)", tag_name, obj["direction"])
    return obj


def assemble_opt_problem(
    variables: Dict[str, Any],
    constraints: List[Dict],
    objective: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Combine variables, constraints, and objective into a single
    optimizer-ready problem dict.
    """
    problem = {
        "variables": variables,
        "constraints": constraints,
        "objective": objective,
        "n_vars": len(variables),
        "n_constraints": len(constraints),
    }
    logger.info(
        "Optimization problem assembled: %d vars | %d constraints | objective=%s (%s)",
        problem["n_vars"], problem["n_constraints"],
        objective["tag_name"], objective["direction"],
    )
    return problem
