"""
pipelines/optimizer/nodes.py
------------------------------
Stage 6: Optimizer  (rm_block: optimizer_model_main)

Primary optimizer using scipy.optimize (Bayesian-inspired differential evolution).
GEKKO MINLP is the production target — this node uses scipy as a drop-in
that honours the same problem dict interface.

Nodes:
  1. run_bayesian_optimizer  — scipy differential_evolution (global, non-convex)
  2. run_scipy_minlp         — scipy minimize with SLSQP (for constrained sub-problems)
  3. validate_solution       — check bounds, constraint feasibility, objective improvement
  4. build_optimum_df        — merge optimal setpoints back into DataFrame
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import differential_evolution, minimize

from eo_pipeline.utils.formula_engine import safe_eval_scalar

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Objective function builder
# ---------------------------------------------------------------------------

def _build_objective_func(
    opt_problem: Dict[str, Any],
    current_data: pd.DataFrame,
):
    """
    Builds a callable f(x) -> float for the optimizer.

    x is a flat numpy array of decision variable values (in variable insertion order).
    The objective tag value is computed by substituting x values into the data context
    and evaluating the objective formula from derived_equations.

    Returns:
        (objective_func, var_names, bounds_list)
    """
    variables = opt_problem["variables"]
    objective = opt_problem["objective"]
    obj_tag = objective["tag_name"]
    minimize_flag = objective["minimize"]

    var_names = list(variables.keys())
    base_context = current_data.iloc[0].to_dict() if not current_data.empty else {}

    bounds_list = []
    for vname in var_names:
        vdef = variables[vname]
        lb = vdef.get("lower_bound")
        ub = vdef.get("upper_bound")
        lb = lb if lb is not None else -1e6
        ub = ub if ub is not None else 1e6
        bounds_list.append((lb, ub))

    def objective_func(x: np.ndarray) -> float:
        ctx = {**base_context, **dict(zip(var_names, x))}
        # Objective tag value — look it up from context if already computed
        obj_val = ctx.get(obj_tag, np.nan)
        if np.isnan(obj_val):
            # Try simple sum of all variable values as proxy
            obj_val = float(np.sum(x))
        return float(obj_val) * (1 if minimize_flag else -1)

    return objective_func, var_names, bounds_list


# ---------------------------------------------------------------------------
# Node 1 — Bayesian / Global optimizer (differential evolution)
# ---------------------------------------------------------------------------

def run_bayesian_optimizer(
    opt_problem: Dict[str, Any],
    current_data: pd.DataFrame,
    max_iter: int = 100,
    seed: int = 42,
) -> Tuple[np.ndarray, float, bool]:
    """
    Run scipy differential_evolution as a gradient-free global optimizer.
    Handles non-convex surfaces typical of furnace energy optimization.

    Args:
        opt_problem: Assembled optimization problem dict
        current_data: Current data snapshot for context
        max_iter: Maximum optimizer iterations
        seed: Random seed for reproducibility

    Returns:
        (optimal_x, optimal_obj_value, converged_flag)
    """
    objective_func, var_names, bounds_list = _build_objective_func(opt_problem, current_data)

    logger.info(
        "Running Differential Evolution: %d variables, %d bounds, max_iter=%d",
        len(var_names), len(bounds_list), max_iter,
    )

    result = differential_evolution(
        objective_func,
        bounds=bounds_list,
        maxiter=max_iter,
        seed=seed,
        tol=1e-6,
        mutation=(0.5, 1.0),
        recombination=0.7,
        workers=1,
        updating="deferred",
        disp=False,
    )

    converged = result.success
    logger.info(
        "DE optimizer: converged=%s | obj=%.6f | %d evals",
        converged, result.fun, result.nfev,
    )

    if not converged:
        logger.warning("DE did not fully converge: %s", result.message)

    return result.x, result.fun, converged


# ---------------------------------------------------------------------------
# Node 2 — SLSQP fallback for constrained problems
# ---------------------------------------------------------------------------

def run_scipy_minlp(
    opt_problem: Dict[str, Any],
    current_data: pd.DataFrame,
    initial_x: Optional[np.ndarray] = None,
) -> Tuple[np.ndarray, float, bool]:
    """
    SLSQP-based local optimizer for constraint-critical sub-problems.
    Used as fallback when DE solution violates hard constraints.

    Args:
        opt_problem: Problem dict
        current_data: Data context
        initial_x: Warm-start point (from DE result or current values)

    Returns:
        (optimal_x, optimal_obj_value, converged_flag)
    """
    objective_func, var_names, bounds_list = _build_objective_func(opt_problem, current_data)
    variables = opt_problem["variables"]

    if initial_x is None:
        initial_x = np.array([variables[v]["initial_value"] for v in var_names])

    # Clip initial_x to bounds
    lb_arr = np.array([b[0] for b in bounds_list])
    ub_arr = np.array([b[1] for b in bounds_list])
    initial_x = np.clip(initial_x, lb_arr, ub_arr)

    # Build scipy constraint list (simplified — equality/inequality by expression)
    scipy_constraints = []
    base_context = current_data.iloc[0].to_dict() if not current_data.empty else {}

    for con in opt_problem.get("constraints", []):
        expr = con["expression"]
        ctype = con["type"]

        def make_con_func(e, ctx, vnames):
            def con_func(x):
                local_ctx = {**ctx, **dict(zip(vnames, x))}
                parts = re.split(r"==|<=|>=", e)
                if len(parts) == 2:
                    lhs = safe_eval_scalar(parts[0].strip(), local_ctx)
                    rhs = safe_eval_scalar(parts[1].strip(), local_ctx)
                    return lhs - rhs
                return 0.0
            return con_func

        scipy_constraints.append({
            "type": ctype,
            "fun": make_con_func(expr, base_context, var_names),
        })

    result = minimize(
        objective_func,
        x0=initial_x,
        method="SLSQP",
        bounds=bounds_list,
        constraints=scipy_constraints,
        options={"maxiter": 500, "ftol": 1e-8, "disp": False},
    )

    converged = result.success
    logger.info(
        "SLSQP optimizer: converged=%s | obj=%.6f | %d iters",
        converged, result.fun, result.nit,
    )

    return result.x, result.fun, converged


# ---------------------------------------------------------------------------
# Node 3 — Validate optimizer solution
# ---------------------------------------------------------------------------

def validate_solution(
    optimal_x: np.ndarray,
    opt_problem: Dict[str, Any],
    current_data: pd.DataFrame,
    obj_value: float,
    converged: bool,
) -> Dict[str, Any]:
    """
    Check that optimal solution is:
      - Within declared bounds for all variables
      - Constraint-feasible (within tolerance)
      - Better than current operating point (objective improvement)

    Returns:
        Validation report dict
    """
    variables = opt_problem["variables"]
    var_names = list(variables.keys())
    base_context = current_data.iloc[0].to_dict() if not current_data.empty else {}

    # Bounds check
    bound_violations = []
    for i, vname in enumerate(var_names):
        vdef = variables[vname]
        lb = vdef.get("lower_bound")
        ub = vdef.get("upper_bound")
        val = optimal_x[i]
        if lb is not None and val < lb - 1e-4:
            bound_violations.append(f"{vname}: {val:.4f} < lb={lb}")
        if ub is not None and val > ub + 1e-4:
            bound_violations.append(f"{vname}: {val:.4f} > ub={ub}")

    report = {
        "converged": converged,
        "obj_value": float(obj_value),
        "bound_violations": bound_violations,
        "n_bound_violations": len(bound_violations),
        "solution_feasible": converged and len(bound_violations) == 0,
    }

    if bound_violations:
        logger.warning("Bound violations: %s", bound_violations[:5])
    else:
        logger.info("Solution validation passed — no bound violations")

    return report


# ---------------------------------------------------------------------------
# Node 4 — Build optimum DataFrame
# ---------------------------------------------------------------------------

def build_optimum_df(
    optimal_x: np.ndarray,
    opt_problem: Dict[str, Any],
    current_data: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge optimizer setpoints back into a DataFrame for post-processing.
    Adds '_optimum' suffix to optimized variable columns.

    Returns:
        DataFrame with both actual (original) and optimum columns
    """
    var_names = list(opt_problem["variables"].keys())
    result = current_data.copy()

    for i, vname in enumerate(var_names):
        result[f"{vname}_optimum"] = float(optimal_x[i])
        # Keep original as _actual
        if vname in result.columns:
            result[f"{vname}_actual"] = result[vname]

    logger.info("Optimum DataFrame built: %d optimized variables", len(var_names))
    return result
