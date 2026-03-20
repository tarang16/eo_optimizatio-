"""
pipelines/ccp_quality/nodes.py
--------------------------------
Stage 2: CCP Quality Check  (rm_block: data_enrichment_pi_tag_imputation)

Applies case_configuration_portal rules per tag BEFORE optimizer sees the data.
Switch codes (from switch_configuration_info):
  NaN switch    : 14 = use_default_warning, 7 = skip, etc.
  Stuck switch  : 7 = use_default_warning
  OOB switch    : 4 = bound_warning
  Default switch: 16 = use_default_no_warning

Nodes:
  1. check_tag_nan          — detect and handle NaN values
  2. check_tag_stuck        — detect constant/stuck signals
  3. check_tag_out_of_bound — enforce lolo/hihi bounds
  4. apply_defaults         — write default_value for any flagged tag
  5. build_quality_report   — return summary DataFrame of interventions
"""

import logging
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# Switch codes from switch_configuration_info
SWITCH_USE_DEFAULT    = {14, 16}  # replace with default_value
SWITCH_SKIP           = {7}       # keep as-is, log warning only
SWITCH_BOUND_CLAMP    = {4}       # clamp to bounds
SWITCH_DEFAULT_WARN   = {14}      # replace + log warning


def check_tag_nan(
    df: pd.DataFrame,
    ccp: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[dict]]:
    """
    Detect NaN values for tags configured in case_configuration_portal.
    Returns updated df and a list of QC event dicts.
    """
    result = df.copy()
    events: List[dict] = []

    for _, cfg_row in ccp.iterrows():
        tag = str(cfg_row.get("tag_name", ""))
        if tag not in result.columns:
            continue

        nan_switch = int(cfg_row.get("tag_nan_switch", 7))
        default_val = cfg_row.get("default_value", np.nan)

        is_nan = result[tag].isna()
        if not is_nan.any():
            continue

        event = {"tag": tag, "check": "NaN", "switch": nan_switch,
                 "rows_affected": int(is_nan.sum())}

        if nan_switch in SWITCH_USE_DEFAULT and not pd.isna(default_val):
            result.loc[is_nan, tag] = float(default_val)
            event["action"] = f"replaced_with_default({default_val})"
            logger.warning("NaN detected in '%s' — replaced with default %.4f", tag, default_val)
        else:
            event["action"] = "skipped_logged"
            logger.warning("NaN detected in '%s' — switch=%d, no replacement", tag, nan_switch)

        events.append(event)

    return result, events


def check_tag_stuck(
    df: pd.DataFrame,
    ccp: pd.DataFrame,
    stuck_window: int = 3,
) -> Tuple[pd.DataFrame, List[dict]]:
    """
    Detect stuck (constant) signals across rows.
    A tag is considered stuck if its std deviation over stuck_window rows == 0.
    Applies default if stuck_switch indicates replacement.
    """
    result = df.copy()
    events: List[dict] = []

    if len(result) < 2:
        return result, events  # single-row snapshot — skip stuck check

    for _, cfg_row in ccp.iterrows():
        tag = str(cfg_row.get("tag_name", ""))
        if tag not in result.columns:
            continue

        stuck_switch = int(cfg_row.get("tag_stuck_switch", 7))
        default_val = cfg_row.get("default_value", np.nan)

        window = min(stuck_window, len(result))
        tail = result[tag].tail(window)
        is_stuck = (tail.nunique() == 1) and (tail.notna().all())

        if not is_stuck:
            continue

        event = {"tag": tag, "check": "stuck", "switch": stuck_switch, "rows_affected": window}

        if stuck_switch in SWITCH_USE_DEFAULT and not pd.isna(default_val):
            result.loc[result.index[-window:], tag] = float(default_val)
            event["action"] = f"replaced_with_default({default_val})"
            logger.warning("Stuck signal in '%s' — replaced with default %.4f", tag, default_val)
        else:
            event["action"] = "skipped_logged"
            logger.warning("Stuck signal in '%s' — switch=%d, no replacement", tag, stuck_switch)

        events.append(event)

    return result, events


def check_tag_out_of_bound(
    df: pd.DataFrame,
    ccp: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[dict]]:
    """
    Enforce lolo/hihi bounds per tag from case_configuration_portal.
    Out-of-bound values are replaced with the default_value.
    """
    result = df.copy()
    events: List[dict] = []

    for _, cfg_row in ccp.iterrows():
        tag = str(cfg_row.get("tag_name", ""))
        if tag not in result.columns:
            continue

        oob_switch = int(cfg_row.get("tag_out_of_bound_switch", 7))
        default_val = cfg_row.get("default_value", np.nan)
        lolo = cfg_row.get("lolo", np.nan)
        hihi = cfg_row.get("hihi", np.nan)

        if pd.isna(lolo) and pd.isna(hihi):
            continue

        series = result[tag]
        is_oob = pd.Series([False] * len(series), index=series.index)
        if not pd.isna(lolo):
            is_oob |= series < float(lolo)
        if not pd.isna(hihi):
            is_oob |= series > float(hihi)

        if not is_oob.any():
            continue

        event = {
            "tag": tag, "check": "OOB", "switch": oob_switch,
            "lolo": lolo, "hihi": hihi, "rows_affected": int(is_oob.sum()),
        }

        if oob_switch in SWITCH_USE_DEFAULT.union(SWITCH_BOUND_CLAMP) and not pd.isna(default_val):
            result.loc[is_oob, tag] = float(default_val)
            event["action"] = f"replaced_with_default({default_val})"
            logger.warning("OOB in '%s' (bounds=[%.2f,%.2f]) — replaced with default",
                           tag, lolo if not pd.isna(lolo) else -999,
                                hihi if not pd.isna(hihi) else 999)
        else:
            event["action"] = "skipped_logged"

        events.append(event)

    return result, events


def apply_defaults(
    df: pd.DataFrame,
    ccp: pd.DataFrame,
    events: List[dict],
) -> pd.DataFrame:
    """
    Final safety pass: apply default_value for any tag still NaN
    after nan/stuck/oob checks if default_switch indicates unconditional default.
    """
    result = df.copy()

    UNCONDITIONAL_DEFAULT_SWITCH = 16

    for _, cfg_row in ccp.iterrows():
        tag = str(cfg_row.get("tag_name", ""))
        if tag not in result.columns:
            continue

        default_switch = int(cfg_row.get("default_switch", 0))
        default_val = cfg_row.get("default_value", np.nan)

        if default_switch == UNCONDITIONAL_DEFAULT_SWITCH and not pd.isna(default_val):
            still_nan = result[tag].isna()
            if still_nan.any():
                result.loc[still_nan, tag] = float(default_val)
                logger.debug("Default fallback applied to '%s': %.4f", tag, default_val)

    return result


def build_quality_report(events: List[dict]) -> pd.DataFrame:
    """Collate all QC events into a summary DataFrame."""
    if not events:
        return pd.DataFrame(columns=["tag", "check", "switch", "rows_affected", "action"])
    return pd.DataFrame(events)
