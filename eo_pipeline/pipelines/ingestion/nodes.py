"""
pipelines/ingestion/nodes.py
-----------------------------
Stage 1: Data Ingestion  (rm_block: data_ingestion_get_pi_data)

Nodes:
  1. create_pi_connection   — establish PI historian connection
  2. fetch_pi_data          — pull tag data for given time window
  3. standardize_columns    — rename pi_name -> tag_name
  4. validate_data_coverage — assert minimum tag coverage before proceeding
"""

import logging
import random
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def create_pi_connection(pi_config: Dict[str, str]) -> Dict[str, Any]:
    """Establish connection to PI historian (or return simulation handle)."""
    server = pi_config.get("pi_server", "localhost")
    simulation = pi_config.get("simulation_mode", "true").lower() == "true"

    if simulation:
        logger.info("SIMULATION MODE — PI connection skipped (server=%s)", server)
        return {"server": server, "connected": True, "simulation": True}

    raise NotImplementedError("Live PI connection requires osisoft.pi SDK")


def fetch_pi_data_simulated(
    pi_connection: Dict[str, Any],
    tag_config: pd.DataFrame,
    pipeline_macros: Dict[str, str],
    reference_timestamp: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Generate synthetic PI data for all PI tags.
    One snapshot row with realistic value ranges per tag type.
    """
    if reference_timestamp is None:
        reference_timestamp = datetime.utcnow().replace(microsecond=0)

    logger.info("Generating simulated PI data for %d tags at %s",
                len(tag_config), reference_timestamp.isoformat())

    rng = np.random.default_rng(seed=42)  # fixed seed = reproducible run
    row: Dict[str, Any] = {"timestamp": reference_timestamp}

    for _, tag_row in tag_config.iterrows():
        tag_name = str(tag_row.get("tag_name", ""))
        tag_type = str(tag_row.get("tag_type", "pi")).lower()
        description = str(tag_row.get("description", "")).lower()

        if tag_type in ("inferred", "constant"):
            row[tag_name] = 0.0
            continue

        name_lower = (tag_name + " " + description).lower()

        if any(k in name_lower for k in ("status", "running", "flag", "switch")):
            val = float(rng.choice([0, 1], p=[0.3, 0.7]))
        elif any(k in name_lower for k in ("temperature", "temp", "_ti_", "ti_")):
            val = float(rng.uniform(100, 500))
        elif any(k in name_lower for k in ("pressure", "press", "_pi_")):
            val = float(rng.uniform(1, 80))
        elif any(k in name_lower for k in ("flow", "rate", "_fi_", "fi_")):
            val = float(rng.uniform(5, 400))
        elif any(k in name_lower for k in ("level", "_li_", "li_")):
            val = float(rng.uniform(20, 80))
        elif any(k in name_lower for k in ("efficiency", "selectivity", "perc", "ratio")):
            val = float(rng.uniform(0.5, 0.99))
        elif any(k in name_lower for k in ("cost", "bill", "price")):
            val = float(rng.uniform(50, 500))
        elif any(k in name_lower for k in ("load", "demand", "consumption")):
            val = float(rng.uniform(50, 300))
        else:
            val = float(rng.uniform(0, 100))

        row[tag_name] = round(val, 4)

    df = pd.DataFrame([row])
    logger.info("Simulated: %d tag columns", len(df.columns) - 1)
    return df


def standardize_columns(raw_df: pd.DataFrame, tag_config: pd.DataFrame) -> pd.DataFrame:
    """Rename pi_name columns to tag_name. Keep only known tags + timestamp."""
    df = raw_df.copy()
    rename_map = dict(zip(tag_config["pi_name"], tag_config["tag_name"]))
    df = df.rename(columns=rename_map)
    known_cols = {"timestamp"} | set(tag_config["tag_name"])
    df = df[[c for c in df.columns if c in known_cols]]
    logger.info("Standardized %d tag columns", len(df.columns) - 1)
    return df


def validate_data_coverage(
    df: pd.DataFrame,
    tag_config: pd.DataFrame,
    min_coverage_pct: float = 70.0,
) -> pd.DataFrame:
    """Assert minimum coverage of PI tags before pipeline proceeds."""
    pi_tags = set(tag_config[tag_config["tag_type"] == "pi"]["tag_name"])
    non_null = {t for t in pi_tags if t in df.columns and df[t].notna().any()}
    coverage = (len(non_null) / len(pi_tags) * 100) if pi_tags else 100.0

    logger.info("Data coverage: %d/%d PI tags (%.1f%%)",
                len(non_null), len(pi_tags), coverage)

    if coverage < min_coverage_pct:
        raise ValueError(
            f"Insufficient coverage: {coverage:.1f}% < {min_coverage_pct}% required. "
            f"Missing: {pi_tags - non_null}"
        )
    return df
