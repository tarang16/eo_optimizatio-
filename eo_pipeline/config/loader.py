"""
config/loader.py
----------------
Loads all configuration tables from the EO feature file (Excel or DB).
Returns a typed config dict injected into every pipeline node.
Zero hardcoded values — all logic lives in the feature file.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

import pandas as pd

logger = logging.getLogger(__name__)

FEATURE_FILE_SHEETS = [
    "tag", "inferred_details", "model_tag", "inferred_tag_rm_block_mapping",
    "constraints", "variables", "derived_equations", "derived_equation_post_optimizer",
    "objective", "sub_model", "sub_model_child", "effect", "cause", "ods",
    "seu_detail", "peeo_based_adjustment", "output_pi_mapping",
    "case_configuration_portal", "pipeline_macros", "rm_blocks", "model_parameter",
    "case_info", "equipment_availability", "demand",
]


@dataclass
class EOConfig:
    """Typed container for all EO configuration tables."""
    model_id: int = 1

    # Core tag tables
    tag: pd.DataFrame = field(default_factory=pd.DataFrame)
    inferred_details: pd.DataFrame = field(default_factory=pd.DataFrame)
    inferred_tag_rm_block_mapping: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Optimizer config
    constraints: pd.DataFrame = field(default_factory=pd.DataFrame)
    variables: pd.DataFrame = field(default_factory=pd.DataFrame)
    objective: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Sub-models
    sub_model: pd.DataFrame = field(default_factory=pd.DataFrame)
    sub_model_child: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Derived equations (pre/post optimizer)
    derived_equations: pd.DataFrame = field(default_factory=pd.DataFrame)
    derived_equation_post_optimizer: pd.DataFrame = field(default_factory=pd.DataFrame)

    # ODS / reporting
    effect: pd.DataFrame = field(default_factory=pd.DataFrame)
    cause: pd.DataFrame = field(default_factory=pd.DataFrame)
    ods: pd.DataFrame = field(default_factory=pd.DataFrame)
    seu_detail: pd.DataFrame = field(default_factory=pd.DataFrame)
    output_pi_mapping: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Quality / CCP
    case_configuration_portal: pd.DataFrame = field(default_factory=pd.DataFrame)

    # Pipeline macros (intervals, windows, paths)
    pipeline_macros: Dict[str, str] = field(default_factory=dict)

    # Raw all-sheets dict for ad-hoc access
    raw: Dict[str, pd.DataFrame] = field(default_factory=dict)


def load_config_from_excel(feature_file_path: str, model_id: int = 1) -> EOConfig:
    """
    Primary config loader.  Reads every sheet from the feature file,
    filters to model_id where the column exists, and returns an EOConfig.

    Args:
        feature_file_path: Absolute path to EO_UN_FF_Rev_XX.xlsx
        model_id: Integer model identifier (default 1)

    Returns:
        EOConfig populated with all tables filtered to model_id
    """
    path = Path(feature_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Feature file not found: {path}")

    logger.info("Loading EO config from %s (model_id=%s)", path.name, model_id)

    xl = pd.ExcelFile(path)
    raw: Dict[str, pd.DataFrame] = {}

    for sheet in FEATURE_FILE_SHEETS:
        if sheet in xl.sheet_names:
            df = xl.parse(sheet)
            # Drop fully empty rows
            df = df.dropna(how="all").reset_index(drop=True)
            # Filter to model_id if column exists
            if "model_id" in df.columns:
                df = df[df["model_id"].fillna(model_id) == model_id].reset_index(drop=True)
            raw[sheet] = df
            logger.debug("  Loaded %-40s  %d rows", sheet, len(df))
        else:
            logger.warning("Sheet '%s' not found in feature file", sheet)
            raw[sheet] = pd.DataFrame()

    # Parse pipeline_macros into a flat dict
    macros: Dict[str, str] = {}
    if not raw["pipeline_macros"].empty:
        for _, row in raw["pipeline_macros"].iterrows():
            macros[str(row.get("parameter", ""))] = str(row.get("value", ""))

    cfg = EOConfig(
        model_id=model_id,
        tag=raw.get("tag", pd.DataFrame()),
        inferred_details=raw.get("inferred_details", pd.DataFrame()),
        inferred_tag_rm_block_mapping=raw.get("inferred_tag_rm_block_mapping", pd.DataFrame()),
        constraints=raw.get("constraints", pd.DataFrame()),
        variables=raw.get("variables", pd.DataFrame()),
        objective=raw.get("objective", pd.DataFrame()),
        sub_model=raw.get("sub_model", pd.DataFrame()),
        sub_model_child=raw.get("sub_model_child", pd.DataFrame()),
        derived_equations=raw.get("derived_equations", pd.DataFrame()),
        derived_equation_post_optimizer=raw.get("derived_equation_post_optimizer", pd.DataFrame()),
        effect=raw.get("effect", pd.DataFrame()),
        cause=raw.get("cause", pd.DataFrame()),
        ods=raw.get("ods", pd.DataFrame()),
        seu_detail=raw.get("seu_detail", pd.DataFrame()),
        output_pi_mapping=raw.get("output_pi_mapping", pd.DataFrame()),
        case_configuration_portal=raw.get("case_configuration_portal", pd.DataFrame()),
        pipeline_macros=macros,
        raw=raw,
    )

    logger.info(
        "Config loaded: %d tags | %d inferred | %d variables | %d constraints | %d SEUs",
        len(cfg.tag), len(cfg.inferred_details),
        len(cfg.variables), len(cfg.constraints), len(cfg.seu_detail),
    )
    return cfg
