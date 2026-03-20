"""
services.py — Business logic: PI connector, UOM engine, Optimizer mock.
"""
import random
import time
from datetime import datetime
from typing import Dict, List


# ── UOM Conversion Tables ──────────────────────────────────────────────

UOM_CONVERSIONS = {
    # (from_unit, to_system) -> (multiplier, offset, target_unit)
    ("°F", "SI"):    (5/9, -32 * 5/9,  "°C"),
    ("°C", "Imperial"): (9/5, 32,       "°F"),
    ("psi", "SI"):   (6.89476, 0,       "kPa"),
    ("kPa", "Imperial"): (0.145038, 0,  "psi"),
    ("lb/hr", "SI"): (0.453592, 0,      "kg/hr"),
    ("kg/hr", "Imperial"): (2.20462, 0, "lb/hr"),
    ("BTU/hr", "SI"): (0.000293071, 0,  "kW"),
    ("kW", "Imperial"): (3412.14, 0,    "BTU/hr"),
    ("GPM", "SI"):   (0.227125, 0,      "m³/hr"),
    ("m³/hr", "Imperial"): (4.40287, 0, "GPM"),
}


def convert_uom(value: float, from_unit: str, target_system: str) -> tuple:
    """Convert a value from one unit system to another. Returns (converted_value, new_unit)."""
    key = (from_unit, target_system)
    if key in UOM_CONVERSIONS:
        mult, offset, new_unit = UOM_CONVERSIONS[key]
        return round(value * mult + offset, 4), new_unit
    return value, from_unit


# ── PI Data Simulator ──────────────────────────────────────────────────

TAG_RANGES = {
    "temperature": (100, 500),
    "pressure":    (1, 80),
    "flow":        (10, 200),
    "level":       (0, 100),
    "power":       (50, 500),
    "speed":       (500, 3600),
    "efficiency":  (70, 99),
    "load":        (40, 120),
    "default":     (10, 500),
}


def _guess_type(tag_name: str, description: str = "") -> str:
    combined = (tag_name + " " + (description or "")).lower()
    for keyword in TAG_RANGES:
        if keyword[:4] in combined:
            return keyword
    if any(k in combined for k in ["temp", "ti-", "tt-"]):
        return "temperature"
    if any(k in combined for k in ["pres", "pi-", "pt-"]):
        return "pressure"
    if any(k in combined for k in ["flow", "fi-", "ft-"]):
        return "flow"
    if any(k in combined for k in ["load", "blr", "boiler"]):
        return "load"
    if any(k in combined for k in ["rpm", "speed", "motor"]):
        return "speed"
    return "default"


def fetch_live_pi_data(tags: List[dict], timestamp: str = None) -> Dict[str, float]:
    """Simulate fetching snapshot data from a PI historian."""
    random.seed(42)
    result = {}
    for tag in tags:
        tag_type = _guess_type(tag["tag_name"], tag.get("description", ""))
        lo, hi = TAG_RANGES[tag_type]
        result[tag["tag_name"]] = round(random.uniform(lo, hi), 2)
    return result


# ── Mock Optimizer ─────────────────────────────────────────────────────

def run_mock_optimizer(tags: List[dict], variables: List[dict],
                       constraints: List[dict], objective: dict) -> dict:
    """Simulate the Differential Evolution optimizer (3s delay)."""
    time.sleep(3)

    n_vars = len(variables)
    n_constr = len(constraints)
    n_tags = len(tags)
    savings_hr = round(random.uniform(20, 60), 2)
    fuel_pct = round(random.uniform(1.5, 4.5), 1)

    recommendations = [
        {"severity": "high",   "icon": "🔴", "message": "Shutdown Boiler E — HP steam over-production detected, save $12.40/hr"},
        {"severity": "medium", "icon": "🟡", "message": "Reduce Air Compressor C motor power by 8% — current IGV position allows reduction"},
        {"severity": "medium", "icon": "🟡", "message": "Reduce Letdown Valve PV-7026 — margin available in C3R compressor RPM"},
        {"severity": "low",    "icon": "🟢", "message": "FD Fan E can be shutdown alongside Boiler E for additional savings"},
    ]

    chart_data = []
    for v in variables[:6]:
        actual = round(random.uniform(60, 110), 1)
        optimal = round(actual * random.uniform(0.85, 0.98), 1)
        chart_data.append({
            "name": v.get("tag_name", v.get("tag_id", "Unknown")),
            "actual": actual,
            "optimal": optimal,
        })

    seu_results = [
        {"name": "Boiler A",      "energy": "Fuel",        "actual": 225.3, "target": 210.1, "gain": 15.2,  "enpi": 0.94},
        {"name": "Boiler B",      "energy": "Fuel",        "actual": 218.7, "target": 205.4, "gain": 13.3,  "enpi": 0.93},
        {"name": "Boiler C",      "energy": "Fuel",        "actual": 230.1, "target": 220.8, "gain": 9.3,   "enpi": 0.96},
        {"name": "Furnace 1",     "energy": "Fuel",        "actual": 45.2,  "target": 42.1,  "gain": 3.1,   "enpi": 0.93},
        {"name": "Compressor A",  "energy": "Electricity", "actual": 320.5, "target": 305.2, "gain": 15.3,  "enpi": 0.95},
        {"name": "Compressor B",  "energy": "Electricity", "actual": 285.1, "target": 278.9, "gain": 6.2,   "enpi": 0.98},
    ]

    return {
        "status": "completed",
        "duration_s": 17.3,
        "savings_per_hour": savings_hr,
        "savings_per_year": round(savings_hr * 8760, 0),
        "fuel_improvement_pct": fuel_pct,
        "n_tags": n_tags,
        "n_variables": n_vars,
        "n_constraints": n_constr,
        "n_alerts": len(recommendations),
        "recommendations": recommendations,
        "seu_results": seu_results,
        "chart_data": chart_data,
    }
