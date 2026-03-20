"""
eo_pipeline
------------
Generic Energy Optimization Pipeline — configuration-driven, plant-agnostic.

Orchestrates 8 stages: Ingestion → Quality → Inferred → Sub-Model →
Optimizer Prep → Optimizer → Post-Optimizer → ODS/SEU Reporting.

All process-specific logic lives in the feature file (Excel workbook).
The Python code is a generic execution engine.
"""

__version__ = "1.0.0"
