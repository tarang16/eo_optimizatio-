"""
models.py — SQLAlchemy ORM models for the EO Platform.
Maps every domain from the architecture: Plants, Tags, Formulas,
Variables, Constraints, Objectives, Quality, SEU, Alerts, Sub-Models, Runs.
"""
import uuid
from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, Float, Boolean, Text, DateTime, ForeignKey, JSON
)
from sqlalchemy.orm import relationship
from database import Base


def new_uuid():
    return str(uuid.uuid4())


# ── 1. Core Configuration ──────────────────────────────────────────────

class Plant(Base):
    __tablename__ = "plants"
    id = Column(String, primary_key=True, default=new_uuid)
    name = Column(String(255), nullable=False)
    location = Column(String(255))
    industry = Column(String(100))
    uom_preference = Column(String(50), default="SI")
    created_at = Column(DateTime, default=datetime.utcnow)

    tags = relationship("Tag", back_populates="plant", cascade="all, delete-orphan")
    constraints = relationship("Constraint", back_populates="plant", cascade="all, delete-orphan")
    runs = relationship("PipelineRun", back_populates="plant", cascade="all, delete-orphan")


class Tag(Base):
    __tablename__ = "tags"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    tag_name = Column(String(100), nullable=False)
    pi_tag_name = Column(String(255))
    tag_type = Column(String(50), default="pi")  # pi, calculated, constant
    description = Column(Text)
    uom = Column(String(50))
    equipment_group = Column(String(100))

    plant = relationship("Plant", back_populates="tags")
    formula = relationship("Formula", back_populates="tag", uselist=False, cascade="all, delete-orphan")
    variable = relationship("Variable", back_populates="tag", uselist=False, cascade="all, delete-orphan")


# ── 2. Formula Engine ──────────────────────────────────────────────────

class Formula(Base):
    __tablename__ = "formulas"
    id = Column(String, primary_key=True, default=new_uuid)
    tag_id = Column(String, ForeignKey("tags.id", ondelete="CASCADE"))
    formula_expression = Column(Text, nullable=False)
    pipeline_stage = Column(String(50), default="inferred_engine")
    execution_block = Column(Integer, default=1)

    tag = relationship("Tag", back_populates="formula")


# ── 3. Data Quality (CCP) ─────────────────────────────────────────────

class QualityRule(Base):
    __tablename__ = "quality_rules"
    id = Column(String, primary_key=True, default=new_uuid)
    tag_id = Column(String, ForeignKey("tags.id", ondelete="CASCADE"))
    nan_switch = Column(Integer, default=0)
    stuck_switch = Column(Integer, default=0)
    oob_switch = Column(Integer, default=0)
    default_value = Column(Float)
    lolo_limit = Column(Float)
    hihi_limit = Column(Float)


# ── 4. Optimization Problem ───────────────────────────────────────────

class Variable(Base):
    __tablename__ = "decision_variables"
    id = Column(String, primary_key=True, default=new_uuid)
    tag_id = Column(String, ForeignKey("tags.id", ondelete="CASCADE"))
    is_integer = Column(Boolean, default=False)
    initial_value = Column(Float)
    lower_bound = Column(Float)
    upper_bound = Column(Float)

    tag = relationship("Tag", back_populates="variable")


class Constraint(Base):
    __tablename__ = "constraints"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    name = Column(String(100))
    expression = Column(Text, nullable=False)
    constraint_type = Column(String(20), default="inequality")
    system_group = Column(String(100))

    plant = relationship("Plant", back_populates="constraints")


class Objective(Base):
    __tablename__ = "objectives"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    tag_name = Column(String(100))
    direction = Column(Integer, default=-1)  # -1=Minimize, 1=Maximize
    weight = Column(Float, default=1.0)


# ── 5. SEU Equipment ──────────────────────────────────────────────────

class EquipmentSEU(Base):
    __tablename__ = "equipment_seus"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    seu_name = Column(String(100), nullable=False)
    display_name = Column(String(255))
    energy_source = Column(String(50))
    actual_duty = Column(Float)
    baseline_duty = Column(Float)
    target_duty = Column(Float)
    gain = Column(Float)
    enpi = Column(Float)


# ── 6. Alerts (ODS) ──────────────────────────────────────────────────

class AlertCause(Base):
    __tablename__ = "alert_causes"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    name = Column(String(100), nullable=False)
    expression = Column(Text)
    message = Column(Text)
    monitoring_tag = Column(String(100))


class AlertEffect(Base):
    __tablename__ = "alert_effects"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    name = Column(String(100), nullable=False)
    description = Column(Text)
    priority = Column(String(20), default="medium")


# ── 7. Sub-Models ────────────────────────────────────────────────────

class SubModel(Base):
    __tablename__ = "sub_models"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    name = Column(String(100))
    model_type = Column(String(50), default="equation")
    execution_order = Column(Integer, default=1)
    expression = Column(Text)


# ── 8. Pipeline Runs ────────────────────────────────────────────────

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    id = Column(String, primary_key=True, default=new_uuid)
    plant_id = Column(String, ForeignKey("plants.id", ondelete="CASCADE"))
    status = Column(String(50), default="pending")
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime)
    duration_s = Column(Float)
    savings_per_hour = Column(Float)
    savings_per_year = Column(Float)
    fuel_improvement_pct = Column(Float)
    n_tags = Column(Integer)
    n_variables = Column(Integer)
    n_constraints = Column(Integer)
    n_alerts = Column(Integer)
    recommendations = Column(JSON)
    seu_results = Column(JSON)
    chart_data = Column(JSON)

    plant = relationship("Plant", back_populates="runs")
