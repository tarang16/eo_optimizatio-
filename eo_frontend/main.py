"""
main.py - FastAPI application for the EO Platform.
Serves both the REST API and the frontend static files.
Covers the FULL 8-stage pipeline architecture:
  Stage 1: Tags & Data Ingestion
  Stage 2: Quality Rules (CCP)
  Stage 3: Inferred Tags / Formulas (pre-optimizer)
  Stage 4: Sub-Models
  Stage 5/6: Optimizer (Variables, Constraints, Objective)
  Stage 7: Post-Optimizer Derived Equations
  Stage 8: ODS Alerts (Causes, Effects, Mappings)
  Dashboard: Results + SEU Metrics

Run: python -m uvicorn main:app --reload --port 8000
"""
import csv
import io
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, Depends, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from database import engine, get_db, Base
from models import (
    Plant, Tag, Formula, QualityRule, SubModel,
    Variable, Constraint, Objective,
    EquipmentSEU, AlertCause, AlertEffect, PipelineRun,
)
from services import run_mock_optimizer

Base.metadata.create_all(bind=engine)

app = FastAPI(title="EO Platform API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ═══════════════════════════════════════════════════════════════════════
#  PYDANTIC SCHEMAS
# ═══════════════════════════════════════════════════════════════════════

class PlantOut(BaseModel):
    id: str; name: str; location: Optional[str]; industry: Optional[str]
    uom_preference: str

class TagOut(BaseModel):
    id: str; tag_name: str; pi_tag_name: Optional[str]; tag_type: str
    description: Optional[str]; uom: Optional[str]; equipment_group: Optional[str]

class FormulaIn(BaseModel):
    tag_name: str; formula_expression: str
    pipeline_stage: str = "inferred_engine"

class FormulaOut(BaseModel):
    id: str; tag_name: str; formula_expression: str
    pipeline_stage: Optional[str] = None

class QualityRuleOut(BaseModel):
    id: str; tag_name: str; nan_switch: int; stuck_switch: int; oob_switch: int
    default_value: Optional[float]; lolo_limit: Optional[float]; hihi_limit: Optional[float]

class QualityRuleIn(BaseModel):
    tag_name: str; nan_switch: int = 1; stuck_switch: int = 1; oob_switch: int = 1
    default_value: Optional[float] = None
    lolo_limit: Optional[float] = None; hihi_limit: Optional[float] = None

class SubModelOut(BaseModel):
    id: str; name: Optional[str]; model_type: str
    execution_order: int; expression: Optional[str]

class SubModelIn(BaseModel):
    name: str; model_type: str = "equation"
    execution_order: int = 1; expression: str = ""

class VariableOut(BaseModel):
    id: str; tag_name: str; lower_bound: Optional[float]; upper_bound: Optional[float]
    is_integer: bool

class VariableBoundsIn(BaseModel):
    lower_bound: float; upper_bound: float

class ConstraintOut(BaseModel):
    id: str; name: Optional[str]; expression: str; constraint_type: str
    system_group: Optional[str]

class ConstraintIn(BaseModel):
    name: str; expression: str; constraint_type: str = "inequality"
    system_group: str = "Custom"

class ObjectiveOut(BaseModel):
    id: str; tag_name: str; direction: int

class AlertCauseOut(BaseModel):
    id: str; name: str; expression: Optional[str]; message: Optional[str]
    monitoring_tag: Optional[str]

class AlertCauseIn(BaseModel):
    name: str; expression: str; message: str; monitoring_tag: str = ""

class AlertEffectOut(BaseModel):
    id: str; name: str; description: Optional[str]; priority: str

class AlertEffectIn(BaseModel):
    name: str; description: str = ""; priority: str = "medium"

class EquipmentSEUOut(BaseModel):
    id: str; seu_name: str; display_name: Optional[str]; energy_source: Optional[str]

class RunOut(BaseModel):
    id: str; status: str; duration_s: Optional[float]
    savings_per_hour: Optional[float]; savings_per_year: Optional[float]
    fuel_improvement_pct: Optional[float]; n_tags: Optional[int]
    n_variables: Optional[int]; n_constraints: Optional[int]
    n_alerts: Optional[int]; recommendations: Optional[list]
    seu_results: Optional[list]; chart_data: Optional[list]


# ═══════════════════════════════════════════════════════════════════════
#  STAGE 1: PLANT & TAG ROUTES
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/plants", response_model=List[PlantOut])
def list_plants(db: Session = Depends(get_db)):
    return db.query(Plant).all()

@app.get("/api/plants/{plant_id}", response_model=PlantOut)
def get_plant(plant_id: str, db: Session = Depends(get_db)):
    p = db.query(Plant).filter(Plant.id == plant_id).first()
    if not p: raise HTTPException(404, "Plant not found")
    return p

@app.get("/api/plants/{plant_id}/tags", response_model=List[TagOut])
def list_tags(plant_id: str, db: Session = Depends(get_db)):
    return db.query(Tag).filter(Tag.plant_id == plant_id).all()

@app.post("/api/plants/{plant_id}/tags/upload")
async def upload_tags_csv(plant_id: str, file: UploadFile = File(...),
                          db: Session = Depends(get_db)):
    content = await file.read()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
    count = 0
    for row in reader:
        tag_name = row.get("tag_name", "").strip()
        db.add(Tag(
            plant_id=plant_id, tag_name=tag_name,
            pi_tag_name=row.get("pi_tag_name", "").strip(),
            description=row.get("description", "").strip(),
            uom=row.get("uom", "").strip(), tag_type="pi",
            equipment_group=_auto_group(tag_name),
        ))
        count += 1
    db.commit()
    return {"uploaded": count}

def _auto_group(tag_name: str) -> str:
    n = tag_name.upper()
    if "BLR" in n or "BOILER" in n: return "Boiler"
    if "FUR" in n or "FURNACE" in n: return "Furnace"
    if "COMP" in n or "COMPRESSOR" in n: return "Compressor"
    if "PUMP" in n: return "Pump"
    return "General"


# ═══════════════════════════════════════════════════════════════════════
#  STAGE 2: QUALITY RULES (CCP)
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/plants/{plant_id}/quality-rules", response_model=List[QualityRuleOut])
def list_quality_rules(plant_id: str, db: Session = Depends(get_db)):
    rows = (
        db.query(QualityRule.id, Tag.tag_name,
                 QualityRule.nan_switch, QualityRule.stuck_switch, QualityRule.oob_switch,
                 QualityRule.default_value, QualityRule.lolo_limit, QualityRule.hihi_limit)
        .join(Tag, QualityRule.tag_id == Tag.id)
        .filter(Tag.plant_id == plant_id).all()
    )
    return [{"id": r[0], "tag_name": r[1], "nan_switch": r[2], "stuck_switch": r[3],
             "oob_switch": r[4], "default_value": r[5], "lolo_limit": r[6], "hihi_limit": r[7]}
            for r in rows]

@app.post("/api/plants/{plant_id}/quality-rules", response_model=QualityRuleOut)
def create_quality_rule(plant_id: str, body: QualityRuleIn, db: Session = Depends(get_db)):
    tag = db.query(Tag).filter(Tag.plant_id == plant_id, Tag.tag_name == body.tag_name).first()
    if not tag: raise HTTPException(404, f"Tag '{body.tag_name}' not found")
    qr = QualityRule(tag_id=tag.id, nan_switch=body.nan_switch, stuck_switch=body.stuck_switch,
                     oob_switch=body.oob_switch, default_value=body.default_value,
                     lolo_limit=body.lolo_limit, hihi_limit=body.hihi_limit)
    db.add(qr); db.commit()
    return {"id": qr.id, "tag_name": body.tag_name, **{k: getattr(qr, k) for k in
            ["nan_switch", "stuck_switch", "oob_switch", "default_value", "lolo_limit", "hihi_limit"]}}

@app.delete("/api/quality-rules/{rule_id}")
def delete_quality_rule(rule_id: str, db: Session = Depends(get_db)):
    r = db.query(QualityRule).filter(QualityRule.id == rule_id).first()
    if r: db.delete(r); db.commit()
    return {"deleted": True}


# ═══════════════════════════════════════════════════════════════════════
#  STAGE 3: FORMULAS (INFERRED TAGS) - pre & post optimizer
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/plants/{plant_id}/formulas", response_model=List[FormulaOut])
def list_formulas(plant_id: str, stage: Optional[str] = None, db: Session = Depends(get_db)):
    q = db.query(Formula.id, Tag.tag_name, Formula.formula_expression, Formula.pipeline_stage)\
          .join(Tag, Formula.tag_id == Tag.id).filter(Tag.plant_id == plant_id)
    if stage:
        q = q.filter(Formula.pipeline_stage == stage)
    return [{"id": r[0], "tag_name": r[1], "formula_expression": r[2], "pipeline_stage": r[3]}
            for r in q.all()]

@app.post("/api/plants/{plant_id}/formulas", response_model=FormulaOut)
def create_formula(plant_id: str, body: FormulaIn, db: Session = Depends(get_db)):
    tag = db.query(Tag).filter(Tag.plant_id == plant_id, Tag.tag_name == body.tag_name).first()
    if not tag:
        tag = Tag(plant_id=plant_id, tag_name=body.tag_name, tag_type="calculated")
        db.add(tag); db.flush()
    f = Formula(tag_id=tag.id, formula_expression=body.formula_expression,
                pipeline_stage=body.pipeline_stage)
    db.add(f); db.commit()
    return {"id": f.id, "tag_name": body.tag_name,
            "formula_expression": body.formula_expression, "pipeline_stage": body.pipeline_stage}

@app.delete("/api/formulas/{formula_id}")
def delete_formula(formula_id: str, db: Session = Depends(get_db)):
    f = db.query(Formula).filter(Formula.id == formula_id).first()
    if f: db.delete(f); db.commit()
    return {"deleted": True}


# ═══════════════════════════════════════════════════════════════════════
#  STAGE 4: SUB-MODELS
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/plants/{plant_id}/sub-models", response_model=List[SubModelOut])
def list_sub_models(plant_id: str, db: Session = Depends(get_db)):
    return db.query(SubModel).filter(SubModel.plant_id == plant_id)\
             .order_by(SubModel.execution_order).all()

@app.post("/api/plants/{plant_id}/sub-models", response_model=SubModelOut)
def create_sub_model(plant_id: str, body: SubModelIn, db: Session = Depends(get_db)):
    sm = SubModel(plant_id=plant_id, name=body.name, model_type=body.model_type,
                  execution_order=body.execution_order, expression=body.expression)
    db.add(sm); db.commit()
    return sm

@app.delete("/api/sub-models/{sm_id}")
def delete_sub_model(sm_id: str, db: Session = Depends(get_db)):
    s = db.query(SubModel).filter(SubModel.id == sm_id).first()
    if s: db.delete(s); db.commit()
    return {"deleted": True}


# ═══════════════════════════════════════════════════════════════════════
#  STAGE 5/6: OPTIMIZER (Variables, Constraints, Objective)
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/plants/{plant_id}/variables", response_model=List[VariableOut])
def list_variables(plant_id: str, db: Session = Depends(get_db)):
    rows = (db.query(Variable.id, Tag.tag_name, Variable.lower_bound,
                     Variable.upper_bound, Variable.is_integer)
            .join(Tag, Variable.tag_id == Tag.id)
            .filter(Tag.plant_id == plant_id).all())
    return [{"id": r[0], "tag_name": r[1], "lower_bound": r[2],
             "upper_bound": r[3], "is_integer": r[4]} for r in rows]

@app.patch("/api/variables/{var_id}")
def update_variable_bounds(var_id: str, body: VariableBoundsIn, db: Session = Depends(get_db)):
    v = db.query(Variable).filter(Variable.id == var_id).first()
    if not v: raise HTTPException(404, "Variable not found")
    v.lower_bound = body.lower_bound; v.upper_bound = body.upper_bound
    db.commit()
    return {"updated": True}

@app.get("/api/plants/{plant_id}/constraints", response_model=List[ConstraintOut])
def list_constraints(plant_id: str, db: Session = Depends(get_db)):
    return db.query(Constraint).filter(Constraint.plant_id == plant_id).all()

@app.post("/api/plants/{plant_id}/constraints", response_model=ConstraintOut)
def create_constraint(plant_id: str, body: ConstraintIn, db: Session = Depends(get_db)):
    c = Constraint(plant_id=plant_id, name=body.name, expression=body.expression,
                   constraint_type=body.constraint_type, system_group=body.system_group)
    db.add(c); db.commit()
    return c

@app.delete("/api/constraints/{constr_id}")
def delete_constraint(constr_id: str, db: Session = Depends(get_db)):
    c = db.query(Constraint).filter(Constraint.id == constr_id).first()
    if c: db.delete(c); db.commit()
    return {"deleted": True}

@app.get("/api/plants/{plant_id}/objective", response_model=Optional[ObjectiveOut])
def get_objective(plant_id: str, db: Session = Depends(get_db)):
    return db.query(Objective).filter(Objective.plant_id == plant_id).first()


# ═══════════════════════════════════════════════════════════════════════
#  STAGE 8: ODS ALERTS — Causes, Effects
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/plants/{plant_id}/alert-causes", response_model=List[AlertCauseOut])
def list_alert_causes(plant_id: str, db: Session = Depends(get_db)):
    return db.query(AlertCause).filter(AlertCause.plant_id == plant_id).all()

@app.post("/api/plants/{plant_id}/alert-causes", response_model=AlertCauseOut)
def create_alert_cause(plant_id: str, body: AlertCauseIn, db: Session = Depends(get_db)):
    ac = AlertCause(plant_id=plant_id, name=body.name, expression=body.expression,
                    message=body.message, monitoring_tag=body.monitoring_tag)
    db.add(ac); db.commit()
    return ac

@app.delete("/api/alert-causes/{cause_id}")
def delete_alert_cause(cause_id: str, db: Session = Depends(get_db)):
    c = db.query(AlertCause).filter(AlertCause.id == cause_id).first()
    if c: db.delete(c); db.commit()
    return {"deleted": True}

@app.get("/api/plants/{plant_id}/alert-effects", response_model=List[AlertEffectOut])
def list_alert_effects(plant_id: str, db: Session = Depends(get_db)):
    return db.query(AlertEffect).filter(AlertEffect.plant_id == plant_id).all()

@app.post("/api/plants/{plant_id}/alert-effects", response_model=AlertEffectOut)
def create_alert_effect(plant_id: str, body: AlertEffectIn, db: Session = Depends(get_db)):
    ae = AlertEffect(plant_id=plant_id, name=body.name,
                     description=body.description, priority=body.priority)
    db.add(ae); db.commit()
    return ae

@app.delete("/api/alert-effects/{effect_id}")
def delete_alert_effect(effect_id: str, db: Session = Depends(get_db)):
    e = db.query(AlertEffect).filter(AlertEffect.id == effect_id).first()
    if e: db.delete(e); db.commit()
    return {"deleted": True}


# ═══════════════════════════════════════════════════════════════════════
#  SEU EQUIPMENT
# ═══════════════════════════════════════════════════════════════════════

@app.get("/api/plants/{plant_id}/equipment", response_model=List[EquipmentSEUOut])
def list_equipment(plant_id: str, db: Session = Depends(get_db)):
    return db.query(EquipmentSEU).filter(EquipmentSEU.plant_id == plant_id).all()


# ═══════════════════════════════════════════════════════════════════════
#  PIPELINE RUN
# ═══════════════════════════════════════════════════════════════════════

@app.post("/api/plants/{plant_id}/run", response_model=RunOut)
def run_optimizer(plant_id: str, db: Session = Depends(get_db)):
    plant = db.query(Plant).filter(Plant.id == plant_id).first()
    if not plant: raise HTTPException(404, "Plant not found")
    tags = db.query(Tag).filter(Tag.plant_id == plant_id).all()
    variables_q = (db.query(Variable, Tag.tag_name).join(Tag, Variable.tag_id == Tag.id)
                   .filter(Tag.plant_id == plant_id).all())
    constraints = db.query(Constraint).filter(Constraint.plant_id == plant_id).all()
    objective = db.query(Objective).filter(Objective.plant_id == plant_id).first()
    tag_dicts = [{"tag_name": t.tag_name, "description": t.description} for t in tags]
    var_dicts = [{"tag_name": tn, "lower": v.lower_bound, "upper": v.upper_bound} for v, tn in variables_q]
    constr_dicts = [{"expression": c.expression} for c in constraints]
    obj_dict = {"tag_name": objective.tag_name, "direction": objective.direction} if objective else {}
    result = run_mock_optimizer(tag_dicts, var_dicts, constr_dicts, obj_dict)
    run = PipelineRun(
        plant_id=plant_id, status=result["status"], duration_s=result["duration_s"],
        savings_per_hour=result["savings_per_hour"], savings_per_year=result["savings_per_year"],
        fuel_improvement_pct=result["fuel_improvement_pct"],
        n_tags=result["n_tags"], n_variables=result["n_variables"],
        n_constraints=result["n_constraints"], n_alerts=result["n_alerts"],
        recommendations=result["recommendations"], seu_results=result["seu_results"],
        chart_data=result["chart_data"], completed_at=datetime.utcnow(),
    )
    db.add(run); db.commit(); db.refresh(run)
    return run

@app.get("/api/plants/{plant_id}/runs/latest", response_model=Optional[RunOut])
def latest_run(plant_id: str, db: Session = Depends(get_db)):
    return db.query(PipelineRun).filter(PipelineRun.plant_id == plant_id)\
             .order_by(PipelineRun.started_at.desc()).first()


# ═══════════════════════════════════════════════════════════════════════
#  SERVE FRONTEND
# ═══════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
def serve_frontend():
    import pathlib
    return (pathlib.Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")
