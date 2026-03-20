"""
seed_data.py — Populate the database with realistic dummy plant data.
Run once: python seed_data.py
"""
from database import engine, SessionLocal, Base
from models import (
    Plant, Tag, Formula, QualityRule, SubModel,
    Variable, Constraint, Objective,
    EquipmentSEU, AlertCause, AlertEffect,
)


def seed():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    # ── Plant ──
    plant = Plant(
        id="plant-001", name="Jubail EO Complex",
        location="Jubail, Saudi Arabia", industry="Petrochemical",
        uom_preference="SI",
    )
    db.add(plant)

    # ── Tags ──
    tag_defs = [
        ("BLR_1_Load",          "FI-101A", "pi",  "Boiler 1 Steam Load",            "TPH",  "Boiler_1"),
        ("BLR_2_Load",          "FI-102A", "pi",  "Boiler 2 Steam Load",            "TPH",  "Boiler_2"),
        ("BLR_3_Load",          "FI-103A", "pi",  "Boiler 3 Steam Load",            "TPH",  "Boiler_3"),
        ("BLR_4_Load",          "FI-104A", "pi",  "Boiler 4 Steam Load",            "TPH",  "Boiler_4"),
        ("BLR_5_Load",          "FI-105A", "pi",  "Boiler 5 Steam Load",            "TPH",  "Boiler_5"),
        ("BLR_1_Fuel",          "FI-111",  "pi",  "Boiler 1 Fuel Gas Flow",         "KSCMH","Boiler_1"),
        ("BLR_2_Fuel",          "FI-112",  "pi",  "Boiler 2 Fuel Gas Flow",         "KSCMH","Boiler_2"),
        ("BLR_3_Fuel",          "FI-113",  "pi",  "Boiler 3 Fuel Gas Flow",         "KSCMH","Boiler_3"),
        ("BLR_1_Stack_Temp",    "TI-101",  "pi",  "Boiler 1 Stack Temperature",     "deg C","Boiler_1"),
        ("BLR_2_Stack_Temp",    "TI-102",  "pi",  "Boiler 2 Stack Temperature",     "deg C","Boiler_2"),
        ("FUR_A_Outlet_Temp",   "TI-201",  "pi",  "Furnace A Outlet Temperature",   "deg C","Furnace_A"),
        ("FUR_A_Fuel_Flow",     "FI-201",  "pi",  "Furnace A Fuel Gas Flow",        "KSCMH","Furnace_A"),
        ("COMP_A_Power",        "JI-301",  "pi",  "Compressor A Motor Power",       "kW",   "Compressor_A"),
        ("COMP_B_Power",        "JI-302",  "pi",  "Compressor B Motor Power",       "kW",   "Compressor_B"),
        ("HP_Steam_Header",     "PI-401",  "pi",  "HP Steam Header Pressure",       "kg/cm2","Utilities"),
        ("Total_HP_Demand",     "FI-501",  "pi",  "Total HP Steam Demand",          "TPH",  "Utilities"),
        ("Ambient_Temp",        "TI-001",  "pi",  "Ambient Temperature",            "deg C","General"),
        ("Relative_Humidity",   "AI-001",  "pi",  "Relative Humidity",              "%",    "General"),
        ("Total_Steam",         None,      "calculated", "Total HP Steam Production","TPH",  "Utilities"),
        ("BLR_1_Efficiency",    None,      "calculated", "Boiler 1 Thermal Efficiency","%",  "Boiler_1"),
        ("BLR_2_Efficiency",    None,      "calculated", "Boiler 2 Thermal Efficiency","%",  "Boiler_2"),
        ("FUR_A_Efficiency",    None,      "calculated", "Furnace A Thermal Efficiency","%", "Furnace_A"),
        ("Total_Fuel_Cost",     None,      "calculated", "Total Fuel Cost Per Hour", "$/hr", "Utilities"),
        ("Fuel_Unit_Price",     None,      "constant",   "Fuel Gas Unit Price",      "$/KSCM","General"),
        # Post-optimizer derived tags
        ("Savings_BLR1",        None,      "calculated", "Boiler 1 Load Saving (Actual - Optimal)", "TPH", "Boiler_1"),
        ("Savings_Total",       None,      "calculated", "Total Savings Opportunity",  "$/hr", "Utilities"),
    ]

    tags = {}
    for name, pi, ttype, desc, uom, grp in tag_defs:
        t = Tag(plant_id=plant.id, tag_name=name, pi_tag_name=pi,
                tag_type=ttype, description=desc, uom=uom, equipment_group=grp)
        db.add(t)
        tags[name] = t

    db.flush()

    # ── S3: Pre-Optimizer Formulas (Inferred Engine) ──
    pre_opt_formulas = [
        ("Total_Steam",      "BLR_1_Load + BLR_2_Load + BLR_3_Load + BLR_4_Load + BLR_5_Load", "inferred_engine"),
        ("BLR_1_Efficiency", "(BLR_1_Load * 2.5) / (BLR_1_Fuel * 9.5) * 100",                  "inferred_engine"),
        ("BLR_2_Efficiency", "(BLR_2_Load * 2.5) / (BLR_2_Fuel * 9.5) * 100",                  "inferred_engine"),
        ("FUR_A_Efficiency", "(FUR_A_Outlet_Temp - Ambient_Temp) / FUR_A_Outlet_Temp * 100",    "inferred_engine"),
        ("Total_Fuel_Cost",  "(BLR_1_Fuel + BLR_2_Fuel + BLR_3_Fuel + FUR_A_Fuel_Flow) * Fuel_Unit_Price", "inferred_engine"),
    ]
    for tag_name, expr, stage in pre_opt_formulas:
        db.add(Formula(tag_id=tags[tag_name].id, formula_expression=expr, pipeline_stage=stage))

    # ── S7: Post-Optimizer Derived Equations ──
    post_opt_formulas = [
        ("Savings_BLR1",  "BLR_1_Load_Actual - BLR_1_Load_Optimal",  "post_optimizer"),
        ("Savings_Total", "Total_Fuel_Cost_Actual - Total_Fuel_Cost_Optimal", "post_optimizer"),
    ]
    for tag_name, expr, stage in post_opt_formulas:
        db.add(Formula(tag_id=tags[tag_name].id, formula_expression=expr, pipeline_stage=stage))

    # ── S2: Quality Rules (CCP) ──
    qr_defs = [
        ("BLR_1_Load",       1, 1, 1, 85.0,  20.0,  130.0),
        ("BLR_2_Load",       1, 1, 1, 85.0,  20.0,  130.0),
        ("BLR_3_Load",       1, 1, 1, 85.0,  20.0,  130.0),
        ("BLR_1_Fuel",       1, 0, 1, None,  0.0,   50.0),
        ("BLR_1_Stack_Temp", 1, 1, 1, None,  80.0,  350.0),
        ("FUR_A_Outlet_Temp",1, 1, 1, None,  200.0, 800.0),
        ("COMP_A_Power",     1, 0, 1, None,  50.0,  500.0),
        ("HP_Steam_Header",  1, 1, 1, 42.0,  35.0,  55.0),
    ]
    for tag_name, nan_sw, stuck_sw, oob_sw, default, lolo, hihi in qr_defs:
        db.add(QualityRule(
            tag_id=tags[tag_name].id, nan_switch=nan_sw, stuck_switch=stuck_sw,
            oob_switch=oob_sw, default_value=default, lolo_limit=lolo, hihi_limit=hihi))

    # ── S4: Sub-Models ──
    sm_defs = [
        ("Boiler Heat Balance",       "equation",  1, "Q_steam = m_steam * (h_out - h_in)"),
        ("Comp Polytropic Head",      "equation",  2, "H_poly = (Z*R*T1/MW) * ((P2/P1)^((n-1)/n) - 1) * n/(n-1)"),
        ("Furnace Radiant Section",   "equation",  3, "Q_rad = sigma * eps * A * (T_flue^4 - T_tube^4)"),
        ("Boiler Efficiency ML",      "ml",        4, "sklearn_model:blr_efficiency_v2.pkl"),
    ]
    for name, mtype, order, expr in sm_defs:
        db.add(SubModel(plant_id=plant.id, name=name, model_type=mtype,
                        execution_order=order, expression=expr))

    # ── S5: Variables ──
    var_defs = [
        ("BLR_1_Load", 50, 120, False),
        ("BLR_2_Load", 50, 120, False),
        ("BLR_3_Load", 50, 120, False),
        ("BLR_4_Load", 40, 110, False),
        ("BLR_5_Load", 0,  110, True),
        ("FUR_A_Fuel_Flow", 5, 40, False),
        ("COMP_A_Power", 200, 450, False),
        ("COMP_B_Power", 180, 420, False),
    ]
    for name, lb, ub, is_int in var_defs:
        db.add(Variable(tag_id=tags[name].id, lower_bound=lb, upper_bound=ub,
                        is_integer=is_int, initial_value=(lb+ub)/2))

    # ── S5: Constraints ──
    constr_defs = [
        ("Steam Balance",        "Total_Steam >= 380",          "inequality", "Steam_Balance"),
        ("Boiler 1 Max",         "BLR_1_Load <= 120",           "inequality", "Equipment_Limits"),
        ("Boiler 2 Max",         "BLR_2_Load <= 120",           "inequality", "Equipment_Limits"),
        ("Min Boilers Running",  "BLR_1_Load + BLR_2_Load + BLR_3_Load >= 200", "inequality", "Operations"),
        ("HP Header Pressure",   "HP_Steam_Header >= 40",       "inequality", "Safety"),
        ("Max Fuel Total",       "BLR_1_Fuel + BLR_2_Fuel + BLR_3_Fuel <= 150", "inequality", "Budget"),
    ]
    for name, expr, ctype, grp in constr_defs:
        db.add(Constraint(plant_id=plant.id, name=name, expression=expr,
                          constraint_type=ctype, system_group=grp))

    # ── S5: Objective ──
    db.add(Objective(plant_id=plant.id, tag_name="Total_Fuel_Cost", direction=-1))

    # ── SEU Equipment ──
    seu_defs = [
        ("BLR_1", "Boiler 1",      "Fuel"),
        ("BLR_2", "Boiler 2",      "Fuel"),
        ("BLR_3", "Boiler 3",      "Fuel"),
        ("BLR_4", "Boiler 4",      "Fuel"),
        ("BLR_5", "Boiler 5",      "Fuel"),
        ("FUR_A", "Furnace A",     "Fuel"),
        ("COMP_A","Compressor A",  "Electricity"),
        ("COMP_B","Compressor B",  "Electricity"),
    ]
    for name, display, energy in seu_defs:
        db.add(EquipmentSEU(plant_id=plant.id, seu_name=name,
                            display_name=display, energy_source=energy))

    # ── S8: Alert Causes ──
    db.add(AlertCause(plant_id=plant.id, name="Boiler E Excess",
                      expression="BLR_5_Load > 0 AND Total_Steam > 400",
                      message="Boiler E is running but HP steam exceeds demand. Shut down to save fuel."))
    db.add(AlertCause(plant_id=plant.id, name="Compressor Overload",
                      expression="COMP_A_Power > 400",
                      message="Compressor A power exceeds 400 kW. Reduce IGV position."))
    db.add(AlertCause(plant_id=plant.id, name="Stack Temp High",
                      expression="BLR_1_Stack_Temp > 300",
                      message="Boiler 1 stack temperature is high. Check air/fuel ratio."))

    # ── S8: Alert Effects ──
    db.add(AlertEffect(plant_id=plant.id, name="Excess Fuel Consumption",
                       description="Plant consuming more fuel than optimal", priority="high"))
    db.add(AlertEffect(plant_id=plant.id, name="High Electricity Usage",
                       description="Motor power exceeding efficient range", priority="medium"))
    db.add(AlertEffect(plant_id=plant.id, name="Emissions Risk",
                       description="Stack temperature indicates incomplete combustion", priority="high"))

    db.commit()
    db.close()
    print("[OK] Database seeded with dummy plant data!")


if __name__ == "__main__":
    seed()
