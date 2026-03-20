import { useState } from "react";

const COLORS = {
  bg: "#0a0e1a",
  surface: "#111827",
  surfaceAlt: "#1a2235",
  border: "#1e2d45",
  accent1: "#00d4ff",
  accent2: "#ff6b35",
  accent3: "#7c3aed",
  accent4: "#10b981",
  text: "#e2e8f0",
  textMuted: "#64748b",
  textDim: "#94a3b8",
};

const Tag = ({ color, children }) => (
  <span style={{
    background: color + "22",
    border: `1px solid ${color}55`,
    color: color,
    padding: "2px 10px",
    borderRadius: 20,
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: "0.06em",
    textTransform: "uppercase",
  }}>{children}</span>
);

const SectionHeader = ({ number, title, accent }) => (
  <div style={{ display: "flex", alignItems: "center", gap: 14, marginBottom: 28 }}>
    <div style={{
      width: 42, height: 42, borderRadius: 10,
      background: `linear-gradient(135deg, ${accent}33, ${accent}11)`,
      border: `1.5px solid ${accent}55`,
      display: "flex", alignItems: "center", justifyContent: "center",
      fontFamily: "'Courier New', monospace",
      fontWeight: 700, color: accent, fontSize: 15,
      flexShrink: 0,
    }}>{String(number).padStart(2, "0")}</div>
    <h2 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: COLORS.text, letterSpacing: "-0.01em" }}>{title}</h2>
  </div>
);

const Card = ({ children, style = {} }) => (
  <div style={{
    background: COLORS.surface,
    border: `1px solid ${COLORS.border}`,
    borderRadius: 14,
    padding: 24,
    ...style
  }}>{children}</div>
);

const PipelineStep = ({ label, sublabel, color, icon, last }) => (
  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", flex: 1 }}>
    <div style={{
      width: 52, height: 52, borderRadius: 14,
      background: `linear-gradient(135deg, ${color}33, ${color}11)`,
      border: `2px solid ${color}66`,
      display: "flex", alignItems: "center", justifyContent: "center",
      fontSize: 22, marginBottom: 8,
    }}>{icon}</div>
    <div style={{ fontSize: 10, fontWeight: 700, color: color, textTransform: "uppercase", letterSpacing: "0.05em", textAlign: "center", marginBottom: 3 }}>{label}</div>
    <div style={{ fontSize: 10, color: COLORS.textMuted, textAlign: "center", maxWidth: 80 }}>{sublabel}</div>
    {!last && (
      <div style={{
        position: "absolute",
        width: 24, height: 2,
        background: `linear-gradient(90deg, ${color}99, ${color}22)`,
        right: -12, top: 25,
      }} />
    )}
  </div>
);

const CompareRow = ({ label, genAI, nonGenAI }) => (
  <tr>
    <td style={{ padding: "10px 14px", color: COLORS.textDim, fontSize: 13, borderBottom: `1px solid ${COLORS.border}`, fontWeight: 500 }}>{label}</td>
    <td style={{ padding: "10px 14px", color: COLORS.accent1, fontSize: 12, borderBottom: `1px solid ${COLORS.border}` }}>{genAI}</td>
    <td style={{ padding: "10px 14px", color: COLORS.accent4, fontSize: 12, borderBottom: `1px solid ${COLORS.border}` }}>{nonGenAI}</td>
  </tr>
);

const StepBlock = ({ step, title, desc, sub, color, icon }) => (
  <div style={{
    display: "flex", gap: 14, marginBottom: 18,
    padding: "16px 18px",
    background: COLORS.surfaceAlt,
    borderRadius: 10,
    border: `1px solid ${COLORS.border}`,
    position: "relative",
    overflow: "hidden",
  }}>
    <div style={{
      position: "absolute", left: 0, top: 0, bottom: 0, width: 3,
      background: color, borderRadius: "3px 0 0 3px"
    }} />
    <div style={{
      width: 38, height: 38, borderRadius: 9, flexShrink: 0,
      background: color + "22", border: `1.5px solid ${color}55`,
      display: "flex", alignItems: "center", justifyContent: "center",
      fontSize: 18,
    }}>{icon}</div>
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
        <span style={{ fontSize: 11, color: color, fontFamily: "monospace", fontWeight: 700 }}>STEP {step}</span>
        <span style={{ fontSize: 13, fontWeight: 700, color: COLORS.text }}>{title}</span>
      </div>
      <div style={{ fontSize: 12, color: COLORS.textDim, lineHeight: 1.6 }}>{desc}</div>
      {sub && <div style={{ fontSize: 11, color: COLORS.textMuted, marginTop: 6, fontStyle: "italic" }}>{sub}</div>}
    </div>
  </div>
);

const AlgoCard = ({ name, type, why, color }) => (
  <div style={{
    padding: "16px 18px",
    background: COLORS.surfaceAlt,
    border: `1px solid ${color}44`,
    borderRadius: 10, marginBottom: 10,
  }}>
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 6 }}>
      <span style={{ fontWeight: 700, color: COLORS.text, fontSize: 14 }}>{name}</span>
      <Tag color={color}>{type}</Tag>
    </div>
    <div style={{ fontSize: 12, color: COLORS.textDim, lineHeight: 1.6 }}>{why}</div>
  </div>
);

export default function Blueprint() {
  const [tab, setTab] = useState("genai");

  const genaiSteps = [
    { step: 1, icon: "📄", title: "Document Ingestion Layer", color: COLORS.accent1,
      desc: "Upload P&ID diagrams, PFDs, and PI tag lists as PDFs or images. A multimodal LLM (GPT-4o / Claude) extracts instrument tag names, process flow connections, and equipment labels via vision parsing.",
      sub: "Handles scanned docs, CAD exports, and Lucidchart/Visio exports." },
    { step: 2, icon: "🔍", title: "Tag Extraction & Classification", color: "#a78bfa",
      desc: "LLM identifies PI tags (e.g., PT-101, FT-203), categorizes them (Temperature, Pressure, Flow, Level, Composition), and maps them to equipment/process units. Outputs a structured tag registry with metadata.",
      sub: "Prompt: 'Extract all instrument tags from this P&ID and classify them by type and process unit.'" },
    { step: 3, icon: "🔗", title: "Relationship & Dependency Graph", color: COLORS.accent2,
      desc: "LLM infers process relationships from the P&ID topology. A NetworkX DAG is auto-constructed — no manual wiring needed. Circular dependency detection (nx.is_directed_acyclic_graph) is run automatically.",
      sub: "Previously manual — now fully automated from diagram understanding." },
    { step: 4, icon: "⚙️", title: "Inferred Tag Formula Generation", color: COLORS.accent4,
      desc: "Given raw sensor tags and process context, the LLM suggests inferred/calculated tag formulas (e.g., efficiency = (T_out - T_in) / T_in). Engineer reviews and approves before saving to config DB.",
      sub: "Human-in-the-loop validation gate before formulas enter production." },
    { step: 5, icon: "🧹", title: "Data Ingestion & Preprocessing", color: COLORS.accent1,
      desc: "PI/Historian data pulled hourly. Kedro preprocessing pipeline: outlier removal → schema validation → unit normalization. Config-driven limits from DB — nothing hardcoded.",
      sub: "Supports PI OSIsoft, mock CSV, and REST historian connectors." },
    { step: 6, icon: "🩹", title: "AI-Assisted Imputation", color: "#f59e0b",
      desc: "Missing sensor values handled by configurable strategy: Clip, Forward Fill, Interpolation, or ML-based imputation (LightGBM trained on plant patterns). Strategy per tag set in DB.",
      sub: "LLM can also explain anomalies in natural language for operators." },
    { step: 7, icon: "🎯", title: "Constraint & Objective Building", color: COLORS.accent3,
      desc: "Constraints (min/max bounds, process safety limits) loaded from DB via load_config_from_db(). Objective function (minimize fuel, maximize throughput) also config-driven — selectable per furnace case_id.",
      sub: "" },
    { step: 8, icon: "🚀", title: "Optimization Engine (MINLP → Bayesian BO)", color: COLORS.accent2,
      desc: "Replaces legacy MINLP with Bayesian Optimization (BoTorch/Optuna) for complex, non-convex furnace setpoint problems. Handles mixed continuous/discrete variables, faster convergence, uncertainty quantification.",
      sub: "Fallback: MINLP via Pyomo/GEKKO for constraint-heavy edge cases." },
    { step: 9, icon: "📊", title: "Reporting & Operator Interface", color: COLORS.accent4,
      desc: "LLM generates plain-English recommendation summaries: 'Increase Zone 2 setpoint by 8°C to improve fuel efficiency by ~3.2%.' Dashboard shows optimal vs. current setpoints with confidence intervals.",
      sub: "" },
  ];

  const nonGenAISteps = [
    { step: 1, icon: "📋", title: "Manual Tag Configuration", color: COLORS.accent4,
      desc: "Engineers provide PI tag lists via CSV/Excel upload or direct DB entry. Tag schema: tag_id, furnace_id, tag_name, pi_tag_name, pi_tag_type, formula. Supports 3 types: pi (live sensor), calculated (formula), constant.",
      sub: "Config table is the single source of truth. Nothing hardcoded in Python." },
    { step: 2, icon: "🔢", title: "Inferred Tag & Formula Engine", color: "#a78bfa",
      desc: "Formulas stored in DB evaluated via df.eval(). DAG constructed from tag dependencies using NetworkX — auto-topological sort resolves calculation order. Circular dependency detection raises exception before execution.",
      sub: "Engineers enter formulas like (T1 + T2) / 2 — no coding needed." },
    { step: 3, icon: "📡", title: "PI Data Ingestion Pipeline", color: COLORS.accent1,
      desc: "Kedro ingestion_pipeline: create_pi_connection → fetch_pi_data → standardize_columns → map_tags. Hourly scheduled runs. Config-driven tag mapping — new tags added to DB, not code.",
      sub: "" },
    { step: 4, icon: "🧹", title: "Preprocessing Pipeline", color: COLORS.accent2,
      desc: "remove_outliers → validate_schema → normalize_units. All thresholds from config. Pure functions only (df.copy() pattern). Each node 10–30 lines max, single responsibility.",
      sub: "" },
    { step: 5, icon: "🩹", title: "Imputation Pipeline", color: "#f59e0b",
      desc: "Strategy per tag from DB: Clip (min/max bounds), Forward Fill (last good value), Interpolation (gap smoothing), Model-based (LightGBM for critical tags). Pipeline can be skipped if impute: False in config.",
      sub: "" },
    { step: 6, icon: "⛓️", title: "Constraint & Objective Pipelines", color: COLORS.accent3,
      desc: "Constraint engine uses df.query() with expressions from DB. Objective function (fuel efficiency, heat retention) assembled from config parameters — no math embedded in code.",
      sub: "" },
    { step: 7, icon: "🚀", title: "Optimization Engine (Bayesian BO)", color: COLORS.accent2,
      desc: "Bayesian Optimization via Optuna or BoTorch replaces MINLP. Advantages: handles non-convex surfaces, fewer solver iterations, probabilistic uncertainty bounds on recommendations. MINLP available as fallback via Pyomo.",
      sub: "Optimizer reads variables/bounds/objective entirely from config — furnace-agnostic." },
    { step: 8, icon: "📊", title: "Reporting Pipeline", color: COLORS.accent4,
      desc: "Output: formatted setpoint recommendations with delta from current, objective value achieved, constraint satisfaction status. Delivered as CSV, JSON API response, or dashboard-ready payload.",
      sub: "" },
  ];

  const steps = tab === "genai" ? genaiSteps : nonGenAISteps;

  return (
    <div style={{
      minHeight: "100vh",
      background: COLORS.bg,
      fontFamily: "'IBM Plex Sans', 'Segoe UI', sans-serif",
      color: COLORS.text,
      padding: "40px 24px",
      maxWidth: 900,
      margin: "0 auto",
    }}>
      {/* Header */}
      <div style={{ marginBottom: 48, borderBottom: `1px solid ${COLORS.border}`, paddingBottom: 36 }}>
        <div style={{ display: "flex", gap: 8, marginBottom: 16, flexWrap: "wrap" }}>
          <Tag color={COLORS.accent1}>Industrial AI</Tag>
          <Tag color={COLORS.accent2}>Energy Optimization</Tag>
          <Tag color={COLORS.accent4}>Furnace Pipeline</Tag>
        </div>
        <h1 style={{
          margin: "0 0 12px",
          fontSize: 32,
          fontWeight: 800,
          letterSpacing: "-0.03em",
          background: `linear-gradient(135deg, ${COLORS.text}, ${COLORS.accent1})`,
          WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent",
          lineHeight: 1.2,
        }}>
          Energy Optimization Pipeline<br />Blueprint — Two Philosophies
        </h1>
        <p style={{ margin: 0, color: COLORS.textDim, fontSize: 15, maxWidth: 620, lineHeight: 1.7 }}>
          A production architecture for furnace setpoint optimization — one leveraging Generative AI for autonomous tag extraction from P&IDs/PFDs, one driven by structured manual configuration. Both built on Kedro, Bayesian Optimization, and a configuration-first principle.
        </p>
      </div>

      {/* Why upgrade from MINLP */}
      <div style={{ marginBottom: 40 }}>
        <SectionHeader number={1} title="Optimization Algorithm Upgrade: Why Move Beyond MINLP" accent={COLORS.accent2} />
        <Card>
          <div style={{ marginBottom: 16, color: COLORS.textDim, fontSize: 13, lineHeight: 1.7 }}>
            The current RapidMiner MINLP solver works for well-defined convex problems, but furnace optimization involves <strong style={{ color: COLORS.text }}>non-convex, noisy, and high-dimensional surfaces</strong> that benefit from probabilistic methods.
          </div>
          <AlgoCard
            name="Bayesian Optimization (BoTorch / Optuna)"
            type="Recommended"
            color={COLORS.accent4}
            why="Builds a surrogate model (Gaussian Process) over the objective surface. Ideal for expensive-to-evaluate furnace simulations. Handles mixed variable types, provides uncertainty estimates, converges in far fewer iterations than gradient-based MINLP. Native Python, integrates cleanly with Kedro nodes."
          />
          <AlgoCard
            name="MINLP via Pyomo + IPOPT/GEKKO"
            type="Fallback"
            color={COLORS.accent1}
            why="Keep as fallback for strictly constrained sub-problems where algebraic constraint satisfaction must be guaranteed (e.g., safety limits as hard constraints). Can be called as a node in the optimization_pipeline when BO solution violates a hard constraint."
          />
          <AlgoCard
            name="Reinforcement Learning (PPO/SAC)"
            type="Future Phase"
            color={COLORS.accent3}
            why="For long-horizon control problems where setpoints evolve over a shift. Requires a plant simulator (digital twin). Appropriate once sufficient historical data and a validated simulator exist."
          />
        </Card>
      </div>

      {/* Pipeline overview */}
      <div style={{ marginBottom: 40 }}>
        <SectionHeader number={2} title="8-Stage Pipeline Architecture (Both Philosophies)" accent={COLORS.accent1} />
        <Card style={{ overflowX: "auto" }}>
          <div style={{ display: "flex", gap: 0, minWidth: 680, position: "relative" }}>
            {[
              { icon: "📡", label: "Ingest", sub: "PI / Mock", color: COLORS.accent1 },
              { icon: "⚙️", label: "Preprocess", sub: "Clean + Validate", color: "#a78bfa" },
              { icon: "🔢", label: "Feature", sub: "Inferred Tags", color: COLORS.accent2 },
              { icon: "🩹", label: "Impute", sub: "Gap Fill", color: "#f59e0b" },
              { icon: "⛓️", label: "Constrain", sub: "Bounds", color: COLORS.accent3 },
              { icon: "🎯", label: "Objective", sub: "Function", color: COLORS.accent4 },
              { icon: "🚀", label: "Optimize", sub: "BO / MINLP", color: COLORS.accent2 },
              { icon: "📊", label: "Report", sub: "Setpoints", color: COLORS.accent1 },
            ].map((s, i, arr) => (
              <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", position: "relative" }}>
                <div style={{
                  width: 48, height: 48, borderRadius: 12,
                  background: `linear-gradient(135deg, ${s.color}33, ${s.color}11)`,
                  border: `2px solid ${s.color}66`,
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 20, marginBottom: 7, zIndex: 1,
                }}>{s.icon}</div>
                {i < arr.length - 1 && (
                  <div style={{
                    position: "absolute", right: 0, top: 22,
                    width: "calc(50% - 24px)", height: 2,
                    background: `linear-gradient(90deg, ${s.color}66, transparent)`,
                  }} />
                )}
                {i > 0 && (
                  <div style={{
                    position: "absolute", left: 0, top: 22,
                    width: "calc(50% - 24px)", height: 2,
                    background: `linear-gradient(90deg, transparent, ${s.color}44)`,
                  }} />
                )}
                <div style={{ fontSize: 9, fontWeight: 800, color: s.color, textTransform: "uppercase", letterSpacing: "0.05em", textAlign: "center" }}>{s.label}</div>
                <div style={{ fontSize: 9, color: COLORS.textMuted, textAlign: "center" }}>{s.sub}</div>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* Philosophy tabs */}
      <div style={{ marginBottom: 40 }}>
        <SectionHeader number={3} title="Detailed Step-by-Step: Choose Your Philosophy" accent={COLORS.accent3} />

        <div style={{ display: "flex", gap: 8, marginBottom: 24 }}>
          {[
            { id: "genai", label: "🤖 Philosophy 1: Gen AI", color: COLORS.accent1 },
            { id: "nongenai", label: "⚙️ Philosophy 2: Configuration-Driven", color: COLORS.accent4 },
          ].map(t => (
            <button key={t.id} onClick={() => setTab(t.id)} style={{
              padding: "10px 20px",
              borderRadius: 8,
              border: tab === t.id ? `2px solid ${t.color}` : `2px solid ${COLORS.border}`,
              background: tab === t.id ? t.color + "22" : COLORS.surface,
              color: tab === t.id ? t.color : COLORS.textMuted,
              fontWeight: 700, fontSize: 13, cursor: "pointer",
              transition: "all 0.15s",
            }}>{t.label}</button>
          ))}
        </div>

        {tab === "genai" && (
          <Card style={{ marginBottom: 16, background: COLORS.accent1 + "0a", border: `1px solid ${COLORS.accent1}33` }}>
            <div style={{ fontSize: 13, color: COLORS.textDim, lineHeight: 1.7 }}>
              <strong style={{ color: COLORS.accent1 }}>Core Idea:</strong> Engineers upload P&ID diagrams, PFDs, and PI tag sheets. A multimodal LLM automatically extracts, classifies, and wires tags — dramatically reducing onboarding time for new plants. Human review gates are placed at critical junctions before any config enters production.
            </div>
          </Card>
        )}
        {tab === "nongenai" && (
          <Card style={{ marginBottom: 16, background: COLORS.accent4 + "0a", border: `1px solid ${COLORS.accent4}33` }}>
            <div style={{ fontSize: 13, color: COLORS.textDim, lineHeight: 1.7 }}>
              <strong style={{ color: COLORS.accent4 }}>Core Idea:</strong> Engineers provide structured tag configurations via CSV/DB. The Kedro pipeline executes those configurations deterministically. All logic lives in config — zero hardcoding. DAG-based dependency resolution handles formula ordering automatically.
            </div>
          </Card>
        )}

        <div>
          {steps.map(s => (
            <StepBlock key={s.step} {...s} />
          ))}
        </div>
      </div>

      {/* Comparison table */}
      <div style={{ marginBottom: 40 }}>
        <SectionHeader number={4} title="Side-by-Side Comparison" accent={COLORS.accent4} />
        <Card style={{ padding: 0, overflow: "hidden" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ background: COLORS.surfaceAlt }}>
                <th style={{ padding: "12px 14px", textAlign: "left", fontSize: 12, color: COLORS.textMuted, fontWeight: 600 }}>Dimension</th>
                <th style={{ padding: "12px 14px", textAlign: "left", fontSize: 12, color: COLORS.accent1, fontWeight: 600 }}>🤖 Gen AI Philosophy</th>
                <th style={{ padding: "12px 14px", textAlign: "left", fontSize: 12, color: COLORS.accent4, fontWeight: 600 }}>⚙️ Config-Driven Philosophy</th>
              </tr>
            </thead>
            <tbody>
              <CompareRow label="Tag Onboarding" genAI="Auto-extracted from P&ID / PFD via LLM vision" nonGenAI="Manual CSV/DB entry by process engineer" />
              <CompareRow label="Inferred Tag Formulas" genAI="LLM suggests formulas from process context" nonGenAI="Engineer writes formulas in DB config" />
              <CompareRow label="Dependency Wiring" genAI="Auto-inferred from P&ID topology + LLM" nonGenAI="Auto-sorted via NetworkX DAG from schema" />
              <CompareRow label="Time to Onboard New Plant" genAI="Hours (upload docs → review → deploy)" nonGenAI="Days (manual tag entry + formula definition)" />
              <CompareRow label="Explainability" genAI="LLM generates plain-English summaries" nonGenAI="Deterministic, fully auditable config chain" />
              <CompareRow label="Infrastructure Cost" genAI="Higher (LLM API + vision model calls)" nonGenAI="Lower (pure Python, no external AI APIs)" />
              <CompareRow label="Risk Profile" genAI="LLM hallucination risk → needs review gates" nonGenAI="Low — engineer owns all formulas explicitly" />
              <CompareRow label="Optimizer" genAI="Bayesian Optimization (BoTorch)" nonGenAI="Bayesian Optimization (Optuna/BoTorch)" />
              <CompareRow label="Best For" genAI="Multi-site rollouts, greenfield plants" nonGenAI="Stable plants with known tag structures" />
            </tbody>
          </table>
        </Card>
      </div>

      {/* Tech stack */}
      <div style={{ marginBottom: 40 }}>
        <SectionHeader number={5} title="Recommended Tech Stack" accent={COLORS.accent2} />
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
          {[
            { layer: "Orchestration", tech: "Kedro 0.19+", note: "Micro/Macro pipeline hierarchy, runner.py entry point", color: COLORS.accent1 },
            { layer: "Optimization", tech: "BoTorch + Optuna", note: "Bayesian BO; Pyomo/GEKKO as MINLP fallback", color: COLORS.accent2 },
            { layer: "Dependency Graph", tech: "NetworkX", note: "DAG construction, topological sort, cycle detection", color: COLORS.accent3 },
            { layer: "Data / Historian", tech: "PI OSIsoft / osisoft.pi", note: "pi_client.py abstraction; mock CSV for dev", color: COLORS.accent4 },
            { layer: "Config Store", tech: "PostgreSQL + YAML", note: "DB for runtime config; YAML for environment params", color: "#f59e0b" },
            { layer: "Imputation", tech: "scikit-learn / LightGBM", note: "Model-based imputation for critical sensor gaps", color: "#a78bfa" },
            { layer: "Gen AI Layer", tech: "Claude API (claude-sonnet-4-6)", note: "Vision parsing, formula suggestion, NL summaries", color: COLORS.accent1 },
            { layer: "Dashboard", tech: "Streamlit / FastAPI", note: "Operator-facing setpoint recommendation UI", color: COLORS.accent4 },
          ].map((item, i) => (
            <div key={i} style={{
              padding: "14px 16px",
              background: COLORS.surfaceAlt,
              borderRadius: 10,
              border: `1px solid ${COLORS.border}`,
            }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 5 }}>
                <span style={{ fontSize: 11, color: COLORS.textMuted, fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.04em" }}>{item.layer}</span>
                <Tag color={item.color}>{item.tech}</Tag>
              </div>
              <div style={{ fontSize: 12, color: COLORS.textDim }}>{item.note}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Golden rules */}
      <div style={{ marginBottom: 24 }}>
        <SectionHeader number={6} title="Non-Negotiable Engineering Standards" accent={COLORS.accent1} />
        <Card>
          {[
            ["🔒", "Everything configurable, nothing hardcoded", "Tag limits, formulas, imputation strategies, objective function — all in DB/YAML. Zero furnace-specific logic in Python."],
            ["⚛️", "Nodes are atomic (10–30 lines, single responsibility)", "create_pi_connection ≠ fetch_pi_data ≠ standardize_columns. Split them."],
            ["🧪", "Pure functions only", "Always df.copy(). No side effects. No global state mutations inside nodes."],
            ["♻️", "Pipelines are composable and reusable", "The same preprocessing_pipeline runs on every furnace. Only config changes between sites."],
            ["🔁", "DAG-enforced execution order", "Never manually order formulas. Let NetworkX topological_sort determine sequence. Always validate with nx.is_directed_acyclic_graph()."],
          ].map(([icon, title, desc], i) => (
            <div key={i} style={{
              display: "flex", gap: 12, padding: "12px 0",
              borderBottom: i < 4 ? `1px solid ${COLORS.border}` : "none",
            }}>
              <span style={{ fontSize: 18, flexShrink: 0 }}>{icon}</span>
              <div>
                <div style={{ fontWeight: 700, fontSize: 13, color: COLORS.text, marginBottom: 3 }}>{title}</div>
                <div style={{ fontSize: 12, color: COLORS.textDim, lineHeight: 1.6 }}>{desc}</div>
              </div>
            </div>
          ))}
        </Card>
      </div>

      <div style={{ textAlign: "center", color: COLORS.textMuted, fontSize: 11, paddingTop: 16, borderTop: `1px solid ${COLORS.border}` }}>
        Energy Optimization Pipeline Blueprint · Kedro + Bayesian Optimization + Configuration-First
      </div>
    </div>
  );
}
