import Section from "./section"
import Image from "next/image"
import Pipeline from "./pipeline"

const chart = `
graph TD
    A[Download Raw Data from Globus] --> B{Create DuckDB Databases}

    B --> C[Mini Database<br/>~200MB per table<br/>mini_database_creation_duckdb.py]
    B --> D[Full Database<br/>~50GB total<br/>database_creation_duckdb.py]

    C --> E[Build Reporting Tables<br/>22 tables<br/>00_build_reporting_tables.sql]
    D --> F[Build Reporting Tables<br/>22 tables<br/>00_build_reporting_tables.sql]

    E --> G[Export Baseline Queries<br/>12 benchmark queries<br/>export_baseline.py]
    F --> H[Wait for Mini Evaluation]

    G --> I[Baseline Results<br/>data/mini/]

    %% CONFIG BEFORE SPLIT
    I --> CFG[Load DP Configuration<br/>epsilons, sensitivities, metrics<br/>dp_config.py]

    %% MINI — BASELINE BRANCH
    CFG --> B_MIN[Baseline Mechanisms<br/><br/>
        • dp_gaussian_mechanism_baseline.py<br/>
        • dp_laplace_mechanism_baseline.py<br/><br/>
        eps: 0.01, 0.05, 0.1, 0.5, 1.0, ∞<br/><br/>
        Output Paths:<br/>
        • data/dp_gaussianmini/baseline/eps/<br/>
        • data/dp_laplacemini/baseline/eps/]

    %% MINI — ADVANCE BRANCH
    CFG --> A_MIN[Advance Mechanisms<br/><br/>
        • dp_gaussian_mechanism_advance.py<br/>
        • dp_laplace_mechanism_advance.py<br/><br/>
        eps: 0.01, 0.05, 0.1, 0.5, 1.0, ∞<br/><br/>
        Output Paths:<br/>
        • data/dp_gaussianmini/advance/eps/<br/>
        • data/dp_laplacemini/advance/eps/]

    %% MINI EVALUATION
    B_MIN --> N
    A_MIN --> N

    N[Evaluate Mini DP Results<br/>Compute RE, TVD, Spearman<br/>evaluate_dp_results.py] --> O

    O[Mini Evaluation Metrics<br/>evaluation_results/] --> P
P[Select Best Epsilon<br/>Based on accuracy threshold<br/>select_best_epsilon.py] --> Q

    Q[Visualize Mini Results<br/>Privacy–Utility Tradeoff<br/>visualize_tradeoff.py] --> R[Mini Visualization<br/>Figures and Charts]

    P --> S{Best Epsilon Found}
    H --> S

    %% FULL — BASELINE + ADVANCE, BEST EPSILON ONLY
    S --> T[Export Full Baseline<br/>12 benchmark queries<br/>export_baseline.py]

    T --> U[Baseline Results<br/>data/full/]

    U --> CFG2[Load DP Configuration<br/>dp_config.py]

    %% FULL — BASELINE BRANCH
    CFG2 --> B_FULL[Baseline Mechanisms BEST ε<br/><br/>
        • dp_gaussian_mechanism_baseline.py<br/>
        • dp_laplace_mechanism_baseline.py<br/><br/>
        Apply with best epsilon<br/>e.g., via --epsilon BEST<br/><br/>
        Output Paths:<br/>
        • data/dp_gaussian_full/baseline/<br/>
        • data/dp_laplace_full/baseline/]

    %% FULL — ADVANCE BRANCH
    CFG2 --> A_FULL[Advance Mechanisms BEST ε<br/><br/>
        • dp_gaussian_mechanism_advance.py<br/>
        • dp_laplace_mechanism_advance.py<br/><br/>
        Apply with best epsilon<br/>e.g., via --epsilon BEST<br/><br/>
        Output Paths:<br/>
        • data/dp_gaussian_full/advance/<br/>
        • data/dp_laplace_full/advance/]

    %% FULL EVALUATION
    B_FULL --> Z
    A_FULL --> Z

    Z[Evaluate Full Results<br/>Final accuracy metrics<br/>evaluate_dp_results.py] --> AA

    AA[Full Evaluation Metrics<br/>evaluation_results/] --> AB

    AB[Visualize Full Results<br/>Final privacy–utility analysis<br/>visualize_tradeoff.py] --> AC[Full Visualization<br/>Publication-ready figures]

    R --> AD[Complete analysis]
    AC --> AD
%% COLORS
    style A fill:#e1f5ff
    style C fill:#fff3cd
    style D fill:#fff3cd
    style I fill:#d4edda
    style CFG fill:#e2e3ff
    style B_MIN fill:#f8d7da
    style A_MIN fill:#f8d7da
    style O fill:#d1ecf1
    style P fill:#d1ecf1
    style R fill:#d4edda
    style U fill:#d4edda
    style CFG2 fill:#e2e3ff
    style AC fill:#d4edda
    style AD fill:#c3e6cb
`
export default function Overview() {
    return (
        <section id="overview">
            <Section title="Project Overview">
                <div className="space-y-8">
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        The Problem: Telemetry Analytics vs. Privacy
                    </h3>
                    <p>
                        Modern hardware and software systems collect telemetry data to analyze device usage in real world environments. The data are stored in logs which reveal useful insights for analyzing and improving product performance, reliability, and feature adoption. However, these datasets often contain detailed, device-specific information that can induce privacy risks, even when user “anonymity” is guaranteed.
                    </p>

                    <p>
                        This project investigates how differential privacy can enable the release of telemetry aggregates or statistics while protecting individual privacy. We implement a differentially private query-release pipeline that introduces calibrated noise to aggregate queries while clipping each device’s contribution to the results.
                    </p>
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Telemetry Dataset
                    </h3>
                    <p>
                        The dataset used in this study contains Intel telemetry logs collected from real-world devices. Each record is associated with a unique device identifier (GUID) and describes system events, such as battery usage, power consumption, browser activity, and hardware configuration. 
                    </p>
                        The raw telemetry data contains 23 source tables, which we transform into 22 reporting tables after pre-processing via SQL build scripts. These reporting tables simplify complex SQL joins and allow the execution of 12 benchmark analytical queries used for evaluation.
                    <p>

                    </p>
                    

                    <p>
                        In our work, we create two DuckDB databases:
                    </p>
                    <ul style={{ paddingLeft: "40px", listStyleType: "square" }}>
                        <li>~5GB subsample for development</li>
                        <li>Full production database via SQL build script</li>
                    </ul>

                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Query Descriptions
                    </h3>

                    <p>
                        As noted above, we work with 12 benchmark queries which Intel's engineering teams use for actual analysis. These queries represent analytical questions which reveal information ranging from battery health by geography, battery health by CPU generation, common software trends, to most popular browser by country. These queries provide a meaningful testbed for evaluating the effectiveness of differential privacy, along with finding an optimal balance between privacy and utility.
                    </p>

                    <Image
                        src="/intel-telemetry/query_table.png"
                        alt="Query Table"
                        width={600}
                        height={600}
                    />
                    <Image
                        src="/intel-telemetry/query_wheel.png"
                        alt="Query Categorization"
                        width={700}
                        height={700}
                    />

                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Differential Privacy Pipeline
                    </h3>
                    <ul style={{ paddingLeft: "40px", listStyleType: "number" }}>
                        <li><strong>Load the raw telemetry data</strong> into DuckDB. Run the build step to produce 22 reporting tables.</li>
                        <li><strong>Run the 12 benchmark queries</strong> with per-GUID clipping in SQL to obtain the non-private baseline.</li>
                        <li><strong>Run the Laplace and Analytic Gaussian mechanism</strong> at each &epsilon; in  ε ∈ {`{`}0.01, 0.05, 0.1, 0.5, 1.0, ∞{`}`} (with ∞ representing no-noise reference) for both baseline and advanced variants.</li>
                        <li><strong>Compute utility scores</strong> by median relative error, total variation distance, and Spearman rank correlation.</li>
                        <li>Evaluation computes <strong>per-query statistics</strong> and <strong>Laplace vs. Gaussian comparison</strong> across &epsilon;.</li>
                        <li>Evaluation outputs <strong>privacy-utility tradeoff</strong> curves, pass rate, and Pareto frontier metrics.</li>
                    </ul>
                    <p>The flowchart below illustrates the complete pipeline for this study:</p>
                </div>
                <Pipeline vis={chart}/>
            </Section>
        </section>
    )
}