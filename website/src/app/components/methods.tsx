import Section from "./section"
import Image from "next/image"

export default function Methods() {
    return (
        <section id="methods">
            <Section title="Methods">
                <div className="space-y-8">
                
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Differential Privacy
                    </h3>
                    <p>
                        Differential privacy provides a mathematical framework, guaranteeing that the output of a query is unaffected by the inclusion or exclusion of a single individual record from the dataset.
                    </p>

                    <p>
                        Mathematically, differential privacy is represented by the following equation:
                    </p>
                    <div className="equation font-semibold flex justify-center">
                        Pr[M(D) ∈ S] ≤ e<sub>&epsilon;</sub> · Pr[M(D′) ∈ S]
                    </div>
                    <ul style={{ paddingLeft: "40px", listStyleType: "square" }}>
                            <li>Where &epsilon; is the privacy parameter which controls the privacy-utility trade-off</li>
                            <li>Smaller values of &epsilon; result in stronger privacy at the cost of utility</li>
                    </ul>
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Contribution Bounding
                    </h3>
                    <p>
                        To account for the disproportionate impact of some devices towards aggregates, we bound or clip each device’s contribution prior to computing statistics. For a value x:
                    </p>
                    <div className="equation font-semibold flex justify-center">
                        x<sub>clipped</sub> = min(x, C)
                    </div>
                    <p>
                        Where C is a predefined clipping bound. This helps limit global sensitivity across the dataset and ensures that “outlier” devices can’t singlehandedly influence results.
                    </p>

                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Privacy Mechanisms
                    </h3>
                
                    <p>
                        In order to guarantee individual privacy, we add noise to the query outputs using two differential privacy mechanisms. A brief description of each mechanism can be seen in the figure below:
                    </p>
                    <Image
                        src="/intel-telemetry/dp_cards.png"
                        alt="DP Mechanisms"
                        width={1200}
                        height={1200}
                    />
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Implementation Variants
                    </h3>
                    <ul style={{ paddingLeft: "40px", listStyleType: "square" }}>
                            <li><strong>Baseline:</strong> Recomputes the noise scale fresh for every (query, column, &epsilon;) triple at run time.</li>
                            <li><strong>Advanced:</strong> Builds a scale cache once per (sensitivity, &epsilon;) pair and looks it up during release; typically 2-4x faster with identical outputs and privacy guarantees.</li>
                    </ul>
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Privacy Budget
                    </h3>
                    <p>
                        When multiple queries are released, privacy loss accumulates through sequential composition. We evaluate the system across multiple privacy budgets:
                    </p>
                    <div className="equation font-semibold flex justify-center">
                        ε ∈ {`{`}0.01, 0.05, 0.1, 0.5, 1.0, ∞{`}`}
                    </div>
                    <p>
                        In doing so, we can analyze the impact of different privacy levels on utility.
                    </p>
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Evaluation Metrics
                    </h3>
                    <p>
                        The table below includes three evaluation metrics, along with the corresponding queries for each one.
                    </p>
                    <Image
                        src="/intel-telemetry/eval_metrics.png"
                        alt="Evaluation Metrics"
                        width={1200}
                        height={1200}
                    />
                </div>
            </Section>
        </section>
    )
}