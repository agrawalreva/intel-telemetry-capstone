import Section from "./section"

export default function Discussion() {
    return (
        <section id="discussion">
            <Section title="Discussion">
                <div className="space-y-8">
                
                <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                    Key Takeaways
                </h3>
                 <ul style={{ paddingLeft: "40px", listStyleType: "square" }}>
                    <li>Laplace Mechanism outperforms Gaussian Mechanism on low-dimensional scalar queries for the majority of &epsilon; values, winning on 8 out of 10 queries.</li>
                    <li>Gaussian Mechanism performs well for high-dimensional outputs (Q7, Q8, Q9), as its geometry features suppress noise more efficiently than the Laplace.</li>
                    <li>At &epsilon;=1.0 on the full database, most queries pass their utility thresholds with Laplace achieving median RE (relative error) &lt; 0.03 for Q1, Q2, Q9 and Spearman p &ge; 0.99 for Q3.</li>
                    <li>The advanced variant achieves comparable accuracy to baseline at &epsilon;=0.5 vs. &epsilon;=1.0, delivering similar utility results while providing a stronger privacy guarantee.</li>
                </ul>
                <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                    Practical Implications
                </h3>
                <p>
                    For Intel telemetry analysis (and other similar work), releasing the 12 query answers at &epsilon;=1.0 per query provides a concrete deployment point. Most queries achieve pass thresholds, and the guarantee is auditable from the published sensitivity and mechanism. Queries such as Q4 (vendor percentage), Q6 (browser winner), and Q12 (ranked processes on mini) can be improved further through increased budget allocation, implementing mechanism-specific changes, or particular tuning for releases. In practice, our pipeline is reproducible (fixed seed, versioned scripts) and can be re-run on updated data or with different &epsilon; allocations.
                </p> 
                <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                    Limitations
                </h3>
                <p>
                    A clear limitation lies in the fact that our project analyzes 12 out of the 22 total Intel queries. Beyond the fixed set of queries we were given, there are still a number of possible queries which future engineers may wish to explore.
                </p>
                <p>
                    An additional limitation involves dataset scale, which could be primarily attributed to resource constraints. Given that the size of the original dataset exceeded 22TB, our group needed to narrow our scope and apply privacy frameworks to a significantly smaller subsample. With access to more time and compute resources, we may be able to stratify our findings.
                </p>
                <p>
                    An important constraint to consider is the number of DP mechanisms which were explored in our work. Additional approaches such as the Exponential Mechanism exist which could theoretically provide high privacy and high utility for analytics.
                </p>

                <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                    Future Outlook
                </h3>
                <p>
                    Our work can be extended to explore adaptive privacy budgets, improved query sensitivity analysis, and deployment in large production telemetry systems. Moreover, this project implements basic composition, whereas advanced composition might tighten the composed &epsilon; for the same per-query budgets.
                </p>
                <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                    Acknowledgements
                </h3>
                <p>
                    We would like to thank Dr. Yu-Xiang Wang for his mentorship and guidance, along with industry fellow Dr. Bijan Arbab for his continued support throughout our work.
                </p>
                <p>
                    To reproduce our project or extend our work, visit our {" "}
                    <a href="https://github.com/agrawalreva/intel-telemetry-capstone"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="underline hover:text-gray-700"
                    >GitHub repository</a> and follow the README.md for instructions.
                </p>
                </div>
            </Section>
        </section>
    )
}