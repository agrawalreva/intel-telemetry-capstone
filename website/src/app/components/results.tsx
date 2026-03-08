import Section from "./section"
import Image from "next/image"

export default function Results() {
    return (
        <section id="results">
            <Section title="Results">
                <div className="space-y-8">
                    <p>
                        Experiments were run on mini (subsample, ~5GB) and full databases for both the baseline and advanced variants.
                    </p>
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Baseline vs. Advanced Pass Rates
                    </h3>

                    <p>
                        Across all &epsilon; values, the Laplace mechanism consistently achieves equal or higher pass rates than the Gaussian mechanism, indicating better overall utility under the same privacy budgets. Compared to the baseline implementation, the advanced counterpart reaches higher pass rates earlier, meaning that more queries satisfy the accuracy thresholds at lower privacy budgets.
                    </p>
                    <figure className="flex flex-col items-center">
                        <Image
                            src="/intel-telemetry/04_pass_rate_mini_baseline.png"
                            alt="Baseline pass rate results"
                            width={800}
                            height={600}
                        />
                        <figcaption className="text-sm text-gray-600 mt-2">
                            Pass rate results for the baseline variant on the mini dataset.
                        </figcaption>
                    </figure>
                    <figure className="flex flex-col items-center">
                        <Image
                            src="/intel-telemetry/04_pass_rate_mini_advanced.png"
                            alt="Baseline pass rate results"
                            width={800}
                            height={600}
                        />
                        <figcaption className="text-sm text-gray-600 mt-2">
                            Pass rate results for the advanced variant on the mini dataset.
                        </figcaption>
                    </figure>
                    <p>
                        At &epsilon;=0.01 few queries pass the threshold, while at &epsilon;=∞, around 80% of queries pass on mini, with some queries missing due to structural qualities or small groups. The advanced variant achieves a similar or better pass rate at &epsilon;=0.5 compared to the baseline at &epsilon;=1.0.
                    </p>
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Privacy-Utility Tradeoff Curves
                    </h3>
                    <p>
                        As &epsilon; increases, noise decreases and query accuracy improves across all three metrics.
                    </p>
                    <ul style={{ paddingLeft: "40px", listStyleType: "square" }}>
                        <li>Median relative error drops</li>
                        <li>Total variation distance shrinks</li>
                        <li>Spearman rank correlation rises</li>
                    </ul>
                    <p>
                        The advanced variant (bottom) reaches acceptable utility at a lower &epsilon;=∞ in comparison to the baseline (top), thus achieving the same accuracy while ensuring stronger privacy.
                    </p>
                    <figure className="flex flex-col items-center">
                        <Image
                            src="/intel-telemetry/01_privacy_utility_curves_mini_baseline.png"
                            alt="Baseline privacy-utility tradeoff"
                            width={800}
                            height={600}
                        />
                        <figcaption className="text-sm text-gray-600 mt-2">
                            Privacy-utility tradeoff visualization for the baseline variant.
                        </figcaption>
                    </figure>
                    <figure className="flex flex-col items-center">
                        <Image
                            src="/intel-telemetry/01_privacy_utility_curves_mini_advanced.png"
                            alt="Advanced privacy-utility tradeoff"
                            width={800}
                            height={600}
                        />
                        <figcaption className="text-sm text-gray-600 mt-2">
                            Privacy-utility tradeoff visualization for the advanced variant.
                        </figcaption>
                    </figure>
                    <p>
                        At low &epsilon; error is high, and as &epsilon; --&gt; ∞, utility approaches the non-private baseline. Advance curves plateau earlier, meaning comparable utility is achieved at stronger privacy.
                    </p>
                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Mechanism Comparison
                    </h3>
                    <p>
                        The Laplace Mechanism achieves lower error and lower distribution distortion compared to the Gaussian at each &epsilon; value tested, across both RE and TVD metrics. The gap narrows as &epsilon; increases, but the Laplace maintains an advantage throughout in both baseline and advanced implementations.
                    </p>
                    <figure className="flex flex-col items-center">
                        <Image
                            src="/intel-telemetry/03_mechanism_comparison_mini_baseline.png"
                            alt="Baseline mechanism comparison"
                            width={800}
                            height={600}
                        />
                        <figcaption className="text-sm text-gray-600 mt-2">
                            Mechanism comparison for the baseline variant.
                        </figcaption>
                    </figure>
                    <figure className="flex flex-col items-center">
                        <Image
                            src="/intel-telemetry/03_mechanism_comparison_mini_advanced.png"
                            alt="Advanced mechanism comparison"
                            width={800}
                            height={600}
                        />
                        <figcaption className="text-sm text-gray-600 mt-2">
                            Mechanism comparison for the advanced variant.
                        </figcaption>
                    </figure>

                    <h3 className="text-2xl md:text-2xl font-semibold text-slate-800 mb-2">
                        Optimal Epsilon Selection
                    </h3>

                    <p>
                        Each mechanism was evaluated across the epsilon grid and computed a utility-preservation score relative to the non-private baseline at &epsilon=∞. The recommended epsilon is selected as the smallest value for which the average utility preservation across all queries reaches at least 80%. These results are summarized below:
                    </p>
                    <Image
                        src="/intel-telemetry/best_epsilon.png"
                        alt="Best Epsilon"
                        width={800}
                        height={600}
                    />
                    <p>
                        The table above includes negative utility values since the results are compared to the non-private baseline where &epsilon;=∞. It’s important to note that this result should not raise concerns; on the mini database, no ε in the grid reached the 80% mean preservation target, so the selection logic correctly falls back to the ε with the highest score (best available). Both chosen operating points (ε = 1.0 for the baseline, ε = 0.5 for the advanced variant) are fully differentially private; only ε = ∞ is non-private.
                    </p>
                </div>
            </Section>
        </section>
    )
}