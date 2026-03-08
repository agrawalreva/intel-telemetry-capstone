import Section from "./section"

export default function DifferentialPrivacy() {
    return (
        <section id="dp">
            <Section title="Differential Privacy">
                <div className="space-y-8">
                    <p>
                        Differential Privacy -- mathematical definition with high-level description. Interactive visualization
                    </p>

                    <p>
                        Differential privacy (DP) is a mathematical definition of what it means for a randomized computation to limit the harm to an individual from participating in a dataset. The idea, due to Dwork and colleagues Dwork et al. (2006); Dwork and Roth (2014), is that the distribution of the mechanism’s output should change only slightly when one person’s data is added or removed. Thus, an adversary who sees the output cannot confidently tell whether any one individual was present, so participation carries bounded privacy risk.
                    </p>

                    <p>
                        From a high-level perspective, imagine two datasets. One of these datasets includes the data of person X, while the other does not. Differential privacy gives a mathematical promise that performing analysis on both datasets would provide indistinguishable results.
                    </p>
                </div>
            </Section>
        </section>
    )
}