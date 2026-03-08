export default function Intro() {
    return (
        <div className="bg-gray-100 text-black py-12 px-10 shadow-inner">
            <div className="max-w-4xl mx-auto px-6 text-lg leading-relaxed space-y-8">
                <p>
                    What does it mean for data to be private? Some may believe that simply omitting an individual’s identification (name, SSN, email, etc.) provides a privacy guarantee to that user. The common misconception is that these data points are the sole identifiers of participants in a dataset. In reality, each data point in a dataset, whether related to demographics, behavior, transactions, or education, comprises part of an individual’s identity. 
                </p>
                <p>
                    <a 
                    href="https://github.com/agrawalreva/intel-telemetry-capstone/blob/main/report/privacy_netflix.pdf"
                    className="text-blue-500 hover:underline"
                    target="_blank"
                    rel="noopener noreferrer"
                    >Privacy attacks</a> can leverage data to reconstruct the entire “profile” of an individual, exposing information ranging from their address, to their finances, to their browsing history; in short, these data attacks can reveal information which seemed to be “private.” 
                </p>
                <p>
                    Our project explores the application of privacy mechanisms for telemetry data logs. Hardware and software vendors such as Intel rely on telemetry to understand how products behave in the field. However, releasing aggregate statistics can still enable re-identification when combined with auxiliary information. The tension between the need for actionable insights and the obligation to protect individuals is the problem we address.
                </p>
            </div>
        </div>
    )
}