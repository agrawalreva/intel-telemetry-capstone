import Contact from "./contact"

type TitleProps = {
    title: string
    subtitle?: string
}

export default function Title({ title, subtitle }: TitleProps) {
    return (
        <section id="title">
            <header className="snap-start bg-slate-700 text-white py-20 text-center">
                <h1 className="text-4xl font-bold max-w-4xl mx-auto px-8">
                    {title}
                </h1>
                {subtitle && (
                    <p className="text-lg mt-6 opacity-90">
                        {subtitle}
                    </p>
                )}
                <section className="flex justify-center items-center py-4">
                    <Contact
                        name="Reva Agrawal"
                        email="ragrawal@ucsd.edu"
                        github="https://github.com/agrawalreva"
                    />
                    <span className="mx-2 text-gray-400 text-sm">·</span>
                    <Contact
                        name="Jordan Lambino"
                        email="jlambino@ucsd.edu"
                        github="https://github.com/jordanlambino"
                    />
                    <span className="mx-2 text-gray-400 text-sm">·</span>
                    <Contact
                        name="Dhruv Patel"
                        email="dhp005@ucsd.edu"
                        github="https://github.com/PDhruv09"
                    />
                </section>

                <section className="flex justify-center items-center">
                    <Contact
                        name="Yu-Xiang Wang"
                        email="yuxiangw@ucsd.edu"
                        github="https://cseweb.ucsd.edu/~yuxiangw/"
                    />
                    <span className="mx-2 text-gray-400 text-sm">·</span>
                    <Contact
                        name="Bijan Arbab"
                        email="barbab@ucsd.edu"
                        github="https://datascience.ucsd.edu/people/bijan-arbab/"
                    />
                </section>
                <hr className="my-6 border-white-100 opacity-500 max-w-3xl mx-auto" />
                <a
                    href="https://github.com/agrawalreva/intel-telemetry-capstone"
                    className="hover:underline"
                    target="_blank"
                    rel="noopener noreferrer"
                    >
                        GitHub
                </a>
                <span className="mx-2 text-gray-400 text-sm">·</span>
                <a 
                    href="https://github.com/agrawalreva/intel-telemetry-capstone/blob/main/report/B15_DSC_Capstone_Final_Report.pdf"
                    className="hover:underline"
                    target="_blank"
                    rel="noopener noreferrer"
                    >Report
                </a>
                <span className="mx-2 text-gray-400 text-sm">·</span>
                <a
                    href="https://github.com/agrawalreva/intel-telemetry-capstone/blob/main/poster/Capstone%20Showcase%20Poster.png"
                    className="hover:underline"
                    target="_blank"
                    rel="noopener noreferrer"
                    >Poster
                </a>
            </header>
        </section>
    )
}