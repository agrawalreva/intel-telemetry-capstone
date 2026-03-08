import { DM_Sans } from "next/font/google"

const titleFont = DM_Sans({
    subsets: ["latin"],
    weight: ["500"]
})

type SectionProps = {
  title: string
  children: React.ReactNode
}

export default function Section({ title, children }: SectionProps) {
  return (
    <section className= "pt-10 snap-start bg-gray-200">
      <div className="sticky max-w-6xl mx-auto top-2 bg-slate-700 py-6 px-12 rounded-lg">
        <h2 className="max-w-3xl mx-auto bg-slate-600 text-white text-2xl font-bold px-12 py-8 rounded-lg shadow-md">
            {title}
        </h2>
      </div>

      <div className="bg-gray-100 text-black py-12 px-10 shadow-inner">
        <div className="max-w-4xl mx-auto px-6 text-lg leading-relaxed">
            {children}
        </div>
      </div>
    </section>
  )
}