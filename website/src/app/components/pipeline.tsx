"use client"

import mermaid from "mermaid"
import { useEffect, useRef} from "react"

type Props = {
    vis: string
}

export default function Pipeline({ vis }: Props) {
    const ref = useRef<HTMLDivElement>(null)

    useEffect(() => {
        mermaid.initialize({
            startOnLoad: false,
            theme: "default",
        })
        if (ref.current) {
            mermaid.render("mermaid-pipeline", vis).then(({ svg }) => {
                ref.current!.innerHTML = svg
            })
        }
    }, [vis])

    return <div ref={ref} />
}