"use client"

import { useEffect, useState } from "react"

export default function ScrollProgress() {
  const [scrollProgress, setScrollProgress] = useState(0)

  useEffect(() => {
    const handleScroll = () => {
      const scrollTop = window.scrollY
      const docHeight =
        document.documentElement.scrollHeight -
        document.documentElement.clientHeight

      const progress = (scrollTop / docHeight) * 100
      setScrollProgress(progress)
    }

    window.addEventListener("scroll", handleScroll)
    return () => window.removeEventListener("scroll", handleScroll)
  }, [])

  return (
    <div className="fixed left-6 top-20 bottom-20 w-[5px] bg-gray-300 z-50 rounded-lg">
      <div
        className="bg-slate-400 w-full transition-all rounded-lg"
        style={{ height: `${scrollProgress}%` }}
      />
    </div>
  )
}