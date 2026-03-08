"use client"

import { useState } from "react"

export default function TableOfContents() {
    const [open, setOpen] = useState(false)
    return (
        <>
            <nav className="hidden lg:block fixed left-10 top-1/2 -translate-y-1/2 z-40">
                <ul className="space-y-24 text-md font-semibold">

                    <li>
                        <a href="#title" className="hover:text-blue-500">
                            Home
                        </a>
                    </li>

                    <li>
                        <a href="#overview" className="hover:text-blue-500">
                            Project Overview
                        </a>
                    </li>

                    <li>
                        <a href="#results" className="hover:text-blue-500">
                            Results
                        </a>
                    </li>

                    <li>
                        <a href="#methods" className="hover:text-blue-500">
                            Methods
                        </a>
                    </li>

                    <li>
                        <a href="#discussion" className="hover:text-blue-500">
                            Discussion
                        </a>
                    </li>
                </ul>
            </nav>


            <div className="lg:hidden fixed top-4 left-4 z-50  text-slate-800">
                <button
                    onClick={() => setOpen(!open)}
                    className="bg-white shadow-md px-4 py-2 rounded-md font-semibold"
                >
                    Sections {open ? "▲" : "▼"}
                </button>
                {open && (
                    <nav className="mt-2 bg-white shadow-md rounded-md p-4">
                        <ul className="space-y-4 text-sm font-semibold">
                            <li>
                                <a href="#title" onClick={() => setOpen(false)}>
                                    Home
                                </a>
                            </li>
                            <li>
                                <a href="#overview" onClick={() => setOpen(false)}>
                                    Project Overview
                                </a>
                            </li>
                            <li>
                                <a href="#results" onClick={() => setOpen(false)}>
                                    Results
                                </a>
                            </li>
                            <li>
                                <a href="#methods" onClick={() => setOpen(false)}>
                                    Methods
                                </a>
                            </li>
                            <li>
                                <a href="#discussion" onClick={() => setOpen(false)}>
                                    Discussion
                                </a>
                            </li>
                        </ul>
                    </nav>
                )}
            </div>
        </>
    )
}