"use client"

import { useState } from "react"

type ContactInfo = {
    name: string
    email: string
    github: string
}

export default function Contact({ name, email, github }: ContactInfo) {
    const [open, setOpen] = useState(false)

    return (
        <>
            <span
                onClick={() => setOpen(true)}
                className="cursor-pointer underline hover:text-blue-400"
            >{name}
            </span>

            {
                open && (
                    <div
                        className="fixed inset-0 flex items-center justify-center bg-black-300 z-[100]"
                        onClick={() => setOpen(false)}
                    >
                        <div
                            className="bg-white text-gray-800 p-6 rounded-xl shadow-lg w-fit"
                            onClick={(e) => e.stopPropagation()}
                        >
                            <h3 className="text-xl font-bold mb-2">{name}</h3>
                            <p className="mt-2 text-blue-500">
                                <a href={`mailto:${email}`}>{email}</a>
                            </p>
                            <p className="text-gray-800">{github}</p>
                            <button
                                className="mt-4 text-sm text-gray-800 hover:text-black"
                                onClick={() => setOpen(false)}
                            >
                                Close
                            </button>
                        </div>
                    </div>
                )}
        </>
    )
}