import "./globals.css";
import { Inter, DM_Sans } from "next/font/google";


const titleFont = DM_Sans({
  subsets: ["latin"]
})

const bodyFont = Inter({ subsets: ["latin"]})

export const metadata = {
  title: "Balancing Privacy and Utility: Query Release in Telemetry Data",
  description: "Applying differential privacy mechanisms to query release for telemetry data logs.",
  icons: {
    icon: "/icon.png",
  }
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body
        className={bodyFont.className}>
        {children}
      </body>
    </html>
  );
}
