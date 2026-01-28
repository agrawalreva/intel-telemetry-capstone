# X Balancing Privacy and Utility: Differentially Private-Synthetic Data Generation for Intel Telemetry

# Introduction
Intel has established itself as a tech industry giant due to their influence in chip manufacturing for the last few decades. The company develops a massive amount of chips--CPUs, GPUs, NPUs, etc.--which are integrated into PCs for the end-user. These PCs are manufactured by Original Equipment Manufacturers (OEMs)--Lenovo, HP, Dell, ASUS, etc.--who create the bulk of the equipment and distribute to customers. 

Since Intel distributes their products to OEMs, they cannot collect data about device usage directly from the PCs. As a result, Intel developed a way to obtain data through the processors (CPUs) themselves. Intel collects large-scale telemetry data from PCs using their processors to track application usage, performance metrics, and simple diagnostic information. To give a simplified example, given the consent of user $u$, Intel would collect data saying that $u$ opened application $x$ at time $t$. They would collect similar information specifying what time application $x$ was closed. In total, Intel's processors collect data for the following events: application open, application close, file save, system reset, error. This information allows Intel to address issues with their products and understand customer behavior.

While Intel designs safeguards to ensure "anonymity," user information is not entirely immune to attacks. In particular, reconstruction attacks can be applied to re-identify individuals by analyzing repeated usage patterns and matching user IDs to device IDs. This presents a challenge for Intel to guarantee user privacy while still collecting meaningful data.

Differential Privacy (DP) offers a mathematical framework to address this challenge. Highly simplified, Differential Privacy ensures that the inclusion or exclusion of any one user's data would have a minimal effect on the results of any analysis. In other words, imagine two datasets; one of these datasets includes my data, while the other does not. Differential Privacy gives a mathematical promise that performing analysis on both datasets would provide indistinguishable results.

---
# Methods
*insert methods details*

---
# Results
*insert results details*

---
# Conclusion
*insert conclusion details*

---
# Outlook
*insert outlook details*