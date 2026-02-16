---
title: "Balancing Privacy and Utility: Differentially Private Synthetic Data Generation for Intel Telemetry"
description: "Reva Agrawal, Jordan Lambino, Dhruv Patel | Yu-Xiang Wang"
layout: default
---

# Introduction.
Intel has established itself as a tech industry giant due to their influence in chip manufacturing for the last few decades. The company develops a massive amount of chips--CPUs, GPUs, NPUs, etc.--which are integrated into PCs for the end-user. These PCs are manufactured by Original Equipment Manufacturers (OEMs)--Lenovo, HP, Dell, ASUS, etc.--who create the bulk of the equipment and distribute to customers. 

Since Intel distributes their products to OEMs, they cannot collect data about device usage directly from the PCs. As a result, Intel developed a way to obtain data through the processors (CPUs) themselves. Intel collects large-scale telemetry data from PCs using their processors to track application usage, performance metrics, and simple diagnostic information. To give a simplified example, given the consent of user $u$, Intel would collect data saying that $u$ opened application $x$ at time $t$. They would collect similar information specifying what time application $x$ was closed. In total, Intel's processors collect data for the following events: application open, application close, file save, system reset, error. This information allows Intel to address issues with their products and understand customer behavior.

While Intel designs safeguards to ensure "anonymity," user information is not entirely immune to attacks. In particular, reconstruction attacks can be applied to re-identify individuals by analyzing repeated usage patterns and matching user IDs to device IDs. This presents a challenge for Intel to guarantee user privacy while still collecting meaningful data.

Differential Privacy (DP) offers a mathematical framework to address this challenge. Highly simplified, Differential Privacy ensures that the inclusion or exclusion of any one user's data would have a minimal effect on the results of any analysis. In other words, imagine two datasets; one of these datasets includes my data, while the other does not. Differential Privacy gives a mathematical promise that performing analysis on both datasets would provide indistinguishable results.

---
# Methods
Our pipeline begins with raw Intel telemetry data accessed through Globus, consisting of 23 source tables spread across two schemas (university_analysis_pad and university_prod). To enable efficient development, we first built two versions of the database using DuckDB: a subsample database (~5 GB, capped at 200 MB per table) for rapid iteration, and a full database (~3.6 TB) reserved for production runs. On top of these raw tables, we constructed a reporting layer of 22 pre-aggregated tables using a SQL build script (00_build_reporting_tables.sql), which resolves joins, normalizes column names, and structures the data for analytics queries. 

From this reporting layer, we wrote 12 benchmark queries covering a range of analytical stories  battery usage by geography and CPU family, display device vendor market share, browser popularity by country, OS-level MODS blockers, RAM utilization distributions, persona-level web category usage, and process power rankings  and exported their results to CSV files as our privacy-free baseline. With the baseline established, we implemented two differential privacy mechanisms by hand (without external DP libraries), a Gaussian mechanism providing (ε, δ)-DP and a Laplace mechanism providing pure ε-DP, both following user-level privacy semantics where adding or removing one device (guid) bounds the sensitivity. Each mechanism loops over 11 epsilon values (0.01 to ∞) using a fixed random seed for reproducibility, adds calibrated noise to the numeric columns of each query result, and post-processes outputs by clamping negatives and re-normalising percentage columns. 

Critically, each query is evaluated using the metric that best matches its analytical intent: z-score + IOU for queries identifying anomalous groups (Q1, Q2, Q5, Q7, Q9), Total Variation Distance for percentage distributions (Q4, Q10, Q11), Kendall's Tau for rankings (Q3, Q12), Top-1 Accuracy for winner-per-group queries (Q6), and KL Divergence for multi-dimensional distributions (Q8). At present, the full codebase including the database creation scripts, SQL reporting layer, baseline export, and both DP mechanism files is complete and tested on the subsample database; the evaluation scripts that will select the best epsilon and generate privacy-utility tradeoff visualizations are the immediate next step before running the final pipeline on the full dataset.us

---
# Results
*insert results details*

---
# Conclusion
*insert conclusion details*

---
# Outlook
*insert outlook details*