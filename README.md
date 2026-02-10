# intel-telemetry-capstone

**Team & Mentor:** 
| Name             | Role    | GitHub       |
|------------------|---------|--------------|
| Dhruv Patel      | Student | [@PDhruv09](https://github.com/PDhruv09)       |
| Reva Agrawal     | Student | [@agrawalreva](https://github.com/agrawalreva) |
| Jordan Lambino   | Student | [@jordanlambino](https://github.com/jordanlambino)  |
| Yu-Xiang Wang    | Advisor | [@yuxiangw](https://github.com/yuxiangw)|

A complete data pipeline for implementing and evaluating differential privacy mechanisms on Intel system telemetry data, from raw data acquisition through privacy-preserving analytics.

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Quick Start Guide](#quick-start-guide)
- [Pipeline Architecture](#pipeline-architecture)
- [Build Instructions](#build-instructions)
- [Usage Examples](#usage-examples)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This project implements a complete differential privacy pipeline for analyzing Intel telemetry data. The pipeline supports:

- **Data ingestion** from Globus endpoints
- **Database creation** with DuckDB (full and sampled versions)
- **Reporting layer** with 22 pre-aggregated analytics tables
- **Differential privacy mechanisms** (Laplace, Gaussian)
- **Privacy-utility evaluation** framework
- **22 benchmark queries** for testing DP mechanisms

**Note**: The database our analsis was completed on is confidential, so only the resulting plots are available. You may run the project on a synthetic demo dataset with argument test
