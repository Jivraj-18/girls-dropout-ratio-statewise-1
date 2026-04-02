# The Hidden Anchor: Analysis of Girls' Dropout Rates in India

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python Version](https://img.shields.io/badge/python-3.12-brightgreen)
![Data Source](https://img.shields.io/badge/data-UDISE%2B-orange)

An end-to-end analytical pipeline and self-contained data story investigating what truly keeps girls enrolled in Indian secondary schools. 

This project ingests 6 years (2018–2024) of Unified District Information System for Education Plus (UDISE+) data across 37 states and union territories. It applies rigorous statistical analysis (including permutation-tested Pearson correlations) to test common policy assumptions against actual retention data.

**The Key Finding:** While infrastructure investments (like functional girls' toilets) have reached saturation and show weak correlation to dropout rates, the **share of female teachers** exhibits a massive, statistically significant protective effect (-0.57 correlation) on keeping adolescent girls in school.

---

## 📖 The Data Story

The culmination of this repository's analysis is compiled into a single, dependency-free interactive HTML document:

**[`index.html`](index.html)**

The file acts as a compelling, Malcolm Gladwell-style data narrative. It uses lightweight, embedded Chart.js visualizations that require zero external data-fetching or backend execution, offering a rapid, elegant, and shareable overview of the findings.

---

## 🛠️ Repository Architecture

This codebase is split into an orchestrated data pipeline (`pipeline/`) and executable runners (`scripts/`).

```text
new_repo/
├── index.html                 # The compiled, interactive Data Story UI
├── README.md                  # Project documentation (you are here)
├── requirements.txt           # Python dependency locks
├── run.sh                     # One-click execution shell script
│
├── pipeline/                  # Modular Python analysis library
│   ├── io_utils.py            # Read/Write JSON and tabular handlers
│   ├── json_utils.py          # JSON serialization helpers
│   ├── metadata.py            # UDISE table definitions
│   ├── run_config.py          # Configuration and paths
│   ├── stats_utils.py         # Specialized ML and stats calculations
│   ├── udise_loaders.py       # High-level dataset loading APIs
│   └── udise_readers.py       # Lower-level CSV/DataFrame parsing 
│
└── scripts/                   # Executable application entry points
    ├── acquire_data.py        # Fetch source data directly
    ├── analyze.py             # Top-level script to calculate correlations
    ├── clean_preprocess.py    # Formatting and missing value pipelines
    ├── contract_check.py      # Output expectations validation
    ├── feature_extract.py     # Isolate statistical features for ML
    ├── schema_discovery.py    # Auto-detection of file structures
    └── test_all.py            # Test suite orchestration
```

---

## 🚀 Quickstart

**1. Clone the repository and install dependencies:**
```bash
git clone git@github.com:Jivraj-18/girls-dropout-ratio-statewise-1.git
cd girls-dropout-ratio-statewise-1
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**2. Run the end-to-end analytical pipeline:**
```bash
# This fetches data, extracts schemas, processes features, 
# and computes the correlation values automatically into the /out directory.
./run.sh
```

**3. View the Results:**
Simply double-click `index.html` or open it in your favorite modern browser to see the complete Data Story.

---

## 📊 Methodological Notes

* **Data Sourcing:** This repository actively excludes direct copies of the data via `.gitignore` to maintain source-agnostic reproducibility. Data is fetched automatically during the `acquire_data.py` stage.
* **Correlations:** Statistical significance is determined using Absolute Pearson bounds, cross-verified with permutation testing across $n=36$ state rows to prevent over-fitting in high-noise macro-demographic datasets.
* **Open Source Frameworks:** The data manipulation heavily leverages `pandas` and `scipy`, whilst the frontend data story relies on `Tailwind CSS` and `Chart.js` requested via robust CDNs.
