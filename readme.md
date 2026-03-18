# Data Engineering Assessment

A hands-on showcase of three core data engineering competencies: **dynamic schema evolution**, **data quality validation**, and **scalable data modelling** — all built with PySpark and Python.

---

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Components](#components)
  - [1. Dynamic Schema Evolution](#1-dynamic-schema-evolution)
  - [2. Data Quality Validation](#2-data-quality-validation)
  - [3. Scalable Data Modelling](#3-scalable-data-modelling)
- [Dataset](#dataset)
- [Setup & Installation](#setup--installation)
- [Running the Notebooks](#running-the-notebooks)

---

## Overview

This project demonstrates production-ready data engineering patterns applied to a real-world retail dataset (~540K transactions). It is structured into three independent modules, each tackling a distinct challenge commonly faced in modern data pipelines.

| Module | Key Skill |
|---|---|
| Schema Evolution | Backward-compatible schema changes in PySpark |
| Data Quality | Automated validation with Great Expectations |
| Data Modelling | Star-schema design for a taxi-service domain |

---

## Tech Stack

- **Python** 3.9
- **Apache Spark / PySpark** 3.5
- **Great Expectations** 0.18
- **Pandas** 1.5
- **Jupyter Notebooks**
- **SQL** (analytical KPI queries)

---

## Project Structure

```
Data_Engineer_Assessment/
│
├── src/
│   ├── utils.py                                        # Reusable PySpark helper functions
│   ├── Data_Schema_Enforcement_with_PySpark.ipynb      # Module 1 – Schema Evolution
│   ├── Data_Quality_with_PySpark.ipynb                 # Module 2 – Data Quality
│   └── Data_Modelling.ipynb                            # Module 3 – Data Modelling
│
├── out/                                                # Output directory for reports & CSVs
├── Online Retail.csv                                   # Source dataset (UCI Online Retail)
├── requirements.txt
└── README.md
```

---

## Components

### 1. Dynamic Schema Evolution

**Notebook:** `src/Data_Schema_Enforcement_with_PySpark.ipynb`
**Helper functions:** `src/utils.py`

Implements a lightweight schema versioning system on top of PySpark DataFrames. The pipeline handles two of the most common real-world schema change scenarios without breaking downstream consumers:

- **New field addition** — dynamically appends a column with a configurable default value and data type (e.g. adding a `City` field to an existing retail dataset).
- **Field type update** — safely casts an existing column to a new type (e.g. promoting `Quantity` from `IntegerType` to `DoubleType`).

Schema snapshots are stored in a versioned dictionary (`schema_store`), providing full traceability of every structural change across pipeline runs.

**Key design decisions:**
- Schema mutations are guarded by existence checks to prevent redundant operations.
- Each schema version is persisted alongside the data it describes, enabling rollback and audit.
- The evolved DataFrame is written out as CSV for downstream consumption.

---

### 2. Data Quality Validation

**Notebook:** `src/Data_Quality_with_PySpark.ipynb`
**Helper functions:** `src/utils.py`

Integrates **Great Expectations** with PySpark to provide automated, rule-based data quality checks. A suite of seven expectations is run against the Online Retail dataset on each pipeline execution, and results are summarised in a structured quality report saved to disk.

**Expectations implemented:**

| Expectation | Description |
|---|---|
| Table Column Count | Column count falls within an expected range |
| Table Row Count | Row count falls within an expected range |
| Column Existence | A required column is present in the schema |
| Ordered Column List | Columns appear in the expected order |
| Column Value Range | All values in a column are within bounds |
| Column Min/Max Range | Column min and max are within bounds |
| Unique Column Values | No duplicate values exist in a column |

Results are written to `data_quality_report.csv` for reporting and monitoring purposes.

---

### 3. Scalable Data Modelling

**Notebook:** `src/Data_Modelling.ipynb`

Designs a scalable relational data model for a **taxi service** platform and defines analytical KPIs backed by SQL queries.

**Entities:**

| Entity | Key Attributes |
|---|---|
| `Users` | UserID, UserType, Name, Email, Phone |
| `Trips` | TripID, UserID, DriverID, VehicleID, StartLocationID, EndLocationID, StartTime, EndTime, Fare, Distance |
| `Vehicles` | VehicleID, DriverID, Model, Registration |
| `Locations` | LocationID, Name, Coordinates, Address |

**Analytical KPIs with SQL:**

- Average trip distance per day / week / month
- Total revenue generated per driver
- Busiest hours of the day for trips
- Average passenger waiting time
- Most frequently visited pickup locations
- Completed vs. cancelled trip ratio
- Driver utilisation rate

---

## Dataset

The **Online Retail Dataset** from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/352/online+retail) is used for testing and demonstration.

- ~540,000 transactions from a UK-based online retailer (2010–2011)
- 8 columns: `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`

---

## Setup & Installation

**Prerequisites:** Python 3.9, Java 8+ (required by Spark), Jupyter

1. **Clone the repository**
   ```bash
   git clone https://github.com/anashas/Data_Engineer_Assessment.git
   cd Data_Engineer_Assessment
   ```

2. **Create and activate a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate        # macOS / Linux
   venv\Scripts\activate           # Windows
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Start Jupyter**
   ```bash
   jupyter notebook
   ```

---

## Running the Notebooks

Open and run the notebooks in the following order from the `src/` directory:

| Step | Notebook | Purpose |
|---|---|---|
| 1 | `Data_Schema_Enforcement_with_PySpark.ipynb` | Schema evolution demo |
| 2 | `Data_Quality_with_PySpark.ipynb` | Data quality validation |
| 3 | `Data_Modelling.ipynb` | Data model & KPI queries |

> **Note:** Ensure `Online Retail.csv` is present in the root directory before running the notebooks. Output files will be saved to the `out/` directory.
