
# 📊 Clickstream Analytics & MapReduce Pipeline from Scratch

An end-to-end distributed data processing project implemented in Python, demonstrating the core principles of the **MapReduce programming model** from scratch using an e-commerce clickstream dataset.

The project simulates distributed data processing workflows without relying on distributed frameworks such as Hadoop or Apache Spark. It includes data auditing, data cleansing, multi-stage MapReduce jobs, independent validation using Pandas, and distributed scalability analysis.

---

## 📌 Project Overview

The objective of this project is to implement and evaluate the fundamental stages of MapReduce:

**Mapper → Shuffle / Grouping → Reducer**

The pipeline processes a sample e-commerce clickstream dataset containing **20,000 events across 8 European countries**.

All MapReduce outputs are independently validated against native, vectorized Pandas operations to ensure **100% numerical correctness**.

### Project Objectives

- Perform data auditing and quality validation.
- Clean invalid records while preserving meaningful operational data.
- Implement one-stage and multi-stage MapReduce algorithms from scratch.
- Calculate weighted statistical metrics using distributed aggregation.
- Validate all outputs independently using Pandas.
- Analyze scalability, network bottlenecks, and data skew.
- Export standardized analytical results as CSV files.

---

## 🏗️ Architecture & Pipeline Workflows

The project follows a structured, multi-layered execution pipeline:

```text
┌──────────────────────────────────────┐
│       Raw Clickstream Data           │
│            20,000 Events              │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│       A1: Data Audit & Validation    │
│                                      │
│  - Schema inspection                 │
│  - Missing value detection           │
│  - Duplicate identification          │
│  - Invalid record filtering          │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│       MapReduce Processing Engine    │
└──────────────────┬───────────────────┘
                   │
          ┌────────┼─────────┐
          │        │         │
          ▼        ▼         ▼
       ┌──────┐ ┌──────┐ ┌──────┐
       │ A2   │ │ A3   │ │ A4   │
       │Event │ │Top   │ │Mean  │
       │Counts│ │Sales │ │Latency│
       └──┬───┘ └──┬───┘ └──┬───┘
          │        │         │
          └────────┼─────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│      Independent Pandas Validation  │
│                                      │
│       100% Numerical Correctness     │
└──────────────────┬───────────────────┘
                   │
                   ▼
┌──────────────────────────────────────┐
│       A5: Scaling & System Design    │
│                                      │
│  - Shuffle / Sort Bottlenecks        │
│  - Data Skew Analysis                │
│  - Optimization Strategies           │
│  - CSV Artifact Export               │
└──────────────────────────────────────┘
```

---

## ⚙️ Core Components

### A1 — Data Audit & Cleansing

The initial stage focuses on inspecting the dataset and ensuring data quality before distributed processing.

**Operations performed:**

- Inspected dataset schema and column data types.
- Verified record uniqueness and primary key integrity.
- Identified missing values across relevant columns.
- Confirmed zero duplicate primary keys.
- Identified and filtered out **16 invalid purchase events** missing required price or quantity information.
- Preserved operational latency outliers, including measurements reaching approximately **24.8 seconds**.
- Excluded records with `NaN` latency values from latency calculations.

The objective was to maintain data integrity without unnecessarily discarding valid operational observations.

### A2 — One-Stage MapReduce: Event Counts

This job calculates the total number of clickstream events for each combination of country and device type.

**Composite key:**

```python
(country, device)
```

**Map Phase:**

Each input event generates a key-value pair:

```python
((country, device), 1)
```

**Shuffle / Grouping Phase:**

Intermediate values are grouped using Python's `defaultdict(list)`.

**Reduce Phase:**

The reducer sums the values associated with each composite key.

**Output:**

A complete **24-row aggregation matrix**, representing:

- 8 European countries.
- 3 device categories.
- 24 unique country-device combinations.

### A3 — Two-Stage MapReduce: Top Product Revenue

This job identifies the highest-revenue product in each country through a two-stage MapReduce pipeline.

**Job 1 — Revenue Aggregation**

Calculate total purchase revenue for every `(country, product_id)` combination.

```text
Key:   (country, product_id)
Value: purchase revenue
```

The reducer aggregates revenue values for each product within each country.

**Job 2 — Top Product Selection**

The intermediate output is transformed into:

```text
Key:   country
Value: (product_id, total_revenue)
```

A second reducer applies `max()` to identify the product generating the highest total revenue in each country.

**Final Output:**

One top-revenue product per country, including its corresponding total gross sales.

### A4 — Distributed Weighted Mean: Response Latency

This job calculates average system response latency for each device category.

A critical consideration is avoiding the **Average of Averages Fallacy**.

Calculating the average of individual group averages can produce incorrect results when groups have different observation counts.

**Map Phase:**

Each valid latency measurement generates a statistical tuple:

```python
(response_ms, 1)
```

**Shuffle / Grouping Phase:**

Measurements are grouped according to device category.

**Reduce Phase:**

The reducer independently aggregates:

- Total response time: `Σ response_ms`
- Total observations: `Σ count`

The final weighted mean is calculated as:

\[
\text{Mean Latency} =
\frac{\sum \text{response\_ms}}
{\sum \text{count}}
\]

This approach guarantees mathematically correct aggregation across distributed partitions.

**Final Results:**

| Device | Average Response Time |
|---|---:|
| Desktop | 292.22 ms |
| Mobile | 278.74 ms |
| Tablet | 269.67 ms |

### A5 — Scaling & Distributed System Design

The final stage examines how the implemented MapReduce pipeline could scale to significantly larger datasets and distributed environments.

**Shuffle / Sort Bottlenecks**

The Shuffle phase can become a major network bottleneck because intermediate key-value pairs must be transferred between workers according to their partitioning keys.

**Data Skew**

The dataset exhibits an uneven distribution of events:

- Greece accounts for approximately **32%** of total traffic.
- Mobile devices represent approximately **62%** of total traffic.

Such imbalances can cause certain reducers to receive significantly more data than others, resulting in stragglers and inefficient resource utilization.

**Proposed Optimization Strategies**

1. **Map-side Combiners:** Perform local aggregation before transferring intermediate results across the network, reducing communication overhead.

2. **Key Salting:** Distribute heavily skewed keys across multiple intermediate partitions to reduce reducer hotspots.

3. **Partition Pruning:** Read only the relevant partitions when querying large datasets.

4. **Columnar Storage:** Use Apache Parquet with Snappy compression to improve storage efficiency and analytical query performance.

---

## 📊 Analytical Outputs & Generated Datasets

The pipeline generates three standardized CSV artifacts.

| File | Description |
|---|---|
| `event_counts_country_device.csv` | Event counts for all 24 country-device combinations. |
| `top_product_by_country.csv` | Highest-revenue product and total gross sales for each country. |
| `avg_response_by_device.csv` | Average system response latency grouped by device category. |

### Summary of Results

| Metric | Result |
|---|---|
| Total input events | 20,000 |
| European countries | 8 |
| Device categories | 3 |
| Duplicate primary keys | 0 |
| Invalid purchase events filtered | 16 |
| Event aggregation combinations | 24 |
| Top product results | 8 |
| Latency aggregation groups | 3 |

---

## 🛠️ Tech Stack & Prerequisites

| Technology | Purpose |
|---|---|
| Python 3.10+ | Core programming language |
| Pandas | Data manipulation and independent validation |
| NumPy | Numerical operations |
| Collections (`defaultdict`) | MapReduce grouping and aggregation |
| Jupyter Notebook | Interactive development and execution |
| CSV | Input and output data format |

**Note:** The project implements the MapReduce execution logic directly in Python and does not depend on Hadoop, Apache Spark, or other distributed processing frameworks.

---

## 📂 Repository Structure

```text
Clickstream-MapReduce/
│
├── 02_CSE701_Εργασία1_Clickstream.csv
│   └── Raw clickstream input dataset
│
├── clickstream_mapreduce_pipeline.ipynb
│   └── Complete MapReduce implementation,
│       validation tests, and analysis
│
├── event_counts_country_device.csv
│   └── Aggregated event counts by country and device
│
├── top_product_by_country.csv
│   └── Highest-revenue product per country
│
├── avg_response_by_device.csv
│   └── Average latency grouped by device
│
└── README.md
    └── Project documentation
```

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/Clickstream-MapReduce.git
```

### 2. Navigate to the Project Directory

```bash
cd Clickstream-MapReduce
```

### 3. Install Dependencies

```bash
pip install pandas numpy jupyter
```

### 4. Launch Jupyter Notebook

```bash
jupyter notebook
```

### 5. Execute the Pipeline

Open:

```text
clickstream_mapreduce_pipeline.ipynb
```

Execute the notebook cells sequentially to perform:

1. Dataset inspection and cleansing.
2. One-stage MapReduce event aggregation.
3. Two-stage MapReduce revenue analysis.
4. Weighted latency calculations.
5. Independent Pandas validation.
6. CSV artifact generation.
7. Scalability and system design analysis.

---

## ✅ Validation & Correctness

A key objective of this project is ensuring that the custom MapReduce implementations produce mathematically correct results.

Each MapReduce job is independently compared against equivalent native Pandas aggregations.

The validation covers:

- Event counts by country and device.
- Total revenue by country and product.
- Highest-revenue product per country.
- Weighted average response latency by device.

The implementation achieves **100% numerical agreement** between the custom MapReduce results and the corresponding Pandas calculations.

---

## 🎓 Academic Context & Attribution

This project was developed as part of the:

**CSE701 — Big Data / Large Scale Data Analysis**

**University:** University of Macedonia (UoM)  
**Department:** Applied Informatics

**Author:** Christos Petridis (`ics24199`)  
**Instructor:** Dr. Georgios Angelidis

---

## 📌 Key Takeaways

This project demonstrates practical understanding of:

- The fundamental MapReduce programming paradigm.
- Mapper, Shuffle/Grouping, and Reducer implementation.
- Multi-stage distributed aggregation.
- Composite keys and key-value transformations.
- Weighted statistical aggregation.
- Data auditing and preprocessing.
- Independent numerical validation.
- Data skew and distributed performance bottlenecks.
- Scalability and optimization strategies for large-scale data systems.

**The project provides a from-scratch implementation of core MapReduce concepts, bridging theoretical distributed computing principles with practical Python data engineering.**
