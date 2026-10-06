# Data Skewness in PySpark: Detection and Correct Mitigation

This repository contains a Databricks-compatible Jupyter notebook, [`solution.ipynb`](solution.ipynb), that demonstrates how to **detect** and **correctly mitigate data skew** in Apache Spark / PySpark.

## Overview

Using a synthetic 10-million-row DataFrame with a deliberately skewed key distribution (90 % of rows share the same key, `A`), the notebook walks through:

1. **Detecting skew** in keys and partitions.
2. Running a **baseline skewed aggregation** with `groupBy("skew_key")`.
3. Fixing skew with **full salting**.
4. A more efficient **targeted salting** variant that salts only the hot key.
5. Explaining why a plain `repartition(N)` is **not** a real fix for key-based aggregation skew.
6. **Validating** that optimized results match the baseline exactly.

> **Note:** Adaptive Query Execution (AQE) skew handling helps mainly with **joins**, not with skewed aggregations such as `groupBy(skew_key)`. This notebook focuses on the aggregation case.

## Repository Contents

| File | Description |
|------|-------------|
| `solution.ipynb` | Complete PySpark notebook with synthetic data generation, skew detection, baseline, salting solutions, and validation. |

## Requirements

- Apache Spark 3.x with PySpark
- A Spark-enabled notebook environment (Databricks, JupyterLab, VS Code, etc.)
- Python 3.x

## How to Run

1. Clone the repository:

   ```bash
   git clone https://github.com/KacprusJeden/data_skewness_solution.git
   cd data_skewness_solution
   ```

2. Open `solution.ipynb` in your notebook environment connected to a Spark cluster.
3. Run the cells from top to bottom.

## Notebook Structure

| Section | What It Shows |
|---------|---------------|
| **0. Generate Skewed DataFrame** | Creates 10 M rows, assigns 90 % to key `A`, and repartitions by `skew_key` to make the imbalance visible. |
| **1. Detect the Skew** | Displays key distribution and per-partition record counts. |
| **2. Baseline Aggregation** | Runs a standard `groupBy("skew_key")` to demonstrate the straggler effect. |
| **3. Full Salting** | Adds a random `salt`, aggregates by `(skew_key, salt)`, then rolls the result back to `skew_key`. |
| **4. Targeted Salting** | Salts only the detected hot key (`A`) while keeping other keys in a single subgroup, lowering overhead. |
| **5. Why `repartition(50)` Is Not Enough** | Shows that increasing partition count alone does not solve hot-key aggregation skew. |
| **6. Validation & Summary** | Uses `exceptAll` to confirm that salted results are identical to the baseline. |

## Key Techniques

### Full Salting

Break every key into `SALT_BUCKETS` sub-keys and aggregate in two stages:

```python
SALT_BUCKETS = 10

df_salted = df.withColumn(
    "salt",
    F.pmod(F.hash("id"), F.lit(SALT_BUCKETS)).cast("int")
)

stage1 = df_salted.groupBy("skew_key", "salt").agg(
    F.sum("value").alias("sum_value"),
    F.count("*").alias("record_count")
)

result = stage1.groupBy("skew_key").agg(
    F.sum("sum_value").alias("total_value"),
    (F.sum("sum_value") / F.sum("record_count")).alias("avg_value"),
    F.sum("record_count").alias("record_count")
)
```

### Targeted Salting

Salt only the hot key to reduce unnecessary shuffle overhead:

```python
HOT_KEY = "A"
SUB_GROUPS = 10

df_targeted = df.withColumn(
    "sub_group",
    F.when(
        F.col("skew_key") == F.lit(HOT_KEY),
        F.pmod(F.hash("id"), F.lit(SUB_GROUPS)).cast("int")
    ).otherwise(F.lit(0))
)
```

## Why This Matters

Data skew can make an otherwise well-tuned Spark job run orders of magnitude slower because a single task ends up processing most of the data. Salting is a deterministic, widely-used technique that redistributes that load across multiple tasks without changing the final business result.

## License

This project is provided as-is for educational purposes.
