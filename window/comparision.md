Below is a **complete, structured comparison of ALL major PySpark SQL window functions**, exactly how interviewers (especially **TCS / service‑based companies**) expect you to understand them.

This covers **purpose, behavior, differences, traps, examples, and when to use what**.

***

# ✅ Full Window Functions Comparison in PySpark (Master Guide)

***

## 1️⃣ What Are Window Functions? (Quick Recall)

Window functions:

*   Work on a **window (group of rows)**
*   Do **not collapse rows** like `groupBy`
*   Require an **OVER() clause**
*   Often need **PARTITION BY + ORDER BY**

```python
Window.partitionBy(...).orderBy(...)
```

***

## 2️⃣ Window Function Categories

| Category           | Functions                                               |
| ------------------ | ------------------------------------------------------- |
| Ranking            | `row_number`, `rank`, `dense_rank`, `percent_rank`      |
| Aggregation        | `sum`, `avg`, `min`, `max`, `count`                     |
| Value (Navigation) | `first_value`, `last_value`, `lag`, `lead`, `nth_value` |
| Distribution       | `cume_dist`, `ntile`                                    |

***

# 🔹 RANKING WINDOW FUNCTIONS (MOST ASKED)

***

## ✅ 1. `row_number()`

### Purpose

Gives a **unique sequential number** to each row within a partition.

### Behavior

*   No gaps
*   Always increases by 1

### Example

```python
row_number().over(w)
```

### Output example

```text
1, 2, 3, 4
```

✅ Use when:

*   You want **exactly one row**
*   De‑duplication
*   Top‑N per group

***

## ✅ 2. `rank()`

### Purpose

Assigns rank with **gaps** if ties exist.

### Behavior

```text
Values: 100, 100, 90
Ranks:  1,   1,   3
```

### Example

```python
rank().over(w)
```

✅ Use when:

*   You want **competition ranking**
*   Ties must affect subsequent ranks

***

## ✅ 3. `dense_rank()`

### Purpose

Ranking **without gaps**

### Behavior

```text
Values: 100, 100, 90
Ranks:  1,   1,   2
```

### Example

```python
dense_rank().over(w)
```

✅ Use when:

*   Ranking categories
*   No gaps allowed

***

### 🔥 Ranking Comparison (INTERVIEW FAVORITE)

| Function    | Gaps  | Unique | Use Case         |
| ----------- | ----- | ------ | ---------------- |
| row\_number | ❌ No  | ✅ Yes  | Dedup, Top‑N     |
| rank        | ✅ Yes | ❌ No   | Sports ranking   |
| dense\_rank | ❌ No  | ❌ No   | Category ranking |

***

# 🔹 AGGREGATE WINDOW FUNCTIONS

***

## ✅ 4. `sum`, `avg`, `min`, `max`, `count`

### Purpose

Compute running or partition‑level aggregates **without collapsing rows**

### Example

```python
sum("sales").over(w)
```

### Output

Each row shows aggregated value.

✅ Use when:

*   Running totals
*   Partition totals
*   Rolling metrics

***

### ⚠️ Important

*   **ORDER BY** → running total
*   No ORDER BY → same value for all rows in partition

***

# 🔹 VALUE / NAVIGATION WINDOW FUNCTIONS (VERY IMPORTANT)

***

## ✅ 5. `first_value()`

### Purpose

Returns **first value** in ordered window.

✅ Respects order  
✅ Needs ORDER BY

Example:

```python
first_value("sales").over(w)
```

✅ Common use:

*   Opening balance
*   First transaction
*   Initial status

***

## ✅ 6. `last_value()` (BIG TRAP 🚨)

### Purpose

Returns **last value in window frame**

⚠️ Default frame ends at CURRENT ROW → often wrong

✅ Correct usage:

```python
rowsBetween(
    Window.unboundedPreceding,
    Window.unboundedFollowing
)
```

✅ Or safer:

```python
ORDER BY desc + first_value
```

✅ Use case:

*   Latest value
*   Closing balance

***

## ✅ 7. `lag()`

### Purpose

Access **previous row’s value**

### Example

```python
lag("sales", 1).over(w)
```

### Output meaning

```text
Current row ← previous row value
```

✅ Use case:

*   Difference calculation
*   Trend analysis
*   Time series

***

## ✅ 8. `lead()`

### Purpose

Access **next row’s value**

```python
lead("sales", 1).over(w)
```

✅ Use case:

*   Forecast comparison
*   Event sequencing

***

## ✅ 9. `nth_value()`

### Purpose

Fetch **Nth value** from window

```python
nth_value("sales", 2).over(w)
```

✅ Less used, but interviewer favorite

***

### 🔥 Navigation Comparison Table

| Function     | Direction | Common Use       |
| ------------ | --------- | ---------------- |
| first\_value | Start     | Initial record   |
| last\_value  | End       | Latest record    |
| lag          | Backward  | Trends           |
| lead         | Forward   | Look‑ahead       |
| nth\_value   | Fixed     | Positional fetch |

***

# 🔹 DISTRIBUTION WINDOW FUNCTIONS

***

## ✅ 10. `cume_dist()`

### Purpose

Returns **cumulative distribution** between 0 and 1

```python
cume_dist().over(w)
```

✅ Use:

*   Percentile‑style analysis

***

## ✅ 11. `percent_rank()`

### Purpose

Relative rank between 0 and 1

```text
(rank − 1) / (rows − 1)
```

✅ Use:

*   Statistical ranking

***

## ✅ 12. `ntile(n)`

### Purpose

Divide rows into **n equal buckets**

```python
ntile(4).over(w)
```

✅ Use:

*   Quartiles
*   Deciles
*   Segmentation

***

# 🔥 COMPLETE WINDOW FUNCTION COMPARISON (ONE VIEW)

| Function     | Order Needed | Collapses Rows | Typical Use    |
| ------------ | ------------ | -------------- | -------------- |
| row\_number  | ✅            | ❌              | Dedup          |
| rank         | ✅            | ❌              | Ranking        |
| dense\_rank  | ✅            | ❌              | Ranking        |
| sum/avg      | ⚠️ Optional  | ❌              | Metrics        |
| first\_value | ✅            | ❌              | Start state    |
| last\_value  | ✅            | ❌              | Latest state   |
| lag          | ✅            | ❌              | Differences    |
| lead         | ✅            | ❌              | Forward values |
| ntile        | ✅            | ❌              | Buckets        |
| cume\_dist   | ✅            | ❌              | Distribution   |

***

## 3️⃣ Common Interview Traps (MUST KNOW)

❌ Using `last_value` without full frame  
❌ Using `min/max` instead of first/last chronologically  
❌ Expecting window functions to reduce rows  
❌ Forgetting ORDER BY  
❌ Large partitions causing performance issues

***

## ✅ Interview‑Perfect Summary (Say This)

> “Window functions allow row‑level analytics over partitions without collapsing data. Ranking functions assign position, navigation functions access relative rows, and aggregation functions compute running or partition metrics. Understanding window frames is critical, especially for last\_value.”

***

## ✅ Memory Tip

> **DESC + first\_value** → safest way to get latest record

***
