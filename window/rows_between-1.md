Great question — this is **one of the most important (and commonly misunderstood) interview concepts in Spark window functions**.

```sql
SELECT 
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM sales;
```
***

# ✅ What happens if you REMOVE:

```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

Your query becomes:

```sql
SUM(amount) OVER (
    PARTITION BY customer_id 
    ORDER BY order_date
)
```

***

# 🚨 Key Answer

👉 Spark applies a **default window frame** when you don’t specify one.

### ✅ Default Behavior:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

***

# 🔥 Difference: `ROWS` vs `RANGE`

| Feature            | ROWS                          | RANGE                        |
| ------------------ | ----------------------------- | ---------------------------- |
| Based on           | Physical row position         | Value of ORDER BY column     |
| Handles duplicates | Processes each row separately | Groups equal values together |
| Predictability     | ✅ Deterministic               | ⚠ Can behave unexpectedly    |

***

# 🎯 What changes in your query?

### ✅ With ROWS (your original query)

```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

✔ Proper **row-by-row cumulative sum**

***

### ⚠ Without ROWS (default RANGE applies)

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

👉 Spark groups rows with **same `order_date` value**

***

# 📊 Example to Understand the Difference

## Input Data

| customer\_id | order\_date | amount |
| ------------ | ----------- | ------ |
| A            | 2024-01-01  | 100    |
| A            | 2024-01-02  | 200    |
| A            | 2024-01-02  | 300    |

***

## ✅ Using ROWS

```sql
SUM(amount) OVER (
 PARTITION BY customer_id
 ORDER BY order_date
 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

### Output:

| date | amount | running\_total                |
| ---- | ------ | ----------------------------- |
| 01   | 100    | 100                           |
| 02   | 200    | 300                           |
| 02   | 300    | 600 ✅ (row-wise accumulation) |

***

## ⚠ Using Default (RANGE)

```sql
SUM(amount) OVER (
 PARTITION BY customer_id
 ORDER BY order_date
)
```

### Output:

| date | amount | running\_total |
| ---- | ------ | -------------- |
| 01   | 100    | 100            |
| 02   | 200    | 600 ❗          |
| 02   | 300    | 600 ❗          |

👉 Why?

Because:

* Both rows have same `order_date = 2024-01-02`
* RANGE treats them as **one group**
* So both rows get same cumulative sum

***

# 🧠 Interview Insight (⚡ very important)

### ✅ Always explicitly define frame when using ORDER BY

Because:

* Default = `RANGE` (can cause incorrect results)
* `ROWS` = expected behavior for most cumulative calculations

***

# ✅ When to use what?

### ✅ Use ROWS when:

* Running totals ✅
* Moving averages ✅
* Any row-wise calculation ✅

✅ Recommended best practice:

```sql
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

***

### ✅ Use RANGE when:

* You *intentionally* want grouping by value
* Example:
  * Same timestamp aggregation
  * Bucketing scenarios

***

# 🚀 Final Takeaway

👉 Without `ROWS BETWEEN...`, Spark uses:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

👉 This can:

* ✅ Work fine if values are unique
* ❗ Break logic if duplicates exist in ORDER BY column

***

# ✅ Golden Rule (Interview Answer)

> “Always specify ROWS explicitly for cumulative calculations to avoid unexpected behavior due to RANGE default, especially when ORDER BY column has duplicates.”

***
# ----------------------------------------------------------------------------------------------

Good follow-up — this is where understanding **default window frames really matters for correctness**.

***

# ✅ Your Query (explicit ROWS)

```sql
AVG(price) OVER (
    PARTITION BY stock_id 
    ORDER BY date
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
)
```

👉 This is a **fixed-size moving window (last 3 rows)**  
✔ Always correct for moving averages  
✔ Works row-by-row

***

# 🤔 What if you REMOVE the frame?

```sql
AVG(price) OVER (
    PARTITION BY stock_id 
    ORDER BY date
)
```

***

# 🚨 Default behavior again

Spark uses:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

***

# 🔥 Critical Difference for Moving Average

## ✅ With ROWS (your current query)

* Takes **exactly last 3 rows**
* Independent of duplicate dates
* ✅ Correct sliding window

***

## ❌ Without ROWS (default RANGE)

* Acts like **cumulative average**
* Groups **same date values together**
* ❌ NOT a moving average anymore

***

# 📊 Example to see the problem

## Input:

| stock\_id | date | price |
| --------- | ---- | ----- |
| S1        | 01   | 100   |
| S1        | 02   | 200   |
| S1        | 02   | 300   |
| S1        | 03   | 400   |

***

# ✅ With ROWS BETWEEN 2 PRECEDING AND CURRENT ROW

```text
Row-wise sliding window of 3 rows
```

| date | price | moving\_avg |
| ---- | ----- | ----------- |
| 01   | 100   | 100         |
| 02   | 200   | 150         |
| 02   | 300   | 200 ✅       |
| 03   | 400   | 300 ✅       |

***

# ❌ Without ROWS (RANGE default)

```text
Cumulative avg grouped by date values
```

| date | price | avg   |
| ---- | ----- | ----- |
| 01   | 100   | 100   |
| 02   | 200   | 200 ❗ |
| 02   | 300   | 200 ❗ |
| 03   | 400   | 250 ❗ |

***

# ⚠ Why this is dangerous

### 1. ❌ Moving window logic breaks

Instead of "last 3 rows", you get:
→ "all rows up to current date"

***

### 2. ❌ Duplicate dates distort results

Rows with same `date` get same average

***

### 3. ❌ Interview trap

If you forget ROWS, interviewer will say:
👉 "This is NOT a moving average"

***

# ✅ Key Insight (important)

### Moving calculations REQUIRE `ROWS`

Because:

* You care about **row count**, not values
* RANGE can’t enforce "last N rows"

***

# ✅ DSL equivalent reminder

```scala
val windowSpec = Window
  .partitionBy("stock_id")
  .orderBy("date")
  .rowsBetween(-2, 0)

df.withColumn("moving_avg", avg("price").over(windowSpec))
```

***

# 🧠 Interview-Ready Answer

> “If we don’t specify ROWS, Spark defaults to RANGE, which calculates cumulative aggregates based on ORDER BY values rather than fixed row counts. This breaks moving average logic because RANGE doesn’t guarantee a fixed-size window and groups duplicate values.”

***

# ✅ Golden Rule

| Use Case             | Correct Frame   |
| -------------------- | --------------- |
| Running totals       | ROWS            |
| Moving average       | ✅ ROWS          |
| Rank                 | No frame needed |
| Value-based grouping | RANGE           |

***

# 🚀 Final Takeaway

👉 For your query:

✅ Correct:

```sql
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
```

❌ Wrong (default):

* Becomes cumulative average
* Breaks sliding window logic

***

# -----------------------------------------------------------------------------------

Great—let’s apply the **same concept to `SUM`** for your moving window query.

***

# ✅ Query with `SUM` (Correct Moving Sum)

```sql
SELECT 
    stock_id,
    date,
    price,
    SUM(price) OVER (
        PARTITION BY stock_id 
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_sum
FROM stocks;
```

***

# ✅ DSL Equivalent

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec = Window
  .partitionBy("stock_id")
  .orderBy("date")
  .rowsBetween(-2, 0)

df.withColumn("moving_sum", sum("price").over(windowSpec))
```

***

# 🎯 What this does

👉 For each row:

* Takes **current row + previous 2 rows**
* Calculates **sum over last 3 rows**

***

# 📊 Example

## Input:

| stock\_id | date | price |
| --------- | ---- | ----- |
| S1        | 01   | 100   |
| S1        | 02   | 200   |
| S1        | 02   | 300   |
| S1        | 03   | 400   |

***

## ✅ Output with ROWS

| date | price | moving\_sum |
| ---- | ----- | ----------- |
| 01   | 100   | 100         |
| 02   | 200   | 300         |
| 02   | 300   | 600 ✅       |
| 03   | 400   | 900 ✅       |

👉 Always **last 3 rows sum**

***

# ❌ If you REMOVE `ROWS BETWEEN`

```sql
SUM(price) OVER (
    PARTITION BY stock_id 
    ORDER BY date
)
```

👉 Default:

```sql
RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
```

***

## ❌ Output (WRONG for moving sum)

| date | price | sum    |
| ---- | ----- | ------ |
| 01   | 100   | 100    |
| 02   | 200   | 600 ❗  |
| 02   | 300   | 600 ❗  |
| 03   | 400   | 1000 ❗ |

***

# 🔥 Why wrong?

👉 RANGE:

* Groups **same date values**
* Acts like **cumulative sum**
* NOT a sliding window

***

# ✅ Key Differences (SUM specifically)

| Scenario        | Result                                     |
| --------------- | ------------------------------------------ |
| With ROWS       | ✅ Fixed-size moving sum                    |
| Without ROWS    | ❌ Cumulative sum (wrong for moving window) |
| Duplicate dates | ❌ RANGE merges them                        |

***

# 🧠 Interview Answer

> “For moving sum, we must use ROWS BETWEEN because we need a fixed number of rows. Without it, Spark defaults to RANGE, which computes cumulative sum based on value groups rather than row count, leading to incorrect results especially with duplicate ORDER BY values.”

***

# ✅ Golden Rule

| Use Case           | Correct Frame                              |
| ------------------ | ------------------------------------------ |
| Moving SUM         | ✅ ROWS BETWEEN N PRECEDING AND CURRENT ROW |
| Running TOTAL      | ✅ ROWS UNBOUNDED PRECEDING                 |
| Default (no frame) | ❌ Avoid                                    |

***

# 🚀 Final Takeaway

👉 Your SUM version behaves exactly like AVG:

* ✅ With ROWS → correct moving window
* ❌ Without ROWS → becomes cumulative & groups duplicates

***
