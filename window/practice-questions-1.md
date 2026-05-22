Aggregation functions with **window techniques** in Spark are used when you want to compute aggregates *without collapsing rows* (unlike `GROUP BY`). They are powerful for analytics, ranking, running totals, and comparisons within partitions.

Below are **common real-world scenarios** with both **Spark SQL** and **DataFrame API (DSL)** code.

***

# 🔹 1. Running Total (Cumulative Sum)

### ✅ Scenario:

Calculate cumulative sales per customer over time.

### 🔸 Spark SQL

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

### 🔸 DSL (DataFrame API)

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec = Window
  .partitionBy("customer_id")
  .orderBy("order_date")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("running_total", sum("amount").over(windowSpec))
```

***

# 🔹 2. Moving Average (Sliding Window)

### ✅ Scenario:

Compute a 3-day moving average of stock prices.

### 🔸 Spark SQL

```sql
SELECT 
    stock_id,
    date,
    price,
    AVG(price) OVER (
        PARTITION BY stock_id 
        ORDER BY date
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg
FROM stocks;
```

### 🔸 DSL

```scala
val windowSpec = Window
  .partitionBy("stock_id")
  .orderBy("date")
  .rowsBetween(-2, 0)

df.withColumn("moving_avg", avg("price").over(windowSpec))
```

***

# 🔹 3. Rank within Group

### ✅ Scenario:

Find top-performing employees per department.

### 🔸 Spark SQL

```sql
SELECT 
    emp_id,
    dept,
    salary,
    RANK() OVER (
        PARTITION BY dept 
        ORDER BY salary DESC
    ) AS rank
FROM employees;
```

### 🔸 DSL

```scala
val windowSpec = Window
  .partitionBy("dept")
  .orderBy(desc("salary"))

df.withColumn("rank", rank().over(windowSpec))
```

***

# 🔹 4. Percentage Contribution

### ✅ Scenario:

Find what % each product contributes to total category sales.

### 🔸 Spark SQL

```sql
SELECT 
    category,
    product,
    sales,
    sales / SUM(sales) OVER (PARTITION BY category) AS contribution_pct
FROM product_sales;
```

### 🔸 DSL

```scala
val windowSpec = Window.partitionBy("category")

df.withColumn(
  "contribution_pct",
  col("sales") / sum("sales").over(windowSpec)
)
```

***

# 🔹 5. Row-wise Comparison (Lag/Lead with Aggregate Context)

### ✅ Scenario:

Compare current day's sales with previous day.

### 🔸 Spark SQL

```sql
SELECT 
    store_id,
    date,
    sales,
    LAG(sales, 1) OVER (
        PARTITION BY store_id 
        ORDER BY date
    ) AS prev_day_sales
FROM store_sales;
```

### 🔸 DSL

```scala
val windowSpec = Window
  .partitionBy("store_id")
  .orderBy("date")

df.withColumn("prev_day_sales", lag("sales", 1).over(windowSpec))
```

***

# 🔹 6. Max/Min within Partition (Without Grouping)

### ✅ Scenario:

Get highest salary in each department along with every employee row.

### 🔸 Spark SQL

```sql
SELECT 
    emp_id,
    dept,
    salary,
    MAX(salary) OVER (PARTITION BY dept) AS max_salary
FROM employees;
```

### 🔸 DSL

```scala
val windowSpec = Window.partitionBy("dept")

df.withColumn("max_salary", max("salary").over(windowSpec))
```

***

# 🔹 7. Deduplication (Keep Latest Record)

### ✅ Scenario:

Keep the latest record per user.

### 🔸 Spark SQL

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY user_id 
               ORDER BY updated_at DESC
           ) AS rn
    FROM users
) t
WHERE rn = 1;
```

### 🔸 DSL

```scala
val windowSpec = Window
  .partitionBy("user_id")
  .orderBy(desc("updated_at"))

df.withColumn("rn", row_number().over(windowSpec))
  .filter(col("rn") === 1)
```

***

# 🔹 8. Cumulative Count

### ✅ Scenario:

Count number of transactions over time per account.

### 🔸 Spark SQL

```sql
SELECT 
    account_id,
    txn_date,
    COUNT(*) OVER (
        PARTITION BY account_id 
        ORDER BY txn_date
    ) AS txn_count
FROM transactions;
```

### 🔸 DSL

```scala
val windowSpec = Window
  .partitionBy("account_id")
  .orderBy("txn_date")

df.withColumn("txn_count", count("*").over(windowSpec))
```

***

# 🔹 9. Difference from Group Average

### ✅ Scenario:

Find how much each employee deviates from department average salary.

### 🔸 Spark SQL

```sql
SELECT 
    emp_id,
    dept,
    salary,
    salary - AVG(salary) OVER (PARTITION BY dept) AS diff_from_avg
FROM employees;
```

### 🔸 DSL

```scala
val windowSpec = Window.partitionBy("dept")

df.withColumn(
  "diff_from_avg",
  col("salary") - avg("salary").over(windowSpec)
)
```

***

# 🔹 10. First/Last Value in Window

### ✅ Scenario:

Get first transaction amount per customer timeline.

### 🔸 Spark SQL

```sql
SELECT 
    customer_id,
    txn_date,
    amount,
    FIRST_VALUE(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY txn_date
    ) AS first_txn
FROM transactions;
```

### 🔸 DSL

```scala
val windowSpec = Window
  .partitionBy("customer_id")
  .orderBy("txn_date")

df.withColumn("first_txn", first("amount").over(windowSpec))
```

***

# ✅ Summary

Use **window aggregations** when:

* You need **aggregates + row-level detail**
* You want **ordered calculations** (running totals, lag/lead)
* You need **group-level metrics without collapsing rows**

***
