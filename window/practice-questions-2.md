Great — let’s level you up beyond basics. I’ll give you:

1. ✅ **30 NEW real-world window aggregation scenarios** (different from previous)
2. ✅ **5 interview-style tricky problems**
3. ✅ **Mini dataset + expected outputs for practice**

***

# 🚀 🔹 30 Additional Window Aggregation Scenarios

***

## 🟢 Advanced Analytical Use Cases

### 1. Detect first purchase date per customer

```sql
MIN(order_date) OVER (PARTITION BY customer_id)
```

```scala
df.withColumn("first_purchase", min("order_date").over(Window.partitionBy("customer_id")))
```

***

### 2. Detect last purchase date per customer

Same pattern with `MAX(order_date)`

***

### 3. Days since last transaction

```sql
DATEDIFF(
    order_date,
    LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date)
)
```

```scala
df.withColumn("days_gap",
  datediff(col("order_date"), lag("order_date",1).over(win))
)
```

***

### 4. Detect gaps (> N days inactivity)

Use lag + conditional flag

***

### 5. Rolling sum excluding current row

```sql
SUM(amount) OVER (
 PARTITION BY id ORDER BY date
 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
)
```

***

### 6. Median within partition

```sql
PERCENTILE_APPROX(salary, 0.5) OVER (PARTITION BY dept)
```

***

### 7. Percentile bucket assignment

```sql
NTILE(4) OVER (PARTITION BY dept ORDER BY salary)
```

***

### 8. Running max (peak value till now)

```sql
MAX(value) OVER (PARTITION BY id ORDER BY time)
```

***

### 9. Running minimum

***

### 10. Normalize value (z-score style)

```sql
(value - AVG(value) OVER w) / STDDEV(value) OVER w
```

***

### 11. Detect duplicates using count

```sql
COUNT(*) OVER (PARTITION BY email)
```

***

### 12. Flag first occurrence only

Use `ROW_NUMBER() = 1`

***

### 13. Sessionization (basic)

Detect new session when gap > threshold

***

### 14. Cumulative distinct count (approx)

```sql
APPROX_COUNT_DISTINCT(user_id) OVER (...)
```

***

### 15. Identify streaks (consecutive days)

Combine `lag` + grouping logic

***

### 16. Calculate retention cohort index

Compare event date vs first event date

***

### 17. Calculate time between first and last event

***

### 18. Running ratio (e.g., success rate)

```sql
SUM(success) OVER w / COUNT(*) OVER w
```

***

### 19. Top N per group with filter

Use `ROW_NUMBER` then filter top N

***

### 20. Compare current value vs previous max

```sql
value - MAX(value) OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)
```

***

### 21. Dense ranking for ties

Use `DENSE_RANK()`

***

### 22. Unique count per sliding window

Approx functions in window

***

### 23. Flag record if equals group max

```sql
CASE WHEN value = MAX(value) OVER (...) THEN 1 END
```

***

### 24. Proportional distribution across group

Divide by partition sum

***

### 25. Weighted moving average

Custom logic with lag multipliers

***

### 26. Identify churn (no activity for X days)

***

### 27. Reverse cumulative sum

```sql
ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
```

***

### 28. Compare current vs next record (lead)

```sql
LEAD(value) OVER (...)
```

***

### 29. Change detection (delta)

```sql
value - LAG(value) OVER (...)
```

***

### 30. Rank change over time (rank drift)

Compute rank at two timestamps and compare

***

# 🧠 🔥 Interview-Style Tricky Problems

***

## ✅ Problem 1: Second highest salary per department

👉 Trap: duplicates

```sql
SELECT *
FROM (
  SELECT *,
         DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) rnk
  FROM emp
) t
WHERE rnk = 2
```

***

## ✅ Problem 2: Find employees earning above department average

```sql
SELECT *
FROM (
  SELECT *,
         AVG(salary) OVER (PARTITION BY dept) avg_sal
  FROM emp
)
WHERE salary > avg_sal
```

***

## ✅ Problem 3: Find continuous login streaks

👉 Trick: use `date - row_number()`

***

## ✅ Problem 4: Running total reset when condition met

👉 Partition dynamically using cumulative flag trick

***

## ✅ Problem 5: Top 3 products contributing 80% of revenue

👉 Combine:

* running sum
* total sum
* percentage filter

***

# 📊 🔥 Mini Dataset for Practice

***

## ✅ Sample Input: `sales`

| customer\_id | date       | amount |
| ------------ | ---------- | ------ |
| A            | 2024-01-01 | 100    |
| A            | 2024-01-02 | 200    |
| A            | 2024-01-05 | 150    |
| B            | 2024-01-01 | 300    |
| B            | 2024-01-03 | 100    |

***

## ✅ Practice 1: Running Total

### Expected Output:

| customer\_id | date | amount | running\_total |
| ------------ | ---- | ------ | -------------- |
| A            | 01   | 100    | 100            |
| A            | 02   | 200    | 300            |
| A            | 05   | 150    | 450            |
| B            | 01   | 300    | 300            |
| B            | 03   | 100    | 400            |

***

## ✅ Practice 2: Previous Transaction Gap

| customer\_id | date | gap\_days |
| ------------ | ---- | --------- |
| A            | 01   | null      |
| A            | 02   | 1         |
| A            | 05   | 3         |

***

## ✅ Practice 3: Contribution %

| customer\_id | amount | contribution |
| ------------ | ------ | ------------ |
| A            | 100    | 0.22         |
| A            | 200    | 0.44         |
| A            | 150    | 0.33         |

***

## ✅ Practice 4: Rank within Customer

| customer\_id | amount | rank |
| ------------ | ------ | ---- |
| A            | 200    | 1    |
| A            | 150    | 2    |
| A            | 100    | 3    |

***

## ✅ Practice 5: Moving Avg (2 window)

| customer\_id | date | avg |
| ------------ | ---- | --- |
| A            | 01   | 100 |
| A            | 02   | 150 |
| A            | 05   | 175 |

***

# ✅ Pro Tip (Interview Insight)

Interviewers usually test:

* ✅ Difference: `GROUP BY vs WINDOW`
* ✅ Frame clause understanding (`ROWS vs RANGE`)
* ✅ Rank vs Dense\_Rank vs Row\_Number
* ✅ Handling duplicates & ties
* ✅ Performance (partition skew)

***
