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

If you want, I can show:
✅ tricky cases with timestamps  
✅ RANGE vs ROWS performance differences  
✅ real production bugs caused by this

Just tell me 👍
