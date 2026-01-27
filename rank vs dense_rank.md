In **PySpark**, both `rank()` and `dense_rank()` are window functions used to assign ordering numbers to rows **within a window partition** — but they behave differently **when there are ties**.

Here’s the simple difference:

***

# ✅ **Difference Between `rank()` and `dense_rank()` in PySpark**

## **1. `rank()`**

*   Assigns the **same rank** to tied values.
*   **Skips subsequent ranks** after a tie (creates gaps).

### Example:

Values: `10, 20, 20, 30`

| Value | rank()                                            |
| ----- | ------------------------------------------------- |
| 10    | 1                                                 |
| 20    | 2                                                 |
| 20    | 2                                                 |
| 30    | 4  ← (rank 3 is skipped because of the tie above) |

***

## **2. `dense_rank()`**

*   Assigns the **same rank** to tied values.
*   **Does NOT skip** the next rank (no gaps).

### Example:

Values: `10, 20, 20, 30`

| Value | dense\_rank() |
| ----- | ------------- |
| 10    | 1             |
| 20    | 2             |
| 20    | 2             |
| 30    | 3  ← (no gap) |

***

# 🧪 PySpark Example Code

```python
from pyspark.sql.window import Window
import pyspark.sql.functions as F

df = spark.createDataFrame(
    [(1, 10), (2, 20), (3, 20), (4, 30)],
    ["id", "value"]
)

windowSpec = Window.orderBy("value")

df.withColumn("rank", F.rank().over(windowSpec)) \
  .withColumn("dense_rank", F.dense_rank().over(windowSpec)) \
  .show()
```

Output:

    +---+-----+----+----------+
    | id|value|rank|dense_rank|
    +---+-----+----+----------+
    |  1|   10|   1|         1|
    |  2|   20|   2|         2|
    |  3|   20|   2|         2|
    |  4|   30|   4|         3|
    +---+-----+----+----------+

***

# 🎯 **Quick Summary**

| Function       | Ties?              | Gaps in ranking?     | Use case                                            |
| -------------- | ------------------ | -------------------- | --------------------------------------------------- |
| `rank()`       | Same rank for ties | **Yes**, gaps appear | When you want “competition style” ranking (1,2,2,4) |
| `dense_rank()` | Same rank for ties | **No gaps**          | When you want sequential ranking (1,2,2,3)          |

***
***

# 🧠 **Super Simple Way to Remember `rank()` vs `dense_rank()`**

## 🎉 **Think of a competition!**

### 🥇 **`rank()` = "Competition Ranking"**

Like in a sports race:

*   If two people tie for 2nd place, both get **rank 2**.
*   The **next person jumps to rank 4** (because the 3rd place is skipped).

👉 **Gaps appear.**

**Example:**

    1st, 2nd, 2nd, 4th

***

### 🧮 **`dense_rank()` = "Compact Ranking"**

Imagine giving ranks **without skipping any numbers**.

*   If two people tie for 2nd place, both get **rank 2**.
*   The next person gets **rank 3**.

👉 **No gaps.**

**Example:**

    1st, 2nd, 2nd, 3rd

***

# 🎯 **One-Line Memory Trick**

### ✔ **`rank()` = ranking with *holes***

### ✔ **`dense_rank()` = ranking with *no holes***

Or even simpler:

> **rank() → leaves gaps  
> dense\_rank() → no gaps**

***

# 🌟 A Fun Visual

Consider the numbers:

    10, 20, 20, 30

### `rank()` gives:

    1, 2, 2, 4   ← gap after tie

### `dense_rank()` gives:

    1, 2, 2, 3   ← no gap

***

If you want, I can make a small diagram or mnemonic card you can save for quick recall!

Great! Here's a **simple, visual mnemonic card** you can save — it makes remembering the difference between `rank()` and `dense_rank()` super easy:

***

# 🧠 **Mnemonic Card: Rank vs Dense Rank (PySpark)**

    RANK()        → Leaves Gaps  
    DENSE_RANK()  → No Gaps

***

## 🎯 **Visual Memory Trick**

### 🏁 Imagine a Race

    Times: 10, 20, 20, 30

### 🥇 Using `rank()`  → "Competition-Style Ranking"

    1   2   2   4
        ↑ gap here

### 🧮 Using `dense_rank()` → "Compact Ranking"

    1   2   2   3

***

# 🔥 **Ultra‑Short Slogan**

    rank()       → numbers jump
    dense_rank() → numbers don't jump

or

    rank()       → holes
    dense_rank() → no holes

***
