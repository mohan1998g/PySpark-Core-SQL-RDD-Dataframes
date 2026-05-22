Window	
	
Window.currentRow	

Window.orderBy(*cols)	Creates a WindowSpec with the ordering defined.

Window.partitionBy(*cols)	Creates a WindowSpec with the partitioning defined.

Window.rangeBetween(start, end)	Creates a WindowSpec with the frame boundaries defined, from start (inclusive) to end (inclusive).

Window.rowsBetween(start, end)	Creates a WindowSpec with the frame boundaries defined, from start (inclusive) to end (inclusive).

Window.unboundedFollowing	

Window.unboundedPreceding	

WindowSpec.orderBy(*cols)	Defines the ordering columns in a WindowSpec.

WindowSpec.partitionBy(*cols)	Defines the partitioning columns in a WindowSpec.

WindowSpec.rangeBetween(start, end)	Defines the frame boundaries, from start (inclusive) to end (inclusive).

WindowSpec.rowsBetween(start, end)	Defines the frame boundaries, from start (inclusive) to end (inclusive).

Examples and definitions

Great — this is a **fundamental concept in Spark window APIs** and often asked in interviews.

Let’s clearly break down:

# ✅ 🔥 Difference: `Window` vs `WindowSpec`

***

# ✅ 1. What is `Window`?

👉 `Window` is a **utility / builder class**

* Used to **create window specifications**
* Think of it as a **factory** that helps construct a window

📌 It does NOT hold the window definition itself  
📌 It provides methods to **build** a `WindowSpec`

***

### ✅ Example

```scala
import org.apache.spark.sql.expressions.Window

val w = Window.partitionBy("dept")
```

👉 Here:

* `Window` is used → to create something
* `w` becomes → a `WindowSpec`

***

# ✅ 2. What is `WindowSpec`?

👉 `WindowSpec` is the **actual window definition**

* Contains:
  * partition columns ✅
  * order columns ✅
  * frame (rows/range) ✅

👉 This is what gets applied to functions like:

```scala
sum("salary").over(windowSpec)
```

***

# ✅ Simple Analogy

| Concept    | Real-world analogy            |
| ---------- | ----------------------------- |
| Window     | Blueprint builder 🧱          |
| WindowSpec | Final constructed building 🏢 |

***

# ✅ 3. Key Difference (Core Answer)

| Feature  | Window                   | WindowSpec              |
| -------- | ------------------------ | ----------------------- |
| Type     | Object (builder)         | Class instance          |
| Purpose  | Create window definition | Holds window definition |
| Used for | Calling methods          | Applied in `.over()`    |
| Mutable? | Builds chain             | Final spec              |

***

# ✅ 4. How They Work Together

***

## Step-by-step Flow

```scala
val w = Window
  .partitionBy("dept")      // returns WindowSpec
  .orderBy("salary")        // modifies WindowSpec
  .rowsBetween(-2, 0)       // final WindowSpec
```

👉 Final object `w` is a **WindowSpec**

***

## Usage

```scala
df.withColumn("sum_salary", sum("salary").over(w))
```

***

# ✅ 5. Method-Level Difference

***

## 🔹 Methods in `Window` (static starters)

```scala
Window.partitionBy(...)
Window.orderBy(...)
Window.rowsBetween(...)
Window.rangeBetween(...)
```

👉 These **start creating a WindowSpec**

***

## 🔹 Methods in `WindowSpec` (chaining)

```scala
windowSpec.partitionBy(...)
windowSpec.orderBy(...)
windowSpec.rowsBetween(...)
windowSpec.rangeBetween(...)
```

👉 These **modify an existing WindowSpec**

***

# ✅ 6. Example to Understand Clearly

***

## ✅ Example 1: Using Window → produce WindowSpec

```scala
val w = Window.partitionBy("dept")

println(w.getClass)
```

👉 Output:

```
WindowSpec
```

***

## ✅ Example 2: Add ordering

```scala
val w = Window
  .partitionBy("dept")
  .orderBy("salary")
```

👉 Still a `WindowSpec`

***

## ✅ Example 3: Full window

```scala
val w = Window
  .partitionBy("dept")
  .orderBy("salary")
  .rowsBetween(-2, 0)
```

👉 Final complete **WindowSpec**

***

# ✅ 7. Why both exist?

👉 Design reason:

* `Window` = entry point
* `WindowSpec` = composable object

✅ Allows:

* Clean chaining
* Reusability
* Separation of concerns

***

# ✅ 8. Real Use Case (Best Practice)

***

## ✅ Reusable WindowSpec

```scala
val baseWindow = Window.partitionBy("dept").orderBy("salary")

val w1 = baseWindow.rowsBetween(-2, 0)
val w2 = baseWindow.rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

👉 Reuse same base logic ✅

***

# ✅ 9. Important Constants

These come from `Window`:

```scala
Window.currentRow
Window.unboundedPreceding
Window.unboundedFollowing
```

Used inside `WindowSpec`

***

### ✅ Example

```scala
val w = Window
  .partitionBy("id")
  .orderBy("date")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

***

# ✅ 10. Common Confusion (Interview Trap)

***

## ❌ Wrong thinking:

> WindowSpec and Window are same

***

## ✅ Correct:

> Window creates WindowSpec; WindowSpec is used in computation

***

# ✅ 11. SQL vs DSL Mapping

| SQL                      | DSL                    |
| ------------------------ | ---------------------- |
| `OVER (PARTITION BY...)` | `Window.partitionBy()` |
| `ORDER BY`               | `.orderBy()`           |
| `ROWS BETWEEN`           | `.rowsBetween()`       |

***

# ✅ 12. Full Example

***

### 🔸 DSL

```scala
val windowSpec = Window
  .partitionBy("customer_id")
  .orderBy("order_date")
  .rowsBetween(-2, 0)

df.withColumn("moving_sum", sum("amount").over(windowSpec))
```

***

### 🔸 What's happening?

1. `Window` → builder
2. chain methods → create `WindowSpec`
3. apply → `.over(windowSpec)`

***

# ✅ 13. Visual Flow

```
Window  ──▶  WindowSpec  ──▶  used in .over()
   (builder)     (definition)        (execution)
```

***

# ✅ 14. Interview Answer (Perfect)

> “Window is a builder object used to define window specifications, while WindowSpec is the actual object that holds partitioning, ordering, and frame boundaries. Window methods return a WindowSpec, which is then passed to functions using the over() method.”

***

# ✅ 15. Final Takeaway

✔ `Window` = starting point  
✔ `WindowSpec` = final definition  
✔ Only `WindowSpec` is used in execution

***
