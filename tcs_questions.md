Got it 👍  
Here is a **TCS‑specific PySpark & Big Data interview preparation guide**, tailored to **how TCS actually interviews** (technical depth + execution thinking, not trick-heavy).

***

# ✅ TCS PySpark / Spark Interview Preparation Guide

## 🎯 What TCS Interviewers Look For

TCS focuses on:

*   ✅ **Strong fundamentals**
*   ✅ **Clear explanations**
*   ✅ **Production awareness**
*   ✅ **Ability to write correct code**
*   ❌ Not deep research-level Spark internals

If you explain clearly and logically, you score high.

***

# 🧱 1. TCS PYSPARK INTERVIEW PATTERN (Typical)

### 🔹 Round 1 – Technical (Spark + Coding)

*   Spark basics (RDD / DataFrame)
*   Execution flow (DAG, stages)
*   Lambda & transformations
*   One or two **coding questions**

### 🔹 Round 2 – Managerial / Tech + Project

*   Your project explanation
*   Why Spark was used
*   Performance issues & fixes
*   Team collaboration

### 🔹 Round 3 – HR

*   Stability
*   Client handling
*   Location/project flexibility

***

# ⚙️ 2. VERY COMMON TCS PYSPARK QUESTIONS

### ✅ Q1. What is RDD?

✅ Answer:

> “RDD is an immutable, distributed collection of objects that provides fault tolerance using lineage.”

***

### ✅ Q2. Transformation vs Action?

✅ Answer:

> “Transformations are lazy and build the DAG. Actions trigger execution.”

***

### ✅ Q3. map vs flatMap?

✅ Answer:

> “map returns one output per input; flatMap can return many and flattens the result.”

***

### ✅ Q4. Why reduceByKey instead of groupByKey?

✅ Answer:

> “reduceByKey performs map-side aggregation and minimizes shuffle, making it memory efficient.”

***

### ✅ Q5. Why avoid collect()?

✅ Answer:

> “collect brings all data to driver and can cause OutOfMemory for large datasets.”

***

# 🚨 3. TCS FAVORITE TRICK QUESTIONS (VERY IMPORTANT)

### 🔹 Q6. Output?

```python
rdd = sc.parallelize([1,2,3,4])
rdd.filter(lambda x: x % 2).collect()
```

✅ Answer:

```text
[1, 3]
```

✅ Explanation (say this):

> “filter checks truthiness; 1 is true, 0 is false.”

***

### 🔹 Q7. How many jobs?

```python
rdd.count()
rdd.collect()
```

✅ Answer:

> **2 jobs**, because each action triggers a job.

***

### 🔹 Q8. Where does lambda execute?

✅ Answer:

> “Lambda executes on executors; the action is triggered from driver.”

***

# 🧪 4. TCS HANDS‑ON CODING QUESTIONS (VERY COMMON)

### ✅ Problem 1: Count words

```python
lines = ["spark is fast", "spark is scalable"]
```

✅ Solution:

```python
sc.parallelize(lines) \
  .flatMap(lambda x: x.split()) \
  .map(lambda x: (x,1)) \
  .reduceByKey(lambda a,b: a+b) \
  .collect()
```

✅ Expected Output:

```text
[('spark',2), ('is',2), ('fast',1), ('scalable',1)]
```

***

### ✅ Problem 2: Find max per key

```python
data = [("a",1), ("a",4), ("b",3)]
```

✅ Solution:

```python
sc.parallelize(data) \
  .reduceByKey(lambda a,b: a if a>b else b) \
  .collect()
```

✅ Output:

```text
[('a',4), ('b',3)]
```

***

### ✅ Problem 3: Even & odd count

✅ Solution:

```python
rdd = sc.parallelize([1,2,3,4,5])
rdd.keyBy(lambda x: x % 2) \
   .mapValues(lambda x: 1) \
   .reduceByKey(lambda a,b: a+b) \
   .collect()
```

✅ Output:

```text
[(1,3), (0,2)]
```

***

# 📊 5. RDD vs DataFrame (TCS EXPECTS THIS ANSWER)

✅ Correct response:

> “RDDs give low-level control but no optimization. DataFrames use Catalyst optimizer and Tungsten and are faster. In projects, I prefer DataFrames unless custom logic is required.”

***

# 🏗️ 6. PROJECT-LEVEL QUESTIONS (VERY IMPORTANT)

### 🔹 Q: Why Spark instead of SQL?

✅ Answer:

> “Because of distributed processing, scalability, and fault tolerance.”

***

### 🔹 Q: How did you improve performance?

✅ Answers they like:

*   Reduced shuffles
*   Used reduceByKey instead of groupByKey
*   Used partitioning
*   Cached reused data
*   Avoided collect()

***

### 🔹 Q: Production issue you faced?

✅ Safe answer:

> “Data skew causing slow stages; fixed using repartitioning and salting.”

***

# 🧠 7. TCS HR ROUND – SAFE ANSWERS

### ✅ Why TCS?

> “TCS provides long-term projects, strong learning culture, and exposure to enterprise systems.”

***

### ✅ Ready to relocate?

> “Yes, as per project requirements.”

***

### ✅ Any bond concerns?

> “No issues; I’m committed to long-term contribution.”

***

# ✅ 8. ONE‑LINE ANSWERS (MEMORIZE THESE)

✅ *“Spark uses DAG-based lazy execution.”*  
✅ *“RDDs are fault tolerant via lineage.”*  
✅ *“Wide transformations create shuffle and stages.”*  
✅ *“DataFrames are optimized via Catalyst.”*  
✅ *“collect() is dangerous on large data.”*

***

# 🏆 FINAL ADVICE FOR TCS INTERVIEW

✅ Speak slowly  
✅ Explain step-by-step  
✅ Don’t over-engineer answers  
✅ Use simple examples  
✅ Confidence > buzzwords

***

If you want next (very useful):
✅ **TCS Spark interview mock (full Q\&A simulation)**  
✅ **Resume points for Spark roles in TCS**  
✅ **Client‑project story templates**

Just tell me which one 💪
