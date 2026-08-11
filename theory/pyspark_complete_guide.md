# PySpark Transformations: Narrow vs Wide (Deep Guide)

## 🔹 Core Concept
- **Narrow Transformation**: No shuffle; data stays within partition
- **Wide Transformation**: Causes shuffle; data moves across partitions

---

# ✅ Narrow Transformations (RDD)

## List
- map()
- flatMap()
- filter()
- mapPartitions()
- mapPartitionsWithIndex()
- sample()
- union()
- coalesce() (no shuffle)
- glom()
- pipe()
- mapValues()
- flatMapValues()

## Example
```python
rdd = sc.parallelize([1,2,3,4])
result = rdd.map(lambda x: x * 2)
```

## DAG Behavior
- Simple linear DAG
- No stage break

---

# ✅ Wide Transformations (RDD)

## List
- groupByKey()
- reduceByKey()
- aggregateByKey()
- combineByKey()
- join()
- leftOuterJoin()
- rightOuterJoin()
- fullOuterJoin()
- cogroup()
- repartition()
- sortByKey()
- sortBy()
- distinct()
- intersection()
- subtractByKey()

## Example
```python
rdd = sc.parallelize([("a",1),("a",2)])
result = rdd.reduceByKey(lambda x,y: x+y)
```

## DAG Behavior
- Causes stage split
- Shuffle boundary created

---

# ✅ DataFrame Transformations

## Narrow-like
- select()
- withColumn()
- filter() / where()
- drop()
- limit()

## Wide
- groupBy()
- join()
- orderBy()
- sort()
- distinct()
- dropDuplicates()

## Example
```python
df.filter(df.age > 25).select("name")
```

---

# ⚠️ Important Exceptions

| Function | Type | Reason |
|--------|------|--------|
| coalesce(n) | Narrow | avoids shuffle |
| repartition(n) | Wide | forces shuffle |
| distinct() | Wide | requires data aggregation |
| sortByKey() | Wide | global sorting |
| groupByKey() | Wide | full shuffle |

---

# 🔄 Execution Plan (DAG)

## Example
```python
rdd.map(...).filter(...).reduceByKey(...)
```

### DAG Stages
1. Stage 1 → map + filter (Narrow)
2. Stage 2 → reduceByKey (Shuffle → Wide)

---

# ⚙️ Spark Optimizations

## Catalyst Optimizer (DataFrame)
- Logical plan optimization
- Predicate pushdown
- Column pruning

## Tungsten Engine
- Memory management (off-heap)
- Binary processing
- Code generation (WholeStageCodeGen)

---

# 🧠 Performance Tips
- Prefer `reduceByKey()` over `groupByKey()`
- Avoid unnecessary `repartition()`
- Use `coalesce()` to reduce partitions
- Filter early before join

---

# ✅ Quick Summary

| Type | Shuffle | Performance |
|------|--------|------------|
| Narrow | No | Fast |
| Wide | Yes | Expensive |

---

# 🎯 Interview Tip

👉 "Wide transformations create shuffle and stage boundaries, while narrow transformations are pipelined and efficient."

