# ✅ TRANSFORMATION vs ACTION → DAG & EXECUTION MODEL

***

## 🔹 Key Concepts

| Concept         | Meaning                                         |
| --------------- | ----------------------------------------------- |
| DAG             | Directed Acyclic Graph of operations            |
| Lineage         | Sequence of transformations used to rebuild RDD |
| Lazy Execution  | Nothing runs until an action                    |
| Fault Tolerance | RDDs recreated using lineage                    |

***

## 🔹 Example

```python
rdd = sc.parallelize([1,2,3,4,5])
rdd2 = rdd.filter(lambda x: x % 2 == 0)
rdd3 = rdd2.map(lambda x: x * 10)
rdd3.collect()
```

### What happens internally:

1.  Spark builds a **DAG**:
        parallelize → filter → map → collect
2.  No execution until `collect()`
3.  Spark:
    *   Splits into stages
    *   Executes tasks per partition
    *   Returns result to driver

✅ DAG allows **recomputation instead of replication**

***

## 🔹 Narrow vs Wide → DAG impact

| Type   | Shuffle | Examples          |
| ------ | ------- | ----------------- |
| Narrow | ❌ No    | map, filter       |
| Wide   | ✅ Yes   | reduceByKey, join |

***

✅ **Interview Gold Line**:

> “Wide transformations create new stages in the DAG due to shuffle.”

***
