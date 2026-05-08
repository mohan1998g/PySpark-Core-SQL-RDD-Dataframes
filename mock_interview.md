# ✅ MOCK INTERVIEW – ADVANCED LEVEL (WITH ANSWERS)

***

### Q1. Why is reduce required to be associative?

✅ Spark executes reduce in parallel → order is undefined.

***

### Q2. Difference between `mapPartitions` and `map`?

✅ `mapPartitions` works per partition → faster for heavy setup logic.

***

### Q3. Why is `groupByKey` dangerous?

✅ Shuffles all values → memory explosion.

***

### Q4. Where does lambda code execute?

✅ On **executors**, not driver.

***

### Q5. How does Spark handle failures?

✅ Uses RDD lineage to recompute lost partitions.

***

### Q6. When should you avoid RDDs?

✅ For SQL, joins, aggregations → use DataFrames.

***

### Q7. What creates new stages in Spark?

✅ Wide transformations (shuffle boundaries).

***

### Q8. Is `foreach()` reliable for output?

✅ No — use for side effects only, not data return.

***

### Q9. Difference: `repartition` vs `coalesce`?

✅ `repartition` = full shuffle  
✅ `coalesce` = no shuffle (reduce only)

***

### Q10. Explain Spark execution in one sentence

✅

> “Spark builds a DAG of transformations, optimizes it into stages, and executes tasks lazily upon an action.”

***

# ✅ FINAL MASTER SUMMARY (Say This in Interviews)

> “RDDs are Spark’s low‑level abstraction with fault tolerance via lineage. Transformations are lazy and form a DAG, while actions trigger execution. Narrow transformations avoid shuffles, wide ones create stages. For performance and optimization, DataFrames are preferred in production.”

***

If you want next (last step mastery):
✅ **End‑to‑end RDD project**
✅ **Spark job optimization case study**
✅ **RDD vs DF vs SQL decision tree**
✅ **Printable PDF notes**

Just say **which one** and I’ll deliver 🚀
