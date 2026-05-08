# ✅ PYSPARK RDD CHEAT SHEET (INTERVIEW‑FRIENDLY)

***

## 🔹 Most Used Transformations

```text
map
flatMap
filter
reduceByKey
mapValues
keyBy
distinct
sortBy
join
repartition / coalesce
```

***

## 🔹 Most Used Actions

```text
collect
count
take
reduce
foreach
saveAsTextFile
```

***

## 🔹 DOs

✅ Use `reduceByKey` instead of `groupByKey`  
✅ Use lambdas for small logic  
✅ Cache reused RDDs

***

## 🔹 DON’Ts

❌ Don’t use `collect()` on big data  
❌ Don’t use non‑associative reduce  
❌ Don’t use RDDs if DataFrames suffice

***

## 🔹 Performance Tips

*   Minimize shuffles
*   Avoid wide transformations
*   Use `cache()` for reused RDDs
*   Prefer DataFrames in production

***
