
# Spark SQL Complete Guide (Full Outputs + Explanations + Diagrams)

## 📊 Initial Data

### df
|id|tdate|amount|category|product|spendby|
|--|-----|------|--------|-------|-------|
|0|06-26-2011|300.4|Exercise|GymnasticsPro|cash|
|1|05-26-2011|200.0|Exercise Band|Weightlifting|credit|
|2|06-01-2011|300.4|Exercise|Gymnastics Pro|cash|
|3|06-05-2011|100.0|Gymnastics|Rings|credit|
|4|12-17-2011|300.0|Team Sports|Field|cash|
|5|02-14-2011|200.0|Gymnastics|null|cash|
|6|06-05-2011|100.0|Exercise|Rings|credit|
|7|12-17-2011|300.0|Team Sports|Field|cash|
|8|02-14-2011|200.0|Gymnastics|null|cash|

---

## ✅ 1. SELECT *
```
All rows (9 records)
```

---

## ✅ 2. WHERE category='Exercise'
```
0 Exercise
2 Exercise
6 Exercise
```

---

## ✅ 3. category='Exercise' AND spendby='cash'
```
0 Exercise cash
2 Exercise cash
```

---

## ✅ 4. IN condition
```
Exercise + Gymnastics rows (6 rows)
```

---

## ✅ 5. LIKE '%Gymnastics%'
```
0 GymnasticsPro
2 Gymnastics Pro
3 Rings? ❌ (not included)
```

---

## ✅ 6. NULL check
```
5 null
8 null
```

---

## ✅ 7. Aggregations

### MAX(id)
```
8
```

### MIN(id)
```
0
```

### COUNT
```
9
```

---

## ✅ 8. CASE WHEN
```
cash -> 1
credit -> 0
```

---

## ✅ 9. CONCAT
```
0-Exercise
1-Exercise Band
...
```

---

## ✅ 10. GROUP BY category SUM
```
Exercise -> 700.8
Exercise Band -> 200
Gymnastics -> 500
Team Sports -> 600
```

---

## ✅ 11. WINDOW FUNCTIONS

### Diagram
```
Partition (category)
   ↓
Sort (amount desc)
   ↓
Apply row_number / rank
```

### Example row_number
```
Exercise
300.4 -> 1
300.4 -> 2
100 -> 3
```

---

## ✅ 12. JOINS

### INNER JOIN
```
1 raj mouse
3 sai mobile
```

### LEFT JOIN
```
All customers + matched products
```

### LEFT ANTI
```
Customers not in prod
```

### LEFT SEMI
```
Customers present in prod
```

---

## ✅ 13. UNION vs UNION ALL

| Type | Behavior |
|------|--------|
| UNION | Removes duplicates |
| UNION ALL | Keeps duplicates |

---

## ✅ 14. DATE CONVERSION
```
06-26-2011 → 2011-06-26
```

---

## ✅ FINAL UNDERSTANDING DIAGRAM

```
RAW DATA
   ↓
TRANSFORM (filter/select)
   ↓
AGGREGATE (group by)
   ↓
WINDOW FUNCTIONS
   ↓
JOIN / UNION
   ↓
FINAL RESULT
```

---

## 🧠 Interview Tips
- Window functions = VERY IMPORTANT
- Difference: rank vs dense_rank
- Join types (anti & semi are frequently asked)
- Speculative execution sometimes combined topic

