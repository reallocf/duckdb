The operators used during TPCH queries

Operators by Query:
------------------
1. Seq Scan, Projection, Hash Group By, Order By
2. Seq Scan, Filter, Hash Join, Projection, Delim Join, Top N
3. Seq Scan, Hash Join, Projection, Hash Group By, Top N
4. Seq Scan, Filter, Projection, Hash Join, Hash Group By, Order By
5. Seq Scan, Hash Join, Projection, Hash Group By, Order By
6. Seq Scan, Projection, Simple Aggregate
7. Seq Scan, Hash Join, Filter, Projection, Hash Group By, Order By
8. Seq Scan, Hash Join, Projection, Perfect Hash Group By, Order By
9. Seq Scan, Hash Join, Filter, Projection, Hash Group By, Order By
10. Seq Scan, Hash Join, Projection, Hash Group By, Top N
11. Seq Scan, Hash Join, Projection, Simple Aggregate, Limit, Hash Group By, Piecewise Merge Join, Order By
12. Seq Scan, Filter, Projection, Hash Join, Hash Group By, Order By
13. Seq Scan, Filter, Projection, Hash Join, Hash Group By, Order By
14. Seq Scan, Hash Join, Projection, Simple Aggregate
15. Seq Scan, Projection, Hash Group By, Simple Aggregate, Limit, Order By
16. Seq Scan, Chunk Scan, Hash Join, Filter, Projection, Hash Group By, Order By
17. Seq Scan, Hash Join, Delim Join, Filter, Projection, Simple Aggregate
18. Seq Scan, Projection, Hash Group By, Filter, Hash Join, Top N
19. Seq Scan, Filter, Projection, Hash Join, Simple Aggregate
20. Seq Scan, Filter, Projection, Hash Join, Delim Join, Order By
21. Seq Scan, Filter, Projection, Hash Join, Delim Join, Hash Group By, Top N
22. Seq Scan, Chunk Scan, Hash Join, Filter, Projection, Simple Aggregate, Limit, Piecewise Merge Join, Order By

op: COUNT(op)
-------------
Seq Scan: 22
Projection: 22
Hash Join: 19
Hash Group By: 15
Order By: 13
Filter: 13
Delim Join: 9
Simple Aggregate: 7
Top N: 5
Limit: 3
Piecewise Merge Join: 2
Chunk Scan: 2
Perfect Hash Group By: 1