Operator list with related lineage info

### GROUP BY
1. `HashAggregate` DONE - Aggregation with grouping and not Perfect

    Need to thread through inputs to outputs across hash table, so we expect this to be pretty hard.
    :x:

2. `PerfectHashAggregate` DONE - Based on statistics and data types, output can fit in a map without collisions

    Again need to thread through inputs to outputs across a hash table.
    Can perhaps use unique hash value to ease process, but still need to store input tuples somewhere which will be costly.
    :x:

3. `SimpleAggregate` DONE - Aggregation without any grouping

    All inputs map to single output - easy.
    :white_check_mark:

4. `Window` TODO

    Selection vectors are used throughout, but a LOT is going on in these functions.
    Definitely requires more exploration if we want to support windowing.

### WHERE/HAVING
5. `Filter` DONE

    Selection Vector used - easy.
    :white_check_mark:

### LIMIT
6. `Limit` DONE

    Selection Vectors used if there's both an offset and a limit.
    Otherwise it just modifies the chunk metadata directly.
    But it should be easy to capture since lineage capture logic can be represented with just numbers here.
    :white_check_mark:

### SAMPLE

`ReservoirSample` TODO

`StreamingSample` DONE

### JOIN
7. `BlockwiseNLJoin` TODO - Arbitrary expression that doesn't compare between each side

    Selection vectors used to capture output values throughout. Awesome.
    Possibly some edge cases that still require some thinking through since there are many returns that could be hit.
    :white_check_mark: 

8. `CrossProduct` TODO

    No selection vectors used. Would require storing intermediate data,
    but since it's single threaded we should be able to infer lineage based on table sizes.
    :white_check_mark:

9. `DelimJoin` TODO - Special "duplicate elimination" join for subquery flattening

    No selection vectors used and data goes into a distinct aggregate hashtable. Seems hard.
    :x: 
    Second look: this join is crazy. I left a test executing it, but it only seems to care about one side of the join?
    Very weird, but unfortunately used fairly often in TPC-H (9/22 queries) so we'll need to figure something out.

10. `HashJoin` DONE - Equality && not IndexJoin

    No selection vectors used and data goes into a hashtable. Hard.
    Perhaps there are clever tricks to do this quickly as Haneen has mentioned.
    :x:

11. `IndexJoin` DONE - Equality && HasIndex && cardinality of indexed side is 100x cardinality of unindexed side

    Selection vectors used throughout. Looks straightforward
    :white_check_mark:

12. `NestedLoopJoin` TODO - Inequality

    Uses selection vectors throughout. Very promising.
    :white_check_mark: 

13. `PiecewiseMergeJoin` DONE - Neither equality nor inequality

    Selection vectors used for ordering which should definitely be usable.
    Also capturing selection vectors as part of ScalarMergeInfo.
    :white_check_mark: 

### ORDER BY
14. `Order` DONE

    A selection vector is captured and passed around that looks promising. More exploration necessary to confirm.
    :white_check_mark: 

15. `TopN` TODO

    Everything is put into a heap, so lineage is definitely lost.
    :x: 

### PROJECT
16. `Projection` DONE

    Pass-through for lineage.
    :white_check_mark:
    
`TableInOut` TODO - what does this do? more exploration necessary

`Unnest` TODO - hard to do since nested structures can't be used in testing framework... hmmm...

### SCAN
17. `ChunkScan` DONE

    No selection vectors used, should be able to capture lineage implicitly or with number capture.
    :white_check_mark: 

18. `TableScan` DONE

    No selection vectors used, but conceptually straightforward.
    The challenge will be handling parallel table scanning well - a bit more exploration needed on this.
    :white_check_mark: 

### UNION
19. `Union` DONE

    No selection vectors used, but lineage capture can be represented with number capture so should be straightforward.
    :white_check_mark:
    
`RecursiveCTE` TODO