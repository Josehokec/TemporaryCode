
# query package

This package contains key class to define the complex event query pattern

Complex event pattern contain 5 parts:
1. event pattern
2. schema name
3. selection strategy
4. predicate constraints (optional)
5. query window


An event pattern describes temporal relationship for each variable
event pattern can split into two categories: 
1. Sequential pattern: only contains SEQ temporal operator, e.g., `SEQ(A v1, B v2, C v3)`
2. Complex pattern: contains AND/OR operators, e.g., `AND(A v1, AND(B v2, C v3))`, `AND(A v1, SEQ(B v2, C v3))`

Notice:
To better transfer complex pattern to tree structure, 
we restrict AND,OR,SEQ only bind two nodes. 
That means we cannot support `AND(A v1, B v2, C v3)` or `SEQ(A v1, B v2, C v3)` expressions in complex pattern.
