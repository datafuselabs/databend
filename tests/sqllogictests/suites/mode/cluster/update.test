statement ok
DROP DATABASE IF EXISTS db1

statement ok
CREATE DATABASE db1

statement ok
USE db1

statement ok
CREATE TABLE IF NOT EXISTS t1(a Int, b Date)

statement ok
INSERT INTO t1 VALUES(1, '2022-12-30')

statement ok
INSERT INTO t1 VALUES(2, '2023-01-01')

query IT
SELECT * FROM t1 ORDER BY b
----
1 2022-12-30
2 2023-01-01

query T
explain fragments UPDATE t1 SET a = 3 WHERE b > '2022-12-31'
----
Fragment 0:
  DataExchange: Merge
    ExchangeSink
    ├── output columns: []
    ├── destination fragment: [1]
    └── UpdateSource
(empty)
(empty)
Fragment 1:
    CommitSink
    └── ExchangeSource
        ├── output columns: []
        └── source fragment: [0]
(empty)

query IT
SELECT * FROM t1 ORDER BY b
----
1 2022-12-30
2 2023-01-01

query T
explain fragments UPDATE t1 SET a = 3 WHERE false
----
Nothing to update
