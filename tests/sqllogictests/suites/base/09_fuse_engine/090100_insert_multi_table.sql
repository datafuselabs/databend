statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

# The number of columns in the target and the source must be the same
statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1,c2) VALUES(c3)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The condition in WHEN clause must be a boolean expression
statement error 1007
INSERT FIRST
  WHEN 'a' THEN
    INTO t1 (c1,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The target column in into clause must be a column ref of the target table
statement error 1005
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1 + 1,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The target column in into clause must be a column ref of the target table
statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c3,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# The source column in into clause must be a column ref of the source table
statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1,c2) VALUES(c1,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# test alias
statement error 1006
INSERT FIRST
  WHEN c3 > 4 THEN
    INTO t1 (c1,c2) VALUES(c3,c4)
  WHEN c3 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT c3,c4 as c5 from s;

# column ref in when clause must be a column ref of the source table
statement error 1065
INSERT FIRST
  WHEN c1 > 4 THEN
    INTO t1 (c1,c2) VALUES(c3,c4)
  WHEN c1 > 3 THEN
    INTO t1
    INTO t2
  ELSE
    INTO t2
SELECT * from s;

# unbranched insert all
statement ok
INSERT ALL
    INTO t1
    INTO t2
SELECT * from s;

query II
select * from t1 order by c1;
----
1 2
3 4
5 6

query II
select * from t2 order by c1;
----
1 2
3 4
5 6

# branched insert all
statement ok
INSERT ALL
    WHEN c3 = 1 THEN
      INTO t1
    WHEN c3 = 3 THEN
      INTO t2
SELECT * from s;

query II
select * from t1 order by c1;
----
1 2
1 2
3 4
5 6

query II
select * from t2 order by c1;
----
1 2
3 4
3 4
5 6

# branched insert first
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

statement ok
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2
SELECT * from s;

query II
select * from t1 order by c1;
----
5 6

query II
select * from t2 order by c1;
----
1 2
3 4

# projected by source columns
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

statement ok
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 values(c4,c3)
    WHEN c3 > 0 THEN
      INTO t2 values(c4,c3)
SELECT * from s;

query II
select * from t1 order by c1;
----
6 5

query II
select * from t2 order by c1;
----
2 1
4 3

# cast schema to target table
statement ok 
create or replace table t1(c1 bigint,c2 int);

statement ok
create or replace table t2(c1 int,c2 bigint);

statement ok
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 values(c4,c3)
    WHEN c3 > 0 THEN
      INTO t2 values(c4,c3)
SELECT * from s;

query II
select * from t1 order by c1;
----
6 5

query II
select * from t2 order by c1;
----
2 1
4 3

# reorder and fill default value
statement ok 
create or replace table t1(c1 bigint,c2 int);

statement ok
create or replace table t2(c1 int,c2 bigint);

statement ok
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 (c2) values(c4)
    WHEN c3 > 0 THEN
      INTO t2 (c2,c1) values(c4,c3)
SELECT * from s;

query II
select * from t1 order by c1;
----
NULL 6

query II
select * from t2 order by c1;
----
1 2
3 4

# test cluster key
statement ok 
create or replace table t1(c1 bigint,c2 int) cluster by (c1 + 1);

statement ok
create or replace table t2(c1 int,c2 bigint) cluster by (c1 + 1);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(3,4),(1,2),(5,6);

statement ok
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 (c2) values(c4)
    WHEN c3 > 0 THEN
      INTO t2 (c2,c1) values(c4,c3)
SELECT * from s;

query II
select * from t1 order by c1;
----
NULL 6

query II
select * from t2;
----
1 2
3 4

# test multi-statement transaction
statement ok 
create or replace table t1(c1 bigint,c2 int) cluster by (c1 + 1);

statement ok
create or replace table t2(c1 int,c2 bigint) cluster by (c1 + 1);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
begin transaction;

statement ok
insert into s values(3,4),(1,2),(5,6);

statement ok
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1 (c2) values(c4)
    WHEN c3 > 0 THEN
      INTO t2 (c2,c1) values(c4,c3)
SELECT * from s;

query II
select * from t1 order by c1;
----
NULL 6

query II
select * from t2;
----
1 2
3 4

statement ok
rollback;

query II
select * from t1 order by c1;
----


query II
select * from t2;
----


# render subquery results
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

statement ok
INSERT FIRST
    WHEN c3 = 6 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2
SELECT (c3 + 1) as c3, (c4 + 2) as c4 from s;

query II
select * from t1 order by c1;
----
6 8

query II
select * from t2 order by c1;
----
2 4
4 6


# same table in different branches
statement ok 
create or replace table t1(c1 int,c2 int);

statement ok
create or replace table t2(c1 int,c2 int);

statement ok
create or replace table s(c3 int,c4 int);

statement ok
insert into s values(1,2),(3,4),(5,6);

statement ok
INSERT FIRST
    WHEN c3 = 5 THEN
      INTO t1
    WHEN c3 > 0 THEN
      INTO t2
      INTO t1
SELECT * from s;

query II
select * from t1 order by c1;
----
1 2
3 4
5 6

query II
select * from t2 order by c1;
----
1 2
3 4
