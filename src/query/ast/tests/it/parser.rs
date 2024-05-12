// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::fmt::Display;
use std::io::Write;

use databend_common_ast::parser::display_parser_error;
use databend_common_ast::parser::expr::*;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::query::*;
use databend_common_ast::parser::quote::quote_ident;
use databend_common_ast::parser::quote::unquote_ident;
use databend_common_ast::parser::script::script_block;
use databend_common_ast::parser::script::script_stmt;
use databend_common_ast::parser::statement::insert_stmt;
use databend_common_ast::parser::token::*;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Backtrace;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::IResult;
use databend_common_ast::parser::Input;
use databend_common_ast::parser::ParseMode;
use databend_common_ast::rule;
use goldenfile::Mint;

fn run_parser<P, O>(file: &mut dyn Write, parser: P, src: &str)
where
    P: FnMut(Input) -> IResult<O>,
    O: Debug + Display,
{
    run_parser_with_dialect(file, parser, Dialect::PostgreSQL, ParseMode::Default, src)
}

fn run_parser_with_dialect<P, O>(
    file: &mut dyn Write,
    parser: P,
    dialect: Dialect,
    mode: ParseMode,
    src: &str,
) where
    P: FnMut(Input) -> IResult<O>,
    O: Debug + Display,
{
    let src = unindent::unindent(src);
    let src = src.trim();
    let tokens = tokenize_sql(src).unwrap();
    let backtrace = Backtrace::new();
    let input = Input {
        tokens: &tokens,
        dialect,
        mode,
        backtrace: &backtrace,
    };
    let parser = parser;
    let mut parser = rule! { #parser ~ &EOI };
    match parser(input) {
        Ok((i, (output, _))) => {
            assert_eq!(i[0].kind, TokenKind::EOI);
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", src).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{}", output).unwrap();
            writeln!(file, "---------- AST ------------").unwrap();
            writeln!(file, "{:#?}", output).unwrap();
            writeln!(file, "\n").unwrap();
        }
        Err(nom::Err::Error(err) | nom::Err::Failure(err)) => {
            let report = display_parser_error(err, src).trim_end().to_string();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", src).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{}", report).unwrap();
            writeln!(file, "\n").unwrap();
        }
        Err(nom::Err::Incomplete(_)) => unreachable!(),
    }
}

#[test]
fn test_statement() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("statement.txt").unwrap();
    let cases = &[
        r#"show databases"#,
        r#"show databases format TabSeparatedWithNamesAndTypes;"#,
        r#"show tables"#,
        r#"show drop tables"#,
        r#"show drop tables like 't%'"#,
        r#"show drop tables where name='t'"#,
        r#"show tables format TabSeparatedWithNamesAndTypes;"#,
        r#"describe "name""with""quote";"#,
        r#"describe "name""""with""""quote";"#,
        r#"show full tables"#,
        r#"show full tables from db"#,
        r#"show full tables from ctl.db"#,
        r#"show full columns in t in db"#,
        r#"show columns in t from ctl.db"#,
        r#"show full columns from t from db like 'id%'"#,
        r#"show processlist like 't%' limit 2;"#,
        r#"show processlist where database='default' limit 2;"#,
        r#"show create table a.b;"#,
        r#"show create table a.b format TabSeparatedWithNamesAndTypes;"#,
        r#"explain pipeline select a from b;"#,
        r#"explain pipeline select a from t1 ignore_result;"#,
        r#"explain(verbose, logical, optimized) select * from t where a = 1"#,
        r#"describe a;"#,
        r#"describe a format TabSeparatedWithNamesAndTypes;"#,
        r#"CREATE AGGREGATING INDEX idx1 AS SELECT SUM(a), b FROM t1 WHERE b > 3 GROUP BY b;"#,
        r#"CREATE OR REPLACE AGGREGATING INDEX idx1 AS SELECT SUM(a), b FROM t1 WHERE b > 3 GROUP BY b;"#,
        r#"CREATE OR REPLACE INVERTED INDEX idx2 ON t1 (a, b);"#,
        r#"create table a (c decimal(38, 0))"#,
        r#"create table a (c decimal(38))"#,
        r#"create or replace table a (c decimal(38))"#,
        r#"create or replace table a (c int(10) unsigned)"#,
        r#"create table if not exists a.b (c integer not null default 1, b varchar);"#,
        r#"create table if not exists a.b (c integer default 1 not null, b varchar) as select * from t;"#,
        r#"create table if not exists a.b (c tuple(m integer, n string), d tuple(integer, string));"#,
        r#"create table if not exists a.b (a string, b string, c string as (concat(a, ' ', b)) stored );"#,
        r#"create table if not exists a.b (a int, b int, c int generated always as (a + b) virtual );"#,
        r#"create table a.b like c.d;"#,
        r#"create table t like t2 engine = memory;"#,
        r#"create table if not exists a.b (a int) 's3://testbucket/admin/data/' connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900');"#,
        r#"
            create table if not exists a.b (a int) 's3://testbucket/admin/data/'
                connection=(aws_key_id='minioadmin' aws_secret_key='minioadmin' endpoint_url='http://127.0.0.1:9900')
                location_prefix = 'db';
        "#,
        r#"truncate table a;"#,
        r#"truncate table "a".b;"#,
        r#"drop table a;"#,
        r#"drop table if exists a."b";"#,
        r#"use "a";"#,
        r#"create catalog ctl type=hive connection=(url='<hive-meta-store>' thrift_protocol='binary');"#,
        r#"create database if not exists a;"#,
        r#"create database ctl.t engine = Default;"#,
        r#"create database t engine = Default;"#,
        r#"CREATE TABLE `t3`(a int not null, b int not null, c int not null) bloom_index_columns='a,b,c' COMPRESSION='zstd' STORAGE_FORMAT='native';"#,
        r#"create or replace database a;"#,
        r#"drop database ctl.t;"#,
        r#"drop database if exists t;"#,
        r#"create table c(a DateTime null, b DateTime(3));"#,
        r#"create view v as select number % 3 as a from numbers(1000);"#,
        r#"alter view v as select number % 3 as a from numbers(1000);"#,
        r#"drop view v;"#,
        r#"create view v1(c1) as select number % 3 as a from numbers(1000);"#,
        r#"create or replace view v1(c1) as select number % 3 as a from numbers(1000);"#,
        r#"alter view v1(c2) as select number % 3 as a from numbers(1000);"#,
        r#"show views"#,
        r#"show views format TabSeparatedWithNamesAndTypes;"#,
        r#"show full views"#,
        r#"show full views from db"#,
        r#"show full views from ctl.db"#,
        r#"create stream test2.s1 on table test.t append_only = false;"#,
        r#"create stream if not exists test2.s2 on table test.t at (stream => test1.s1) comment = 'this is a stream';"#,
        r#"create stream if not exists test2.s3 on table test.t at (TIMESTAMP => '2023-06-26 09:49:02.038483'::TIMESTAMP) append_only = false;"#,
        r#"create stream if not exists test2.s3 on table test.t at (SNAPSHOT => '9828b23f74664ff3806f44bbc1925ea5') append_only = true;"#,
        r#"create or replace stream test2.s1 on table test.t append_only = false;"#,
        r#"show full streams from default.test2 like 's%';"#,
        r#"describe stream test2.s2;"#,
        r#"drop stream if exists test2.s2;"#,
        r#"rename table d.t to e.s;"#,
        r#"truncate table test;"#,
        r#"truncate table test_db.test;"#,
        r#"DROP table table1;"#,
        r#"DROP table IF EXISTS table1;"#,
        r#"CREATE TABLE t(c1 int null, c2 bigint null, c3 varchar null);"#,
        r#"CREATE TABLE t(c1 int not null, c2 bigint not null, c3 varchar not null);"#,
        r#"CREATE TABLE t(c1 varbinary, c2 binary(10));"#,
        r#"CREATE TABLE t(c1 int default 1);"#,
        r#"create table abc as (select * from xyz limit 10)"#,
        r#"ALTER USER u1 IDENTIFIED BY '123456';"#,
        r#"ALTER USER u1 WITH disabled = false;"#,
        r#"ALTER USER u1 WITH default_role = role1;"#,
        r#"ALTER USER u1 WITH DEFAULT_ROLE = role1, DISABLED=true, TENANTSETTING;"#,
        r#"ALTER USER u1 WITH SET NETWORK POLICY = 'policy1';"#,
        r#"ALTER USER u1 WITH UNSET NETWORK POLICY;"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH DEFAULT_ROLE='role123', TENANTSETTING"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH SET NETWORK POLICY='policy1'"#,
        r#"CREATE USER u1 IDENTIFIED BY '123456' WITH disabled=true"#,
        r#"DROP database if exists db1;"#,
        r#"select distinct a, count(*) from t where a = 1 and b - 1 < a group by a having a = 1;"#,
        r#"select * from t4;"#,
        r#"select top 2 * from t4;"#,
        r#"select * from aa.bb;"#,
        r#"select * from a, b, c;"#,
        r#"select * from a, b, c order by "db"."a"."c1";"#,
        r#"select * from a join b on a.a = b.a;"#,
        r#"select * from a left outer join b on a.a = b.a;"#,
        r#"select * from a right outer join b on a.a = b.a;"#,
        r#"select * from a left semi join b on a.a = b.a;"#,
        r#"select * from a semi join b on a.a = b.a;"#,
        r#"select * from a left anti join b on a.a = b.a;"#,
        r#"select * from a anti join b on a.a = b.a;"#,
        r#"select * from a right semi join b on a.a = b.a;"#,
        r#"select * from a right anti join b on a.a = b.a;"#,
        r#"select * from a full outer join b on a.a = b.a;"#,
        r#"select * from a inner join b on a.a = b.a;"#,
        r#"select * from a left outer join b using(a);"#,
        r#"select * from a right outer join b using(a);"#,
        r#"select * from a full outer join b using(a);"#,
        r#"select * from a inner join b using(a);"#,
        r#"select * from a where a.a = any (select b.a from b);"#,
        r#"select * from a where a.a = all (select b.a from b);"#,
        r#"select * from a where a.a = some (select b.a from b);"#,
        r#"select * from a where a.a > (select b.a from b);"#,
        r#"select 1 from numbers(1) where ((1 = 1) or 1)"#,
        r#"select * from read_parquet('p1', 'p2', 'p3', prune_page => true, refresh_meta_cache => true);"#,
        r#"select * from @foo (pattern=>'[.]*parquet' file_format=>'tsv');"#,
        r#"select 'stringwith''quote'''"#,
        r#"select 'stringwith"doublequote'"#,
        r#"select '🦈'"#,
        r#"select * FROM t where ((a));"#,
        r#"select * FROM t where ((select 1) > 1);"#,
        r#"select ((t1.a)>=(((((t2.b)<=(t3.c))) IS NOT NULL)::INTEGER));"#,
        r#"insert into t (c1, c2) values (1, 2), (3, 4);"#,
        r#"insert into t (c1, c2) values (1, 2);"#,
        r#"insert into table t select * from t2;"#,
        r#"select parse_json('{"k1": [0, 1, 2]}').k1[0];"#,
        r#"SELECT avg((number > 314)::UInt32);"#,
        r#"CREATE STAGE ~"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage 's3://load/files/' credentials=(aws_key_id='1a2b3c', aws_secret_key='4x5y6z') file_format=(type = CSV, compression = GZIP record_delimiter=',')"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage url='s3://load/files/' credentials=(aws_key_id='1a2b3c', aws_secret_key='4x5y6z') file_format=(type = CSV, compression = GZIP record_delimiter=',')"#,
        r#"CREATE STAGE IF NOT EXISTS test_stage url='azblob://load/files/' connection=(account_name='1a2b3c' account_key='4x5y6z') file_format=(type = CSV compression = GZIP record_delimiter=',')"#,
        r#"CREATE OR REPLACE STAGE test_stage url='azblob://load/files/' connection=(account_name='1a2b3c' account_key='4x5y6z') file_format=(type = CSV compression = GZIP record_delimiter=',')"#,
        r#"DROP STAGE abc"#,
        r#"DROP STAGE ~"#,
        r#"list @stage_a;"#,
        r#"list @~;"#,
        r#"create user 'test-e' identified by 'password';"#,
        r#"drop user if exists 'test-j';"#,
        r#"alter user 'test-e' identified by 'new-password';"#,
        r#"create role test"#,
        r#"create role 'test'"#,
        r#"create role `a"a`"#,
        r#"create role `a'a`"#,
        r#"create user `a'a` identified by '123'"#,
        r#"drop role if exists test"#,
        r#"drop role if exists 'test'"#,
        r#"OPTIMIZE TABLE t COMPACT SEGMENT LIMIT 10;"#,
        r#"OPTIMIZE TABLE t COMPACT LIMIT 10;"#,
        r#"OPTIMIZE TABLE t PURGE BEFORE (SNAPSHOT => '9828b23f74664ff3806f44bbc1925ea5') LIMIT 10;"#,
        r#"OPTIMIZE TABLE t PURGE BEFORE (TIMESTAMP => '2023-06-26 09:49:02.038483'::TIMESTAMP) LIMIT 10;"#,
        r#"ALTER TABLE t CLUSTER BY(c1);"#,
        r#"ALTER TABLE t COMMENT='t1-commnet';"#,
        r#"ALTER TABLE t DROP CLUSTER KEY;"#,
        r#"ALTER TABLE t RECLUSTER FINAL WHERE c1 > 0 LIMIT 10;"#,
        r#"ALTER TABLE t ADD c int null;"#,
        r#"ALTER TABLE t ADD COLUMN c int null;"#,
        r#"ALTER TABLE t ADD COLUMN a float default 1.1 COMMENT 'hello' FIRST;"#,
        r#"ALTER TABLE t ADD COLUMN b string default 'b' AFTER a;"#,
        r#"ALTER TABLE t RENAME COLUMN a TO b;"#,
        r#"ALTER TABLE t DROP COLUMN b;"#,
        r#"ALTER TABLE t DROP b;"#,
        r#"ALTER TABLE t MODIFY COLUMN b SET MASKING POLICY mask;"#,
        r#"ALTER TABLE t MODIFY COLUMN b UNSET MASKING POLICY;"#,
        r#"ALTER TABLE t MODIFY COLUMN a int DEFAULT 1, COLUMN b float;"#,
        r#"ALTER TABLE t MODIFY COLUMN a int NULL DEFAULT 1, COLUMN b float NOT NULL COMMENT 'column b';"#,
        r#"ALTER TABLE t MODIFY COLUMN a int;"#,
        r#"ALTER TABLE t MODIFY a int;"#,
        r#"ALTER TABLE t MODIFY COLUMN a DROP STORED;"#,
        r#"ALTER TABLE t SET OPTIONS(SNAPSHOT_LOCATION='1/7/_ss/101fd790dbbe4238a31a8f2e2f856179_v4.mpk',block_per_segment = 500);"#,
        r#"ALTER DATABASE IF EXISTS ctl.c RENAME TO a;"#,
        r#"ALTER DATABASE c RENAME TO a;"#,
        r#"ALTER DATABASE ctl.c RENAME TO a;"#,
        r#"VACUUM TABLE t;"#,
        r#"VACUUM TABLE t DRY RUN;"#,
        r#"VACUUM TABLE t DRY RUN SUMMARY;"#,
        r#"VACUUM DROP TABLE;"#,
        r#"VACUUM DROP TABLE DRY RUN;"#,
        r#"VACUUM DROP TABLE DRY RUN SUMMARY;"#,
        r#"VACUUM DROP TABLE FROM db;"#,
        r#"VACUUM DROP TABLE FROM db LIMIT 10;"#,
        r#"CREATE TABLE t (a INT COMMENT 'col comment') COMMENT='table comment';"#,
        r#"GRANT CREATE, CREATE USER ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON *.* TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO USER 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON * TO ROLE role1;"#,
        r#"GRANT ALL ON *.* TO 'test-grant';"#,
        r#"GRANT ALL ON *.* TO ROLE role2;"#,
        r#"GRANT ALL PRIVILEGES ON * TO 'test-grant';"#,
        r#"GRANT ALL PRIVILEGES ON * TO ROLE role3;"#,
        r#"GRANT ROLE test TO 'test-user';"#,
        r#"GRANT ROLE test TO USER 'test-user';"#,
        r#"GRANT ROLE test TO ROLE `test-user`;"#,
        r#"GRANT SELECT ON db01.* TO 'test-grant';"#,
        r#"GRANT SELECT ON db01.* TO USER 'test-grant';"#,
        r#"GRANT SELECT ON db01.* TO ROLE role1"#,
        r#"GRANT SELECT ON db01.tb1 TO 'test-grant';"#,
        r#"GRANT SELECT ON db01.tb1 TO USER 'test-grant';"#,
        r#"GRANT SELECT ON db01.tb1 TO ROLE role1;"#,
        r#"GRANT SELECT ON tb1 TO ROLE role1;"#,
        r#"GRANT ALL ON tb1 TO 'u1';"#,
        r#"SHOW GRANTS;"#,
        r#"SHOW GRANTS FOR 'test-grant';"#,
        r#"SHOW GRANTS FOR USER 'test-grant';"#,
        r#"SHOW GRANTS FOR ROLE role1;"#,
        r#"SHOW GRANTS FOR ROLE 'role1';"#,
        r#"REVOKE SELECT, CREATE ON * FROM 'test-grant';"#,
        r#"REVOKE SELECT ON tb1 FROM ROLE role1;"#,
        r#"REVOKE SELECT ON tb1 FROM ROLE 'role1';"#,
        r#"drop role 'role1';"#,
        r#"GRANT ROLE test TO ROLE 'test-user';"#,
        r#"GRANT ROLE test TO ROLE `test-user`;"#,
        r#"SET ROLE `test-user`;"#,
        r#"SET ROLE 'test-user';"#,
        r#"SET ROLE ROLE1;"#,
        r#"REVOKE ALL ON tb1 FROM 'u1';"#,
        r#"
            COPY INTO mytable
                FROM '@~/mybucket/my data.csv'
                size_limit=10;
        "#,
        r#"
            COPY INTO mytable
                FROM @~/mybucket/data.csv
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;
        "#,
        r#"
            COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10
                max_files=10;
        "#,
        r#"
            COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10
                max_files=3000;
        "#,
        r#"
            COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                CONNECTION = (
                    ENDPOINT_URL = 'http://127.0.0.1:9900'
                )
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;
        "#,
        r#"
            COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                CONNECTION = (
                    ENDPOINT_URL = 'http://127.0.0.1:9900'
                )
                size_limit=10
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                );
        "#,
        r#"
            COPY INTO mytable
                FROM 'https://127.0.0.1:9900';
        "#,
        r#"
            COPY INTO mytable
                FROM 'https://127.0.0.1:';
        "#,
        r#"
            COPY INTO mytable
                FROM @my_stage
                FILE_FORMAT = (
                    type = CSV,
                    field_delimiter = ',',
                    record_delimiter = '\n',
                    skip_header = 1,
                    error_on_column_count_mismatch = FALSE
                )
                size_limit=10;
        "#,
        r#"
            COPY INTO 's3://mybucket/data.csv'
                FROM mytable
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
        "#,
        r#"
            COPY INTO '@my_stage/my data'
                FROM mytable;
        "#,
        r#"
            COPY INTO @my_stage
                FROM mytable
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                );
        "#,
        r#"
            COPY INTO mytable
                FROM 's3://mybucket/data.csv'
                CREDENTIALS = (
                    AWS_KEY_ID = 'access_key'
                    AWS_SECRET_KEY = 'secret_key'
                )
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;
        "#,
        r#"
            COPY INTO mytable
                FROM @external_stage/path/to/file.csv
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;
        "#,
        r#"
            COPY INTO mytable
                FROM @external_stage/path/to/dir/
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10;
        "#,
        r#"
            COPY INTO mytable
                FROM @external_stage/path/to/file.csv
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                force=true;
        "#,
        r#"
            COPY INTO mytable
                FROM 'fs:///path/to/data.csv'
                FILE_FORMAT = (
                    type = CSV
                    field_delimiter = ','
                    record_delimiter = '\n'
                    skip_header = 1
                )
                size_limit=10
                disable_variant_check=true;
        "#,
        r#"
            COPY INTO books FROM 's3://databend/books.csv'
                CONNECTION = (
                    ENDPOINT_URL = 'http://localhost:9000/',
                    ACCESS_KEY_ID = 'ROOTUSER',
                    SECRET_ACCESS_KEY = 'CHANGEME123',
                    region = 'us-west-2'
                )
                FILE_FORMAT = (type = CSV);
        "#,
        // We used to support COPY FROM a quoted at string
        // r#"
        //     COPY INTO mytable
        //         FROM '@external_stage/path/to/file.csv'
        //         FILE_FORMAT = (
        //             type = 'CSV'
        //             field_delimiter = ','
        //             record_delimiter = '\n'
        //             skip_header = 1
        //         )
        //         size_limit=10;
        // "#,
        r#"CALL system$test(a)"#,
        r#"CALL system$test('a')"#,
        r#"show settings like 'enable%' limit 1"#,
        r#"show settings where name='max_memory_usage' limit 1"#,
        r#"show functions like 'today%' limit 1"#,
        r#"show functions where name='to_day_of_year' limit 1"#,
        r#"show engines like 'FU%' limit 1"#,
        r#"show engines where engine='MEMORY' limit 1"#,
        r#"show metrics like '%parse%' limit 1"#,
        r#"show metrics where metric='session_connect_numbers' limit 1"#,
        r#"show table_functions like 'fuse%' limit 1"#,
        r#"show table_functions where name='fuse_snapshot' limit 1"#,
        r#"show indexes like 'test%' limit 1"#,
        r#"show indexes where name='test_idx' limit 1"#,
        r#"PRESIGN @my_stage"#,
        r#"PRESIGN @my_stage/path/to/dir/"#,
        r#"PRESIGN @my_stage/path/to/file"#,
        r#"PRESIGN @my_stage/my\ file.csv"#,
        r#"PRESIGN @my_stage/\"file\".csv"#,
        r#"PRESIGN @my_stage/\'file\'.csv"#,
        r#"PRESIGN @my_stage/\\file\\.csv"#,
        r#"PRESIGN DOWNLOAD @my_stage/path/to/file"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file EXPIRE=7200"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file EXPIRE=7200 CONTENT_TYPE='application/octet-stream'"#,
        r#"PRESIGN UPLOAD @my_stage/path/to/file CONTENT_TYPE='application/octet-stream' EXPIRE=7200"#,
        r#"GRANT all ON stage s1 TO a;"#,
        r#"GRANT read ON stage s1 TO a;"#,
        r#"GRANT write ON stage s1 TO a;"#,
        r#"REVOKE write ON stage s1 FROM a;"#,
        r#"GRANT all ON UDF a TO 'test-grant';"#,
        r#"GRANT usage ON UDF a TO 'test-grant';"#,
        r#"REVOKE usage ON UDF a FROM 'test-grant';"#,
        r#"REVOKE all ON UDF a FROM 'test-grant';"#,
        r#"UPDATE db1.tb1 set a = a + 1, b = 2 WHERE c > 3;"#,
        r#"SET max_threads = 10;"#,
        r#"SET max_threads = 10*2;"#,
        r#"UNSET max_threads;"#,
        r#"UNSET (max_threads, sql_dialect);"#,
        r#"select $1 FROM '@my_stage/my data/'"#,
        r#"
            SELECT t.c1 FROM @stage1/dir/file
                ( file_format => 'PARQUET', FILES => ('file1', 'file2')) t;
        "#,
        r#"
            select table0.c1, table1.c2 from
                @stage1/dir/file ( FILE_FORMAT => 'parquet', FILES => ('file1', 'file2')) table0
                left join table1;
        "#,
        r#"SELECT c1 FROM 's3://test/bucket' (PATTERN => '*.parquet', connection => (ENDPOINT_URL = 'xxx')) t;"#,
        r#"
            CREATE FILE FORMAT my_csv
                type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1;
        "#,
        r#"
            CREATE OR REPLACE FILE FORMAT my_csv
                type = CSV field_delimiter = ',' record_delimiter = '\n' skip_header = 1;
        "#,
        r#"SHOW FILE FORMATS"#,
        r#"DROP FILE FORMAT my_csv"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS (a, b, c, d)"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS (a, b, (c, d))"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ((a, b), (c), (d, e))"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ((a, b), (), (d, e))"#,
        r#"SELECT * FROM t GROUP BY CUBE (a, b, c)"#,
        r#"SELECT * FROM t GROUP BY ROLLUP (a, b, c)"#,
        r#"CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() IN ('ANALYST') THEN VAL ELSE '*********'END comment = 'this is a masking policy'"#,
        r#"CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) RETURNS STRING -> CASE WHEN current_role() IN ('ANALYST') THEN VAL ELSE '*********'END comment = 'this is a masking policy'"#,
        r#"DESC MASKING POLICY email_mask"#,
        r#"DROP MASKING POLICY IF EXISTS email_mask"#,
        r#"CREATE VIRTUAL COLUMN (a['k1']['k2'], b[0][1]) FOR t"#,
        r#"CREATE OR REPLACE VIRTUAL COLUMN (a['k1']['k2'], b[0][1]) FOR t"#,
        r#"ALTER VIRTUAL COLUMN (a['k1']['k2'], b[0][1]) FOR t"#,
        r#"DROP VIRTUAL COLUMN FOR t"#,
        r#"REFRESH VIRTUAL COLUMN FOR t"#,
        r#"CREATE NETWORK POLICY mypolicy ALLOWED_IP_LIST=('192.168.10.0/24') BLOCKED_IP_LIST=('192.168.10.99') COMMENT='test'"#,
        r#"CREATE OR REPLACE NETWORK POLICY mypolicy ALLOWED_IP_LIST=('192.168.10.0/24') BLOCKED_IP_LIST=('192.168.10.99') COMMENT='test'"#,
        r#"ALTER NETWORK POLICY mypolicy SET ALLOWED_IP_LIST=('192.168.10.0/24','192.168.255.1') BLOCKED_IP_LIST=('192.168.1.99') COMMENT='test'"#,
        // dynamic tables
        r#"
            CREATE OR REPLACE DYNAMIC TABLE db.MyDynamic LIKE t
                TARGET_LAG = 10 SECOND
                WAREHOUSE = 'MyWarehouse'
                REFRESH_MODE = FULL
                INITIALIZE = ON_CREATE
                COMMENT = 'This is test dynamic table'
            AS
                SELECT * FROM t
        "#,
        r#"
            CREATE DYNAMIC TABLE IF NOT EXISTS db.MyDynamic (a int, b string)
                TARGET_LAG = 10 MINUTE
                WAREHOUSE = 'MyWarehouse'
                REFRESH_MODE = INCREMENTAL
                INITIALIZE = ON_SCHEDULE
                COMMENT = 'This is test dynamic table'
            AS
                SELECT * FROM t
        "#,
        r#"
            CREATE DYNAMIC TABLE db.MyDynamic (a int, b string)
                CLUSTER BY (a)
                TARGET_LAG = 10 HOUR
                REFRESH_MODE = AUTO
                COMMENT = 'This is test dynamic table'
                STORAGE_FORMAT = 'native'
            AS
                SELECT c, d FROM t
        "#,
        r#"
            CREATE TRANSIENT DYNAMIC TABLE IF NOT EXISTS MyDynamic (a int, b string)
                CLUSTER BY (a)
                TARGET_LAG = 10 DAY
            AS
                SELECT avg(a), d FROM t GROUP BY d
        "#,
        r#"
            CREATE TRANSIENT DYNAMIC TABLE IF NOT EXISTS MyDynamic (a int, b string)
                CLUSTER BY (a)
                REFRESH_MODE = INCREMENTAL
                TARGET_LAG = DOWNSTREAM
            AS
                SELECT avg(a), d FROM db.t GROUP BY d
        "#,
        // tasks
        r#"CREATE TASK IF NOT EXISTS MyTask1 WAREHOUSE = 'MyWarehouse' SCHEDULE = 15 MINUTE SUSPEND_TASK_AFTER_NUM_FAILURES = 3 ERROR_INTEGRATION = 'notification_name' COMMENT = 'This is test task 1' DATABASE = 'target', TIMEZONE = 'America/Los Angeles' AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 WAREHOUSE = 'MyWarehouse' SCHEDULE = 15 SECOND SUSPEND_TASK_AFTER_NUM_FAILURES = 3 COMMENT = 'This is test task 1' AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 WAREHOUSE = 'MyWarehouse' SCHEDULE = 1215 SECOND SUSPEND_TASK_AFTER_NUM_FAILURES = 3 COMMENT = 'This is test task 1' AS SELECT * FROM MyTable1"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 6 * * *' 'America/Los_Angeles' COMMENT = 'serverless + cron' AS insert into t (c1, c2) values (1, 2), (3, 4)"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 12 * * *' AS copy into streams_test.paper_table from @stream_stage FILE_FORMAT = (TYPE = PARQUET) PURGE=true"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 SCHEDULE = USING CRON '0 13 * * *' AS COPY INTO @my_internal_stage FROM canadian_city_population FILE_FORMAT = (TYPE = PARQUET)"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 AFTER 'task2', 'task3' WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('task_name') != 'VALIDATION' AS VACUUM TABLE t"#,
        r#"CREATE TASK IF NOT EXISTS MyTask1 DATABASE = 'target', TIMEZONE = 'America/Los Angeles'  AS VACUUM TABLE t"#,
        r#"
            CREATE TASK IF NOT EXISTS MyTask1 DATABASE = 'target', TIMEZONE = 'America/Los Angeles' as
                BEGIN
                begin;
                insert into t values('a;');
                delete from t where c = ';';
                vacuum table t;
                merge into t using s on t.id = s.id when matched then update *;
                commit;
                END
        "#,
        r#"
            CREATE TASK IF NOT EXISTS merge_task WAREHOUSE = 'test-parser' SCHEDULE = 1 SECOND
            AS BEGIN
            MERGE INTO t USING s ON t.c = s.c
                WHEN MATCHED THEN
                UPDATE
                    *
                    WHEN NOT MATCHED THEN
                INSERT
                    *;
            END"#,
        r#"CREATE TASK IF NOT EXISTS merge_task WAREHOUSE = 'test-parser' SCHEDULE = 1 SECOND
            AS BEGIN
            MERGE INTO t USING s ON t.c = s.c
                WHEN MATCHED THEN
                UPDATE
                    *
                    WHEN NOT MATCHED THEN
                INSERT values('a;', 1, "str");
            END
        "#,
        r#"ALTER TASK MyTask1 RESUME"#,
        r#"ALTER TASK MyTask1 SUSPEND"#,
        r#"ALTER TASK MyTask1 ADD AFTER 'task2', 'task3'"#,
        r#"ALTER TASK MyTask1 REMOVE AFTER 'task2'"#,
        r#"ALTER TASK MyTask1 SET WAREHOUSE= 'MyWarehouse' SCHEDULE = USING CRON '0 6 * * *' 'America/Los_Angeles' COMMENT = 'serverless + cron'"#,
        r#"ALTER TASK MyTask1 SET WAREHOUSE= 'MyWarehouse' SCHEDULE = 13 MINUTE SUSPEND_TASK_AFTER_NUM_FAILURES = 10 COMMENT = 'serverless + cron'"#,
        r#"ALTER TASK MyTask1 SET WAREHOUSE= 'MyWarehouse' SCHEDULE = 5 SECOND SUSPEND_TASK_AFTER_NUM_FAILURES = 10 COMMENT = 'serverless + cron'"#,
        r#"ALTER TASK MyTask1 SET DATABASE='newDB', TIMEZONE='America/Los_Angeles'"#,
        r#"ALTER TASK MyTask1 SET ERROR_INTEGRATION = 'candidate_notifictaion'"#,
        r#"ALTER TASK MyTask2 MODIFY AS SELECT CURRENT_VERSION()"#,
        r#"
            ALTER TASK MyTask2 MODIFY AS
            BEGIN
              begin;
              insert into t values('a;');
              delete from t where c = ';';
              vacuum table t;
              merge into t using s on t.id = s.id when matched then update *;
              commit;
            END
        "#,
        r#"ALTER TASK MyTask1 MODIFY WHEN SYSTEM$GET_PREDECESSOR_RETURN_VALUE('task_name') != 'VALIDATION'"#,
        r#"DROP TASK MyTask1"#,
        r#"SHOW TASKS"#,
        r#"EXECUTE TASK MyTask"#,
        r#"DESC TASK MyTask"#,
        r#"CREATE CONNECTION IF NOT EXISTS my_conn STORAGE_TYPE='s3'"#,
        r#"CREATE CONNECTION IF NOT EXISTS my_conn STORAGE_TYPE='s3' any_arg='any_value'"#,
        r#"CREATE OR REPLACE CONNECTION my_conn STORAGE_TYPE='s3' any_arg='any_value'"#,
        r#"DROP CONNECTION IF EXISTS my_conn;"#,
        r#"DESC CONNECTION my_conn;"#,
        r#"SHOW CONNECTIONS;"#,
        r#"SHOW LOCKS IN ACCOUNT"#,
        // pipes
        r#"CREATE PIPE IF NOT EXISTS MyPipe1 AUTO_INGEST = TRUE COMMENT = 'This is test pipe 1' AS COPY INTO MyTable1 FROM '@~/MyStage1' FILE_FORMAT = (TYPE = 'CSV')"#,
        r#"CREATE PIPE pipe1 AS COPY INTO db1.MyTable1 FROM @~/mybucket/data.csv"#,
        r#"ALTER PIPE mypipe REFRESH"#,
        r#"ALTER PIPE mypipe REFRESH PREFIX='d1/'"#,
        r#"ALTER PIPE mypipe REFRESH PREFIX='d1/' MODIFIED_AFTER='2018-07-30T13:56:46-07:00'"#,
        r#"ALTER PIPE mypipe SET PIPE_EXECUTION_PAUSED = true"#,
        r#"DROP PIPE mypipe"#,
        r#"DESC PIPE mypipe"#,
        // notification
        r#"CREATE NOTIFICATION INTEGRATION IF NOT EXISTS SampleNotification type = webhook enabled = true webhook = (url = 'https://example.com', method = 'GET', authorization_header = 'bearer auth')"#,
        r#"CREATE NOTIFICATION INTEGRATION SampleNotification type = webhook enabled = true webhook = (url = 'https://example.com') COMMENT = 'notify'"#,
        r#"ALTER NOTIFICATION INTEGRATION SampleNotification SET enabled = true"#,
        r#"ALTER NOTIFICATION INTEGRATION SampleNotification SET webhook = (url = 'https://example.com')"#,
        r#"ALTER NOTIFICATION INTEGRATION SampleNotification SET comment = '1'"#,
        r#"DROP NOTIFICATION INTEGRATION SampleNotification"#,
        r#"DESC NOTIFICATION INTEGRATION SampleNotification"#,
        "--各环节转各环节转各环节转各环节转各\n  select 34343",
        "-- 96477300355	31379974136	3.074486292973661\nselect 34343",
        "-- xxxxx\n  select 34343;",
        r#"REMOVE @t;"#,
        r#"SELECT sum(d) OVER (w) FROM e;"#,
        r#"SELECT sum(d) OVER w FROM e WINDOW w AS (PARTITION BY f ORDER BY g);"#,
        r#"GRANT OWNERSHIP ON d20_0014.* TO ROLE 'd20_0015_owner';"#,
        r#"GRANT OWNERSHIP ON d20_0014.t TO ROLE 'd20_0015_owner';"#,
        r#"GRANT OWNERSHIP ON STAGE s1 TO ROLE 'd20_0015_owner';"#,
        r#"GRANT OWNERSHIP ON UDF f1 TO ROLE 'd20_0015_owner';"#,
        r#"attach table t 's3://a' connection=(access_key_id ='x' secret_access_key ='y' endpoint_url='http://127.0.0.1:9900')"#,
        r#"CREATE FUNCTION IF NOT EXISTS isnotempty AS(p) -> not(is_null(p));"#,
        r#"CREATE OR REPLACE FUNCTION isnotempty_test_replace AS(p) -> not(is_null(p))  DESC = 'This is a description';"#,
        r#"CREATE FUNCTION binary_reverse (BINARY) RETURNS BINARY LANGUAGE python HANDLER = 'binary_reverse' ADDRESS = 'http://0.0.0.0:8815';"#,
        r#"CREATE OR REPLACE FUNCTION binary_reverse (BINARY) RETURNS BINARY LANGUAGE python HANDLER = 'binary_reverse' ADDRESS = 'http://0.0.0.0:8815';"#,
        r#"CREATE STAGE s file_format=(record_delimiter='\n' escape='\\');"#,
        r#"
            create or replace function addone(int)
            returns int
            language python
            handler = 'addone_py'
            as
            $$
            def addone_py(i):
            return i+1
            $$;
        "#,
        r#"DROP FUNCTION binary_reverse;"#,
        r#"DROP FUNCTION isnotempty;"#,
        r#"
            EXECUTE IMMEDIATE 
            $$
            BEGIN
                LOOP
                    RETURN 1;
                END LOOP;
            END;
            $$
        "#,
    ];

    for case in cases {
        let src = unindent::unindent(case);
        let src = src.trim();
        let tokens = tokenize_sql(src).unwrap();
        let (stmt, fmt) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", src).unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", stmt).unwrap();
        writeln!(file, "---------- AST ------------").unwrap();
        writeln!(file, "{:#?}", stmt).unwrap();
        writeln!(file, "\n").unwrap();
        if fmt.is_some() {
            writeln!(file, "---------- FORMAT ------------").unwrap();
            writeln!(file, "{:#?}", fmt).unwrap();
        }
    }
}

#[test]
fn test_statement_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("statement-error.txt").unwrap();

    let cases = &[
        r#"create table a.b (c integer not null 1, b float(10))"#,
        r#"create table a (c float(10))"#,
        r#"create table a (c varch)"#,
        r#"create table a (c tuple())"#,
        r#"create table a (c decimal)"#,
        r#"create table a (b tuple(c int, uint64));"#,
        r#"create table a (b tuple("c-1" int, "c-2" uint64));"#,
        r#"CREATE TABLE t(c1 NULLABLE(int) NOT NULL);"#,
        r#"drop table if a.b"#,
        r#"truncate table a.b.c.d"#,
        r#"truncate a"#,
        r#"drop a"#,
        r#"insert into t format"#,
        r#"show tables format"#,
        r#"alter database system x rename to db"#,
        r#"create user 'test-e' identified bi 'password';"#,
        r#"create user 'test-e'@'localhost' identified by 'password';"#,
        r#"drop usar if exists 'test-j';"#,
        r#"alter user 'test-e' identifies by 'new-password';"#,
        r#"create role 'test'@'%';"#,
        r#"drop role 'test'@'%';"#,
        r#"SHOW GRANT FOR ROLE 'role1';"#,
        r#"GRANT ROLE 'test' TO ROLE test-user;"#,
        r#"GRANT SELECT, ALL PRIVILEGES, CREATE ON * TO 'test-grant';"#,
        r#"GRANT SELECT, CREATE ON *.c TO 'test-grant';"#,
        r#"GRANT select ON UDF a TO 'test-grant';"#,
        r#"REVOKE SELECT, CREATE, ALL PRIVILEGES ON * FROM 'test-grant';"#,
        r#"REVOKE SELECT, CREATE ON * TO 'test-grant';"#,
        r#"COPY INTO mytable FROM 's3://bucket' CREDENTIAL = ();"#,
        r#"COPY INTO mytable FROM @mystage CREDENTIALS = ();"#,
        r#"CALL system$test"#,
        r#"CALL system$test(a"#,
        r#"show settings ilike 'enable%'"#,
        r#"PRESIGN INVALID @my_stage/path/to/file"#,
        r#"SELECT c a as FROM t"#,
        r#"SELECT c a as b FROM t"#,
        r#"SELECT top -1 c a as b FROM t"#,
        r#"SELECT top c a as b FROM t"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS a, b"#,
        r#"SELECT * FROM t GROUP BY GROUPING SETS ()"#,
        r#"select * from aa.bb limit 10 order by bb;"#,
        r#"select * from aa.bb offset 10 order by bb;"#,
        r#"select * from aa.bb offset 10 limit 1;"#,
        r#"select * from aa.bb order by a order by b;"#,
        r#"select * from aa.bb offset 10 offset 20;"#,
        r#"select * from aa.bb limit 10 limit 20;"#,
        r#"select * from aa.bb limit 10,2 offset 2;"#,
        r#"select * from aa.bb limit 10,2,3;"#,
        r#"with a as (select 1) with b as (select 2) select * from aa.bb;"#,
        r#"with as t2(tt) as (select a from t) select t2.tt from t2"#,
        r#"copy into t1 from "" FILE"#,
        r#"select $1 from @data/csv/books.csv (file_format => 'aa' bad_arg => 'x', pattern => 'bb')"#,
        r#"copy into t1 from "" FILE_FORMAT"#,
        r#"copy into t1 from "" FILE_FORMAT = "#,
        r#"copy into t1 from "" FILE_FORMAT = ("#,
        r#"copy into t1 from "" FILE_FORMAT = (TYPE"#,
        r#"copy into t1 from "" FILE_FORMAT = (TYPE ="#,
        r#"copy into t1 from "" FILE_FORMAT = (TYPE ="#,
        r#"COPY INTO t1 FROM "" PATTERN = '.*[.]csv' FILE_FORMAT = (type = TSV field_delimiter = '\t' skip_headerx = 0);"#,
        r#"COPY INTO mytable
                FROM @my_stage
                FILE_FORMAT = (
                    type = CSV,
                    error_on_column_count_mismatch = 1
                )"#,
        r#"CREATE CONNECTION IF NOT EXISTS my_conn"#,
        r#"select $0 from t1"#,
        r#"GRANT OWNERSHIP, SELECT ON d20_0014.* TO ROLE 'd20_0015_owner';"#,
        r#"GRANT OWNERSHIP ON d20_0014.* TO USER A;"#,
        r#"REVOKE OWNERSHIP, SELECT ON d20_0014.* FROM ROLE 'd20_0015_owner';"#,
        r#"REVOKE OWNERSHIP ON d20_0014.* FROM USER A;"#,
        r#"REVOKE OWNERSHIP ON d20_0014.* FROM ROLE A;"#,
        r#"GRANT OWNERSHIP ON *.* TO ROLE 'd20_0015_owner';"#,
        r#"CREATE FUNCTION IF NOT EXISTS isnotempty AS(p) -> not(is_null(p)"#,
        r#"drop table :a"#,
        r#"drop table IDENTIFIER(a)"#,
        r#"drop table IDENTIFIER(:a)"#,
    ];

    for case in cases {
        let tokens = tokenize_sql(case).unwrap();
        let err = parse_sql(&tokens, Dialect::PostgreSQL).unwrap_err();
        writeln!(file, "---------- Input ----------").unwrap();
        writeln!(file, "{}", case).unwrap();
        writeln!(file, "---------- Output ---------").unwrap();
        writeln!(file, "{}", err.message()).unwrap();
    }
}

#[test]
fn test_raw_insert_stmt() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("raw-insert.txt").unwrap();
    let cases = &[
        r#"insert into t (c1, c2) values (1, 2), (3, 4);"#,
        r#"insert into t (c1, c2) values (1, 2);"#,
        r#"insert into table t format json;"#,
        r#"insert into table t select * from t2;"#,
    ];

    for case in cases {
        run_parser(file, insert_stmt(true), case);
    }
}

#[test]
fn test_query() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("query.txt").unwrap();
    let cases = &[
        r#"select * exclude c1, b.* exclude (c2, c3, c4) from customer inner join orders on a = b limit 1"#,
        r#"select columns('abc'), columns(a -> length(a) = 3) from t"#,
        r#"select * from customer at(offset => -10 * 30)"#,
        r#"select * from customer inner join orders"#,
        r#"select * from customer cross join orders"#,
        r#"select * from customer inner join orders on (a = b)"#,
        r#"select * from customer inner join orders on a = b limit 1"#,
        r#"select * from customer inner join orders on a = b limit 2 offset 3"#,
        r#"select * from customer natural full join orders"#,
        r#"select * from customer natural join orders left outer join detail using (id)"#,
        r#"with t2(tt) as (select a from t) select t2.tt from t2  where t2.tt > 1"#,
        r#"with t2(tt) as materialized (select a from t) select t2.tt from t2  where t2.tt > 1"#,
        r#"with t2 as (select a from t) select t2.a from t2  where t2.a > 1"#,
        r#"with t2(tt) as materialized (select a from t), t3 as materialized (select * from t), t4 as (select a from t where a > 1) select t2.tt, t3.a, t4.a from t2, t3, t4 where t2.tt > 1"#,
        r#"with recursive t2(tt) as (select a from t1 union select tt from t2) select t2.tt from t2"#,
        r#"with t(a,b) as (values(1,1),(2,null),(null,5)) select t.a, t.b from t"#,
        r#"select c_count cc, count(*) as custdist, sum(c_acctbal) as totacctbal
            from customer, orders ODS,
                (
                    select
                        c_custkey,
                        count(o_orderkey)
                    from
                        customer left outer join orders on
                            c_custkey = o_custkey
                            and o_comment not like '%:1%:2%'
                    group by
                        c_custkey
                ) as c_orders
            group by c_count
            order by custdist desc nulls first, c_count asc, totacctbal nulls last
            limit 10, totacctbal"#,
        r#"select * from t1 union select * from t2"#,
        r#"select * from t1 except select * from t2"#,
        r#"select * from t1 union select * from t2 union select * from t3"#,
        r#"select * from t1 union select * from t2 union all select * from t3"#,
        r#"select * from t1 union select * from t2 intersect select * from t3"#,
        r#"(select * from t1 union select * from t2) union select * from t3"#,
        r#"select * from t1 union (select * from t2 union select * from t3)"#,
        r#"SELECT * FROM ((SELECT *) EXCEPT (SELECT *)) foo"#,
        r#"SELECT * FROM (((SELECT *) EXCEPT (SELECT *))) foo"#,
        r#"SELECT * FROM (SELECT * FROM xyu ORDER BY x, y) AS xyu"#,
        r#"select * from monthly_sales pivot(sum(amount) for month in ('JAN', 'FEB', 'MAR', 'APR')) order by empid"#,
        r#"select * from monthly_sales_1 unpivot(sales for month in (jan, feb, mar, april)) order by empid"#,
        r#"select * from range(1, 2)"#,
        r#"select sum(a) over w from customer window w as (partition by a order by b)"#,
        r#"select a, sum(a) over w, sum(a) over w1, sum(a) over w2 from t1 window w as (partition by a), w2 as (w1 rows current row), w1 as (w order by a) order by a"#,
        r#"SELECT * FROM ((SELECT * FROM xyu ORDER BY x, y)) AS xyu"#,
        r#"SELECT * FROM (VALUES(1,1),(2,null),(null,5)) AS t(a,b)"#,
        r#"VALUES(1,'a'),(2,'b'),(null,'c') order by col0 limit 2"#,
        r#"select * from t left join lateral(select 1) on true, lateral(select 2)"#,
        r#"select * from t, lateral flatten(input => u.col) f"#,
        r#"select * from flatten(input => parse_json('{"a":1, "b":[77,88]}'), outer => true)"#,
    ];

    for case in cases {
        run_parser(file, query, case);
    }
}

#[test]
fn test_query_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("query-error.txt").unwrap();
    let cases = &[
        r#"select * from customer join where a = b"#,
        r#"from t1 select * from t2"#,
        r#"from t1 select * from t2 where a = b"#,
        r#"select * from join customer"#,
        r#"select * from customer natural inner join orders on a = b"#,
        r#"select * order a"#,
        r#"select * order"#,
        r#"select number + 5 as a, cast(number as float(255))"#,
        r#"select 1 1"#,
    ];

    for case in cases {
        run_parser(file, query, case);
    }
}

#[test]
fn test_expr() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("expr.txt").unwrap();

    let cases = &[
        r#"a"#,
        r#"'I''m who I\'m.'"#,
        r#"'\776 \n \t \u0053 \xaa'"#,
        r#"char(0xD0, 0xBF, 0xD1)"#,
        r#"[42, 3.5, 4., .001, 5e2, 1.925e-3, .38e+7, 1.e-01, 0xfff, x'deedbeef']"#,
        r#"123456789012345678901234567890"#,
        r#"$$ab123c$$"#,
        r#"x'123456789012345678901234567890'"#,
        r#"1e100000000000000"#,
        r#"100_100_000"#,
        r#"1_12200_00"#,
        r#".1"#,
        r#"-1"#,
        r#"(1)"#,
        r#"(1,)"#,
        r#"(1,2)"#,
        r#"(1,2,)"#,
        r#"[1]"#,
        r#"[1,]"#,
        r#"[[1]]"#,
        r#"[[1],[2]]"#,
        r#"[[[1,2,3],[4,5,6]],[[7,8,9]]][0][1][2]"#,
        r#"((1 = 1) or 1)"#,
        r#"typeof(1 + 2)"#,
        r#"- - + + - 1 + + - 2"#,
        r#"0XFF + 0xff + 0xa + x'ffff'"#,
        r#"1 - -(- - -1)"#,
        r#"1 + a * c.d"#,
        r#"number % 2"#,
        r#""t":k1.k2"#,
        r#""t":k1.k2.0"#,
        r#"t.0"#,
        r#"(NULL,).0"#,
        r#"col1 not between 1 and 2"#,
        r#"sum(col1)"#,
        r#""random"()"#,
        r#"random(distinct)"#,
        r#"covar_samp(number, number)"#,
        r#"CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"TRY_CAST(col1 AS BIGINT UNSIGNED)"#,
        r#"TRY_CAST(col1 AS TUPLE(BIGINT UNSIGNED NULL, BOOLEAN))"#,
        r#"trim(leading 'abc' from 'def')"#,
        r#"extract(year from d)"#,
        r#"date_part(year, d)"#,
        r#"position('a' in str)"#,
        r#"substring(a from b for c)"#,
        r#"substring(a, b, c)"#,
        r#"col1::UInt8"#,
        r#"(arr[0]:a).b"#,
        r#"arr[4]["k"]"#,
        r#"a rlike '^11'"#,
        r#"'中文'::text not in ('a', 'b')"#,
        r#"G.E.B IS NOT NULL AND col1 not between col2 and (1 + col3) DIV sum(col4)"#,
        r#"sum(CASE WHEN n2.n_name = 'GERMANY' THEN ol_amount ELSE 0 END) / CASE WHEN sum(ol_amount) = 0 THEN 1 ELSE sum(ol_amount) END"#,
        r#"p_partkey = l_partkey
            AND p_brand = 'Brand#12'
            AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l_quantity >= CAST (1 AS smallint) AND l_quantity <= CAST (1 + 10 AS smallint)
            AND p_size BETWEEN CAST (1 AS smallint) AND CAST (5 AS smallint)
            AND l_shipmode IN ('AIR', 'AIR REG')
            AND l_shipinstruct = 'DELIVER IN PERSON'"#,
        r#"nullif(1, 1)"#,
        r#"nullif(a, b)"#,
        r#"coalesce(1, 2, 3)"#,
        r#"coalesce(a, b, c)"#,
        r#"ifnull(1, 1)"#,
        r#"ifnull(a, b)"#,
        r#"1 is distinct from 2"#,
        r#"a is distinct from b"#,
        r#"1 is not distinct from null"#,
        r#"{'k1':1,'k2':2}"#,
        // window expr
        r#"ROW_NUMBER() OVER (ORDER BY salary DESC)"#,
        r#"SUM(salary) OVER ()"#,
        r#"AVG(salary) OVER (PARTITION BY department)"#,
        r#"SUM(salary) OVER (PARTITION BY department ORDER BY salary DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"#,
        r#"AVG(salary) OVER (PARTITION BY department ORDER BY hire_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS UNBOUNDED PRECEDING)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS CURRENT ROW)"#,
        r#"COUNT() OVER (ORDER BY hire_date ROWS 3 PRECEDING)"#,
        r#"ARRAY_APPLY([1,2,3], x -> x + 1)"#,
        r#"ARRAY_FILTER(col, y -> y % 2 = 0)"#,
        r#"(current_timestamp, current_timestamp(), now())"#,
        r#"ARRAY_REDUCE([1,2,3], (acc,t) -> acc + t)"#,
    ];

    for case in cases {
        run_parser(file, expr, case);
    }
}

#[test]
fn test_expr_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("expr-error.txt").unwrap();

    let cases = &[
        r#"5 * (a and ) 1"#,
        r#"a + +"#,
        r#"CAST(col1 AS foo)"#,
        r#"1 a"#,
        r#"CAST(col1)"#,
        r#"a.add(b)"#,
        r#"[ x * 100 FOR x in [1,2,3] if x % 2 = 0 ]"#,
        r#"
            G.E.B IS NOT NULL
            AND col1 NOT BETWEEN col2 AND
            AND 1 + col3 DIV sum(col4)
        "#,
    ];

    for case in cases {
        run_parser(file, expr, case);
    }
}

#[test]
fn test_dialect() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("dialect.txt").unwrap();

    let cases = &[
        r#"'a'"#,
        r#""a""#,
        r#"`a`"#,
        r#"'a''b'"#,
        r#"'a""b'"#,
        r#"'a\'b'"#,
        r#"'a"b'"#,
        r#"'a`b'"#,
        r#""a''b""#,
        r#""a""b""#,
        r#""a'b""#,
        r#""a\"b""#,
        r#""a`b""#,
    ];

    for case in cases {
        run_parser_with_dialect(file, expr, Dialect::PostgreSQL, ParseMode::Default, case);
    }

    for case in cases {
        run_parser_with_dialect(file, expr, Dialect::MySQL, ParseMode::Default, case);
    }

    let cases = &[
        r#"a"#,
        r#"a.add(b)"#,
        r#"a.sub(b).add(e)"#,
        r#"a.sub(b).add(e)"#,
        r#"1 + {'k1': 4}.k1"#,
        r#"'3'.plus(4)"#,
        r#"(3).add({'k1': 4 }.k1)"#,
        r#"[ x * 100 FOR x in [1,2,3] if x % 2 = 0 ]"#,
    ];

    for case in cases {
        run_parser_with_dialect(file, expr, Dialect::Experimental, ParseMode::Default, case);
    }
}

#[test]
fn test_script() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("script.txt").unwrap();

    let cases = &[
        r#"LET cost := 100.0"#,
        r#"LET t1 RESULTSET := SELECT * FROM numbers(100)"#,
        r#"profit := revenue - cost"#,
        r#"RETURN"#,
        r#"RETURN profit"#,
        r#"RETURN TABLE(t1)"#,
        r#"RETURN TABLE(select count(*) from t1)"#,
        r#"
            FOR i IN REVERSE 1 TO maximum_count DO
                counter := counter + 1;
            END FOR label1
        "#,
        r#"
            FOR rec IN resultset DO
                CONTINUE;
            END FOR label1
        "#,
        r#"
            FOR rec IN SELECT * FROM numbers(100) DO
                CONTINUE;
            END FOR label1
        "#,
        r#"
            WHILE counter < maximum_count DO
                CONTINUE label1;
            END WHILE label1
        "#,
        r#"
            REPEAT
                BREAK;
            UNTIL counter = maximum_count
            END REPEAT label1
        "#,
        r#"
            LOOP
                BREAK label1;
            END LOOP label1
        "#,
        r#"
            CASE
                WHEN counter = 1 THEN
                    counter := counter + 1;
                WHEN counter = 2 THEN
                    counter := counter + 2;
                ELSE
                    counter := counter + 3;
            END
        "#,
        r#"
            CASE counter
                WHEN 1 THEN
                    counter := counter + 1;
                WHEN 2 THEN
                    counter := counter + 2;
                ELSE
                    counter := counter + 3;
            END CASE
        "#,
        r#"
            IF counter = 1 THEN
                counter := counter + 1;
            ELSEIF counter = 2 THEN
                counter := counter + 2;
            ELSE
                counter := counter + 3;
            END IF
        "#,
        r#"
            LOOP
                SELECT c1, c2 FROM t WHERE c1 = 1;
            END LOOP
        "#,
        r#"select :a + 1"#,
        r#"select IDENTIFIER(:b)"#,
        r#"select a.IDENTIFIER(:b).c + minus(:d)"#,
    ];

    for case in cases {
        run_parser_with_dialect(
            file,
            script_stmt,
            Dialect::PostgreSQL,
            ParseMode::Template,
            case,
        );
    }

    let cases = vec![
        r#"
            BEGIN
                LOOP
                    CONTINUE;
                END LOOP;
            END;
        "#,
        r#"
            DECLARE
                x := 1;
            BEGIN
                FOR y in x TO 10 DO
                    CONTINUE;
                END FOR;
            END;
        "#,
    ];

    for case in cases {
        run_parser_with_dialect(
            file,
            script_block,
            Dialect::PostgreSQL,
            ParseMode::Template,
            case,
        );
    }
}

#[test]
fn test_quote() {
    let cases = &[
        ("a", "a"),
        ("_", "_"),
        ("_abc", "_abc"),
        ("_abc12", "_abc12"),
        ("_12a", "_12a"),
        ("12a", "\"12a\""),
        ("12", "\"12\""),
        ("🍣", "\"🍣\""),
        ("価格", "\"価格\""),
        ("\t", "\"\t\""),
        ("complex \"string\"", "\"complex \"\"string\"\"\""),
        ("\"\"\"", "\"\"\"\"\"\"\"\""),
        ("'''", "\"'''\""),
        ("name\"with\"quote", "\"name\"\"with\"\"quote\""),
    ];
    for (input, want) in cases {
        let quoted = quote_ident(input, '"', false);
        assert_eq!(quoted, *want);
        let unquoted = unquote_ident(&quoted, '"');
        assert_eq!(unquoted, *input, "unquote({}) got {}", quoted, unquoted);
    }
}
