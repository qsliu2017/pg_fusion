SET extra_float_digits = 0;

CREATE TEMP TABLE char_tbl(f1 char(4));
INSERT INTO char_tbl VALUES ('a'), ('ab'), ('abcd'), ('abcd    ');
ANALYZE char_tbl;

CREATE TEMP TABLE text_tbl(f1 text);
INSERT INTO text_tbl VALUES ('doh!'), ('hi de ho neighbor');
ANALYZE text_tbl;

CREATE TEMP TABLE varchar_tbl(f1 varchar(4));
INSERT INTO varchar_tbl VALUES ('a'), ('ab'), ('abcd'), ('abcd    ');
ANALYZE varchar_tbl;

CREATE TEMP TABLE booltbl1(f1 bool);
INSERT INTO booltbl1 VALUES (true), (true), (true), (false);
ANALYZE booltbl1;

CREATE TEMP TABLE booltbl2(f1 bool);
INSERT INTO booltbl2 VALUES (false), (false), (false), (false);
ANALYZE booltbl2;

CREATE TEMP TABLE onek (
    unique1 int4,
    unique2 int4,
    two int4,
    four int4,
    ten int4,
    twenty int4,
    hundred int4,
    thousand int4,
    twothousand int4,
    fivethous int4,
    tenthous int4,
    odd int4,
    even int4,
    stringu1 text,
    stringu2 text,
    string4 text
);

INSERT INTO onek
SELECT
    g,
    999 - g,
    g % 2,
    g % 4,
    g % 10,
    g % 20,
    g % 100,
    g % 1000,
    g % 2000,
    g % 5000,
    g % 10000,
    CASE WHEN g % 2 = 1 THEN g ELSE NULL END,
    CASE WHEN g % 2 = 0 THEN g ELSE NULL END,
    'U' || lpad(g::text, 4, '0'),
    'V' || lpad((999 - g)::text, 4, '0'),
    'S' || (g % 4)::text
FROM generate_series(0, 999) AS g;

ANALYZE onek;

CREATE TEMP TABLE onek2 AS SELECT * FROM onek;
ANALYZE onek2;

CREATE TEMP TABLE tenk1 AS
SELECT
    g AS unique1,
    1999 - g AS unique2,
    g % 2 AS two,
    g % 4 AS four,
    g % 10 AS ten,
    g % 20 AS twenty,
    g % 100 AS hundred,
    g % 1000 AS thousand,
    g % 2000 AS twothousand,
    g % 5000 AS fivethous,
    g % 10000 AS tenthous,
    CASE WHEN g % 2 = 1 THEN g ELSE NULL END AS odd,
    CASE WHEN g % 2 = 0 THEN g ELSE NULL END AS even,
    'TU' || lpad(g::text, 4, '0') AS stringu1,
    'TV' || lpad((1999 - g)::text, 4, '0') AS stringu2,
    'TS' || (g % 4)::text AS string4
FROM generate_series(0, 1999) AS g;

ANALYZE tenk1;

CREATE TEMP TABLE tenk2 AS SELECT * FROM tenk1;
ANALYZE tenk2;

CREATE TEMP TABLE int2_tbl(f1 int2);
INSERT INTO int2_tbl VALUES (0), (1234), (-1234), (32767), (-32767);
ANALYZE int2_tbl;

CREATE TEMP TABLE int4_tbl(f1 int4);
INSERT INTO int4_tbl VALUES (0), (123456), (-123456), (2147483647), (-2147483647);
ANALYZE int4_tbl;

CREATE TEMP TABLE int8_tbl(q1 int8, q2 int8);
INSERT INTO int8_tbl VALUES
    (123, 456),
    (123, 4567890123456789),
    (4567890123456789, 123),
    (4567890123456789, 4567890123456789),
    (4567890123456789, -4567890123456789);
ANALYZE int8_tbl;

CREATE TEMP TABLE float4_tbl(f1 float4);
INSERT INTO float4_tbl VALUES (0.0), (1004.30), (-34.84), (1.2345678e20), (1.2345678e-20);
ANALYZE float4_tbl;

CREATE TEMP TABLE float8_tbl(f1 float8);
INSERT INTO float8_tbl VALUES (0.0), (1004.30), (-34.84), (1.2345678901234e200), (1.2345678901234e-200);
ANALYZE float8_tbl;

CREATE TEMP TABLE aggtest(a int2, b float4);
INSERT INTO aggtest VALUES (10, 1.5), (20, 2.5), (30, 3.5), (100, 4.5), (120, 5.5);
ANALYZE aggtest;

CREATE TEMP TABLE case_tbl(i int4, f float8);
INSERT INTO case_tbl VALUES (1, 1.0), (2, 2.0), (3, NULL), (4, 4.0), (101, 101.5);
ANALYZE case_tbl;

CREATE TEMP TABLE case2_tbl(i int4, j int4);
INSERT INTO case2_tbl VALUES (1, -1), (2, -2), (3, -3), (2, -4), (1, NULL), (NULL, -6);
ANALYZE case2_tbl;

CREATE TEMP TABLE foo(f1 int4);
INSERT INTO foo VALUES (42), (3), (10), (7), (NULL), (NULL), (1);
ANALYZE foo;

CREATE TEMP TABLE j1_tbl(i int4, j int4, t text);
CREATE TEMP TABLE j2_tbl(i int4, k int4);

INSERT INTO j1_tbl VALUES
    (1, 4, 'one'),
    (2, 3, 'two'),
    (3, 2, 'three'),
    (4, 1, 'four'),
    (5, 0, 'five'),
    (6, 6, 'six'),
    (7, 7, 'seven'),
    (8, 8, 'eight'),
    (0, NULL, 'zero'),
    (NULL, NULL, 'null'),
    (NULL, 0, 'zero');

INSERT INTO j2_tbl VALUES
    (1, -1),
    (2, 2),
    (3, -3),
    (2, 4),
    (5, -5),
    (5, -5),
    (0, NULL),
    (NULL, NULL),
    (NULL, 0);

ANALYZE j1_tbl;
ANALYZE j2_tbl;

CREATE TEMP TABLE booltbl3(d text, b bool, o int4);
INSERT INTO booltbl3 VALUES
    ('true', true, 1),
    ('false', false, 2),
    ('null', NULL, 3);
ANALYZE booltbl3;

CREATE TEMP TABLE booltbl4(isfalse bool, istrue bool, isnul bool);
INSERT INTO booltbl4 VALUES (false, true, NULL);
ANALYZE booltbl4;

CREATE TEMP TABLE bool_test(
    b1 bool,
    b2 bool,
    b3 bool,
    b4 bool
);
INSERT INTO bool_test VALUES
    (true, NULL, false, NULL),
    (false, true, NULL, NULL),
    (NULL, true, false, NULL);
ANALYZE bool_test;

CREATE TEMP TABLE test_having(a int4, b int4, c char(8), d char);
INSERT INTO test_having VALUES
    (0, 1, 'XXXX', 'A'),
    (1, 2, 'AAAA', 'b'),
    (2, 2, 'AAAA', 'c'),
    (3, 3, 'BBBB', 'D'),
    (4, 3, 'BBBB', 'e'),
    (5, 3, 'bbbb', 'F'),
    (6, 4, 'cccc', 'g'),
    (7, 4, 'cccc', 'h'),
    (8, 4, 'CCCC', 'I'),
    (9, 4, 'CCCC', 'j');
ANALYZE test_having;

CREATE TEMP TABLE subselect_tbl(
    f1 int4,
    f2 int4,
    f3 float4
);
INSERT INTO subselect_tbl VALUES
    (1, 2, 3),
    (2, 3, 4),
    (3, 4, 5),
    (1, 1, 1),
    (2, 2, 2),
    (3, 3, 3),
    (6, 7, 8),
    (8, 9, NULL);
ANALYZE subselect_tbl;

CREATE TEMP TABLE disttable(f1 int4);
INSERT INTO disttable VALUES (1), (2), (3), (NULL);
ANALYZE disttable;

CREATE TEMP TABLE x(x1 int4, x2 int4);
INSERT INTO x VALUES
    (1, 11),
    (2, 22),
    (3, NULL),
    (4, 44),
    (5, NULL);
ANALYZE x;

CREATE TEMP TABLE y(y1 int4, y2 int4);
INSERT INTO y VALUES
    (1, 111),
    (2, 222),
    (3, 333),
    (4, NULL);
ANALYZE y;

CREATE TEMP TABLE gstest1(a int4, b int4, v int4);
INSERT INTO gstest1 VALUES
    (1, 1, 10),
    (1, 1, 11),
    (1, 2, 12),
    (1, 2, 13),
    (1, 3, 14),
    (2, 3, 15),
    (3, 3, 16),
    (3, 4, 17),
    (4, 1, 18),
    (4, 1, 19);
ANALYZE gstest1;

CREATE TEMP TABLE gstest2(
    a int4,
    b int4,
    c int4,
    d int4,
    e int4,
    f int4,
    g int4,
    h int4
);
INSERT INTO gstest2 VALUES
    (1, 1, 1, 1, 1, 1, 1, 1),
    (1, 1, 1, 1, 1, 1, 1, 2),
    (1, 1, 1, 1, 1, 1, 2, 2),
    (1, 1, 1, 1, 1, 2, 2, 2),
    (1, 1, 1, 1, 2, 2, 2, 2),
    (1, 1, 1, 2, 2, 2, 2, 2),
    (1, 1, 2, 2, 2, 2, 2, 2),
    (1, 2, 2, 2, 2, 2, 2, 2),
    (2, 2, 2, 2, 2, 2, 2, 2);
ANALYZE gstest2;

CREATE TEMP TABLE gstest3(a int4, b int4, c int4, d int4);
INSERT INTO gstest3 VALUES (1, 1, 1, 1), (2, 2, 2, 2);
ANALYZE gstest3;

CREATE TEMP TABLE gstest_empty(a int4, b int4, v int4);
ANALYZE gstest_empty;

CREATE TEMP TABLE student(gpa float8);
INSERT INTO student VALUES (3.0), (3.4), (3.7);
ANALYZE student;

CREATE TEMP TABLE regr_test(x float8, y float8);
INSERT INTO regr_test VALUES
    (10, 150),
    (20, 250),
    (30, 350),
    (80, 540),
    (100, 200);
ANALYZE regr_test;

CREATE TEMP TABLE minmaxtest(f1 int4);
INSERT INTO minmaxtest VALUES (11), (12), (13), (14), (15), (16), (17), (18);
ANALYZE minmaxtest;

CREATE TEMP TABLE empsalary(
    depname varchar,
    empno int8,
    salary int4,
    enroll_date text
);
INSERT INTO empsalary VALUES
    ('develop', 10, 5200, '2007-08-01'),
    ('sales', 1, 5000, '2006-10-01'),
    ('personnel', 5, 3500, '2007-12-10'),
    ('sales', 4, 4800, '2007-08-08'),
    ('personnel', 2, 3900, '2006-12-23'),
    ('develop', 7, 4200, '2008-01-01'),
    ('develop', 9, 4500, '2008-01-01'),
    ('sales', 3, 4800, '2007-08-01'),
    ('develop', 8, 6000, '2006-10-01'),
    ('develop', 11, 5200, '2007-08-15');
ANALYZE empsalary;
