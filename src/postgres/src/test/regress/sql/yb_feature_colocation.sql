--
-- Colocation
--

CREATE DATABASE colocation_test colocated = true;
\c colocation_test

-- This test should be changed once we complete https://github.com/yugabyte/yugabyte-db/issues/3034
CREATE TABLE foo1 (a INT); -- fail
CREATE TABLE foo1 (a INT, PRIMARY KEY (a ASC));
CREATE TABLE foo2 (a INT, b INT, PRIMARY KEY (a ASC));
-- opt out of using colocated tablet
CREATE TABLE foo3 (a INT) WITH (colocated = false);
-- multi column primary key table
CREATE TABLE foo4(a INT, b INT, PRIMARY KEY (a ASC, b DESC));

INSERT INTO foo1 (a) VALUES (0), (1), (2);
INSERT INTO foo1 (a, b) VALUES (0, '0'); -- fail
INSERT INTO foo2 (a, b) VALUES (0, '0'), (1, '1');
INSERT INTO foo3 (a) VALUES (0), (1), (2), (3);
INSERT INTO foo4 (a, b) VALUES (0, 0), (0, 1), (1, 0), (1, 1);

SELECT * FROM foo1;
SELECT * FROM foo1 WHERE a = 2;
SELECT * FROM foo1 WHERE n = '0'; -- fail
SELECT * FROM foo2;
SELECT * FROM foo3 ORDER BY a ASC;
SELECT * FROM foo4;

-- table with index
CREATE TABLE bar (a INT, b INT, PRIMARY KEY (a ASC));
CREATE INDEX ON bar (a);
INSERT INTO bar (a, b) VALUES (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
EXPLAIN SELECT * FROM bar WHERE a = 1;
SELECT * FROM bar WHERE a = 1;
UPDATE bar SET b = b + 1 WHERE a > 3;
SELECT * FROM bar;
DELETE FROM bar WHERE a > 3;
SELECT * FROM bar;
