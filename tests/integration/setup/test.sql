CREATE DATABASE IF NOT EXISTS db_name;
USE db_name;
CREATE TABLE IF NOT EXISTS db_name.table_name (number Int, word String);
INSERT INTO db_name.table_name VALUES (1, "foo"), (2, "bar"), (3, "grok");
SELECT CONCAT("Inserted Rows: ", COUNT(*)) FROM db_name.table_name;
!quit
