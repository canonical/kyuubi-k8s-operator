-- Copyright 2024 Canonical Ltd.
-- See LICENSE file for licensing details.

CREATE DATABASE IF NOT EXISTS kyuubidb;
USE kyuubidb;
CREATE TABLE IF NOT EXISTS kyuubidb.testTable (number Int, word String);
INSERT INTO kyuubidb.testTable VALUES (1, "foo"), (2, "bar"), (3, "grok");
SELECT CONCAT("Inserted Rows: ", COUNT(*)) FROM kyuubidb.testTable;
!quit