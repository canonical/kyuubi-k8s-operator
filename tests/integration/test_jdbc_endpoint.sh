#!/bin/bash
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

POD_NAME=$1
JDBC_ENDPOINT=$2
DB_NAME=${3:-testdb}
TABLE_NAME=${4:-testtable}
COMMANDS=$(cat ./tests/integration/setup/test.sql | sed "s/db_name/$DB_NAME/g" | sed "s/table_name/$TABLE_NAME/g")

echo -e "$(kubectl exec $POD_NAME -- \
        env CMDS="$COMMANDS" ENDPOINT="$JDBC_ENDPOINT" \
        /bin/bash -c 'echo "$CMDS" | /opt/kyuubi/bin/beeline -u $ENDPOINT'
    )" > /tmp/test_beeline.out

num_rows_inserted=$(cat /tmp/test_beeline.out | grep "Inserted Rows:" | sed 's/|/ /g' | tail -n 1 | xargs | rev | cut -d' ' -f1 | rev )
echo -e "${num_rows_inserted} rows were inserted."

if [ "${num_rows_inserted}" != "3" ]; then
    echo "ERROR: Test failed. ${num_rows_inserted} out of 3 rows were inserted. Aborting with exit code 1."
    exit 1
fi

rm /tmp/test_beeline.out
